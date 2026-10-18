//! Receive ordinary frames into reserved slots while continuing to service acknowledgements.

use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use bytes::Bytes;
use microsandbox_protocol::{
    codec,
    relay_input::{self, Position},
};
use tokio::{
    io::{AsyncRead, ReadBuf},
    sync::{Notify, Semaphore, mpsc, watch},
    task::JoinHandle,
};

use super::{
    ClientWrite, ClientWriteData, ControlWrite, ControlWriter, OUTPUT_BUDGET_GRANULE,
    is_client_frame_allowed,
};

//--------------------------------------------------------------------------------------------------
// Types
//--------------------------------------------------------------------------------------------------

struct Queued {
    wire: Vec<u8>,
    offset: usize,
    charged: bool,
}

pub(super) struct InputReader {
    queue: mpsc::Receiver<Queued>,
    current: Option<Queued>,
    consumed: watch::Sender<Position>,
    task: JoinHandle<()>,
    demand: Arc<Notify>,
    requested: bool,
}

//--------------------------------------------------------------------------------------------------
// Trait Implementations
//--------------------------------------------------------------------------------------------------

impl AsyncRead for InputReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        if self.current.is_none() {
            if !self.requested {
                self.demand.notify_one();
                self.requested = true;
            }
            self.current = match self.queue.poll_recv(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(Ok(())),
                Poll::Ready(Some(frame)) => {
                    self.requested = false;
                    Some(frame)
                }
            };
        }
        let frame = self.current.as_mut().unwrap();
        let count = buf.remaining().min(frame.wire.len() - frame.offset);
        buf.put_slice(&frame.wire[frame.offset..frame.offset + count]);
        frame.offset += count;
        if frame.offset == frame.wire.len() {
            let frame = self.current.take().unwrap();
            if frame.charged {
                self.consumed.send_modify(|position| {
                    position.bytes += frame.wire.len() as u64;
                    position.frames += 1;
                });
            }
        }
        Poll::Ready(Ok(()))
    }
}

impl Drop for InputReader {
    fn drop(&mut self) {
        self.task.abort();
    }
}

//--------------------------------------------------------------------------------------------------
// Functions
//--------------------------------------------------------------------------------------------------

pub(super) fn wrap(
    mut reader: impl AsyncRead + Unpin + Send + 'static,
    agent_tx: ControlWriter,
    write_tx: mpsc::UnboundedSender<ClientWrite>,
    write_budget: Arc<Semaphore>,
    id_start: u32,
    id_end: u32,
    frame_limit: u32,
) -> InputReader {
    // Ordinary traffic uses the advertised slots. Local arena upgrade/release records have a separate
    // bounded reserve (the arena has at most 80 outstanding owned slots).
    let (tx, queue) = mpsc::channel(frame_limit as usize + 128);
    let (consumed, mut consumed_rx) = watch::channel(Position::default());
    let demand = Arc::new(Notify::new());
    let read_demand = demand.clone();
    let task = tokio::spawn(async move {
        let mut enabled = false;
        let mut sent = Position::default();
        loop {
            // Before opt-in, preserve demand-driven reads rather than prefetching old clients'
            // data into the new queue. Once enabled, advertised reservations bound read-ahead.
            if !enabled {
                read_demand.notified().await;
            }
            // Keep one read future alive while dispatch acknowledgements are coalesced.
            let read = codec::read_raw_frame(&mut reader);
            tokio::pin!(read);
            let frame = loop {
                tokio::select! {
                    result = &mut read => match result { Ok(frame) => break frame, Err(_) => return },
                    changed = consumed_rx.changed(), if enabled => {
                        if changed.is_err() { return; }
                        let position = *consumed_rx.borrow_and_update();
                        let mut wire = Vec::new();
                        if codec::encode_raw_to_buf(&relay_input::acknowledgement(position), &mut wire).is_err() { return; }
                        let charge = wire.len().div_ceil(OUTPUT_BUDGET_GRANULE) * OUTPUT_BUDGET_GRANULE;
                        let Ok(permit) = write_budget.clone().try_acquire_many_owned(charge as u32) else { return; };
                        if write_tx.send(ClientWrite { data: ClientWriteData::Inline(Bytes::from(wire)), _lane_permit: None, _client_permit: permit }).is_err() { return; }
                    }
                }
            };
            if relay_input::is_extension(&frame) {
                if enabled || frame.body != relay_input::start().body {
                    return;
                }
                enabled = true;
                continue;
            }
            let local = frame.id == 0 && frame.flags == 0 && frame.body.starts_with(b"MSBS");
            let progress = relay_input::is_progress(&frame);
            let mut wire = Vec::new();
            if codec::encode_raw_to_buf(&frame, &mut wire).is_err() {
                return;
            }
            if enabled && progress && !local {
                if !is_client_frame_allowed(frame.id, frame.flags, id_start, id_end) {
                    return;
                }
                let Ok(message) = codec::raw_frame_to_message(frame.clone()) else {
                    return;
                };
                let mut write = ControlWrite::ordinary(Bytes::from(wire), frame.id, false);
                write.classify_message_order(frame.id, None, Some(&message));
                if agent_tx.send(write).await.is_err() {
                    return;
                }
                continue;
            }
            let charged = enabled && !progress;
            if charged {
                let consumed = *consumed_rx.borrow();
                let Some(bytes) = sent.bytes.checked_add(wire.len() as u64) else {
                    return;
                };
                let Some(frames) = sent.frames.checked_add(1) else {
                    return;
                };
                if bytes - consumed.bytes > u64::from(relay_input::BYTES)
                    || frames - consumed.frames > u64::from(frame_limit)
                {
                    return;
                }
                sent = Position { bytes, frames };
            } else if enabled && wire.len() > 512 {
                return;
            }
            let item = Queued {
                wire,
                offset: 0,
                charged,
            };
            if tx.try_send(item).is_err() {
                return;
            }
        }
    });
    InputReader {
        queue,
        current: None,
        consumed,
        task,
        demand,
        requested: false,
    }
}

//--------------------------------------------------------------------------------------------------
// Tests
//--------------------------------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use microsandbox_agent_client::OptimizedAgentClient;
    use microsandbox_protocol::{
        core::Ready,
        exec::{ExecOutputCredit, ExecStdin},
        message::{Message, MessageType},
    };
    use tokio::io::AsyncWriteExt;

    use super::*;

    /// The real SDK and relay adapter must carry acknowledgements while ordinary forwarding
    /// is blocked. A small byte stream models a tunneled connection without host socket access.
    #[tokio::test]
    async fn shared_stream_ack_progress_under_saturated_input() {
        for (enabled, wrapped) in [(false, false), (false, true), (true, true)] {
            let (client_io, server_io) = tokio::io::duplex(1024);
            let (server_read, mut server_write) = tokio::io::split(server_io);
            let (agent_tx, mut agent_rx) = ControlWriter::new();
            let (output_tx, mut output_rx) = mpsc::unbounded_channel::<ClientWrite>();
            let ready = Message::with_payload(MessageType::Ready, 0, &Ready::default()).unwrap();
            let mut wire = Vec::new();
            codec::encode_to_buf(&ready, &mut wire).unwrap();
            if enabled {
                wire = relay_input::advertise(&wire, relay_input::FRAMES).unwrap();
            }
            let server = tokio::spawn(async move {
                server_write.write_all(&1u32.to_be_bytes()).await.unwrap();
                server_write
                    .write_all(&1024u32.to_be_bytes())
                    .await
                    .unwrap();
                server_write.write_all(&wire).await.unwrap();
                while let Some(write) = output_rx.recv().await {
                    let ClientWriteData::Inline(bytes) = write.data else {
                        panic!("unexpected shared-memory output");
                    };
                    if server_write.write_all(&bytes).await.is_err() {
                        break;
                    }
                }
            });
            let client = Arc::new(
                OptimizedAgentClient::connect_stream(client_io)
                    .await
                    .unwrap(),
            );
            let mut reader: Box<dyn AsyncRead + Unpin + Send> = if wrapped {
                Box::new(wrap(
                    server_read,
                    agent_tx.clone(),
                    output_tx,
                    Arc::new(Semaphore::new(1024 * 1024)),
                    1,
                    1024,
                    relay_input::FRAMES,
                ))
            } else {
                Box::new(server_read)
            };
            let forward = tokio::spawn(async move {
                while let Ok(frame) = codec::read_raw_frame(&mut reader).await {
                    let mut wire = Vec::new();
                    codec::encode_raw_to_buf(&frame, &mut wire).unwrap();
                    let mut write = ControlWrite::ordinary(Bytes::from(wire), frame.id, true);
                    let message = codec::raw_frame_to_message(frame).unwrap();
                    write.classify_message_order(message.id, None, Some(&message));
                    if agent_tx.send(write).await.is_err() {
                        break;
                    }
                }
            });
            let (seen_tx, seen_rx) = tokio::sync::oneshot::channel();
            let guest = tokio::spawn(async move {
                let mut retained = Vec::new();
                while let Some(write) = agent_rx.recv().await {
                    let message = codec::decode_message_frame(&write.data).unwrap();
                    if message.t == MessageType::ExecOutputCredit {
                        let _ =
                            seen_tx.send(message.payload::<ExecOutputCredit>().unwrap().consumed);
                        break;
                    }
                    // Retain all eight data permits, exactly like a guest with exhausted input
                    // admission. Control frames have an independent reservation.
                    retained.push(write);
                }
            });
            let (sent_tx, mut sent_rx) = watch::channel(0usize);
            let sender_client = client.clone();
            let sender = tokio::spawn(async move {
                for n in 0..64 {
                    if sender_client
                        .send(
                            1 + (n % 2) as u32,
                            MessageType::ExecStdin,
                            &ExecStdin {
                                data: vec![b'x'; 16384],
                            },
                        )
                        .await
                        .is_err()
                    {
                        break;
                    }
                    sent_tx.send_replace(n + 1);
                }
            });
            tokio::time::timeout(Duration::from_secs(2), sent_rx.wait_for(|sent| *sent >= 9))
                .await
                .unwrap()
                .unwrap();
            let ack_client = client.clone();
            let ack = tokio::spawn(async move {
                ack_client
                    .send(
                        1,
                        MessageType::ExecOutputCredit,
                        &ExecOutputCredit { consumed: 4096 },
                    )
                    .await
            });
            let observed = tokio::time::timeout(Duration::from_millis(300), seen_rx).await;
            if enabled {
                assert_eq!(observed.unwrap().unwrap(), 4096);
            } else {
                assert!(
                    observed.is_err(),
                    "negative control unexpectedly delivered through blocked stdin"
                );
            }
            sender.abort();
            ack.abort();
            forward.abort();
            guest.abort();
            server.abort();
        }
    }
}

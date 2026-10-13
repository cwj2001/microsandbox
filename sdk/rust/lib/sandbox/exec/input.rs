//! Separate SDK stdin buffering from the bounded transport allowance.

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

use microsandbox_protocol::{
    exec::{EXEC_STDIN_CHUNK_SIZE, ExecStdin},
    message::MessageType,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, SemaphorePermit, mpsc, oneshot, watch};

use crate::{MicrosandboxError, MicrosandboxResult, agent::AgentClient};

//--------------------------------------------------------------------------------------------------
// Constants
//--------------------------------------------------------------------------------------------------

const MAX_IN_FLIGHT_CHUNKS: usize = 4;

//--------------------------------------------------------------------------------------------------
// Types
//--------------------------------------------------------------------------------------------------

pub(super) struct InputBuffer {
    sender: mpsc::UnboundedSender<BufferedInput>,
    capacity: Option<Arc<Semaphore>>,
    chunk_size: usize,
    task: tokio::task::JoinHandle<()>,
}

struct BufferedInput {
    data: Vec<u8>,
    _permit: Option<OwnedSemaphorePermit>,
    completion: oneshot::Sender<MicrosandboxResult<()>>,
}

pub(super) struct InputWindow {
    available: Semaphore,
    frames: Semaphore,
    offsets: Mutex<Offsets>,
    closed: watch::Sender<bool>,
}

pub(super) struct InputReservation<'a> {
    window: &'a InputWindow,
    permit: Option<SemaphorePermit<'a>>,
    frame: Option<SemaphorePermit<'a>>,
    bytes: u32,
}

#[derive(Default)]
struct Offsets {
    sent: u64,
    consumed: u64,
    frame_ends: VecDeque<u64>,
}

//--------------------------------------------------------------------------------------------------
// Methods
//--------------------------------------------------------------------------------------------------

impl InputBuffer {
    pub(super) fn new(
        id: u32,
        client: Arc<AgentClient>,
        window: Arc<InputWindow>,
        limit: Option<u32>,
    ) -> Self {
        let capacity = limit.map(|bytes| Arc::new(Semaphore::new(bytes as usize)));
        let chunk_size = limit.map_or(EXEC_STDIN_CHUNK_SIZE, |bytes| {
            (bytes as usize).min(EXEC_STDIN_CHUNK_SIZE)
        });
        let (sender, mut receiver) = mpsc::unbounded_channel::<BufferedInput>();
        let worker_capacity = capacity.clone();
        let task = tokio::spawn(async move {
            let mut closed = window.closed.subscribe();
            loop {
                let input = tokio::select! {
                    _ = closed.wait_for(|closed| *closed) => break,
                    input = receiver.recv() => match input { Some(input) => input, None => break },
                };
                let result = tokio::select! {
                    _ = closed.wait_for(|closed| *closed) => Err(stdin_closed()),
                    result = send_chunks(&client, id, &window, &input.data) => result,
                };
                let failed = result.is_err();
                let _ = input.completion.send(result);
                if failed {
                    break;
                }
            }
            if let Some(capacity) = worker_capacity {
                capacity.close();
            }
            window.close();
        });
        Self {
            sender,
            capacity,
            chunk_size,
            task,
        }
    }

    pub(super) async fn write(&self, data: &[u8]) -> MicrosandboxResult<()> {
        let mut completion = None;
        // Empty input is an ordered EOF, not an absent operation.
        let chunks = data
            .chunks(self.chunk_size)
            .chain(data.is_empty().then_some(data));
        for chunk in chunks {
            let permit = match &self.capacity {
                Some(capacity) => Some(
                    capacity
                        .clone()
                        .acquire_many_owned(chunk.len() as u32)
                        .await
                        .map_err(|_| stdin_closed())?,
                ),
                None => None,
            };
            let (sent, received) = oneshot::channel();
            self.sender
                .send(BufferedInput {
                    data: chunk.to_vec(),
                    _permit: permit,
                    completion: sent,
                })
                .map_err(|_| stdin_closed())?;
            completion = Some(received);
            tokio::task::consume_budget().await;
        }
        completion
            .expect("data or EOF queued")
            .await
            .map_err(|_| stdin_closed())?
    }
}

impl InputWindow {
    pub(super) fn new(bytes: u32) -> Arc<Self> {
        Arc::new(Self {
            available: Semaphore::new(bytes as usize),
            frames: Semaphore::new(MAX_IN_FLIGHT_CHUNKS),
            offsets: Mutex::new(Offsets::default()),
            closed: watch::channel(false).0,
        })
    }

    pub(super) async fn reserve(&self, bytes: u32) -> MicrosandboxResult<InputReservation<'_>> {
        let frame = self.frames.acquire().await.map_err(|_| stdin_closed())?;
        let permit = self
            .available
            .acquire_many(bytes)
            .await
            .map_err(|_| MicrosandboxError::Runtime("execution stdin closed".into()))?;
        let mut offsets = self.offsets.lock().unwrap();
        offsets.sent = offsets
            .sent
            .checked_add(u64::from(bytes))
            .ok_or_else(|| MicrosandboxError::Runtime("execution stdin offset overflow".into()))?;
        let end = offsets.sent;
        offsets.frame_ends.push_back(end);
        Ok(InputReservation {
            window: self,
            permit: Some(permit),
            frame: Some(frame),
            bytes,
        })
    }

    pub(super) fn acknowledge(&self, consumed: u64) -> bool {
        let mut offsets = self.offsets.lock().unwrap();
        if consumed < offsets.consumed || consumed > offsets.sent {
            return false;
        }
        self.available
            .add_permits((consumed - offsets.consumed) as usize);
        while offsets
            .frame_ends
            .front()
            .is_some_and(|end| *end <= consumed)
        {
            offsets.frame_ends.pop_front();
            self.frames.add_permits(1);
        }
        offsets.consumed = consumed;
        true
    }

    pub(super) fn close(&self) {
        self.available.close();
        self.frames.close();
        self.closed.send_replace(true);
    }
}

impl InputReservation<'_> {
    pub(super) fn commit(mut self) {
        self.permit.take().unwrap().forget();
        self.frame.take().unwrap().forget();
    }
}

//--------------------------------------------------------------------------------------------------
// Trait Implementations
//--------------------------------------------------------------------------------------------------

impl Drop for InputBuffer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Drop for InputReservation<'_> {
    fn drop(&mut self) {
        if self.permit.is_some() {
            let mut offsets = self.window.offsets.lock().unwrap();
            offsets.sent -= u64::from(self.bytes);
            offsets.frame_ends.pop_back();
        }
    }
}

//--------------------------------------------------------------------------------------------------
// Functions
//--------------------------------------------------------------------------------------------------

pub(super) async fn send_chunks(
    client: &AgentClient,
    id: u32,
    window: &InputWindow,
    data: impl AsRef<[u8]>,
) -> MicrosandboxResult<()> {
    let data = data.as_ref();
    if data.is_empty() {
        client
            .send(id, MessageType::ExecStdin, &ExecStdin { data: Vec::new() })
            .await?;
    }
    for chunk in data.chunks(EXEC_STDIN_CHUNK_SIZE) {
        let reservation = window.reserve(chunk.len() as u32).await?;
        client
            .send_with_admission(
                id,
                MessageType::ExecStdin,
                &ExecStdin {
                    data: chunk.to_vec(),
                },
                || reservation.commit(),
            )
            .await?;
    }
    Ok(())
}

fn stdin_closed() -> MicrosandboxError {
    MicrosandboxError::Runtime("execution stdin closed".into())
}

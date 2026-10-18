//! Sender-side reservations for the shared relay connection.

#![cfg_attr(not(feature = "stream"), allow(dead_code))]

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

use microsandbox_protocol::relay_input::{Position, RelayInputReady};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::error::{AgentClientError, AgentClientResult};

//--------------------------------------------------------------------------------------------------
// Types
//--------------------------------------------------------------------------------------------------

pub(crate) struct Window {
    bytes: Arc<Semaphore>,
    frames: Arc<Semaphore>,
    limit: u32,
    ledger: Mutex<Ledger>,
}

#[derive(Default)]
struct Ledger {
    acknowledged: Position,
    pending: VecDeque<u32>,
}

pub(crate) struct Reservation {
    window: Arc<Window>,
    bytes: OwnedSemaphorePermit,
    frame: OwnedSemaphorePermit,
    charge: u32,
}

//--------------------------------------------------------------------------------------------------
// Methods
//--------------------------------------------------------------------------------------------------

impl Window {
    pub(crate) fn new(ready: RelayInputReady) -> AgentClientResult<Arc<Self>> {
        if ready.format != microsandbox_protocol::relay_input::FORMAT
            || ready.frames == 0
            || ready.frames > 256
            || ready.bytes == 0
            || ready.bytes > 64 * 1024 * 1024
        {
            return Err(AgentClientError::LocalTransport(
                "invalid relay input capability".into(),
            ));
        }
        Ok(Arc::new(Self {
            bytes: Arc::new(Semaphore::new(ready.bytes as usize)),
            frames: Arc::new(Semaphore::new(ready.frames as usize)),
            limit: ready.bytes,
            ledger: Mutex::new(Ledger::default()),
        }))
    }

    pub(crate) async fn reserve(self: &Arc<Self>, bytes: usize) -> AgentClientResult<Reservation> {
        let charge = u32::try_from(bytes)
            .ok()
            .filter(|n| *n <= self.limit)
            .ok_or_else(|| {
                AgentClientError::LocalTransport("frame exceeds relay input allowance".into())
            })?;
        let frame = self
            .frames
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| AgentClientError::Closed)?;
        let bytes = self
            .bytes
            .clone()
            .acquire_many_owned(charge)
            .await
            .map_err(|_| AgentClientError::Closed)?;
        Ok(Reservation {
            window: self.clone(),
            bytes,
            frame,
            charge,
        })
    }

    pub(crate) fn acknowledge(&self, position: Position) -> AgentClientResult<()> {
        let mut ledger = self.ledger.lock().unwrap();
        let frames = position
            .frames
            .checked_sub(ledger.acknowledged.frames)
            .and_then(|n| usize::try_from(n).ok());
        let bytes = position.bytes.checked_sub(ledger.acknowledged.bytes);
        let valid = frames.zip(bytes).filter(|(frames, bytes)| {
            *frames <= ledger.pending.len()
                && ledger
                    .pending
                    .iter()
                    .take(*frames)
                    .map(|n| u64::from(*n))
                    .sum::<u64>()
                    == *bytes
        });
        let Some((frames, bytes)) = valid else {
            return Err(AgentClientError::LocalTransport(
                "invalid relay input acknowledgement".into(),
            ));
        };
        ledger.pending.drain(..frames);
        ledger.acknowledged = position;
        self.bytes.add_permits(bytes as usize);
        self.frames.add_permits(frames);
        Ok(())
    }

    pub(crate) fn close(&self) {
        self.bytes.close();
        self.frames.close();
    }
}

impl Reservation {
    // Called by the sole socket writer, preserving the exact wire ordering of charges.
    pub(crate) fn commit(self) {
        self.window
            .ledger
            .lock()
            .unwrap()
            .pending
            .push_back(self.charge);
        self.bytes.forget();
        self.frame.forget();
    }
}

//--------------------------------------------------------------------------------------------------
// Tests
//--------------------------------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn acknowledgements_cannot_create_extra_capacity() {
        let window = Window::new(RelayInputReady {
            format: 1,
            bytes: 16,
            frames: 1,
        })
        .unwrap();
        window.reserve(8).await.unwrap().commit();
        assert!(
            window
                .acknowledge(Position {
                    bytes: 8,
                    frames: 2
                })
                .is_err()
        );
        assert!(
            window
                .acknowledge(Position {
                    bytes: 9,
                    frames: 1
                })
                .is_err()
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(20), window.reserve(1))
                .await
                .is_err()
        );

        let acknowledgement = Position {
            bytes: 8,
            frames: 1,
        };
        window.acknowledge(acknowledgement).unwrap();
        window.acknowledge(acknowledgement).unwrap();
        let next = window.reserve(16).await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), window.reserve(1))
                .await
                .is_err()
        );
        assert!(window.acknowledge(Position::default()).is_err());
        drop(next);
        window.reserve(16).await.unwrap();
    }

    #[tokio::test]
    async fn disconnect_wakes_a_sender_waiting_for_admission() {
        let window = Window::new(RelayInputReady {
            format: 1,
            bytes: 16,
            frames: 1,
        })
        .unwrap();
        window.reserve(16).await.unwrap().commit();
        window.close();
        assert!(matches!(
            window.reserve(1).await,
            Err(AgentClientError::Closed)
        ));
    }
}

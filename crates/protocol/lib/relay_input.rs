//! Optional connection-wide admission on the SDK-to-runtime byte stream.

use serde::{Deserialize, Serialize};

use crate::codec::RawFrame;
use crate::message::{FRAME_HEADER_SIZE, MessageType};

//--------------------------------------------------------------------------------------------------
// Constants
//--------------------------------------------------------------------------------------------------

/// First relay input admission format; independent of the guest protocol generation.
pub const FORMAT: u8 = 1;
/// Maximum admitted frames retained by one connection's input dispatcher.
pub const FRAMES: u32 = 8;
/// Maximum admitted wire bytes retained by one connection's input dispatcher.
pub const BYTES: u32 = 32 * 1024 * 1024;
const MAGIC: &[u8; 4] = b"MSBI";

//--------------------------------------------------------------------------------------------------
// Types
//--------------------------------------------------------------------------------------------------

/// Host-owned capability advertised in the client-facing Ready payload.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct RelayInputReady {
    /// Framing and accounting contract.
    pub format: u8,
    /// Connection-wide maximum outstanding wire bytes.
    pub bytes: u32,
    /// Connection-wide maximum outstanding ordinary frames.
    pub frames: u32,
}

/// Backwards-readable Ready extension.
#[derive(Default, Deserialize)]
pub struct Discovery {
    /// Absent on runtimes without input admission.
    pub relay_input: Option<RelayInputReady>,
}

/// Cumulative dispatch acknowledgement, excluding exempt progress messages.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Position {
    /// Wire bytes removed from the connection's receive queue.
    pub bytes: u64,
    /// Frames removed from the connection's receive queue.
    pub frames: u64,
}

//--------------------------------------------------------------------------------------------------
// Functions
//--------------------------------------------------------------------------------------------------

/// Client opt-in. Old clients never enable accounting merely by receiving Ready.
pub fn start() -> RawFrame {
    RawFrame {
        id: 0,
        flags: 0,
        body: [MAGIC.as_slice(), &[FORMAT, 0]].concat(),
    }
}

/// Encode an acknowledgement on the same framed stream.
pub fn acknowledgement(position: Position) -> RawFrame {
    let mut body = [MAGIC.as_slice(), &[FORMAT, 1]].concat();
    body.extend_from_slice(&position.bytes.to_be_bytes());
    body.extend_from_slice(&position.frames.to_be_bytes());
    RawFrame {
        id: 0,
        flags: 0,
        body,
    }
}

/// Whether a frame belongs to this host-only extension.
pub fn is_extension(frame: &RawFrame) -> bool {
    frame.id == 0 && frame.flags == 0 && frame.body.starts_with(MAGIC)
}

/// Decode an exact-format acknowledgement; malformed frames are rejected.
pub fn decode_ack(frame: &RawFrame) -> Option<Position> {
    if !is_extension(frame) || frame.body.len() != 22 || frame.body[4..6] != [FORMAT, 1] {
        return None;
    }
    Some(Position {
        bytes: u64::from_be_bytes(frame.body[6..14].try_into().ok()?),
        frames: u64::from_be_bytes(frame.body[14..22].try_into().ok()?),
    })
}

/// Messages needed to free capacity must not wait for ordinary input admission.
pub fn is_progress(frame: &RawFrame) -> bool {
    if frame.flags != 0 {
        return false;
    }
    // Local arena upgrade/release records are already bounded by arena ownership. References
    // remain ordinary traffic and consume a connection slot like an inline bulk record.
    if frame.id == 0 && frame.body.starts_with(b"MSBS") {
        return matches!(frame.body.get(5), Some(1 | 3));
    }
    #[derive(Deserialize)]
    struct Kind {
        t: MessageType,
    }
    frame.body.len() <= 4096
        && ciborium::from_reader::<Kind, _>(&frame.body[..]).is_ok_and(|message| {
            matches!(
                message.t,
                MessageType::ExecOutputCredit | MessageType::BulkCredit
            )
        })
}

/// Wire allocation charged for one ordinary frame, including both headers.
pub fn wire_bytes(frame: &RawFrame) -> usize {
    4 + FRAME_HEADER_SIZE + frame.body.len()
}

/// Add the host-only capability while preserving all other Ready extensions.
pub fn advertise(wire: &[u8], frames: u32) -> crate::ProtocolResult<Vec<u8>> {
    use crate::{ProtocolError, codec};
    let mut message = codec::decode_message_frame(wire)?;
    let mut payload: ciborium::Value = ciborium::from_reader(&message.p[..])?;
    let ciborium::Value::Map(ref mut fields) = payload else {
        return Err(ProtocolError::InvalidRelayInput(
            "Ready payload is not a map".into(),
        ));
    };
    fields.retain(|(key, _)| key.as_text() != Some("relay_input"));
    let capability = RelayInputReady {
        format: FORMAT,
        bytes: BYTES,
        frames,
    };
    fields.push((
        ciborium::Value::Text("relay_input".into()),
        ciborium::Value::serialized(&capability)
            .map_err(|e| ProtocolError::InvalidRelayInput(e.to_string()))?,
    ));
    message.p.clear();
    ciborium::into_writer(&payload, &mut message.p)?;
    let mut wire = Vec::new();
    codec::encode_to_buf(&message, &mut wire)?;
    Ok(wire)
}

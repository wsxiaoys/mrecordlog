pub const HEADER_LEN: usize = 4 + 2 + 1;

fn crc32(data: &[u8], frame_type: u8) -> u32 {
    let mut hash = crc32fast::Hasher::default();
    hash.update(&[frame_type]);
    hash.update(data);
    hash.finalize()
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct Header {
    checksum: u32,
    len: u16,
    frame_type: FrameType,
}

impl Header {
    pub fn for_payload(frame_type: FrameType, payload: &[u8]) -> Header {
        assert!(payload.len() < crate::BLOCK_NUM_BYTES);
        Header {
            checksum: crc32(payload, frame_type as u8),
            len: payload.len() as u16,
            frame_type,
        }
    }

    pub fn len(&self) -> usize {
        self.len as usize
    }

    pub fn frame_type(&self) -> FrameType {
        self.frame_type
    }

    pub fn check(&self, payload: &[u8]) -> bool {
        crc32(payload, self.frame_type as u8) == self.checksum
    }

    /// Serialize the header
    ///
    /// # Panics
    /// panic if `dest` isn't exactly `HEADER_LEN` bytes long
    pub fn serialize(&self, dest: &mut [u8]) {
        assert_eq!(dest.len(), HEADER_LEN);
        dest[..4].copy_from_slice(&self.checksum.to_le_bytes()[..]);
        dest[4..6].copy_from_slice(&self.len.to_le_bytes()[..]);
        dest[6] = self.frame_type.to_u8();
    }

    /// Deserialize a header
    ///
    /// # Panics
    /// panic if `data` isn't exactly `HEADER_LEN` bytes long
    pub fn deserialize(data: &[u8]) -> Option<Header> {
        assert_eq!(data.len(), HEADER_LEN);
        let checksum = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        let len = u16::from_le_bytes([data[4], data[5]]);
        let frame_type = FrameType::from_u8(data[6])?;
        Some(Header {
            checksum,
            len,
            frame_type,
        })
    }
}

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum FrameType {
    Full = 1u8,
    First = 2u8,
    Middle = 3u8,
    Last = 4u8,
    /// Like [`FrameType::Full`], but the reassembled record payload is zstd-compressed.
    ///
    /// Uses a new code (5) rather than reusing `Full`, so an older binary that does not know it
    /// treats the header as corruption and drops the block instead of misparsing compressed bytes
    /// as a record. `Middle`/`Last` are shared with the uncompressed case; the reader learns a
    /// record is compressed from its first frame.
    FullCompressed = 5u8,
    /// Like [`FrameType::First`], but the reassembled record payload is zstd-compressed.
    FirstCompressed = 6u8,
}

impl FrameType {
    fn from_u8(b: u8) -> Option<FrameType> {
        match b {
            1u8 => Some(FrameType::Full),
            2u8 => Some(FrameType::First),
            3u8 => Some(FrameType::Middle),
            4u8 => Some(FrameType::Last),
            5u8 => Some(FrameType::FullCompressed),
            6u8 => Some(FrameType::FirstCompressed),
            _ => None,
        }
    }

    fn to_u8(self) -> u8 {
        self as u8
    }

    pub fn is_first_frame_of_record(&self) -> bool {
        match self {
            FrameType::Full
            | FrameType::First
            | FrameType::FullCompressed
            | FrameType::FirstCompressed => true,
            FrameType::Last | FrameType::Middle => false,
        }
    }

    pub fn is_last_frame_of_record(&self) -> bool {
        match self {
            FrameType::Full | FrameType::Last | FrameType::FullCompressed => true,
            FrameType::First | FrameType::Middle | FrameType::FirstCompressed => false,
        }
    }

    /// True when this is the first frame of a record whose reassembled payload is
    /// zstd-compressed (and must be decompressed before deserialization).
    pub fn is_compressed_first_frame(&self) -> bool {
        matches!(
            self,
            FrameType::FullCompressed | FrameType::FirstCompressed
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::frame::header::{Header, HEADER_LEN};
    use crate::frame::FrameType;

    #[test]
    fn test_frame_type_serialize_deserialize() {
        const ALL_FRAME_TYPES: [FrameType; 6] = [
            FrameType::Full,
            FrameType::First,
            FrameType::Middle,
            FrameType::Last,
            FrameType::FullCompressed,
            FrameType::FirstCompressed,
        ];
        for frame_type in ALL_FRAME_TYPES {
            assert_eq!(FrameType::from_u8(frame_type.to_u8()), Some(frame_type));
        }
    }

    #[test]
    fn test_frame_deserialize_invalid() {
        assert_eq!(FrameType::from_u8(14u8), None);
    }

    #[test]
    fn test_compressed_frame_type_classification() {
        assert!(FrameType::FullCompressed.is_first_frame_of_record());
        assert!(FrameType::FullCompressed.is_last_frame_of_record());
        assert!(FrameType::FullCompressed.is_compressed_first_frame());

        assert!(FrameType::FirstCompressed.is_first_frame_of_record());
        assert!(!FrameType::FirstCompressed.is_last_frame_of_record());
        assert!(FrameType::FirstCompressed.is_compressed_first_frame());

        // The non-first frames of a compressed record reuse the plain codes.
        assert!(!FrameType::Middle.is_first_frame_of_record());
        assert!(!FrameType::Middle.is_last_frame_of_record());
        assert!(!FrameType::Middle.is_compressed_first_frame());
        assert!(!FrameType::Last.is_first_frame_of_record());
        assert!(FrameType::Last.is_last_frame_of_record());
        assert!(!FrameType::Last.is_compressed_first_frame());

        assert!(!FrameType::Full.is_compressed_first_frame());
        assert!(!FrameType::First.is_compressed_first_frame());
    }

    #[test]
    fn test_header_serialize_deserialize() {
        let header = Header {
            checksum: 17u32,
            len: 42,
            frame_type: FrameType::Full,
        };
        let mut buffer = [0u8; HEADER_LEN];
        header.serialize(&mut buffer);
        let serdeser_header = Header::deserialize(&buffer).unwrap();
        assert_eq!(header, serdeser_header);
    }

    #[test]
    fn test_header_deserialize_invalid() {
        let invalid_header_buffer = [14u8; HEADER_LEN];
        assert_eq!(Header::deserialize(&invalid_header_buffer), None);
    }
}

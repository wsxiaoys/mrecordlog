use std::io;

use zstd::zstd_safe::CParameter;

use crate::block_read_write::VecBlockWriter;
use crate::frame::{FrameType, FrameWriter};
use crate::rolling::{Directory, FileNumber, RollingWriter};
use crate::{BlockWrite, PersistAction, Serializable};

/// Serialized records shorter than this are written uncompressed: the fixed zstd frame overhead
/// (~13 bytes) would eat most of the gain on the tiny control records (`Truncate`/`Touch`/
/// `DeleteQueue`). Document batches are hundreds of KiB to a few MiB.
const MIN_COMPRESS_LEN: usize = 4 * 1024;

/// zstd level for WAL records. On the OTel NDJSON batches level 1 already compresses ~19x at
/// ~1.4 GB/s, so the serialized writer stays far below the disk-write time it replaces.
const COMPRESS_LEVEL: i32 = 1;

/// zstd window log for WAL records. A batch is hundreds of KiB to a few MiB, i.e. larger than the
/// level-1 default window, so the long-range template matches between documents are lost. A 4 MiB
/// window (log 22) is large enough to hold the whole batch; it only changes the frame header, so
/// the WAL stays byte-compatible (the reader uses `zstd::decompressed_size`/`decompress`).
const COMPRESS_WINDOW_LOG: u32 = 22;

pub struct RecordWriter<W> {
    frame_writer: FrameWriter<W>,
    buffer: Vec<u8>,
    compressed_buffer: Vec<u8>,
    compressor: zstd::bulk::Compressor<'static>,
}

fn frame_type(is_first_frame: bool, is_last_frame: bool, compressed: bool) -> FrameType {
    match (is_first_frame, is_last_frame, compressed) {
        (true, true, false) => FrameType::Full,
        (true, false, false) => FrameType::First,
        (false, true, false) => FrameType::Last,
        (false, false, false) => FrameType::Middle,
        (true, true, true) => FrameType::FullCompressed,
        (true, false, true) => FrameType::FirstCompressed,
        // A compressed record's non-first frames reuse `Middle`/`Last`: the reader knows the
        // record is compressed from its first frame.
        (false, true, true) => FrameType::Last,
        (false, false, true) => FrameType::Middle,
    }
}

impl<W: BlockWrite + Unpin> From<FrameWriter<W>> for RecordWriter<W> {
    fn from(frame_writer: FrameWriter<W>) -> Self {
        // The level and window log are compile-time constants within zstd's accepted range, so
        // construction cannot fail; the context is reused across records to avoid re-allocating
        // it per batch.
        let mut compressor = zstd::bulk::Compressor::new(COMPRESS_LEVEL)
            .expect("zstd compression level should be valid");
        compressor
            .set_parameter(CParameter::WindowLog(COMPRESS_WINDOW_LOG))
            .expect("zstd window log should be valid");
        RecordWriter {
            frame_writer,
            buffer: Vec::with_capacity(10_000),
            compressed_buffer: Vec::new(),
            compressor,
        }
    }
}

impl<W: BlockWrite + Unpin> RecordWriter<W> {
    #[cfg(test)]
    pub fn into_writer(self) -> W {
        self.frame_writer.into_writer()
    }
}

impl<W: BlockWrite + Unpin> RecordWriter<W> {
    /// Writes a record. Returns the total number of bytes pushed to the underlying writer
    /// for this record, including frame headers and any block padding.
    ///
    /// Even if this call returns `Ok(_)`, at this point the data
    /// is likely to be not durably stored on disk.
    ///
    /// For instance, the data could be stale in a library level buffer,
    /// by a writer level buffer, or an application buffer,
    /// or could not be flushed to disk yet by the OS.
    pub fn write_record<'a>(&mut self, record: impl Serializable<'a>) -> io::Result<u64> {
        let mut is_first_frame = true;
        let mut num_bytes_written: u64 = 0;

        self.buffer.clear();
        record.serialize(&mut self.buffer);

        // Compress the whole serialized record as one zstd blob when it is large enough and
        // compression actually shrinks it. The first frame's `FrameType` marks it compressed; the
        // per-frame CRC still covers the (compressed) payload, and the decompressed size is
        // recorded in the zstd frame header.
        let mut compressed = false;
        if self.buffer.len() >= MIN_COMPRESS_LEN {
            if let Ok(zstd_payload) = self.compressor.compress(&self.buffer) {
                if zstd_payload.len() < self.buffer.len() {
                    self.compressed_buffer = zstd_payload;
                    compressed = true;
                }
            }
        }
        let mut payload: &[u8] = if compressed {
            &self.compressed_buffer
        } else {
            &self.buffer
        };

        loop {
            let frame_payload_len = self
                .frame_writer
                .max_writable_frame_length()
                .min(payload.len());
            let frame_payload = &payload[..frame_payload_len];
            payload = &payload[frame_payload_len..];
            let is_last_frame = payload.is_empty();
            let frame_type = frame_type(is_first_frame, is_last_frame, compressed);
            num_bytes_written += self.frame_writer.write_frame(frame_type, frame_payload)? as u64;
            is_first_frame = false;

            if is_last_frame {
                break;
            }
        }
        Ok(num_bytes_written)
    }

    /// Persist the data to disk, according to the persist_action.
    pub fn persist(&mut self, persist_action: PersistAction) -> io::Result<()> {
        self.frame_writer.persist(persist_action)
    }

    pub fn get_underlying_wrt(&self) -> &W {
        self.frame_writer.get_underlying_wrt()
    }
}

impl RecordWriter<RollingWriter> {
    pub fn directory(&mut self) -> &mut Directory {
        self.frame_writer.directory()
    }

    pub fn current_file(&mut self) -> &FileNumber {
        self.get_underlying_wrt().current_file()
    }

    pub fn size(&self) -> usize {
        self.get_underlying_wrt().size()
    }
}

impl RecordWriter<VecBlockWriter> {
    #[cfg(test)]
    pub fn in_memory() -> Self {
        FrameWriter::create(VecBlockWriter::default()).into()
    }
}

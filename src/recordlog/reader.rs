use std::io;

use crate::error::ReadRecordError;
use crate::frame::{FrameReader, FrameWriter, ReadFrameError};
use crate::recordlog::RecordWriter;
use crate::rolling::{RollingReader, RollingWriter};
use crate::{BlockRead, Serializable};

/// Upper bound on the size a compressed record may decompress to. Compressed payloads are
/// CRC-checked per frame before decompression, so this only bounds the allocation a corrupt (but
/// CRC-valid) frame could request; it sits comfortably above the default ingest
/// `max_decompressed_body_size` (256 MiB).
const MAX_DECOMPRESSED_RECORD_BYTES: u64 = 512 * 1024 * 1024;

pub struct RecordReader<R> {
    frame_reader: FrameReader<R>,
    record_buffer: Vec<u8>,
    /// Holds the decompressed form of the last reassembled record when it was compressed. Left
    /// empty for uncompressed records.
    decompressed_buffer: Vec<u8>,
    /// True when the record currently in `record_buffer` is zstd-compressed and must be read
    /// through `decompressed_buffer`.
    record_compressed: bool,
    // true if we are in the middle of reading a multifragment record.
    // This is useful, as it makes it possible to drop a record
    // if one of its fragment was corrupted.
    within_record: bool,
}

impl<R: BlockRead + Unpin> RecordReader<R> {
    pub fn open(reader: R) -> Self {
        let frame_reader = FrameReader::open(reader);
        RecordReader {
            frame_reader,
            record_buffer: Vec::with_capacity(10_000),
            decompressed_buffer: Vec::new(),
            record_compressed: false,
            within_record: false,
        }
    }

    pub fn read(&self) -> &R {
        self.frame_reader.read()
    }

    /// Deserialize a record without actually consuming data.
    pub fn record<'a, S: Serializable<'a>>(&'a self) -> Option<S> {
        if self.record_compressed {
            S::deserialize(&self.decompressed_buffer)
        } else {
            S::deserialize(&self.record_buffer)
        }
    }

    /// Advance cursor and deserialize the next record.
    pub fn read_record<'a, S: Serializable<'a>>(
        &'a mut self,
    ) -> Result<Option<S>, ReadRecordError> {
        let has_record = self.go_next()?;
        if has_record {
            let record = self.record().ok_or(ReadRecordError::Corruption)?;
            Ok(Some(record))
        } else {
            Ok(None)
        }
    }

    // Attempts to position the reader to the next record and return
    // true or false whether such a record is available or not.
    pub fn go_next(&mut self) -> Result<bool, ReadRecordError> {
        loop {
            let frame = self.frame_reader.read_frame();
            match frame {
                Ok((frame_type, frame_payload)) => {
                    if frame_type.is_first_frame_of_record() {
                        self.within_record = true;
                        self.record_buffer.clear();
                        self.record_compressed = frame_type.is_compressed_first_frame();
                    }
                    if self.within_record {
                        self.record_buffer.extend_from_slice(frame_payload);
                        if frame_type.is_last_frame_of_record() {
                            self.within_record = false;
                            if self.record_compressed {
                                self.decompress_current_record()?;
                            }
                            return Ok(true);
                        }
                    }
                }
                Err(ReadFrameError::Corruption) => {
                    self.within_record = false;
                    return Err(ReadRecordError::Corruption);
                }
                Err(ReadFrameError::IoError(io_err)) => {
                    self.within_record = false;
                    return Err(ReadRecordError::IoError(io_err));
                }
                Err(ReadFrameError::NotAvailable) => {
                    return Ok(false);
                }
            }
        }
    }

    /// Decompress the reassembled `record_buffer` into `decompressed_buffer`. A blob whose zstd
    /// content size is missing, or above [`MAX_DECOMPRESSED_RECORD_BYTES`], or that fails to
    /// decompress, is reported as `Corruption` so the caller drops the record exactly like a
    /// failed frame CRC.
    fn decompress_current_record(&mut self) -> Result<(), ReadRecordError> {
        let decompressed_size = zstd::decompressed_size(&self.record_buffer)
            .ok_or(ReadRecordError::Corruption)?;
        if decompressed_size > MAX_DECOMPRESSED_RECORD_BYTES {
            return Err(ReadRecordError::Corruption);
        }
        self.decompressed_buffer =
            zstd::bulk::decompress(&self.record_buffer, decompressed_size as usize)
                .map_err(|_| ReadRecordError::Corruption)?;
        Ok(())
    }
}

impl RecordReader<RollingReader> {
    pub fn into_writer(self) -> io::Result<RecordWriter<RollingWriter>> {
        let frame_writer: FrameWriter<RollingWriter> = self.frame_reader.into_writer()?;
        Ok(RecordWriter::from(frame_writer))
    }
}

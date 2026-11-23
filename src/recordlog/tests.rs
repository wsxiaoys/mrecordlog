use super::{RecordReader, RecordWriter};
use crate::block_read_write::ArrayReader;
use crate::error::ReadRecordError;
use crate::frame::HEADER_LEN;
use crate::{PersistAction, BLOCK_NUM_BYTES};

#[test]
fn test_no_data() {
    let data = vec![0u8; BLOCK_NUM_BYTES * 4];
    let mut reader = RecordReader::open(ArrayReader::from(&data[..]));
    assert_eq!(reader.read_record::<&str>().unwrap(), None);
}

#[test]
fn test_empty_record() {
    let mut writer = RecordWriter::in_memory();
    writer.write_record("").unwrap();
    writer.persist(PersistAction::Flush).unwrap();
    let buf: Vec<u8> = writer.into_writer().into();
    let mut reader = RecordReader::open(ArrayReader::from(&buf[..]));
    assert_eq!(reader.read_record::<&str>().unwrap(), Some(""));
    assert_eq!(reader.read_record::<&str>().unwrap(), None);
}

#[test]
fn test_simple_record() {
    let mut writer = RecordWriter::in_memory();
    let record = "hello";
    writer.write_record(record).unwrap();
    writer.persist(PersistAction::Flush).unwrap();
    let buf: Vec<u8> = writer.into_writer().into();
    let mut reader = RecordReader::open(ArrayReader::from(&buf[..]));
    assert!(matches!(reader.read_record::<&str>(), Ok(Some("hello"))));
    assert!(matches!(reader.read_record::<&str>(), Ok(None)));
}

fn make_long_entry(len: usize) -> String {
    "A".repeat(len)
}

/// Deterministic pseudo-random lowercase-ASCII payload: compressible enough that zstd shrinks it
/// (so it takes the compressed path) but with enough entropy that the compressed form is larger
/// than one 32 KiB block, forcing a multi-frame compressed record.
fn make_pseudo_random_entry(len: usize, seed: u64) -> String {
    let mut x = seed | 1;
    let mut out = String::with_capacity(len);
    for _ in 0..len {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        out.push((b'a' + (x % 26) as u8) as char);
    }
    out
}

#[test]
fn test_spans_over_more_than_one_block() {
    let long_entry: String = make_long_entry(80_000);
    let mut writer = RecordWriter::in_memory();
    writer.write_record(long_entry.as_str()).unwrap();
    writer.persist(PersistAction::Flush).unwrap();
    let buf: Vec<u8> = writer.into_writer().into();
    let mut reader = RecordReader::open(ArrayReader::from(&buf[..]));
    let record_payload: &str = reader.read_record().unwrap().unwrap();
    assert_eq!(record_payload, &long_entry);
    assert_eq!(reader.read_record::<&str>().unwrap(), None);
}

#[test]
fn test_block_requires_padding() {
    // We'll miss 1 byte to be able to fit our next chunk header in the
    // first block.
    let long_record = make_long_entry(BLOCK_NUM_BYTES - HEADER_LEN - HEADER_LEN - 1 - 8);
    let short_record = "hello";
    let mut writer = RecordWriter::in_memory();
    writer.write_record(long_record.as_str()).unwrap();
    writer.write_record(short_record).unwrap();
    writer.persist(PersistAction::Flush).unwrap();
    let buffer: Vec<u8> = writer.into_writer().into();
    let mut reader = RecordReader::open(ArrayReader::from(&buffer[..]));
    assert_eq!(
        reader.read_record::<&str>().unwrap(),
        Some(long_record.as_str())
    );
    assert_eq!(reader.read_record::<&str>().unwrap(), Some(short_record));
    assert_eq!(reader.read_record::<&str>().unwrap(), None);
}

#[test]
fn test_first_chunk_empty() {
    // We'll miss 1 byte to be able to fit our next chunk header in the
    // first block.
    let long_record = make_long_entry(BLOCK_NUM_BYTES - HEADER_LEN - HEADER_LEN);
    let short_record = "hello";
    let mut writer = RecordWriter::in_memory();
    writer.write_record(&long_record[..]).unwrap();
    writer.write_record(short_record).unwrap();
    writer.persist(PersistAction::Flush).unwrap();
    let buf: Vec<u8> = writer.into_writer().into();
    let mut reader = RecordReader::open(ArrayReader::from(&buf[..]));
    assert_eq!(
        reader.read_record::<&str>().unwrap(),
        Some(long_record.as_str())
    );
    assert_eq!(reader.read_record::<&str>().unwrap(), Some(short_record));
    assert_eq!(reader.read_record::<&str>().unwrap(), None);
}

#[test]
fn test_large_compressible_record_is_compressed_and_round_trips() {
    // Above MIN_COMPRESS_LEN and highly compressible: the writer must take the compressed path
    // and the reader must transparently decompress.
    let long_record = make_long_entry(64 * 1024);
    let mut writer = RecordWriter::in_memory();
    let num_bytes_written = writer.write_record(long_record.as_str()).unwrap();
    writer.persist(PersistAction::Flush).unwrap();
    assert!(
        (num_bytes_written as usize) < long_record.len(),
        "a compressible record must write fewer bytes than its raw size ({num_bytes_written} vs {})",
        long_record.len()
    );
    let buf: Vec<u8> = writer.into_writer().into();
    let mut reader = RecordReader::open(ArrayReader::from(&buf[..]));
    assert_eq!(
        reader.read_record::<&str>().unwrap(),
        Some(long_record.as_str())
    );
    assert_eq!(reader.read_record::<&str>().unwrap(), None);
}

#[test]
fn test_mixed_compressed_and_uncompressed_records_round_trip() {
    let short_record = "hello";
    let long_record = make_long_entry(64 * 1024);
    let mut writer = RecordWriter::in_memory();
    writer.write_record(short_record).unwrap();
    writer.write_record(long_record.as_str()).unwrap();
    writer.write_record(short_record).unwrap();
    writer.persist(PersistAction::Flush).unwrap();
    let buf: Vec<u8> = writer.into_writer().into();
    let mut reader = RecordReader::open(ArrayReader::from(&buf[..]));
    assert_eq!(reader.read_record::<&str>().unwrap(), Some(short_record));
    assert_eq!(
        reader.read_record::<&str>().unwrap(),
        Some(long_record.as_str())
    );
    assert_eq!(reader.read_record::<&str>().unwrap(), Some(short_record));
    assert_eq!(reader.read_record::<&str>().unwrap(), None);
}

#[test]
fn test_compressed_record_spans_multiple_blocks() {
    // 2 MiB of pseudo-random text compresses to well over one 32 KiB block, so the compressed
    // record is split into FirstCompressed/Middle.../Last frames that must be reassembled before
    // decompression.
    let long_record = make_pseudo_random_entry(2 * 1024 * 1024, 0x9E37_79B9_7F4A_7C15);
    let mut writer = RecordWriter::in_memory();
    let num_bytes_written = writer.write_record(long_record.as_str()).unwrap();
    writer.persist(PersistAction::Flush).unwrap();
    assert!((num_bytes_written as usize) < long_record.len());
    let buf: Vec<u8> = writer.into_writer().into();
    let mut reader = RecordReader::open(ArrayReader::from(&buf[..]));
    assert_eq!(
        reader.read_record::<&str>().unwrap(),
        Some(long_record.as_str())
    );
    assert_eq!(reader.read_record::<&str>().unwrap(), None);
}

#[test]
fn test_compression_window_reaches_across_the_record() {
    // Two copies of a 1 MiB pseudo-random block: the second copy only matches the first when the
    // zstd window can reach back ~1 MiB. The writer sets `WindowLog(22)` (4 MiB), so its output
    // must be smaller than a one-shot `zstd::bulk::compress` at the default (level-1) window.
    let half = make_pseudo_random_entry(1024 * 1024, 0x9E37_79B9_7F4A_7C15);
    let record = format!("{half}{half}");
    let mut writer = RecordWriter::in_memory();
    let num_bytes_written = writer.write_record(record.as_str()).unwrap();
    writer.persist(PersistAction::Flush).unwrap();
    let default_window = zstd::bulk::compress(record.as_bytes(), 1).unwrap();
    assert!(
        (num_bytes_written as usize) < default_window.len(),
        "the writer's window must beat the default one-shot window ({num_bytes_written} vs {})",
        default_window.len()
    );
    let buf: Vec<u8> = writer.into_writer().into();
    let mut reader = RecordReader::open(ArrayReader::from(&buf[..]));
    assert_eq!(
        reader.read_record::<&str>().unwrap(),
        Some(record.as_str())
    );
    assert_eq!(reader.read_record::<&str>().unwrap(), None);
}

#[test]
fn test_behavior_upon_corruption() {
    let records: Vec<String> = (0..1_000).map(|i| format!("hello{i}")).collect();
    let mut writer = RecordWriter::in_memory();
    for record in &records {
        writer.write_record(record.as_str()).unwrap();
    }
    writer.persist(PersistAction::Flush).unwrap();
    let mut buffer: Vec<u8> = writer.into_writer().into();
    {
        let mut reader = RecordReader::open(ArrayReader::from(&buffer[..]));
        for record in &records {
            assert_eq!(reader.read_record::<&str>().unwrap(), Some(record.as_str()));
        }
        assert_eq!(reader.read_record::<&str>().unwrap(), None);
    }
    // Introducing a corruption.
    buffer[1_000] = 3;
    {
        let mut reader = RecordReader::open(ArrayReader::from(&buffer[..]));
        for record in &records[0..72] {
            // bug at i=72
            assert_eq!(reader.read_record::<&str>().unwrap(), Some(record.as_str()));
        }
        assert!(matches!(
            reader.read_record::<&str>(),
            Err(ReadRecordError::Corruption)
        ));
    }
}

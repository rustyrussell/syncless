//! Each write appends an ondisk record has a header, and a tailer.
//! [logical_offset: le64]
//! [length: le24]
//! [data...: length]
//! [hash: le64] (covers offset, length, and data)
use std::io::{Seek, Read, Write};
use std::fs::File;
use crc64fast;
use std::ops::Bound::*;
use std::collections::BTreeMap;
use crate::Error;
use crate::store::Span;

pub(crate) const MAX_RECORD_SIZE: usize = 1 << 24;

pub(crate) struct RecordHeader {
    pub logical_offset: u64,
    pub length: u64,
}

pub(crate) struct Record {
    pub hdr: RecordHeader,
    pub file_data_offset: u64,
}

// Read bytes, but seek back if it fails.  Return false if couldn't read all.
fn read_bytes_fail_back(file: &mut File,
                        buf: &mut [u8],
                        total_read: &mut u64) -> Result<bool, Error>
{
    let length = file.read(buf)?;
    *total_read += length as u64;
    if length == buf.len() {
        return Ok(true);
    }
    // Short read, stop at this point.
    if length != 0 {
        file.seek_relative(-(*total_read as i64))?;
    }
    return Ok(false);
}

pub(crate) fn read_next_record(file: &mut File, file_offset: &mut u64) -> Result<Option<Record>, Error>
{
    let mut hdrbytes = [0u8; 8 + 3];
    let mut total_read: u64 = 0;

    if !read_bytes_fail_back(file, &mut hdrbytes, &mut total_read)? {
        return Ok(None);
    }

    let len24 = (hdrbytes[8] as u32) | ((hdrbytes[9] as u32) << 8) | ((hdrbytes[10] as u32) << 16);
    let rhdr = RecordHeader {
        logical_offset: u64::from_le_bytes(hdrbytes[..8].try_into().unwrap()),
        length: len24 as u64,
    };

    let rec = Record {
        hdr: rhdr,
        file_data_offset: *file_offset + hdrbytes.len() as u64,
    };

    let mut data = vec![0u8; rec.hdr.length as usize];
    if !read_bytes_fail_back(file, &mut data, &mut total_read)? {
        return Ok(None);
    }

    let mut tlrbytes = [0u8; 8];
    if !read_bytes_fail_back(file, &mut tlrbytes, &mut total_read)? {
        return Ok(None);
    }

    // Calculate and check hash: my laptop does this at 38Gbytes/sec,
    // vs siphash13 at 6Gbytes/sec.
    let mut d = crc64fast::Digest::new();
    d.write(&hdrbytes);
    d.write(&data);
    
    if d.sum64() != u64::from_le_bytes(tlrbytes.try_into().unwrap()) {
        file.seek_relative(-(total_read as i64))?;
        return Ok(None);
    }

    *file_offset += total_read;
    return Ok(Some(rec));
}

/// Appends a record to the end of the store (must be < 16MB!)
/// 
/// The file cursor must be positioned at the end of the valid log.
/// Atomicity is provided by the trailer checksum; durability is not guaranteed.
pub(crate) fn write_record(file: &mut File,
                           logical_offset: u64,
                           data: &[u8],
                           file_size: &mut u64)
                           -> Result<u64, Error>
{
    let offhdr = logical_offset.to_le_bytes();
    let len = data.len();

    debug_assert!((len >> 24) == 0);

    let lenhdr = [(len & 0xFF) as u8,
                  ((len >> 8) & 0xFF) as u8,
                  ((len >> 16) & 0xFF) as u8];

    file.write_all(&offhdr)?;
    file.write_all(&lenhdr)?;
    let data_off = *file_size + offhdr.len() as u64 + lenhdr.len() as u64;
    file.write_all(data)?;

    let mut d = crc64fast::Digest::new();
    d.write(&offhdr);
    d.write(&lenhdr);
    d.write(data);
    let tlr = u64::to_le_bytes(d.sum64());
    file.write_all(&tlr)?;
    *file_size = data_off + data.len() as u64 + tlr.len() as u64;

    Ok(data_off)
}

/// Insert a record into our in-memory span map.
pub(crate) fn add_record(spans: &mut BTreeMap<u64, Span>, 
                         logical_offset: u64,
                         len: u64,
                         file_data_offset: u64)
{
    // Do we partially overlap a span?  Trim it if so.
    match spans.range((Included(0), Excluded(logical_offset))).next_back() {
        None => {}
        Some((&offset, span)) => {
            if offset + span.len > logical_offset {
                spans.get_mut(&offset).unwrap().len = logical_offset - offset;
            }
        }
    }

    // Collect overlaps
    let overlaps: Vec<u64> = spans
        .range((Included(logical_offset), Excluded(logical_offset + len)))
        .map(|(&k, _)| k)
        .collect();

    // May need to actually split last one: create new one here, remove below.
    if let Some(&last) = overlaps.last() {
        let span = spans.get(&last).unwrap();
        if last + span.len > logical_offset + len {
            let front_trim = logical_offset + len - last;
            let span_tail = Span {
                len: span.len - front_trim,
                file_data_offset: span.file_data_offset + front_trim
            };
            spans.insert(logical_offset + len, span_tail);
        }
    }

    // Delete all.
    for k in overlaps {
        spans.remove(&k);
    }

    // Insert new span.
    spans.insert(logical_offset, Span { len: len, file_data_offset: file_data_offset });
}

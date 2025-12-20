//! Each write appends an ondisk record has a header, and a tailer.
//! [logical_offset: le64]
//! [length: le24]
//! [data...: length]
//! [hash: le64] (covers offset, length, and data)
use std::io::{Seek, SeekFrom, Read, Write};
use std::fs::File;
use crc64fast;
use std::ops::Bound::*;
use std::collections::BTreeMap;
use crate::Error;
use crate::store::Span;

pub(crate) const MAX_RECORD_SIZE: usize = 1 << 24;
const RECORD_HDR_SIZE: usize = 8 + 3;

pub(crate) struct RecordHeader {
    pub logical_offset: u64,
    pub length: u64,
}

pub(crate) struct Record {
    pub hdr: RecordHeader,
    pub file_data_offset: u64,
}

// No zero-length spans, no overlapping.
fn debug_check_spans(spans: &BTreeMap<u64, Span>)
{
    let mut prev_end = None;

    for (&off, span) in spans {
        debug_assert!(span.len > 0);
        let end = off + span.len;

        if let Some(pe) = prev_end {
            debug_assert!(off >= pe);
        }
        prev_end = Some(end);
    }
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

pub(crate) fn validate(file: &mut File,
                       data_offset: u64,
                       data_length: usize) -> Result<bool, Error>
{
    let mut bytes = vec![0u8; RECORD_HDR_SIZE + data_length + 8];

    file.seek(SeekFrom::Start(data_offset - RECORD_HDR_SIZE as u64))?;
    file.read_exact(&mut bytes)?;

    let mut d = crc64fast::Digest::new();
    d.write(&bytes[..RECORD_HDR_SIZE + data_length]);

    let csum_start = bytes.len() - 8;
    Ok(d.sum64() == u64::from_le_bytes(bytes[csum_start..csum_start + 8].try_into().unwrap()))
}

pub(crate) fn read_next_record(file: &mut File, file_offset: &mut u64) -> Result<Option<Record>, Error>
{
    let mut hdrbytes = [0u8; RECORD_HDR_SIZE];
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

    debug_assert!(len < MAX_RECORD_SIZE);
    debug_assert!(MAX_RECORD_SIZE - 1 <= 0x00FF_FFFF);
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

/// If a span overlaps logical_offset, split it in two.
fn split_span(spans: &mut BTreeMap<u64, Span>, logical_offset: u64)
{
    if let Some((&offset, span)) = spans.range((Included(0), Excluded(logical_offset))).next_back() {
        if offset + span.len > logical_offset {
            let before_len = logical_offset - offset;
            // We cannot validate spans after splitting, since they no longer correspond to
            // the record on disk.  So caller must have done this!
            assert!(span.validated);
            let newspan = Span { len: span.len - before_len,
                                 file_data_offset: span.file_data_offset + before_len,
                                 validated: span.validated };
            spans.insert(logical_offset, newspan);
            spans.get_mut(&offset).unwrap().len = before_len;
        }
    }
}

/// Insert a record into our in-memory span map.
pub(crate) fn add_record(spans: &mut BTreeMap<u64, Span>, 
                         logical_offset: u64,
                         len: u64,
                         file_data_offset: u64,
                         validated: bool)
{
    // Do we partially overlap some spans?  Split if so.
    split_span(spans, logical_offset);
    split_span(spans, logical_offset + len);

    // Collect overlaps (can't delete during iteration).
    let overlaps: Vec<u64> = spans
        .range((Included(logical_offset), Excluded(logical_offset + len)))
        .map(|(&k, _)| k)
        .collect();

    // Delete all.
    for k in overlaps {
        spans.remove(&k);
    }

    // Insert new span.
    spans.insert(logical_offset, Span { len: len,
                                        file_data_offset: file_data_offset,
                                        validated: validated,
    });
    debug_check_spans(spans);
}

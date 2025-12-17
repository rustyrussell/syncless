use std::fs::File;
use std::path::Path;
use std::collections::BTreeMap;
use std::io::{Read, Seek, SeekFrom};
use std::ops::Bound::*;
use std::cmp::{min, max};
use crate::Error;
use crate::header;
use crate::record;
use crate::OpenMode;

/// An open Syncless store.
pub struct Store {
    file: File,
    spans: BTreeMap<u64, Span>,
    logical_size: u64,
    file_size: u64,
}

pub(crate) struct Span {
    pub len: u64,
    pub file_data_offset: u64,
}

/// Opens an existing syncless store or creates a new one.
///
/// On success, the returned [`Store`] represents a logically consistent
/// view reconstructed from the on-disk log.
///
/// # Errors
///
/// Returns an error if the file cannot be opened (using the
/// underlying OS error), is not a valid syncless store, or is a
/// future incompatible version.
pub fn open<P: AsRef<Path>>(
    path: P,
    mode: OpenMode,
) -> Result<Store, Error> {
    let mut oo = std::fs::OpenOptions::new();
    oo.read(true);

    match mode {
        OpenMode::ReadOnly => {}
        OpenMode::WriteMustExist => { oo.write(true); }
        OpenMode::WriteMayCreate => { oo.write(true); oo.create(true); }
    }

    let file = oo.open(path)?;

    let mut store = Store {
        file: file,
        spans: BTreeMap::new(),
        logical_size: 0,
        file_size: 0,
    };

    // Special case: empty file, we write header.
    if store.file.metadata()?.len() == 0 && !matches!(mode, OpenMode::ReadOnly) {
        store.file_size = header::write_header(&mut store.file)?;
        // FIXME: fdatasync!
        return Ok(store);
    }

    let hver = header::read_header(&mut store.file, &mut store.file_size)?;

    if !hver.is_read_compatible() {
        return Err(Error::UnsupportedVersion);
    }
    
    if !matches!(mode, OpenMode::ReadOnly) && !hver.is_write_compatible() {
        return Err(Error::UnsupportedVersion);
    }

    while let Some(record) = record::read_next_record(&mut store.file, &mut store.file_size)? {
        record::add_record(&mut store.spans,
                           record.hdr.logical_offset,
                           record.hdr.length,
                           record.file_data_offset);
        store.logical_size = max(store.logical_size, record.hdr.logical_offset + record.hdr.length);
    }
    Ok(store)
}

impl Store {
    /// Returns the logical size of the store in bytes.
    ///
    /// Can't read past this, can write past it (which, if successful, may
    /// increase future logical size).
    pub fn size(&self) -> u64 { self.logical_size }

    /// Reads `buf.len()` bytes starting at `offset`.
    ///
    /// The read is performed against the reconstructed logical view
    /// of the store.  If there's a hole (created by a write past the
    /// prior end of file) it will read as all zeros.
    ///
    /// # Errors
    ///
    /// Return zeros past the logical size of the store (see size()),
    /// and an error on underlying I/O error.
    pub fn read(&mut self, offset: u64, buf: &mut [u8]) -> Result<(), Error> {
        let mut buf_off: u64 = 0;

        // Holes are zeros, so simply zero it out to start.
        buf.fill(0);

        if let Some((&off, span)) = self.spans.range((Included(0), Excluded(offset))).next_back() {
            if off + span.len > offset {
                // FIXME: mmap
                let bytes_before = offset - off;
                let len = min(span.len - bytes_before, buf.len() as u64);
                self.file.seek(SeekFrom::Start(off + bytes_before))?;
                self.file.read_exact(&mut buf[..len as usize])?;
                buf_off += len;
            }
        }

        for (&off, span) in self.spans.range((Included(offset + buf_off), Excluded(offset + buf.len() as u64))) {
            let len = min(span.len, buf.len() as u64 - buf_off);
            self.file.seek(SeekFrom::Start(off))?;
            self.file.read_exact(&mut buf[buf_off as usize..buf_off as usize + len as usize])?;
            buf_off += len;
        }
        Ok(())
    }

    /// Writes `buf.len()` bytes starting at `offset`.
    ///
    /// You can write anywhere, but if you create holes they will be
    /// zero-filled.  Writes are ordered and become atomically visible
    /// on success.  No durability guarantees: the effects of this
    /// write may be lost on crash or power failure.  However, the
    /// effects of this write will never be observed without also
    /// observing the effects of all previous successful writes.
    ///
    /// # Errors
    ///
    /// Returns an error on underlying I/O problems (probably out of disk space).
    pub fn write(&mut self, offset: u64, buf: &[u8]) -> Result<(), Error> {
        let data_off = record::write_record(&mut self.file, offset, buf, &mut self.file_size)?;
        record::add_record(&mut self.spans, offset, buf.len() as u64, data_off);
        Ok(())
    }
}

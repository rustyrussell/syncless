use std::fs::File;
use std::path::Path;
use std::collections::BTreeMap;
use std::io::{Read, Seek, SeekFrom};
use std::ops::Bound::*;
use std::cmp::min;
use std::marker::PhantomData;
use crate::Error;
use crate::header;
use crate::record;
use crate::Store;
use crate::{ReadOnly, Writable, WriteOpenMode};

/// An open Syncless store.
pub(crate) struct StoreBase {
    file: File,
    spans: BTreeMap<u64, Span>,
    file_size: u64,
}

pub(crate) struct Span {
    /// How long is the data in this span (in practice, less than MAX_RECORD_SIZE).
    pub len: u64,
    /// Where the physical file is the span data (i.e. after header).
    pub file_data_offset: u64,
    /// Did we freshly write this span?  If so, ZFS on Ubuntu (at least) may fart back zeroes
    /// at us: we need to recheck this and fdatasync if we see this.  Thanks Obama!
    pub validated: bool,
}

/// Parse header of new file, load up records.
fn read_newfile(base: &mut StoreBase, compatible: fn(&header::HeaderVer) -> bool) -> Result<(), Error>
{
    let hver = header::read_header(&mut base.file, &mut base.file_size)?;

    if !compatible(&hver) {
        return Err(Error::UnsupportedVersion);
    }

    while let Some(record) = record::read_next_record(&mut base.file, &mut base.file_size)? {
        record::add_record(&mut base.spans,
                           record.hdr.logical_offset,
                           record.hdr.length,
                           record.file_data_offset, true);
    }
    Ok(())
}

/// Opens an existing syncless store readonly.
///
/// On success, the returned [`Store`] represents a logically consistent
/// view reconstructed from the on-disk log.
///
/// # Errors
///
/// Returns an error if the file cannot be opened (using the
/// underlying OS error), is not a valid syncless store, or is a
/// future incompatible version.
pub fn open_readonly<P: AsRef<Path>>(
    path: P,
) -> Result<Store<ReadOnly>, Error> {
    let mut oo = std::fs::OpenOptions::new();
    oo.read(true);

    let file = oo.open(path)?;

    let mut base = StoreBase {
        file: file,
        spans: BTreeMap::new(),
        file_size: 0,
    };

    read_newfile(&mut base, header::HeaderVer::is_read_compatible)?;
    Ok(Store {base, writable: false, _mode: PhantomData })
}

pub(crate) fn open_writable_base<P: AsRef<Path>>(
    path: P,
    mode: WriteOpenMode,
) -> Result<StoreBase, Error> {
    let mut oo = std::fs::OpenOptions::new();
    oo.read(true);
    oo.write(true);

    match mode {
        WriteOpenMode::MustExist => { oo.create(false); }
        WriteOpenMode::MustNotExist => { oo.create_new(true); }
        WriteOpenMode::MayExist => { oo.create(true); }
    }

    let file = oo.open(path)?;

    let mut base = StoreBase {
        file: file,
        spans: BTreeMap::new(),
        file_size: 0,
    };

    // Special case: empty file, we write header.
    if base.file.metadata()?.len() == 0 {
        base.file_size = header::write_header(&mut base.file)?;
        base.file.sync_all()?;
    } else {
        read_newfile(&mut base, header::HeaderVer::is_write_compatible)?;
    }
    Ok(base)
}


/// Opens an existing syncless store for reading and writing.
///
/// On success, the returned [`Store`] represents a logically consistent
/// view reconstructed from the on-disk log.
///
/// # Errors
///
/// Returns an error if the file cannot be opened for writing (using the
/// underlying OS error), is not a valid syncless store, or is a
/// future incompatible version.
pub fn open<P: AsRef<Path>>(
    path: P,
    mode: WriteOpenMode,
) -> Result<Store<Writable>, Error> {
    Ok(Store {base: open_writable_base::<P>(path, mode)?,
              writable: true,
              _mode: PhantomData})
}

fn validate_record_with_retry(
    file: &mut File,
    file_data_offset: u64,
    length: u64,
) -> Result<(), Error> {
    if record::validate(file, file_data_offset, length as usize)? {
        return Ok(());
    }

    file.sync_data()?;

    if record::validate(file, file_data_offset, length as usize)? {
        return Ok(());
    }

    Err(Error::CorruptRecord)
}

impl<M> Store<M>
{
    /// Returns the logical size of the store in bytes.
    ///
    /// Reading past this gives zeros.  Writing past this successfully is
    /// the only way to increase its value.
    pub fn size(&self) -> u64 {
        self.base.spans
            .last_key_value()
            .map(|(off, span)| off + span.len)
            .unwrap_or(0)
    }

    /// Get offset of prior record (or 0)
    fn prev_offset(&self, offset: u64) -> u64 {
        self.base.spans
            .range((Included(0), Excluded(offset)))
            .next_back()
            .map(|(&off, _)| off)
            .unwrap_or(0)
    }

    /// Validate any spans in this range not already validated.
    fn validate_range(&mut self, start: u64, end: u64) -> Result<(), Error> {
        if !self.writable {
            return Ok(());
        }

        let to_validate: Vec<(u64, u64, u64)> = self.base.spans
            .range((Included(start), Excluded(end)))
            .filter_map(|(&off, span)| {
                if span.validated {
                    None
                } else {
                    Some((off, span.file_data_offset, span.len))
                }
            })
            .collect();

        // Validate them all.
        for &(_, file_data_offset, length) in &to_validate {
            validate_record_with_retry(&mut self.base.file, file_data_offset, length)?;
        }

        // Set them all valid.
        for &(off, _, _) in &to_validate {
            let span = self.base.spans.get_mut(&off).unwrap();
            span.validated = true;
        }
        Ok(())
    }

    /// Reads `buf.len()` bytes starting at `offset`.
    ///
    /// The read is performed against the reconstructed logical view of the
    /// store.  If there's a hole, or past EOF, it will read as all zeros.
    ///
    /// # Errors
    ///
    /// Return zeros past the logical size of the store (see size()), and an
    /// error on underlying I/O error.
    pub fn read(&mut self, mut offset: u64, mut buf: &mut [u8]) -> Result<(), Error> {
        // Holes are zeros, so simply zero it out to start.
        buf.fill(0);

        let prev = self.prev_offset(offset);
        self.validate_range(prev, offset + buf.len() as u64)?;

        // End of previous span may overlap.
        if let Some(span) = self.base.spans.get(&prev) {
            if prev + span.len > offset {
                // FIXME: mmap
                let bytes_before = offset - prev;
                let len = min(span.len - bytes_before, buf.len() as u64);
                self.base.file.seek(SeekFrom::Start(span.file_data_offset + bytes_before))?;
                self.base.file.read_exact(&mut buf[..len as usize])?;
                offset += len;
                buf = &mut buf[len as usize..];
            }
        }

        for (&off, span) in self.base.spans.range((Included(offset), Excluded(offset + buf.len() as u64))) {
            // Skip over any bytes not covered by span.
            let bytes_until_span = off - offset;
            if bytes_until_span != 0 {
                offset += bytes_until_span;
                buf = &mut buf[bytes_until_span as usize..];
            }

            // Read in span.
            let len = min(span.len, buf.len() as u64);
            self.base.file.seek(SeekFrom::Start(span.file_data_offset))?;
            self.base.file.read_exact(&mut buf[..len as usize])?;
            offset += len;
            buf = &mut buf[len as usize..];
        }
        Ok(())
    }
}


impl Store<Writable> {
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
    pub fn write(&mut self, mut offset: u64, mut buf: &[u8]) -> Result<(), Error> {
        // Validate anything we're going to overwrite.
        self.validate_range(self.prev_offset(offset), offset + buf.len() as u64)?;

        while !buf.is_empty() {
            let chunk = &buf[..min(buf.len(), record::MAX_RECORD_SIZE)];

            let data_off = record::write_record(&mut self.base.file, offset, chunk, &mut self.base.file_size)?;
            record::add_record(&mut self.base.spans, offset, chunk.len() as u64, data_off, false);
            buf = &buf[chunk.len()..];
            offset += chunk.len() as u64;
        }
        Ok(())
    }

    /// Convert this writable store into a readonly one.
    pub fn into_readonly(mut self) -> Result<Store<ReadOnly>, Error> {
        // Before we make it readonly, make sure all spans are validated!
        self.validate_range(0, self.size())?;

        Ok(Store {
            base: self.base,
            writable: false,
            _mode: PhantomData,
        })
    }
}

#[cfg(test)]

#[test]
fn empty_store_size_zero() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");

    let store = open(&path, WriteOpenMode::MayExist).unwrap();
    assert_eq!(store.size(), 0);
}

#[test]
fn write_then_read() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");

    let mut store = open(&path, WriteOpenMode::MayExist).unwrap();

    store.write(0, b"hello").unwrap();
    assert_eq!(store.size(), 5);

    let mut buf = [0u8; 5];
    store.read(0, &mut buf).unwrap();

    assert_eq!(&buf, b"hello");
}

#[test]
fn overwrite_middle() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");

    let mut store = open(&path, WriteOpenMode::MayExist).unwrap();

    store.write(0, b"abcdefgh").unwrap();
    store.write(2, b"XYZ").unwrap();

    let mut buf = [0u8; 8];
    store.read(0, &mut buf).unwrap();

    assert_eq!(&buf, b"abXYZfgh");
}

#[test]
fn holes_are_zero() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");

    let mut store = open(&path, WriteOpenMode::MayExist).unwrap();
    store.write(10, b"hi").unwrap();

    let mut buf = [0u8; 12];
    store.read(0, &mut buf).unwrap();

    assert_eq!(&buf[..10], &[0u8; 10]);
    assert_eq!(&buf[10..12], b"hi");
}

#[test]
fn replay_reconstructs_state() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");

    {
        let mut store = open(&path, WriteOpenMode::MayExist).unwrap();
        store.write(0, b"abc").unwrap();
        store.write(5, b"xyz").unwrap();
    }

    let mut store = open(&path, WriteOpenMode::MustExist).unwrap();

    let mut buf = [0u8; 8];
    store.read(0, &mut buf).unwrap();

    assert_eq!(&buf, b"abc\0\0xyz");
}

//! Parsing and validating magic + version.  This header is created once at offset 0.
//! Magic food (8 bytes):                "Syncless"
//! Version (4 bytes, Little Endian):    Majorver (8 bits) Formatver (8 bits) Minorver (16 bits);
//!
//!                                      Majorver:  if not compatible, fail open.
//!                                      Formatver: if not compatible, only allow read-only open.
//!                                      Minorver:  ignore, informational only.
use std::fs::File;
use std::io::{Read, Write};
use crate::Error;

const MAGIC: &[u8; 8] = b"Syncless";

pub(crate) struct HeaderVer {
    major: u8,
    format: u8,
    _minor: u16,
}

impl HeaderVer {
    const CURRENT_MAJOR: u8 = 0;
    const CURRENT_FORMAT: u8 = 0;
    const CURRENT_MINOR: u16 = 0;

    pub(crate) fn is_read_compatible(&self) -> bool {
        self.major <= Self::CURRENT_MAJOR
    }
    pub(crate) fn is_write_compatible(&self) -> bool {
        self.is_read_compatible() && self.format <= Self::CURRENT_FORMAT
    }
}

pub(crate) fn read_header(file: &mut File, file_offset: &mut u64) -> Result<HeaderVer, Error> {
    let mut magic_and_header = [0u8; 8 + 4];

    match file.read_exact(&mut magic_and_header) {
        Ok(()) => {
            *file_offset += magic_and_header.len() as u64;
        }
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
            return Err(Error::NotSyncless);
        }
        Err(error) => {
            return Err(Error::Io(error));
        }
    }
    if &magic_and_header[..8] != MAGIC {
        return Err(Error::NotSyncless);
    }

    let hver = HeaderVer {
        major: magic_and_header[8],
        format: magic_and_header[9],
        _minor: u16::from_le_bytes([
            magic_and_header[10],
            magic_and_header[11],
        ]),
    };
    Ok(hver)
}

pub(crate) fn write_header(file: &mut File) -> Result<u64, Error> {
    let mut magic_and_header = [0u8; 8 + 4];

    magic_and_header[..8].copy_from_slice(MAGIC);
    magic_and_header[8] = HeaderVer::CURRENT_MAJOR;
    magic_and_header[9] = HeaderVer::CURRENT_FORMAT;
    magic_and_header[10..12].copy_from_slice(&HeaderVer::CURRENT_MINOR.to_le_bytes());

    file.write_all(&magic_and_header)?;
    Ok(magic_and_header.len() as u64)
}

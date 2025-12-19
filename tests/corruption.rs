use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use tempfile::tempdir;

use syncless::{open_readonly, open, WriteOpenMode};

const ALL_WRITES: usize = 3;

fn write_base_file(path: &std::path::Path, num_writes: usize) {
    let mut store = open(path, WriteOpenMode::MayExist).unwrap();

    const WRITES: &[(u64, &[u8])] = &[
        (1, b"AB"),
        (2, b"C"),
        (1, b"D"),
    ];

    for &(off, data) in WRITES.iter().take(num_writes) {
        store.write(off, data).unwrap();
    }
}

fn read_contents(path: &std::path::Path) -> Vec<u8> {
    let mut store = open_readonly(path).unwrap();
    let mut buf = vec![0u8; store.size() as usize];
    store.read(0, &mut buf).unwrap();
    buf
}

fn write_bytes(path: &std::path::Path, bytes: &[u8]) {
    let mut f = OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(path)
        .unwrap();
    f.write_all(bytes).unwrap();
}

fn measure_boundaries() -> Vec<usize> {
    let dir = tempdir().unwrap();
    let path = dir.path().join("store");

    // Establish length of each record.
    let mut boundaries: Vec<usize> = Vec::new();
    for i in 0..=ALL_WRITES {
        // Ensure each run starts from an empty file
        std::fs::remove_file(&path).ok();
        write_base_file(&path, i);
        let mut contents = Vec::new();
        File::open(&path).unwrap().read_to_end(&mut contents).unwrap();
        boundaries.push(contents.len());
    }
    boundaries
}

/// Given a (first) corrupted byte, how many records do we expect?
fn max_record(byte_corrupted: usize) -> usize {
    let boundaries = measure_boundaries();
    for (i, &boundary) in boundaries.iter().enumerate() {
        if byte_corrupted < boundary {
            return i - 1;
        }
    }
    return boundaries.len() - 1;
}

fn is_valid_result(buf: &[u8], records: usize) -> bool {
    const CONTENTS: &[&[u8]] = &[
        b"",
        b"\0AB",
        b"\0AC",
        b"\0DC",
    ];
    return buf == CONTENTS[records];
}

#[test]
fn single_bit_corruption() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("store");

    write_base_file(&path, ALL_WRITES);

    let mut original = Vec::new();
    File::open(&path).unwrap().read_to_end(&mut original).unwrap();

    // Skip header (12 bytes: magic + version)
    for i in 12 * 8..original.len() * 8 {
        let mut corrupted = original.clone();

        // Flip a bit deterministically
        corrupted[i / 8] ^= 1 << i%8;

        write_bytes(&path, &corrupted);

        let result = read_contents(&path);
        assert!(
            is_valid_result(&result, max_record(i / 8)),
            "bit flip at byte {} bit {} ({}->{}) produced invalid state: {:?}",
            i / 8, i % 8,
            corrupted[i / 8] ^ (1 << i%8),
            corrupted[i / 8],
            result
        );
    }
}

#[test]
fn zero_each_nonzero_byte() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("store");

    write_base_file(&path, ALL_WRITES);

    let mut original = Vec::new();
    File::open(&path).unwrap().read_to_end(&mut original).unwrap();

    // Exhaustive map of each non-zero byte is too large (34 bits)
    // so we only do one of the checksum bytes, so it's only 13 bits.

    // Layout:
    // header: 12
    // record 1: offset(8) len(3) data(2) csum(8)
    // record 2: offset(8) len(3) data(1) csum(8)
    // record 3: offset(8) len(3) data(1) csum(8)
    const HEADER_LEN: usize = 12;
    const OFFSET_LEN: usize = 8;
    const LEN_LEN: usize = 3;
    const CSUM_LEN: usize = 8;

    let csum_offsets = {
        let r1 = HEADER_LEN + OFFSET_LEN + LEN_LEN + 2;
        let r2 = r1 + CSUM_LEN + OFFSET_LEN + LEN_LEN + 1;
        let r3 = r2 + CSUM_LEN + OFFSET_LEN + LEN_LEN + 1;
        [r1, r2, r3]
    };

    let is_checksum_byte = |i: usize| {
        csum_offsets
            .iter()
            .any(|&off| i > off && i < off + CSUM_LEN)
    };

    let nonzero_bytes: Vec<usize> = (HEADER_LEN..original.len())
        .filter(|&i| !is_checksum_byte(i))
        .filter(|&i| original[i] != 0)
        .collect();

    assert!(nonzero_bytes.len() < 16);

    // Now we do exhaustive possibilities.
    for mask in 0..1 << nonzero_bytes.len() {
        let mut corrupted = original.clone();
        let mut first_corrupted = original.len() + 1;

        for (bit, &idx) in nonzero_bytes.iter().enumerate() {
            if (mask & (1 << bit)) != 0 {
                corrupted[idx] = 0;
                if idx < first_corrupted {
                    first_corrupted = idx;
                }
            }
        }

        write_bytes(&path, &corrupted);

        let result = read_contents(&path);
        assert!(
            is_valid_result(&result, max_record(first_corrupted)),
            "zeroing mask {:b} (first corrupted byte {}, max record {}, boundaries {:?}) produced invalid state: {:?}",
            mask, first_corrupted, max_record(first_corrupted), measure_boundaries(),
            result
        );
    }
}

#[test]
fn truncation_at_any_prefix_is_handled() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("store");

    write_base_file(&path, ALL_WRITES);

    let mut original = Vec::new();
    File::open(&path).unwrap().read_to_end(&mut original).unwrap();

    // Header must remain intact or open will fail.
    for len in 12..=original.len() {
        write_bytes(&path, &original[..len]);

        let result = read_contents(&path);
        assert!(
            is_valid_result(&result, max_record(len)),
            "invalid state after truncation to {} bytes: {:?}",
            len,
            result
        );
    }
}

#[test]
fn truncation_after_corruption_is_handled() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("store");

    write_base_file(&path, ALL_WRITES);
    let original = std::fs::read(&path).unwrap();

    for len in 13..original.len() {
        let mut corrupted = original[..len].to_vec();
        if let Some(b) = corrupted.last_mut() {
            *b = 0;
        }

        write_bytes(&path, &corrupted);

        let result = read_contents(&path);
        assert!(is_valid_result(&result, max_record(len - 1)));
    }
}

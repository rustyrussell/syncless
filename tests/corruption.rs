use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use tempfile::tempdir;

use syncless::{open, OpenMode};

fn write_base_file(path: &std::path::Path) {
    let mut store = open(path, OpenMode::WriteMayCreate).unwrap();

    store.write(1, b"AB").unwrap();
    store.write(2, b"C").unwrap();
    store.write(1, b"D").unwrap();
}

fn read_contents(path: &std::path::Path) -> Vec<u8> {
    let mut store = match open(path, OpenMode::ReadOnly) {
        Ok(s) => s,
        Err(_) => return Vec::new(),
    };

    let mut buf = vec![0u8; store.size() as usize];
    store.read(0, &mut buf).unwrap();
    buf
}

fn is_valid_result(buf: &[u8]) -> bool {
    matches!(
        buf,
        [] |
        [0, b'A', b'B'] |
        [0, b'A', b'C'] |
        [0, b'D', b'C']
    )
}

#[test]
fn single_bit_corruption() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("store");

    write_base_file(&path);

    let mut original = Vec::new();
    File::open(&path).unwrap().read_to_end(&mut original).unwrap();

    // Skip header (12 bytes: magic + version)
    for i in 12 * 8..original.len() * 8 {
        let mut corrupted = original.clone();

        // Flip a bit deterministically
        corrupted[i / 8] ^= 1 << i%8;

        let mut f = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        f.write_all(&corrupted).unwrap();
        f.flush().unwrap();

        let result = read_contents(&path);
        assert!(
            is_valid_result(&result),
            "bit flip at {} ({}->{})produced invalid state: {:?}",
            i,
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

    write_base_file(&path);

    let mut original = Vec::new();
    File::open(&path).unwrap().read_to_end(&mut original).unwrap();

    for i in 12..original.len() {
        if original[i] == 0 {
            continue;
        }

        let mut corrupted = original.clone();
        corrupted[i] = 0;

        let mut f = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        f.write_all(&corrupted).unwrap();
        f.flush().unwrap();

        let result = read_contents(&path);
        assert!(
            is_valid_result(&result),
            "zeroing byte at {} produced invalid state: {:?}",
            i,
            result
        );
    }
}

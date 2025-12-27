use tempfile::tempdir;
use syncless::{open, WriteOpenMode, open_readonly};
use std::fs;

#[test]
fn compact() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("store");

    let mut store = open(&path, WriteOpenMode::MustNotExist).unwrap();

    let mut prev_len = fs::metadata(&path).unwrap().len();

    let mut off = 0usize;
    loop {
        store.write(off as u64, b"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789").unwrap();
        assert!(store.size() == off as u64 + 36);
        let new_len = fs::metadata(&path).unwrap().len();
        if new_len < prev_len {
            break;
        }
        prev_len = new_len;
        off += 1;
    }

    assert!(store.size() == off as u64 + 36);
    let mut compacted_contents = vec![0u8; store.size() as usize];
    store.read(0, &mut compacted_contents).unwrap();

    for i in 0..off {
        assert_eq!(compacted_contents[i], b'A');
    }
    assert_eq!(&compacted_contents[off..], b"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789");

    let mut store = open_readonly(path).unwrap();
    assert!(store.size() == off as u64 + 36);
    let mut reread_contents = vec![0u8; store.size() as usize];
    store.read(0, &mut reread_contents).unwrap();

    assert!(reread_contents == compacted_contents);
}

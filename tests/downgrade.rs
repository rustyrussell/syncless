use tempfile::tempdir;
use syncless::{open, WriteOpenMode};

#[test]
fn downgrade() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("store");

    let mut store = open(path, WriteOpenMode::MustNotExist).unwrap();

    const WRITES: &[(u64, &[u8])] = &[
        (1, b"AB"),
        (2, b"C"),
        (1, b"D"),
    ];

    for &(off, data) in WRITES {
        store.write(off, data).unwrap();
    }

    let mut orig = [0u8; 3];
    store.read(0, &mut orig).unwrap();

    let mut ro_store = store.into_readonly().unwrap();
    let mut ro = [0u8; 3];
    ro_store.read(0, &mut ro).unwrap();

    assert!(ro == orig);
}

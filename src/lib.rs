//! syncless: ordered, atomic storage without durability guarantees.
//!
//! Many times you don't want to pay the cost of continuous fsyncs,
//! and are ok with losing the latest updates if an OS crash/power
//! outage were to happen and the user is unlucky BUT it's not OK to
//! corrupt older data.  Think of cases like "browser bookmarks" or
//! "history": synchronous requirements are overkill for these.
#![deny(warnings)]
#![deny(missing_docs)]
#![forbid(unsafe_op_in_unsafe_fn)]
mod header;
mod record;
mod store;

/// Errors from our functions.
#[derive(Debug)]
pub enum Error {
    /// Underlying filesystem issue (ENOENT, ENOSPC, etc).
    Io(std::io::Error),
    /// Open: not a file created by Syncless.
    NotSyncless,
    /// Open: a future version of Syncless, which says we're not compatible.
    UnsupportedVersion,
    /// Read: we just wrote a record, and it wasn't valid when we read it back.
    /// This should not happen.
    CorruptRecord,
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::Io(err)
    }
}

/// Store comes in two flavors: ReadOnly and Writable.
pub struct Store<M> {
    base: StoreBase,
    writable: bool,
    _mode: std::marker::PhantomData<M>,
}

/// Phantom data to make Store<Readonly>
pub struct ReadOnly;
/// Phantom data to make Store<Writable>
pub struct Writable;

/// How to open the Syncless store file:
pub enum WriteOpenMode {
    /// Must exist, must be a Syncless store file.
    MustExist,
    /// Must not exist.
    MustNotExist,
    /// Must not exist or be a Syncless store file.
    MayExist,
}

pub use store::open_readonly;
pub use store::open;
pub use store::StoreBase;

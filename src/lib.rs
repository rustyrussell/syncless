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

/// Errors from open
#[derive(Debug)]
pub enum Error {
    /// Underlying filesystem issue (ENOENT, ENOSPC, etc).
    Io(std::io::Error),
    /// Not a file created by Syncless.
    NotSyncless,
    /// A future version of Syncless, which says we're not compatible.
    UnsupportedVersion,
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::Io(err)
    }
}

/// How to open the Syncless store file:
pub enum OpenMode {
    /// Must exist, must be a Syncless store file, file may be unwritable, will not use Store::write().
    ReadOnly,
    /// Must exist, must be empty or a Syncless store file, must be writable, can use Store::write().
    WriteMustExist,
    /// Must be a Syncless store file or will be created, must be writable, can use Store::write().
    WriteMayCreate,
}

pub use store::open;
pub use store::Store;

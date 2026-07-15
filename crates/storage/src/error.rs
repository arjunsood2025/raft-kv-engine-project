use std::fmt;
use std::io;

/// Storage-layer errors. `Corruption` means on-disk bytes failed a checksum
/// or decode — the durable invariant is that we detect it rather than serve
/// garbage.
#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Corruption(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "io error: {e}"),
            Error::Corruption(msg) => write!(f, "corruption: {msg}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Io(e)
    }
}

pub type Result<T> = std::result::Result<T, Error>;

use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub enum Error {
    HttpFailed { status: u16 },
    IO(std::io::Error),
    External(Box<dyn std::error::Error + Send + Sync + 'static>),
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::HttpFailed { status } => {
                write!(f, "HTTP requests failed with status: {status}")
            }
            Error::IO(io) => io.fmt(f),
            Error::External(external) => external.fmt(f),
        }
    }
}

impl std::error::Error for Error {}
pub type Result<T> = std::result::Result<T, Error>;

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::IO(value)
    }
}

use flatbuffers::InvalidFlatbuffer;
use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub enum Error {
    MissingMagicBytes,
    NoIndex,
    #[cfg(feature = "http")]
    ChunkedHttpClient(http_range_client::HttpError),
    #[cfg(feature = "http")]
    StreamingHttpClient(streaming_http_range_client::Error),
    IllegalHeaderSize(usize),
    InvalidFlatbuffer(InvalidFlatbuffer),
    IO(std::io::Error),
}
pub type Result<T> = std::result::Result<T, Error>;

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::MissingMagicBytes => "Missing magic bytes. Is this an fgb file?".fmt(f),
            Error::NoIndex => "Index missing".fmt(f),
            #[cfg(feature = "http")]
            // TODO: consolidate HTTP errors
            Error::ChunkedHttpClient(http_client) => http_client.fmt(f),
            Error::StreamingHttpClient(http_client) => http_client.fmt(f),
            Error::IllegalHeaderSize(size) => write!(f, "Illegal header size: {size}"),
            Error::InvalidFlatbuffer(invalid_flatbuffer) => invalid_flatbuffer.fmt(f),
            Error::IO(io) => io.fmt(f),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::IO(value)
    }
}

impl From<InvalidFlatbuffer> for Error {
    fn from(value: InvalidFlatbuffer) -> Self {
        Error::InvalidFlatbuffer(value)
    }
}

#[cfg(feature = "http")]
impl From<http_range_client::HttpError> for Error {
    fn from(value: http_range_client::HttpError) -> Self {
        Error::ChunkedHttpClient(value)
    }
}

#[cfg(feature = "http")]
impl From<streaming_http_range_client::Error> for Error {
    fn from(value: streaming_http_range_client::Error) -> Self {
        Error::StreamingHttpClient(value)
    }
}

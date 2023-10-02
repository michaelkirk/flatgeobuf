mod error;
mod http_range;

use futures_util::TryStreamExt;
use std::ops::{Range, RangeFrom};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncReadExt, ReadBuf};
use tokio_util::io::StreamReader;

#[macro_use]
extern crate log;

pub use error::{Error, Result};
pub use http_range::HttpRange;

use async_trait::async_trait;

/// A stream centric HTTP client.
///
/// ```
/// use streaming_http_range_client::HttpClient;
/// use tokio::io::AsyncReadExt;
///
/// use tokio;
/// # tokio_test::block_on(async {
/// let mut new_client = HttpClient::new("https://georust.org");
/// new_client.set_range(2..14).await.unwrap();
///
/// let mut output = String::new();
/// new_client.read_to_string(&mut output).await.unwrap();
///
/// // This `expected_text` may need to be updated someday if someone updates the site.
/// let expected_text = "DOCTYPE html";
/// assert_eq!(expected_text, output)
/// # });
///
/// ```
pub struct HttpClient {
    client: Box<dyn ReaderSource>,
    reader: Reader,
    range: Option<HttpRange>,
    pos: usize,
    stats: ReqStats,
}

#[derive(Debug, Default)]
struct ReqStats {
    wasted_bytes: usize,
    used_bytes: usize,
    req_count: usize,
}

impl HttpClient {
    /// Create a new client. To get data from the client, call [`set_range`] and start reading
    /// from the client.
    pub fn new(url: &str) -> Self {
        Self {
            client: Box::new(ReqwestClient::new(url)),
            reader: empty(),
            pos: 0,
            range: None,
            stats: ReqStats::default(),
        }
    }

    //
    pub async fn set_range(&mut self, range: Range<usize>) -> Result<()> {
        assert!(!range.is_empty());
        self.pos = range.start;

        self.stats.req_count += 1;
        self.reader = self.client.get_byte_range(range.clone()).await?;
        self.range = Some(HttpRange::Range(range));

        Ok(())
    }

    /// Advance client to `to_pos`, discarding any intermediate data, without fetching further data.
    ///
    /// `to_pos` must be within the current range and must not occur before the current position.
    pub async fn fast_forward(&mut self, to_pos: usize) -> Result<()> {
        assert!(to_pos >= self.pos, "can't rewind");

        let len = to_pos - self.pos;
        if len == 0 {
            return Ok(());
        }
        self.stats.wasted_bytes += len;

        let mut ff_reader = empty();
        std::mem::swap(&mut ff_reader, &mut self.reader);
        let mut ff_reader = ff_reader.take(len as u64);
        tokio::io::copy(&mut ff_reader, &mut tokio::io::sink()).await?;
        let reader = ff_reader.into_inner();
        self.pos += len;
        assert_eq!(self.pos, to_pos);

        self.reader = reader;
        Ok(())
    }

    /// Fast forwards to the beginning of range, fetching additional data if necessary.
    pub async fn seek_to_range(&mut self, range: HttpRange) -> Result<()> {
        let Some(HttpRange::Range(existing_range)) = &mut self.range else {
            panic!("can only fast forward from double ended range");
        };
        assert!(range.start() >= self.pos, "can't rewind");
        match range {
            HttpRange::Range(range) => {
                if range.start == self.pos {
                    if range.end <= existing_range.end {
                        trace!("nothing to do");
                        Ok(())
                    } else {
                        self.append_contiguous_range(range).await
                    }
                } else if range.end <= existing_range.end {
                    self.fast_forward(range.start).await
                } else if range.start > existing_range.end {
                    self.set_range(range).await
                } else {
                    assert!(range.start > self.pos);
                    assert!(
                        range.end > existing_range.end,
                        "failed: {range_end}, > {existing_range_end}",
                        range_end = range.end,
                        existing_range_end = existing_range.end
                    );
                    self.fast_forward(range.start).await?;
                    self.append_contiguous_range(range).await
                }
            }
            HttpRange::RangeFrom(range) => {
                if range.start == self.pos {
                    trace!("nothing to do");
                    Ok(())
                } else {
                    // TODO optimize for skipping over a lot of middle content
                    // e.g. self.set_range(range)
                    self.extend_to_end().await?;
                    self.fast_forward(range.start).await
                }
            }
        }
    }

    /// Fetch all the bytes to the end of the file.
    ///
    /// Panics when the current range is:
    ///  - not set (see [`set_range`])
    ///  - an open ended (RangeFrom) range, since it can't be extended
    pub async fn extend_to_end(&mut self) -> Result<()> {
        debug!("extending to end");
        let Some(HttpRange::Range(prev_range)) = &self.range else {
            panic!("must call set_range before you can extendToRange");
        };

        self.stats.req_count += 1;
        let reader = self.client.get_byte_range_from(prev_range.end..).await?;

        let mut tmp = empty();
        std::mem::swap(&mut self.reader, &mut tmp);
        self.reader = Box::pin(tmp.chain(reader));

        let new_range = prev_range.start..;
        self.range = Some(HttpRange::RangeFrom(new_range));

        Ok(())
    }

    /// Append a contiguous extension to the current range.
    ///
    /// Panics when the current range is:
    ///  - not set (see [`set_range`])
    ///  - an open ended (RangeFrom) range, since it can't be extended
    ///  - not contiguous with the extension
    pub async fn append_contiguous_range(&mut self, extension: Range<usize>) -> Result<()> {
        let Some(range) = &self.range else {
            panic!("must call set_range before you can extend a range");
        };

        let HttpRange::Range(prev_range) = range else {
            panic!("cannot extend an already open-ended range");
        };

        assert!(
            prev_range.end >= extension.start,
            "new range must be contiguous with old range"
        );

        if prev_range.end >= extension.end {
            debug!(
                "skipping extension {extension:?} which is within existing range: {prev_range:?}"
            );
            return Ok(());
        }

        let uncovered_range = prev_range.end..extension.end;
        self.stats.req_count += 1;
        let reader = self.client.get_byte_range(uncovered_range.clone()).await?;

        let mut tmp = empty();
        std::mem::swap(&mut self.reader, &mut tmp);
        self.reader = Box::pin(tmp.chain(reader));
        let new_range = prev_range.start..extension.end;
        self.range = Some(HttpRange::Range(new_range));

        Ok(())
    }


    /// Move all the unread data from this client into a new instance.
    pub fn split_off(&mut self) -> Self {
        let Some(range) = &mut self.range else {
            panic!("must set_range before splitting off");
        };

        let after = range.split(self.pos);
        assert_eq!(range.end(), Some(self.pos));

        let mut old_reader = empty();
        std::mem::swap(&mut self.reader, &mut old_reader);

        let child = Self {
            client: self.client.boxed_clone(),
            reader: old_reader,
            pos: self.pos,
            range: Some(after),
            stats: ReqStats::default(),
        };

        child
    }
}

impl Drop for HttpClient {
    fn drop(&mut self) {
        debug!("Finished using an HTTP client. used_bytes={used_bytes}, wasted_bytes={wasted_bytes}, req_count={req_count}", used_bytes=self.stats.used_bytes, wasted_bytes=self.stats.wasted_bytes, req_count=self.stats.req_count)
    }
}

impl AsyncRead for HttpClient {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        assert!(
            self.range.is_some(),
            "must call set_range (and await) before attempting read"
        );

        let len_before = buf.filled().len();
        let result = self.reader.as_mut().poll_read(cx, buf);

        let distance = buf.filled().len() - len_before;
        self.pos += distance;
        self.stats.used_bytes += distance;
        trace!("read {distance} bytes. New pos={pos}", pos = self.pos);

        result
    }
}

#[async_trait]
trait ReaderSource: Sync + Send {
    async fn get_byte_range(&self, range: Range<usize>) -> Result<Reader>;

    async fn get_byte_range_from(&self, range: RangeFrom<usize>) -> Result<Reader>;

    fn boxed_clone(&self) -> Box<dyn ReaderSource>;
}

#[derive(Clone)]
struct ReqwestClient {
    client: reqwest::Client,
    url: String,
}

impl ReqwestClient {
    fn new(url: &str) -> Self {
        Self {
            client: reqwest::Client::new(),
            url: url.to_string(),
        }
    }

    async fn get_byte_range_with_header(&self, range_header: &str) -> Result<Reader> {
        debug!("getting range: {range_header}");

        let response = self
            .client
            .get(&self.url)
            .header(reqwest::header::RANGE, range_header)
            .send()
            .await
            .map_err(|e| Error::External(Box::new(e)))?;

        let status = response.status();
        match response.headers().get("Content-Length") {
            Some(content_length) => debug!("content length: {content_length:?}"),
            None => debug!("Response lacks a content length header"),
        }

        if !status.is_success() {
            return Err(Error::HttpFailed {
                status: status.as_u16(),
            });
        }
        let bytes_stream = response
            .bytes_stream()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e));

        Ok(Box::pin(StreamReader::new(bytes_stream)))
    }
}

#[async_trait]
impl ReaderSource for ReqwestClient {
    async fn get_byte_range(&self, range: Range<usize>) -> Result<Reader> {
        let range_header = format!("bytes={}-{}", range.start, (range.end - 1));
        self.get_byte_range_with_header(&range_header).await
    }

    async fn get_byte_range_from(&self, range: RangeFrom<usize>) -> Result<Reader> {
        let range_header = format!("bytes={}-", range.start);
        self.get_byte_range_with_header(&range_header).await
    }

    fn boxed_clone(&self) -> Box<dyn ReaderSource> {
        Box::new(self.clone())
    }
}

type Reader = Pin<Box<dyn AsyncRead + Sync + Send>>;

fn empty() -> Reader {
    Box::pin(std::io::Cursor::new(vec![]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    /// Helpful for tests
    #[async_trait]
    impl ReaderSource for Vec<u8> {
        async fn get_byte_range(&self, range: Range<usize>) -> Result<Reader> {
            let range_data = self[range].to_owned();
            Ok(Box::pin(std::io::Cursor::new(range_data)))
        }

        async fn get_byte_range_from(&self, range: RangeFrom<usize>) -> Result<Reader> {
            let range_data = self[range].to_owned();
            Ok(Box::pin(std::io::Cursor::new(range_data)))
        }

        fn boxed_clone(&self) -> Box<dyn ReaderSource> {
            Box::new(self.clone())
        }
    }

    impl HttpClient {
        pub fn test_client(data: &[u8]) -> Self {
            Self {
                client: Box::new(data.to_vec()),
                reader: empty(),
                pos: 0,
                range: None,
                stats: ReqStats::default(),
            }
        }
    }

    #[tokio::test]
    async fn single_reader() {
        ensure_logging();

        let input = (0..4).collect::<Vec<u8>>();
        let mut reader = HttpClient::test_client(&input);
        reader.set_range(0..4).await.unwrap();

        let mut output = vec![];
        reader.read_to_end(&mut output).await.unwrap();
        assert_eq!(output, input);
    }

    #[tokio::test]
    async fn empty_reader() {
        ensure_logging();

        let input = (0..4).collect::<Vec<u8>>();
        let mut reader = HttpClient::test_client(&input);
        reader.set_range(0..4).await.unwrap();
        let mut output = vec![];
        reader.read_to_end(&mut output).await.unwrap();
        assert_eq!(output, input);

        let mut remainder = Vec::<u8>::new();
        reader.read_to_end(&mut remainder).await.unwrap();
        assert!(remainder.is_empty());
    }

    #[tokio::test]
    async fn extend_range() {
        ensure_logging();

        let input = (0..7).collect::<Vec<u8>>();
        let mut reader = HttpClient::test_client(&input);
        reader.set_range(0..3).await.unwrap();

        let mut output = vec![];
        reader.read_to_end(&mut output).await.unwrap();
        assert_eq!(output, vec![0, 1, 2]);

        reader.append_contiguous_range(3..6).await.unwrap();
        reader.read_to_end(&mut output).await.unwrap();
        assert_eq!(output, vec![0, 1, 2, 3, 4, 5]);
    }

    #[tokio::test]
    async fn read_le_u3() {
        let input: [u8; 4] = [140, 1, 0, 0];
        let mut reader = HttpClient::test_client(&input);
        reader.set_range(0..4).await.unwrap();
        let result = reader.read_u32_le().await.unwrap();
        assert_eq!(result, 396);
    }

    #[tokio::test]
    async fn split_off() {
        let input = (0..8).collect::<Vec<u8>>();
        let mut parent_reader = HttpClient::test_client(&input);
        parent_reader.set_range(0..7).await.unwrap();

        let mut output = [0; 4];
        parent_reader.read_exact(&mut output).await.unwrap();
        assert_eq!(output, [0, 1, 2, 3]);

        let mut child_reader = parent_reader.split_off();

        let mut remainder = vec![];
        parent_reader.read_to_end(&mut remainder).await.unwrap();
        assert!(remainder.is_empty());

        let mut output = [0; 4];
        child_reader.append_contiguous_range(7..8).await.unwrap();
        child_reader.read_exact(&mut output).await.unwrap();
        assert_eq!(output, [4, 5, 6, 7]);
    }

    #[tokio::test]
    async fn extend_to_end() {
        let input = (0..8).collect::<Vec<u8>>();
        let mut reader = HttpClient::test_client(&input);

        reader.set_range(4..5).await.unwrap();
        reader.extend_to_end().await.unwrap();

        let mut output = vec![];
        reader.read_to_end(&mut output).await.unwrap();

        assert_eq!(output, [4, 5, 6, 7])
    }

    #[tokio::test]
    async fn fast_forward() {
        let input = (0..8).collect::<Vec<u8>>();
        let mut reader = HttpClient::test_client(&input);

        reader.set_range(2..7).await.unwrap();
        reader.fast_forward(3).await.unwrap();
        let next = reader.read_u8().await.unwrap();
        assert_eq!(next, 3);
    }

    #[should_panic]
    #[tokio::test]
    async fn fast_forward_to_far() {
        let input = (0..8).collect::<Vec<u8>>();
        let mut reader = HttpClient::test_client(&input);

        reader.set_range(2..7).await.unwrap();
        reader.fast_forward(3).await.unwrap();
        let next = reader.read_u8().await.unwrap();
        assert_eq!(next, 3);

        // panics
        reader.fast_forward(2).await.unwrap();
        reader.fast_forward(3).await.unwrap();
    }

    #[cfg(test)]
    fn ensure_logging() {
        static ONCE: std::sync::Once = std::sync::Once::new();
        ONCE.call_once(|| env_logger::builder().format_timestamp_millis().init());
    }
}

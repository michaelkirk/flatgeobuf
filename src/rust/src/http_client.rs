use bytes::{Buf, BufMut, Bytes, BytesMut};
use geozero::error::{GeozeroError, Result};
use std::cmp::max;
use std::str;

struct HttpClient {
    client: reqwest::Client,
    url: String,
}

impl HttpClient {
    fn new(url: &str) -> Self {
        HttpClient {
            client: reqwest::Client::new(),
            url: url.to_string(),
        }
    }
    async fn get(&self, begin: usize, length: usize) -> Result<Bytes> {
        let response = self
            .client
            .get(&self.url)
            .header("Range", format!("bytes={}-{}", begin, begin + length - 1))
            .send()
            .await
            .map_err(|e| GeozeroError::HttpError(e.to_string()))?;
        if !response.status().is_success() {
            return Err(GeozeroError::HttpStatus(response.status().as_u16()));
        }
        response
            .bytes()
            .await
            .map_err(|e| GeozeroError::HttpError(e.to_string()))
    }

    async fn get_ranges(&self, ranges: Vec<(usize, usize)>) -> Result<Bytes> {
        let multi_range_string = ranges
            .iter()
            .map(|(start, length)| format!("{}-{}", start, start + length - 1))
            .collect::<Vec<_>>()
            .join(",");
        debug!("getting Range: bytes={}", multi_range_string);
        let response = self
            .client
            .get(&self.url)
            .header("Range", format!("bytes={}", multi_range_string))
            .send()
            .await
            .map_err(|e| GeozeroError::HttpError(e.to_string()))?;

        //dbg!(&response);
        if !response.status().is_success() {
            return Err(GeozeroError::HttpStatus(response.status().as_u16()));
        }

        let header = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .expect("missing ContentType header");
        // dbg!(&header);
        let header_value = header
            .to_str()
            .expect("failed to convert header to str")
            .to_string();
        // dbg!(&header_value);
        // mime fails, I think because of the ":" in the CloudFront boundary:
        //      &header_value = "multipart/byteranges; boundary=CloudFront:AAD77D12D1FEC64732F00EA78AA347CB"
        //      FromStrError { inner: InvalidToken { pos: 41, byte: 58 } }
        // let parsed_header_value = header_value.parse::<mime::Mime>().expect("failed to parse header value to mime");
        let split_token = "; boundary=";
        let parsed_header_value: Vec<_> = header_value.split_terminator(split_token).collect();

        match (parsed_header_value.get(0), parsed_header_value.get(1)) {
            (Some(&"multipart/byteranges"), Some(&boundary)) => {
                debug_assert_eq!(response.status().as_u16(), 206);
                debug!("matched byteranges with boundary: {}", boundary);
                let all_bytes = response
                    .bytes()
                    .await
                    .map_err(|e| GeozeroError::HttpError(e.to_string()))?;

                fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
                    haystack
                        .windows(needle.len())
                        .position(|window| window == needle)
                }
                fn trimmed<'a>(bytes: &'a [u8], pattern: &[u8]) -> &'a [u8] {
                    debug_assert!(!pattern.is_empty());

                    let mut result = bytes;

                    if result.len() < pattern.len() {
                        return result;
                    }

                    // trim front
                    while &result[0..pattern.len()] == pattern {
                        result = &result[pattern.len()..];
                        if result.len() < pattern.len() {
                            return result;
                        }
                    }

                    // trim back
                    while &result[result.len() - pattern.len()..] == pattern {
                        result = &result[0..result.len() - pattern.len()];
                        if result.len() < pattern.len() {
                            return result;
                        }
                    }

                    result
                }

                fn split_iter<'a>(
                    whole: &'a [u8],
                    separator: &'a [u8],
                ) -> impl Iterator<Item = &'a [u8]> {
                    struct Iter<'a> {
                        remaining: &'a [u8],
                        separator: &'a [u8], // todo seems weird that separator has same lifetime
                    }
                    impl<'a> Iterator for Iter<'a> {
                        type Item = &'a [u8];
                        fn next(&mut self) -> Option<Self::Item> {
                            if self.remaining.is_empty() {
                                return None;
                            }

                            match find_subsequence(self.remaining, self.separator) {
                                None => {
                                    let result = self.remaining;
                                    self.remaining = &[];
                                    Some(result)
                                }
                                Some(next) => {
                                    let result = &self.remaining[0..next];
                                    self.remaining = &self.remaining[next + self.separator.len()..];
                                    Some(result)
                                }
                            }
                        }
                    }
                    Iter {
                        remaining: whole,
                        separator,
                    }
                }

                let part_split = format!("--{}", boundary);
                // let split_at = find_subsequence(all_bytes, part_split.as_bytes());
                // dbg!((split_at, all_bytes));
                let mut has_seen_last_part = false;
                let mut bytes = BytesMut::new();
                for part in split_iter(&all_bytes, part_split.as_bytes()) {
                    let part = trimmed(part, "\r\n".as_bytes());
                    // dbg!(String::from_utf8_lossy(part));
                    if part.is_empty() {
                        trace!("skipping empty body part");
                        continue;
                    }
                    if part == "--".as_bytes() {
                        // indicates final closure, should only see once I think
                        debug_assert!(!has_seen_last_part);
                        has_seen_last_part = true;
                        continue;
                    }

                    let (_headers, body) = {
                        let mut splits = split_iter(part, "\r\n\r\n".as_bytes());
                        let headers = splits.next().expect("missing headers");
                        let body = splits.next().expect("missing body");
                        (headers, body)
                    };
                    //dbg!(String::from_utf8_lossy(headers));
                    // dbg!(String::from_utf8_lossy(body));
                    bytes.extend(body);
                }
                //dbg!(&bytes);

                // let all_str = String::from_utf8_lossy(&all_bytes);
                // let trimmed_parts = all_str.split_terminator(&part_split).filter_map(|p| {
                //     let trimmed = p.trim();
                //     if trimmed.is_empty() { None } else { Some(trimmed) }
                // });
                // dbg!(&trimmed_parts);
                // let mut bytes = BytesMut::new();
                // for part in trimmed_parts {
                //     if part == "--" {
                //         // indicates final closure, should only see once I think
                //         debug_assert!(!has_seen_last_part);
                //         has_seen_last_part = true;
                //         continue;
                //     }
                //     dbg!(&part);
                //     // remove leading/trailing newlines
                //     let mut headers_and_body= part.trim().split_terminator("\r\n\r\n");
                //     let (headers, body) = (headers_and_body.next().expect("missing headers"), headers_and_body.next().expect("missing body"));
                //     debug_assert!(headers_and_body.next().is_none());

                //     for header in headers.lines() {
                //         dbg!(header);
                //     }
                //     for body in body.lines() {
                //         dbg!(body);
                //         // seeing null bytes?
                //         bytes.extend(body.trim().bytes());
                //     }
                // }
                // dbg!(&bytes);
                Ok(bytes.to_bytes())
            }
            _ => {
                debug!("didn't match multipart");
                response
                    .bytes()
                    .await
                    .map_err(|e| GeozeroError::HttpError(e.to_string()))
            }
        }
    }
}

pub struct BufferedHttpClient {
    http_client: HttpClient,
    buf: BytesMut,
    /// Lower index of buffer relative to input stream
    head: usize,
    /// byte count for aggregate usage statistics
    bytes_ever_requested: usize,
}

impl BufferedHttpClient {
    pub fn new(url: &str) -> Self {
        BufferedHttpClient {
            http_client: HttpClient::new(url),
            buf: BytesMut::new(),
            head: 0,
            bytes_ever_requested: 0,
        }
    }

    pub async fn get_byte_ranges(
        &mut self,
        byte_ranges: Vec<(usize, usize)>,
        _min_req_size: usize,
    ) -> Result<&[u8]> {
        self.buf.clear();
        let ranges_length: usize = byte_ranges.iter().map(|(_start, len)| len).sum();
        self.bytes_ever_requested += ranges_length;
        debug!(
            "ranges: {:?}, length: {}, bytes_ever_requested: {}",
            byte_ranges, ranges_length, self.bytes_ever_requested
        );
        let bytes = self.http_client.get_ranges(byte_ranges).await?;
        self.buf.put(bytes);
        self.head = 0;
        // TODO: Any benefit to not just blowing everything away?
        let lower = self.head;
        let upper = self.buf.len();
        Ok(&self.buf[lower..upper])
    }

    pub async fn get(
        &mut self,
        begin: usize,
        length: usize,
        _min_req_size: usize,
    ) -> Result<&[u8]> {
        let tail = self.head + self.buf.len();
        if begin + length > tail || begin < self.head {
            // Remove bytes before new begin
            if begin > self.head && begin < tail {
                let _ = self.buf.split_to(begin - self.head);
                self.head = begin;
            } else if begin >= tail || begin < self.head {
                self.buf.clear();
                self.head = begin;
            }

            // Read additional bytes
            let range_begin = max(begin, tail);
            //let range_length = max(begin + length - range_begin, min_req_size);
            let range_length = begin + length - range_begin;
            self.bytes_ever_requested += range_length;
            debug!(
                "range: ({} , {}), length: {}, bytes_ever_requested: {}",
                range_begin,
                range_begin + range_length,
                range_length,
                self.bytes_ever_requested
            );
            let bytes = self.http_client.get(range_begin, range_length).await?;
            self.buf.put(bytes);
        }
        let lower = begin - self.head;
        let upper = begin + length - self.head;
        Ok(&self.buf[lower..upper])
    }
}

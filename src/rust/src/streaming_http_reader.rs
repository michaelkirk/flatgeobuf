use crate::packed_r_tree::{NodeItem, PackedRTree};
use crate::{Error, Result};

use bytes::{Bytes, BytesMut};
use futures_util::{Stream, StreamExt};
use std::cmp::min;
use std::collections::VecDeque;
use std::ops::Range;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::AsyncReadExt;

pub struct HttpFgbReader {
    header_buffer: Box<Bytes>,
    client: HttpClient,
    // request up to this many extra bytes if it means we can eliminate an extra request
    combine_request_threshold: usize,
}

use crate::{
    check_magic_bytes, size_prefixed_root_as_header, size_prefixed_root_as_header_unchecked,
    Header, HEADER_MAX_BUFFER_SIZE, MAGIC_BYTES,
};
use streaming_http_range_client::{HttpClient, HttpRange};

impl HttpFgbReader {
    /// Because we use a buffered HTTP reader, anything extra we fetch can be utilized to skip
    /// subsequent fetches.
    ///
    /// Immediately following the header is the optional spatial index, we deliberately fetch
    /// a small part of that to skip subsequent requests
    ///
    /// `prefetch_levels`: How many levels to fetch up front.
    /// NOTE: each additional level is exponentially larger, and depends on the branching factor of
    /// the Rtree.
    const fn estimate_bytes_for_index_levels(prefetch_levels: u32) -> usize {
        // The actual branching factor will be in the header, but since we don't have the header
        // yet we guess. The consequence of getting this wrong isn't catastrophic, it just means
        // we may be wastefully fetching more than we need or if we undershoot it, that'll we'll
        // need make an extra request later.
        let assumed_branching_factor = PackedRTree::DEFAULT_NODE_SIZE as usize;

        let mut size = 0;

        let mut i = 0;
        while i <= prefetch_levels {
            size += assumed_branching_factor.pow(i) * std::mem::size_of::<NodeItem>();
            i += 1
        }
        size
    }

    pub async fn open(url: &str) -> Result<Self> {
        trace!("starting: opening http reader, reading header");
        let mut client = HttpClient::new(url);

        let prefetch_index_bytes = Self::estimate_bytes_for_index_levels(3);
        // In reality, the header is probably less than half this size, but better to overshoot and
        // fetch an extra kb rather than have to issue a second request.
        let assumed_header_size = 2024;
        let req_size = assumed_header_size + prefetch_index_bytes as u64;
        debug!("fetching header. req_size: {req_size} (assumed_header_size: {assumed_header_size}, prefetched_index_bytes: {prefetch_index_bytes})");

        client.set_range(0..req_size).await?;

        let mut magic_bytes = [0u8; MAGIC_BYTES.len()];
        client.read_exact(&mut magic_bytes).await?;
        if !check_magic_bytes(&magic_bytes) {
            return Err(Error::MissingMagicBytes);
        }

        let header_len = client.read_u32_le().await? as usize;
        // dbg!(header_len);

        if header_len > HEADER_MAX_BUFFER_SIZE || header_len < 8 {
            // minimum size check avoids panic in FlatBuffers header decoding
            return Err(Error::IllegalHeaderSize(header_len));
        }

        let mut header_buffer = BytesMut::new();
        header_buffer.extend_from_slice(&(header_len as u32).to_le_bytes());
        header_buffer.resize(header_len + 4, 0);

        let header_start = 12;
        let header_end = header_start + header_len as u64;
        client
            .append_contiguous_range(header_start..header_end)
            .await?;
        client.read_exact(&mut header_buffer[4..]).await?;

        let header_buffer = header_buffer.freeze();

        // Safety: we verify flatbuffer once here, and then afterwards use the
        // faster unverified instantiation
        let _header = size_prefixed_root_as_header(&header_buffer)?;
        // dbg!(_header);
        trace!("completed: opening http reader");
        let reader = Self {
            header_buffer: Box::new(header_buffer),
            client,
            // TODO: remove this env var. Just for testing
            combine_request_threshold: std::env::var("FGB_COMBINE_REQ_THRESHOLD")
                .map(|str| str.parse().unwrap())
                .unwrap_or(256 * 1024),
        };
        Ok(reader)
    }

    pub fn header(&self) -> Header {
        // SAFETY: we already verified the buffer in `open`
        unsafe { size_prefixed_root_as_header_unchecked(&self.header_buffer) }
    }

    fn header_len(&self) -> usize {
        MAGIC_BYTES.len() + self.header_buffer.len()
    }

    pub async fn select_all(&mut self) -> Result<FeatureStream> {
        let mut client = self.client.split_off();

        let header = self.header();

        let features_count = header.features_count();
        if features_count == 0 {
            warn!("features_count == 0")
        }
        let index_size = if header.index_node_size() > 0 {
            PackedRTree::index_size(features_count as usize, header.index_node_size())
        } else {
            0
        };

        // fast forward over any index to the feature data.
        let feature_base = self.header_len() as u64 + index_size as u64;
        client
            .seek_to_range(HttpRange::RangeFrom(feature_base..))
            .await?;

        let select_all = SelectAll {
            features_left: features_count,
            client,
        };
        let stream = FeatureSelection::SelectAll(select_all)
            .into_feature_buffer_stream()
            .await?;
        Ok(FeatureStream::new(stream, self.header()))
    }

    pub async fn select_bbox(
        &mut self,
        min_x: f64,
        min_y: f64,
        max_x: f64,
        max_y: f64,
    ) -> Result<FeatureStream> {
        let mut client = self.client.split_off();

        let header = self.header();
        let count = header.features_count() as usize;
        let header_len = self.header_len();

        let list = SelectBbox::http_stream_search(
            &mut client,
            header_len as u64,
            count,
            PackedRTree::DEFAULT_NODE_SIZE,
            min_x,
            min_y,
            max_x,
            max_y,
            self.combine_request_threshold,
        )
        .await?;
        debug_assert!(
            list.windows(2).all(|w| w[0].start() < w[1].start()),
            "Since the tree is traversed breadth first, list should be sorted by construction."
        );

        let feature_batches: Vec<FeatureBatch> =
            FeatureBatch::make_batches(list, client, self.combine_request_threshold).await?;

        trace!("completed: select_bbox");
        let select_bbox = SelectBbox { feature_batches };
        let stream = FeatureSelection::SelectBbox(select_bbox)
            .into_feature_buffer_stream()
            .await?;
        Ok(FeatureStream::new(stream, self.header()))
    }
}

struct SelectAll {
    features_left: u64,
    client: HttpClient,
}

struct SelectBbox {
    feature_batches: Vec<FeatureBatch>,
}

pub struct FeatureStream<'a> {
    inner: Box<dyn Stream<Item = Result<OwnedFeature<'a>>> + Unpin + 'a>,
    header: Header<'a>,
}

impl<'a> FeatureStream<'a> {
    fn new(stream: impl Stream<Item = Result<Bytes>> + Unpin + 'a, header: Header<'a>) -> Self {
        let inner = stream.map(move |feature_buffer| OwnedFeature::new(feature_buffer?, header));
        Self {
            inner: Box::new(inner),
            header,
        }
    }
}

impl<'a> Stream for FeatureStream<'a> {
    type Item = Result<OwnedFeature<'a>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}

enum FeatureSelection {
    SelectAll(SelectAll),
    SelectBbox(SelectBbox),
}

struct FeatureBatch {
    feature_ranges: std::vec::IntoIter<HttpRange>,
    client: HttpClient,
}

impl FeatureBatch {
    async fn make_batches(
        feature_ranges: Vec<HttpRange>,
        mut client: HttpClient,
        combine_request_threshold: usize,
    ) -> Result<Vec<Self>> {
        let mut batched_ranges = vec![];

        for range in feature_ranges.into_iter() {
            let Some(prev_batch) = batched_ranges.last_mut() else {
                batched_ranges.push(vec![range]);
                continue;
            };

            let prev_range = prev_batch.last().expect("we never push an empty batch");
            let HttpRange::Range(Range { end: prev_end, .. }) = prev_range else {
                debug_assert!(false, "This shouldn't happen. Only the very last feature is expected to have an unknown length");
                batched_ranges.push(vec![range]);
                continue;
            };

            let wasted_bytes = range.start() - prev_end;
            if wasted_bytes < combine_request_threshold as u64 {
                if wasted_bytes == 0 {
                    trace!("adjacent feature");
                } else {
                    trace!("wasting {wasted_bytes} to avoid an extra request");
                }
                prev_batch.push(range)
            } else {
                debug!("creating a new request for batch rather than wasting {wasted_bytes} bytes");
                batched_ranges.push(vec![range]);
            }
        }

        let mut batches = vec![];
        for feature_range_batch in batched_ranges {
            let client = client.split_off();
            let batch = FeatureBatch::new(feature_range_batch, client);
            batches.push(batch);
        }
        batches.reverse();

        let results: Vec<Result<_>> = futures_util::future::join_all(batches).await;
        let batches: Vec<FeatureBatch> = results.into_iter().collect::<Result<_>>()?;
        Ok(batches)
    }

    pub(crate) async fn new(
        feature_ranges: Vec<HttpRange>,
        mut client: HttpClient,
    ) -> Result<Self> {
        let (Some(first), Some(last)) = (feature_ranges.first(), feature_ranges.last()) else {
            unreachable!("We never create empty batches");
        };
        let covering_range = first.clone().with_end(last.end());
        client.seek_to_range(covering_range).await?;

        let batch = Self {
            feature_ranges: feature_ranges.into_iter(),
            client,
        };

        Ok(batch)
    }

    pub async fn next_buffer(&mut self) -> Result<Option<Bytes>> {
        let Some(feature_range) = self.feature_ranges.next() else {
            return Ok(None);
        };

        self.client.fast_forward(feature_range.start()).await?;

        let len = self.client.read_u32_le().await? as usize;

        let mut feature_buffer = BytesMut::zeroed(len + 4);
        feature_buffer[0..4].copy_from_slice((len as u32).to_le_bytes().as_slice());
        self.client.read_exact(&mut feature_buffer[4..]).await?;

        Ok(Some(feature_buffer.freeze()))
    }
}

impl SelectBbox {
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn http_stream_search(
        client: &mut HttpClient,
        index_begin: u64,
        num_items: usize,
        node_size: u16,
        min_x: f64,
        min_y: f64,
        max_x: f64,
        max_y: f64,
        combine_request_threshold: usize,
    ) -> Result<Vec<HttpRange>> {
        let bounds = NodeItem::bounds(min_x, min_y, max_x, max_y);
        if num_items == 0 {
            return Ok(vec![]);
        }
        let level_bounds = PackedRTree::generate_level_bounds(num_items, node_size);
        let leaf_nodes_offset = level_bounds
            .first()
            .expect("non-empty index must have at least one level")
            .0;
        debug!("http_stream_search - index_begin: {index_begin}, num_items: {num_items}, node_size: {node_size}, level_bounds: {level_bounds:?}, GPS bounds:[({min_x}, {min_y}), ({max_x},{max_y})]");

        let feature_begin = index_begin + PackedRTree::index_size(num_items, node_size) as u64;

        #[derive(Debug, PartialEq, Eq)]
        struct NodeRange {
            level: usize,
            nodes: std::ops::Range<usize>,
        }

        let mut queue = VecDeque::new();
        queue.push_back(NodeRange {
            nodes: 0..1,
            level: level_bounds.len() - 1,
        });
        let mut results = Vec::new();

        while let Some(next) = queue.pop_front() {
            debug!(
                "popped node: {next:?},  remaining queue len: {}",
                queue.len()
            );
            let start_node = next.nodes.start;
            let is_leaf_node = start_node >= leaf_nodes_offset;
            // find the end index of the nodes
            let mut end_node = min(
                next.nodes.end + node_size as usize,
                level_bounds[next.level].1,
            );
            if is_leaf_node && end_node < num_items {
                // We can infer the length of *this* feature by getting the start of the *next*
                // feature, so we get an extra node.
                // This approach doesn't work for the final node in the index,
                // but in that case we know that the feature runs to the end of the FGB file and
                // can make an open ended range request to get "the rest of the data".
                end_node += 1;
            }
            let node_items =
                read_http_node_items(client, index_begin, start_node..end_node).await?;

            // search through child nodes
            for node_id in start_node..end_node {
                let node_pos = node_id - start_node;
                let node_item = &node_items[node_pos];
                if !bounds.intersects(node_item) {
                    continue;
                }
                if is_leaf_node {
                    let start = feature_begin + node_item.offset;
                    if let Some(next_node_item) = &node_items.get(node_pos + 1) {
                        let end = feature_begin + next_node_item.offset;
                        results.push(HttpRange::Range(start..end));
                    } else {
                        debug_assert_eq!(node_pos, num_items);
                        debug!("No next length");
                        results.push(HttpRange::RangeFrom(start..));
                    }
                    continue;
                }

                // Add node to search recursion
                match (queue.back_mut(), node_item.offset as usize) {
                    // There is an existing node for this level, and it's close to this node.
                    // Merge the ranges to avoid an extra request
                    (Some(tail), offset)
                        if tail.level == next.level - 1
                            && offset < tail.nodes.end + combine_request_threshold =>
                    {
                        debug_assert!(tail.nodes.end < offset);
                        tail.nodes.end = offset;
                    }

                    (tail, offset) => {
                        let node_range = NodeRange {
                            nodes: offset..(offset + 1),
                            level: next.level - 1,
                        };

                        if tail
                            .as_ref()
                            .map(|head| head.level == next.level - 1)
                            .unwrap_or(false)
                        {
                            debug!("requesting new NodeRange for offset: {offset} rather than merging with distant NodeRange: {tail:?}");
                        } else {
                            debug!(
                                "pushing new level for NodeRange: {node_range:?} onto Queue with tail: {:?}",
                                queue.back()
                            );
                        }

                        queue.push_back(node_range);
                    }
                }
            }
        }
        Ok(results)
    }

    async fn next_buffer(&mut self) -> Result<Option<Bytes>> {
        let mut next_buffer = None;
        while next_buffer.is_none() {
            let Some(feature_batch) = self.feature_batches.last_mut() else {
                debug!("no batches remain");
                break;
            };
            let Some(buffer) = feature_batch.next_buffer().await? else {
                self.feature_batches
                    .pop()
                    .expect("already asserted feature_batches was non-empty");
                continue;
            };
            next_buffer = Some(buffer)
        }

        Ok(next_buffer)
    }
}

impl SelectAll {
    async fn next_buffer(&mut self) -> Result<Option<Bytes>> {
        if self.features_left == 0 {
            debug_assert!(self.client.read_u8().await.is_err(), "should be empty");
            return Ok(None);
        }
        self.features_left -= 1;

        let len = self.client.read_u32_le().await? as usize;
        let mut feature_buffer = BytesMut::zeroed(len + 4);
        feature_buffer[0..4].copy_from_slice((len as u32).to_le_bytes().as_slice());
        self.client.read_exact(&mut feature_buffer[4..]).await?;

        Ok(Some(feature_buffer.freeze()))
    }
}

mod owned_feature {
    use super::Result;
    use crate::{
        size_prefixed_root_as_feature, size_prefixed_root_as_feature_unchecked, Feature, Geometry,
        Header,
    };
    use bytes::Bytes;
    use geozero::error::GeozeroError;
    use geozero::{GeomProcessor, GeozeroGeometry, PropertyProcessor};

    pub struct OwnedFeature<'a> {
        feature_buffer: Bytes,
        header: Header<'a>,
    }

    impl<'a> OwnedFeature<'a> {
        pub fn new(feature_buffer: Bytes, header: Header<'a>) -> Result<Self> {
            _ = size_prefixed_root_as_feature(&feature_buffer)?;
            Ok(Self {
                feature_buffer,
                header,
            })
        }

        pub fn feature(&self) -> Feature {
            // SAFETY: we've already done the "checked" version once
            // in the initializer
            unsafe { size_prefixed_root_as_feature_unchecked(&self.feature_buffer) }
        }

        pub fn header(&self) -> Header {
            self.header
        }

        pub fn geometry(&self) -> Option<Geometry> {
            self.feature().geometry()
        }
    }

    impl GeozeroGeometry for OwnedFeature<'_> {
        fn process_geom<P: GeomProcessor>(&self, processor: &mut P) -> geozero::error::Result<()>
        where
            Self: Sized,
        {
            let geometry = self.geometry().ok_or(GeozeroError::GeometryFormat)?;
            let geometry_type = self.header.geometry_type();
            geometry.process(processor, geometry_type)
        }
    }

    impl geozero::FeatureAccess for OwnedFeature<'_> {}

    impl geozero::FeatureProperties for OwnedFeature<'_> {
        fn process_properties<P: PropertyProcessor>(
            &self,
            reader: &mut P,
        ) -> geozero::error::Result<bool> {
            crate::process_properties(self.feature(), self.header(), reader)
        }
    }
}

impl FeatureSelection {
    pub async fn into_feature_buffer_stream(mut self) -> Result<impl Stream<Item = Result<Bytes>>> {
        let stream = async_stream::try_stream! {
            loop {
                match self.next_feature_buffer().await? {
                    None => break,
                    Some(feature) => {
                        trace!("yielding feature");
                        yield feature
                    }
                }
            }
        };
        Ok(Box::pin(stream))
    }

    async fn next_feature_buffer(&mut self) -> Result<Option<Bytes>> {
        trace!("");
        match self {
            FeatureSelection::SelectAll(select_all) => select_all.next_buffer().await,
            FeatureSelection::SelectBbox(select_bbox) => select_bbox.next_buffer().await,
        }
    }
}

mod geozero_integration {
    use crate::streaming_http_reader::FeatureStream;
    use futures_util::StreamExt;
    use geozero::FeatureProcessor;

    impl FeatureStream<'_> {
        /// Read and process all selected features
        pub async fn process_features<W: FeatureProcessor>(
            &mut self,
            out: &mut W,
        ) -> geozero::error::Result<()> {
            use geozero::FeatureAccess;

            out.dataset_begin(self.header.name())?;
            let mut cnt = 0;
            while let Some(owned_feature) = self.next().await {
                let feature = owned_feature.expect("todo");
                feature.process(out, cnt)?;
                cnt += 1;
            }
            out.dataset_end()
        }
    }
}

use crate::streaming_http_reader::node_items::read_http_node_items;
use crate::streaming_http_reader::owned_feature::OwnedFeature;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::streaming_http_reader::HttpFgbReader;
    use futures_util::StreamExt;
    use geozero::geojson::GeoJsonWriter;
    use geozero::FeatureProperties;
    use std::io::{BufWriter, Read, Seek, SeekFrom};

    fn get_string(feature: &OwnedFeature, field_name: &str) -> String {
        let props = feature.properties().unwrap();
        props.get(field_name).unwrap().clone()
    }

    #[test]
    fn prefetch_size() {
        ensure_logging();
        assert_eq!(HttpFgbReader::estimate_bytes_for_index_levels(3), 174760)
    }

    #[tokio::test]
    async fn fetch_header() {
        ensure_logging();
        let url = "https://github.com/flatgeobuf/flatgeobuf/raw/master/test/data/countries.fgb";
        let reader = HttpFgbReader::open(url).await.unwrap();

        assert_eq!("countries", reader.header().name().unwrap());
    }

    #[tokio::test]
    async fn select_countries_by_bbox() {
        ensure_logging();
        let url = "https://github.com/flatgeobuf/flatgeobuf/raw/master/test/data/countries.fgb";
        let mut reader = HttpFgbReader::open(url).await.unwrap();
        let stream = reader.select_bbox(-86.0, 10.0, -85.0, 40.0).await.unwrap();

        let names = stream
            .map(|feature| get_string(&feature.unwrap(), "name"))
            .collect::<Vec<_>>()
            .await;

        assert_eq!(
            names,
            vec![
                "Costa Rica",
                "Nicaragua",
                "Honduras",
                "United States of America"
            ]
        );
    }

    #[tokio::test]
    async fn select_us_counties_by_bbox() {
        ensure_logging();
        let url = "https://github.com/flatgeobuf/flatgeobuf/raw/master/test/data/UScounties.fgb";
        let mut reader = HttpFgbReader::open(url).await.unwrap();
        let stream = reader.select_bbox(-86.0, 10.0, -85.0, 40.0).await.unwrap();

        let names = stream
            .map(|feature| get_string(&feature.unwrap(), "NAME"))
            .collect::<Vec<_>>()
            .await;

        assert_eq!(names.len(), 140);
        assert_eq!(names[0..4], vec!["Union", "Fayette", "Rush", "Marion"]);
    }

    // panics upon error
    #[tokio::test]
    async fn select_all() {
        ensure_logging();

        let url = "https://github.com/flatgeobuf/flatgeobuf/raw/master/test/data/countries.fgb";
        // let url = "https://data.maps.earth/openaddresses/planet-addresses-2023-08-16.fgb",

        let mut reader = HttpFgbReader::open(url).await.unwrap();
        let mut stream = reader.select_all().await.unwrap();

        let Some(Ok(first_feature)) = stream.next().await else {
            panic!("failed to get feature")
        };
        let name = get_string(&first_feature, "name");
        assert_eq!(name, "Antarctica");

        let Some(Ok(second_feature)) = stream.next().await else {
            panic!("failed to get feature")
        };
        let name = get_string(&second_feature, "name");
        assert_eq!(name, "French Southern and Antarctic Lands");

        let remainder: Vec<_> = stream.collect().await;
        let remainder: Vec<OwnedFeature> =
            remainder.into_iter().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(remainder.len(), 177);

        let stream = reader.select_all().await.unwrap();
        let remainder: Vec<_> = stream.collect().await;
        let remainder: Vec<OwnedFeature> =
            remainder.into_iter().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(remainder.len(), 179);
    }

    #[tokio::test]
    async fn to_geojson() {
        let url = "https://github.com/flatgeobuf/flatgeobuf/raw/master/test/data/UScounties.fgb";
        let mut reader = HttpFgbReader::open(url).await.unwrap();
        let mut stream = reader.select_bbox(-86.0, 10.0, -85.0, 40.0).await.unwrap();

        let mut output = tempfile::NamedTempFile::new().unwrap();
        {
            let mut json_writer = GeoJsonWriter::new(BufWriter::new(&mut output));
            stream.process_features(&mut json_writer).await.unwrap();
        }
        output.seek(SeekFrom::Start(0)).unwrap();

        let expected_byte_len = 871246;
        assert_eq!(
            output.as_file().metadata().unwrap().len(),
            expected_byte_len
        );
        output.seek(SeekFrom::Start(88)).unwrap();

        let mut actual_bytes = vec![0; 103];
        output.read_exact(&mut actual_bytes).unwrap();

        let actual = String::from_utf8(actual_bytes).unwrap();

        let expected = r#""properties": {"STATE_FIPS": "18", "COUNTY_FIP": "161", "FIPS": "18161", "STATE": "IN", "NAME": "Union""#;
        assert_eq!(actual, expected);
    }

    #[cfg(test)]
    fn ensure_logging() {
        static ONCE: std::sync::Once = std::sync::Once::new();
        ONCE.call_once(|| env_logger::builder().format_timestamp_millis().init());
    }
}

mod node_items {
    use super::Result;
    use crate::packed_r_tree::NodeItem;
    use std::mem::size_of;
    use std::ops::Range;
    use streaming_http_range_client::{HttpClient, HttpRange};
    use tokio::io::AsyncReadExt;

    pub async fn read_http_node_items(
        client: &mut HttpClient,
        base: u64,
        nodes: Range<usize>,
    ) -> Result<Vec<NodeItem>> {
        let begin = base + (nodes.start * size_of::<NodeItem>()) as u64;
        let end = base + (nodes.end * size_of::<NodeItem>()) as u64;
        let range = HttpRange::Range(begin..end);
        client.seek_to_range(range).await?;

        let mut node_items = Vec::with_capacity(nodes.len());
        for _i in 0..nodes.len() {
            let item = NodeItem::bounds_and_offset(
                client.read_f64_le().await?,
                client.read_f64_le().await?,
                client.read_f64_le().await?,
                client.read_f64_le().await?,
                client.read_u64_le().await?,
            );
            node_items.push(item)
        }
        Ok(node_items)
    }
}

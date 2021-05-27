use flatbuffers::FlatBufferBuilder;

use std::io::Write;

use crate::FeatureBuilder;

// TODO:
// -[x] write empty fgb
// -[ ] write un-indexed fgb w/ features
//   -[ ] columns
//     -[ ] fixed schema
//     -[ ] schema-less
//   -[ ] geometries
//     -[ ] one geometry type
//     -[ ] mixed geometries
// -[ ] write indexed fgb
// -[ ] write from FeatureIterator, rather than slice
// -[ ] write very large fgb

trait FeatureSource {
    fn build_feature<'a, 'b>(fbb: FeatureBuilder<'a, 'b>) -> FeatureBuilder<'a, 'b>;
}

#[derive(Debug)]
struct Writer<'w, W: Write> {
    inner: &'w mut W,
    bytes_written: usize,
}

// TODO: better errors
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

impl<'w, W: Write> Writer<'w, W> {
    pub fn new(writer: &'w mut W) -> Self {
        Self {
            inner: writer,
            bytes_written: 0,
        }
    }

    pub fn write(&mut self, features: &[impl FeatureSource]) -> Result<()> {
        self.write_magic_bytes()?;
        self.write_header(features.len())?;
        // TODO: reserve index size? self.write_index(features)?;
        self.write_features(features)?;
        Ok(())
    }

    fn write_buf(&mut self, buf: &[u8]) -> Result<()> {
        let count = self.inner.write(buf)?;
        self.bytes_written += count;
        Ok(())
    }

    fn write_magic_bytes(&mut self) -> Result<()> {
        self.write_buf(&crate::MAGIC_BYTES)
    }

    fn write_header(&mut self, features_count: usize /*extent: [f64; 4]*/) -> Result<()> {
        use crate::header_generated::HeaderBuilder;
        let mut fbb = FlatBufferBuilder::new();

        let mut header = HeaderBuilder::new(&mut fbb);
        header.add_features_count(features_count as u64);

        // TODO: header.add_envelope(&extent);

        // TODO: columns
        // TODO: crs

        let offset = header.finish();
        fbb.finish_size_prefixed(offset, None);

        self.write_buf(fbb.finished_data())
    }

    fn write_features(&mut self, features: &[impl FeatureSource]) -> Result<()> {
        let mut fbb = FlatBufferBuilder::new();
        for feature in features {
            todo!()
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FgbReader;

    struct MyFeature;
    impl FeatureSource for MyFeature {
        fn build_feature<'a, 'b>(fbb: FeatureBuilder<'a, 'b>) -> FeatureBuilder<'a, 'b> {
            todo!()
        }
    }

    #[test]
    fn test_write_features() {
        let input: Vec<MyFeature> = vec![];

        let mut output: Vec<u8> = vec![];
        {
            let mut writer = Writer::new(&mut output);
            let result = writer.write(&input);
            assert!(result.is_ok());
        }

        use std::io::Cursor;
        let mut cursor = Cursor::new(&*output);
        let reader = FgbReader::open(&mut cursor).unwrap();
        assert_eq!(0, reader.features_count());

        let header = reader.header();

        assert_eq!(header.name(), None);
        assert_eq!(header.envelope().map(|e| e.safe_slice()), None);
        assert_eq!(
            header.geometry_type(),
            crate::header_generated::GeometryType::Unknown
        );
        assert_eq!(header.hasZ(), false);
        assert_eq!(header.hasM(), false);
        assert_eq!(header.hasT(), false);
        assert_eq!(header.hasTM(), false);
        assert!(header.columns().is_none());
        assert_eq!(header.features_count(), 0);
        assert_eq!(header.index_node_size(), 16);
        assert_eq!(header.crs(), None);
        assert_eq!(header.title(), None);
        assert_eq!(header.description(), None);
        assert_eq!(header.metadata(), None);
    }

    #[test]
    fn test_write_empty() {
        let input: Vec<MyFeature> = vec![];

        let mut output: Vec<u8> = vec![];
        {
            let mut writer = Writer::new(&mut output);
            let result = writer.write(&input);
            assert!(result.is_ok());
        }

        use std::io::Cursor;
        let mut cursor = Cursor::new(&*output);
        let reader = FgbReader::open(&mut cursor).unwrap();
        assert_eq!(0, reader.features_count());

        let header = reader.header();

        assert_eq!(header.name(), None);
        assert_eq!(header.envelope().map(|e| e.safe_slice()), None);
        assert_eq!(
            header.geometry_type(),
            crate::header_generated::GeometryType::Unknown
        );
        assert_eq!(header.hasZ(), false);
        assert_eq!(header.hasM(), false);
        assert_eq!(header.hasT(), false);
        assert_eq!(header.hasTM(), false);
        assert!(header.columns().is_none());
        assert_eq!(header.features_count(), 0);
        assert_eq!(header.index_node_size(), 16);
        assert_eq!(header.crs(), None);
        assert_eq!(header.title(), None);
        assert_eq!(header.description(), None);
        assert_eq!(header.metadata(), None);
    }
}

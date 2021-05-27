use flatbuffers::FlatBufferBuilder;

use std::io::Write;

use crate::{FeatureBuilder, GeometryBuilder};

// TODO:
// -[x] write empty fgb
// -[ ] write fgb w/ single point
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
    fn build_geometry<'a, 'b>(&self, geometry_builder: &mut GeometryBuilder<'a, 'b>);
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
        for feature_source in features {
            let mut fbb = FlatBufferBuilder::new();
            let mut geometry_builder = GeometryBuilder::new(&mut fbb);
            feature_source.build_geometry(&mut geometry_builder);
            let geometry = geometry_builder.finish();

            let mut feature_builder = FeatureBuilder::new(&mut fbb);
            feature_builder.add_geometry(geometry);
            let feature = feature_builder.finish();
            fbb.finish_size_prefixed(feature, None);
            self.write_buf(fbb.finished_data())?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FgbReader, GeometryBuilder, GeometryType};

    struct MyPoint {
        xy: [f64; 2],
    }

    impl FeatureSource for MyPoint {
        fn build_geometry<'a, 'b>(&self, builder: &mut GeometryBuilder<'a, 'b>) {
            builder.add_type_(GeometryType::Point);
            // FIXME: how to add vector? Do I really need to manually coerce the [u8]?
            // builder.add_xy(flatbuffers::Vector::new(&self.xy, 0));
        }
    }

    #[test]
    fn test_write_features() {
        let input: Vec<MyPoint> = vec![MyPoint { xy: [1.0, 2.0] }, MyPoint { xy: [3.0, 4.0] }];

        let mut output: Vec<u8> = vec![];
        {
            let mut writer = Writer::new(&mut output);
            let result = writer.write(&input);
            assert!(result.is_ok());
        }

        use std::io::Cursor;
        let mut cursor = Cursor::new(&*output);
        let mut reader = FgbReader::open(&mut cursor).unwrap();

        let header = reader.header();
        assert_eq!(None, header.name());
        assert_eq!(None, header.envelope().map(|e| e.safe_slice()));
        assert_eq!(
            crate::header_generated::GeometryType::Unknown,
            header.geometry_type()
        );
        assert_eq!(false, header.hasZ());
        assert_eq!(false, header.hasM());
        assert_eq!(false, header.hasT());
        assert_eq!(false, header.hasTM());
        assert!(header.columns().is_none());
        assert_eq!(2, header.features_count());
        assert_eq!(16, header.index_node_size());
        assert_eq!(None, header.crs());
        assert_eq!(None, header.title());
        assert_eq!(None, header.description());
        assert_eq!(None, header.metadata());

        assert_eq!(0, reader.features_count());
        let count = reader.select_all().unwrap();
        assert_eq!(2, count);
        assert_eq!(2, reader.features_count());
    }

    #[test]
    fn test_write_empty() {
        let input: Vec<MyPoint> = vec![];

        let mut output: Vec<u8> = vec![];
        {
            let mut writer = Writer::new(&mut output);
            let result = writer.write(&input);
            assert!(result.is_ok());
        }

        use std::io::Cursor;
        let mut cursor = Cursor::new(&*output);
        let mut reader = FgbReader::open(&mut cursor).unwrap();

        let header = reader.header();

        assert_eq!(None, header.name());
        assert_eq!(None, header.envelope().map(|e| e.safe_slice()));
        assert_eq!(
            crate::header_generated::GeometryType::Unknown,
            header.geometry_type()
        );
        assert_eq!(false, header.hasZ());
        assert_eq!(false, header.hasM());
        assert_eq!(false, header.hasT());
        assert_eq!(false, header.hasTM());
        assert!(header.columns().is_none());
        assert_eq!(0, header.features_count());
        assert_eq!(16, header.index_node_size());
        assert_eq!(None, header.crs());
        assert_eq!(None, header.title());
        assert_eq!(None, header.description());
        assert_eq!(None, header.metadata());

        assert_eq!(0, reader.features_count());
        let count = reader.select_all().unwrap();
        assert_eq!(0, count);
        assert_eq!(0, reader.features_count());
    }
}

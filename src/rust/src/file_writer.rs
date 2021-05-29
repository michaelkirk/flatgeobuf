use flatbuffers::{FlatBufferBuilder, ForwardsUOffset, Vector, WIPOffset};

use std::io::Write;

use crate::{Column, FeatureBuilder, Geometry};

// TODO:
// -[x] write empty fgb
// -[ ] write un-indexed fgb w/ features
//   -[ ] columns
//     -[ ] fixed schema
//     -[ ] schema-less
//   -[ ] geometries
//     -[x] one geometry type
//     -[ ] mixed geometries
// -[ ] write indexed fgb
// -[ ] write from FeatureIterator, rather than slice
// -[ ] write very large fgb

pub trait FeatureSource {
    fn build_geometry<'a>(
        &'a self,
        flatbuffer_builder: &mut FlatBufferBuilder<'a>,
    ) -> WIPOffset<Geometry<'a>>;

    /// Use when this feature has its own properties schema, rather than using the shared uniform
    /// schema from the header's columns
    fn build_feature_schema_properties<'a>(
        &'a self,
        flatbuffer_builder: &mut FlatBufferBuilder<'a>,
    ) -> (
        Option<WIPOffset<Vector<'a, ForwardsUOffset<Column<'a>>>>>,
        Option<WIPOffset<Vector<'a, u8>>>,
    );
}

#[derive(Debug)]
pub struct Writer<W: Write> {
    include_index: bool,
    inner: W,
    bytes_written: usize,
}

// TODO: better errors
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

impl<W: Write> Writer<W> {
    pub fn new(writer: W) -> Self {
        Self {
            include_index: false,
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
        if self.include_index {
            // TODO
        } else {
            header.add_index_node_size(0);
        }

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
            let geometry = feature_source.build_geometry(&mut fbb);
            let (columns, properties) = feature_source.build_feature_schema_properties(&mut fbb);

            let mut feature_builder = FeatureBuilder::new(&mut fbb);
            feature_builder.add_geometry(geometry);

            if let Some(columns) = columns {
                feature_builder.add_columns(columns);
            }

            if let Some(properties) = properties {
                feature_builder.add_properties(properties);
            }

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
    use crate::{FgbReader, Geometry, GeometryBuilder, GeometryType};
    use byteorder::{ByteOrder, LittleEndian};
    use flatbuffers::{ForwardsUOffset, Vector, WIPOffset};

    struct MyCoord {
        x: f64,
        y: f64,
    }

    struct MyPoint {
        coord: MyCoord,
        my_prop: bool,
    }

    impl MyPoint {
        pub fn as_vec(&self) -> Vec<f64> {
            vec![self.coord.x, self.coord.y]
        }
    }

    struct MyLineString(Vec<MyCoord>);

    impl MyLineString {
        pub fn as_vec(&self) -> Vec<f64> {
            let mut output: Vec<f64> = Vec::with_capacity(self.0.len() * 2);
            for coord in &self.0 {
                output.push(coord.x);
                output.push(coord.y);
            }
            output
        }
    }

    impl FeatureSource for MyPoint {
        fn build_geometry<'a>(
            &'a self,
            flatbuffer_builder: &mut FlatBufferBuilder<'a>,
        ) -> WIPOffset<Geometry<'a>> {
            let coord = flatbuffer_builder.create_vector(&self.as_vec());
            let mut geometry_builder = GeometryBuilder::new(flatbuffer_builder);
            geometry_builder.add_type_(GeometryType::Point);
            geometry_builder.add_xy(coord);
            geometry_builder.finish()
        }

        fn build_feature_schema_properties<'a>(
            &'a self,
            flatbuffer_builder: &mut FlatBufferBuilder<'a>,
        ) -> (
            Option<WIPOffset<Vector<'a, ForwardsUOffset<Column<'a>>>>>,
            Option<WIPOffset<Vector<'a, u8>>>,
        ) {
            use crate::{ColumnArgs, ColumnType};

            let column_name = flatbuffer_builder.create_string("my_prop");
            let column = Column::create(
                flatbuffer_builder,
                &ColumnArgs {
                    name: Some(column_name),
                    type_: ColumnType::Bool,
                    // MJK: what's the diff between title and name?
                    title: None,
                    description: None,
                    width: 0,
                    precision: 0,
                    scale: 0,
                    nullable: false,
                    unique: false,
                    primary_key: false,
                    metadata: None,
                },
            );

            let columns = flatbuffer_builder.create_vector(&[column]);

            // ColumnType::Bool => {
            //     finish = reader.property(
            //         i,
            //         &column.name(),
            //         &ColumnValue::Bool(properties[offset] != 0),
            //     )?;
            //     offset += size_of::<u8>();
            // }
            // TODO: extract a per-type property serializer
            let mut properties_bytes: Vec<u8> = vec![];

            // for each column:
            // write column idx as u16 (little endian)
            let mut column_index = [0u8; 2];
            LittleEndian::write_u16(&mut column_index, 0);
            properties_bytes.extend_from_slice(&column_index);

            // write column data
            properties_bytes.push(if self.my_prop { 1 } else { 0 });

            // FIXME: for some reason property reading expects an extra garbage byte on the end?
            properties_bytes.push(0);

            let properties_offset = flatbuffer_builder.create_vector(&properties_bytes);

            (Some(columns), Some(properties_offset))
        }
    }

    impl FeatureSource for MyLineString {
        fn build_geometry<'a>(
            &'a self,
            flatbuffer_builder: &mut FlatBufferBuilder<'a>,
        ) -> WIPOffset<Geometry<'a>> {
            let coords = flatbuffer_builder.create_vector(&self.as_vec());
            let mut geometry_builder = GeometryBuilder::new(flatbuffer_builder);
            geometry_builder.add_type_(GeometryType::LineString);
            geometry_builder.add_xy(coords);
            geometry_builder.finish()
        }

        fn build_feature_schema_properties<'a>(
            &'a self,
            _flatbuffer_builder: &mut FlatBufferBuilder<'a>,
        ) -> (
            Option<WIPOffset<Vector<'a, ForwardsUOffset<Column<'a>>>>>,
            Option<WIPOffset<Vector<'a, u8>>>,
        ) {
            // Nothing yet...
            (None, None)
        }
    }

    // "One-of" enum which just delegates to its inner type
    enum MyGeometry {
        Point(MyPoint),
        LineString(MyLineString),
    }

    impl FeatureSource for MyGeometry {
        fn build_geometry<'a>(
            &'a self,
            flatbuffer_builder: &mut FlatBufferBuilder<'a>,
        ) -> WIPOffset<Geometry<'a>> {
            match self {
                MyGeometry::Point(g) => g.build_geometry(flatbuffer_builder),
                MyGeometry::LineString(g) => g.build_geometry(flatbuffer_builder),
            }
        }

        fn build_feature_schema_properties<'a>(
            &'a self,
            flatbuffer_builder: &mut FlatBufferBuilder<'a>,
        ) -> (
            Option<WIPOffset<Vector<'a, ForwardsUOffset<Column<'a>>>>>,
            Option<WIPOffset<Vector<'a, u8>>>,
        ) {
            match self {
                MyGeometry::Point(g) => g.build_feature_schema_properties(flatbuffer_builder),
                MyGeometry::LineString(g) => g.build_feature_schema_properties(flatbuffer_builder),
            }
        }
    }

    // Test cases:
    // empty file, no props
    // empty file, properties defined
    // file with 1pt, no properties
    // empty file, no index
    // file with 1pt, w/ per feature properties
    // file with 1pt, w/ per file properties
    // file with 1pt, w/ per feature and per file properties
    // file with per file properties, but mix of per feature props

    #[test]
    fn test_write_features() {
        pretty_env_logger::init();

        let input: Vec<MyGeometry> = vec![
            MyGeometry::Point(MyPoint {
                coord: MyCoord { x: 1.0, y: 2.0 },
                my_prop: true,
            }),
            MyGeometry::LineString(MyLineString(vec![
                MyCoord { x: 5.0, y: 6.0 },
                MyCoord { x: 7.0, y: 8.0 },
                MyCoord { x: 9.0, y: 10.0 },
            ])),
            MyGeometry::Point(MyPoint {
                coord: MyCoord { x: 3.0, y: 4.0 },
                my_prop: false,
            }),
        ];

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
        assert_eq!(3, header.features_count());
        assert_eq!(0, header.index_node_size());
        assert_eq!(None, header.crs());
        assert_eq!(None, header.title());
        assert_eq!(None, header.description());
        assert_eq!(None, header.metadata());

        assert_eq!(0, reader.features_count());
        let count = reader.select_all().unwrap();
        assert_eq!(3, count);
        assert_eq!(3, reader.features_count());

        use fallible_streaming_iterator::FallibleStreamingIterator;
        use geozero::FeatureProperties;

        let mut types = vec![];
        let mut coords = vec![];
        let mut props = vec![];
        while let Some(next) = reader.next().unwrap() {
            let geometry = next.geometry().unwrap();
            types.push(geometry.type_());
            coords.push(geometry.xy().unwrap().safe_slice().to_vec());
            props.push(next.properties().ok().and_then(|props| {
                match props.get("my_prop").map(String::as_str) {
                    Some("true") => Some(true),
                    Some("false") => Some(false),
                    Some(other) => panic!("unexpected: {}", other),
                    None => None,
                }
            }));
        }

        assert_eq!(
            vec![
                GeometryType::Point,
                GeometryType::LineString,
                GeometryType::Point
            ],
            types
        );

        assert_eq!(
            vec![
                vec![1.0, 2.0],
                vec![5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
                vec![3.0, 4.0]
            ],
            coords
        );

        assert_eq!(vec![Some(true), None, Some(false)], props)
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
        assert_eq!(0, header.index_node_size());
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

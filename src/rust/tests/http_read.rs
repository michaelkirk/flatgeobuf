#[cfg(feature = "http")]
mod http {
    use flatgeobuf::*;
    use geozero::geojson::GeoJsonWriter;
    use std::io::{BufWriter, Read, Seek, SeekFrom};

    type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

    #[tokio::test]
    async fn http_read() -> Result<()> {
        let url = "https://github.com/flatgeobuf/flatgeobuf/raw/master/test/data/countries.fgb";
        let fgb = HttpFgbReader::open(url).await?;
        assert_eq!(fgb.header().geometry_type(), GeometryType::MultiPolygon);
        assert_eq!(fgb.header().features_count(), 179);
        let mut fgb = fgb.select_all().await?;
        let feature = fgb.next().await?.unwrap();
        let props = feature.properties()?;
        assert_eq!(props["name"], "Antarctica".to_string());
        Ok(())
    }

    #[tokio::test]
    async fn medium_select_bbox() {
        let url = "http://localhost:8001/UScounties.fgb";
        let reader = HttpFgbReader::open(url).await.unwrap();
        let mut stream = reader.select_bbox(-86.0, 10.0, -85.0, 40.0).await.unwrap();

        let mut count = 0;
        while let Some(feature) = stream.next().await.transpose() {
            let _feature = feature.unwrap();
            count += 1
        }
        assert_eq!(count, 140);
    }

    #[tokio::test]
    async fn http_bbox_read() -> Result<()> {
        let url = "https://github.com/flatgeobuf/flatgeobuf/raw/master/test/data/countries.fgb";
        let fgb = HttpFgbReader::open(url).await?;
        assert_eq!(fgb.header().geometry_type(), GeometryType::MultiPolygon);
        assert_eq!(fgb.header().features_count(), 179);
        let mut fgb = fgb.select_bbox(8.8, 47.2, 9.5, 55.3).await?;
        let feature = fgb.next().await?.unwrap();
        let props = feature.properties()?;
        assert_eq!(props["name"], "Denmark".to_string());
        Ok(())
    }

    #[tokio::test]
    async fn http_read_unknown_feature_count() -> Result<()> {
        let url = "https://github.com/flatgeobuf/flatgeobuf/raw/master/test/data/unknown_feature_count.fgb";
        let fgb = HttpFgbReader::open(url).await?;
        assert_eq!(fgb.header().features_count(), 0);
        let mut fgb = fgb.select_all().await?;
        assert_eq!(fgb.features_count(), None);
        let feature = fgb.next().await?;
        assert!(feature.is_none()); // TODO: support reading unknown feature count

        let url = "https://github.com/flatgeobuf/flatgeobuf/raw/master/test/data/unknown_feature_count.fgb";
        let fgb = HttpFgbReader::open(url)
            .await?
            .select_bbox(8.8, 47.2, 9.5, 55.3)
            .await;
        assert_eq!(fgb.err().unwrap().to_string(), "Index missing");
        Ok(())
    }

    #[tokio::test]
    async fn http_bbox_big() -> Result<()> {
        let url = "https://pkg.sourcepole.ch/osm-buildings-ch.fgb";
        let fgb = HttpFgbReader::open(url).await?;
        assert_eq!(fgb.header().geometry_type(), GeometryType::MultiPolygon);
        assert_eq!(fgb.header().features_count(), 2396905);
        let mut fgb = fgb
            .select_bbox(8.522086, 47.363333, 8.553521, 47.376020)
            .await?;
        let feature = fgb.next().await?.unwrap();
        let props = feature.properties()?;
        assert_eq!(props["building"], "residential".to_string());
        Ok(())
    }

    #[tokio::test]
    async fn http_err() {
        let url = "https://github.com/flatgeobuf/flatgeobuf/raw/master/test/data/wrong.fgb";
        let fgb = HttpFgbReader::open(url).await;
        assert_eq!(
            fgb.err().unwrap().to_string(),
            "http status 404".to_string()
        );

        let url = "https://wrong.example.com/countries.fgb";
        let fgb = HttpFgbReader::open(url).await;
        let error_text = fgb.err().unwrap().to_string();
        let expected_error_text = "error trying to connect";
        assert!(
            error_text.contains(expected_error_text),
            "expected to find {expected_error_text} in {error_text}"
        );
    }

    #[tokio::test]
    async fn to_geojson() {
        let url = "https://github.com/flatgeobuf/flatgeobuf/raw/master/test/data/UScounties.fgb";
        let reader = HttpFgbReader::open(url).await.unwrap();
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
}

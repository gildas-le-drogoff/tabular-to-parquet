// tests/schema_tests.rs

use arrow::datatypes::DataType;
use std::io::Write;
use tabular_to_parquet::schema::inferer_schema;
use tempfile::NamedTempFile;

fn csv_temp(contenu: &str) -> NamedTempFile {
    let mut f = NamedTempFile::new().unwrap();
    writeln!(f, "{contenu}").unwrap();
    f
}

#[test]
fn test_inferer_schema_simple() {
    let csv = csv_temp(
        "a,b,c\n\
         1,2,3\n\
         4,5,6\n",
    );

    let schema = inferer_schema(csv.path(), b',', true).unwrap();

    assert_eq!(schema.fields().len(), 3);
    assert_eq!(schema.fields()[0].data_type(), &DataType::UInt64);
    assert_eq!(schema.fields()[1].data_type(), &DataType::UInt64);
    assert_eq!(schema.fields()[2].data_type(), &DataType::UInt64);
}

#[test]
fn test_inferer_schema_mixte() {
    let csv = csv_temp(
        "a,b,c\n\
         true,2024-01-01,3.14\n\
         false,2024-01-02,2.71\n",
    );

    let schema = inferer_schema(csv.path(), b',', true).unwrap();

    assert_eq!(schema.fields()[0].data_type(), &DataType::Boolean);
    assert_eq!(schema.fields()[1].data_type(), &DataType::Date32);
    assert_eq!(schema.fields()[2].data_type(), &DataType::Float64);
}

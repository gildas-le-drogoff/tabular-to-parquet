// tests/analyse_tests.rs

use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;
use tabular_to_parquet::analyse::analyser_bloc;

#[test]
fn test_analyser_bloc_simple() {
    let lignes = vec![
        "1,true,2024-01-01".to_string(),
        "2,false,2024-01-02".to_string(),
    ];

    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Boolean, true),
        Field::new("c", DataType::Date32, true),
    ]));

    let batch = analyser_bloc(&lignes, schema.clone(), b',').unwrap();

    assert_eq!(batch.num_rows(), 2);
    assert_eq!(batch.num_columns(), 3);
}

// tests/utils_tests.rs

use tabular_to_parquet::utils::{est_null_texte, parse_bool, parse_date_ymd, parse_timestamp_ms};

#[test]
fn test_est_null_texte() {
    assert!(est_null_texte(""));
    assert!(est_null_texte(" "));
    assert!(est_null_texte("NULL"));
    assert!(est_null_texte("NaN"));
    assert!(!est_null_texte("0"));
    assert!(!est_null_texte("false"));
}

#[test]
fn test_parse_bool() {
    assert_eq!(parse_bool("true"), Some(true));
    assert_eq!(parse_bool("FALSE"), Some(false));
    assert_eq!(parse_bool("1"), Some(true));
    assert_eq!(parse_bool("0"), Some(false));
    assert_eq!(parse_bool("yes"), Some(true));
    assert_eq!(parse_bool("no"), Some(false));
    assert_eq!(parse_bool("maybe"), None);
}

#[test]
fn test_parse_date_ymd() {
    let d1 = parse_date_ymd("1970-01-01").unwrap();
    let d2 = parse_date_ymd("02/01/1970").unwrap();
    assert_eq!(d1, 0);
    assert_eq!(d2, 1);
    assert!(parse_date_ymd("invalid").is_none());
}

#[test]
fn test_parse_timestamp_ms() {
    let t1 = parse_timestamp_ms("1970-01-01 00:00:01").unwrap();
    assert_eq!(t1, 1_000);

    let t2 = parse_timestamp_ms("1000000000").unwrap();
    assert_eq!(t2, 1_000_000_000_000);

    assert!(parse_timestamp_ms("invalid").is_none());
}

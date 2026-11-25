//! Time on the wire: RFC 3339 strings, read from Airflow and the cluster, written as UTC with `Z`.
//! One module, over `chrono` (already a dependency), instead of a hand-written calendar per file.

use std::time::SystemTime;

use chrono::{DateTime, SecondsFormat, Utc};

/// Seconds since the Unix epoch for an RFC 3339 timestamp, fraction included. `None` for
/// anything that is not one (a missing date, an unknown zone, trailing text).
pub(crate) fn parse_rfc3339(text: &str) -> Option<f64> {
    let at = DateTime::parse_from_rfc3339(text).ok()?;
    Some(at.timestamp() as f64 + f64::from(at.timestamp_subsec_nanos()) / 1_000_000_000.0)
}

/// `YYYY-MM-DDTHH:MM:SSZ` for whole seconds since the epoch, always UTC and always with `Z`.
pub(crate) fn format_rfc3339(seconds: i64) -> String {
    DateTime::<Utc>::from_timestamp(seconds, 0)
        .unwrap_or_default()
        .to_rfc3339_opts(SecondsFormat::Secs, true)
}

/// RFC 3339 in UTC, second precision: `2026-10-20T08:15:00Z`.
pub(crate) fn rfc3339_utc(time: SystemTime) -> String {
    DateTime::<Utc>::from(time).to_rfc3339_opts(SecondsFormat::Secs, true)
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, UNIX_EPOCH};

    use super::*;

    #[test]
    fn the_three_shapes_round_trip() {
        assert_eq!(parse_rfc3339("1970-01-01T00:00:00Z"), Some(0.0));
        assert_eq!(
            parse_rfc3339("2026-10-20T07:23:50.608876Z"),
            Some(1_792_481_030.608_876)
        );
        assert_eq!(
            parse_rfc3339("2026-10-20T09:23:50+02:00"),
            parse_rfc3339("2026-10-20T07:23:50Z")
        );
        assert_eq!(parse_rfc3339("2026-10-20T07:23:50Zjunk"), None);
        assert_eq!(parse_rfc3339("not a date"), None);
        assert_eq!(format_rfc3339(0), "1970-01-01T00:00:00Z");
        assert_eq!(format_rfc3339(1_792_481_030), "2026-10-20T07:23:50Z");
        assert_eq!(
            rfc3339_utc(UNIX_EPOCH + Duration::from_millis(1_792_481_030_900)),
            "2026-10-20T07:23:50Z"
        );
    }
}

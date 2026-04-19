//! `CloudWatch` Embedded Metric Format emission.
//!
//! Writing a JSON line in EMF shape to stdout lets `CloudWatch` Logs
//! auto-extract metrics (`DispatchCount`, `DispatchLatency`) under the
//! configured namespace with {Command, Outcome} dimensions. No AWS SDK call
//! needed.
//!
//! Gated on `Settings::emf_namespace` being set — unset in local dev so the
//! terminal stays clean.

use serde_json::{Value, json};

/// Build the EMF JSON value. Separated from the emit function so tests can
/// assert the exact shape without intercepting stdout.
pub fn build_emf(namespace: &str, command: &str, outcome: &str, duration_ms: u64, timestamp_ms: i64) -> Value {
    json!({
        "_aws": {
            "Timestamp": timestamp_ms,
            "CloudWatchMetrics": [{
                "Namespace": namespace,
                "Dimensions": [["Command", "Outcome"]],
                "Metrics": [
                    {"Name": "DispatchCount", "Unit": "Count"},
                    {"Name": "DispatchLatency", "Unit": "Milliseconds"}
                ]
            }]
        },
        "Command": command,
        "Outcome": outcome,
        "DispatchCount": 1,
        "DispatchLatency": duration_ms,
    })
}

pub fn emit_command_metrics(namespace: &str, command: &str, outcome: &str, duration_ms: u64) {
    let v = build_emf(namespace, command, outcome, duration_ms, crate::storage::now_ms());
    println!("{v}");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn emf_has_required_shape() {
        let v = build_emf("rustyant", "GET", "ok", 12, 1_712_345_678_000);
        assert_eq!(v["Command"], "GET");
        assert_eq!(v["Outcome"], "ok");
        assert_eq!(v["DispatchCount"], 1);
        assert_eq!(v["DispatchLatency"], 12);
        let aws = &v["_aws"];
        assert_eq!(aws["Timestamp"], 1_712_345_678_000_i64);
        let metric_block = &aws["CloudWatchMetrics"][0];
        assert_eq!(metric_block["Namespace"], "rustyant");
        assert_eq!(metric_block["Dimensions"][0][0], "Command");
        assert_eq!(metric_block["Dimensions"][0][1], "Outcome");
        let metric_names: Vec<&str> =
            metric_block["Metrics"].as_array().unwrap().iter().map(|m| m["Name"].as_str().unwrap()).collect();
        assert_eq!(metric_names, vec!["DispatchCount", "DispatchLatency"]);
    }

    #[test]
    fn emf_serializes_to_valid_json() {
        let v = build_emf("ns", "SET", "ok", 0, 0);
        let s = v.to_string();
        let reparsed: Value = serde_json::from_str(&s).expect("roundtrip");
        assert_eq!(reparsed, v);
    }
}

//! Prometheus metrics parsing utilities.

use serde_json::{json, Value};

/// Parse Prometheus text format into JSON with counters, gauges, and histograms.
///
/// Handles labeled metrics by flattening them: `metric{label="value"}` becomes `metric_value`.
pub fn parse_prometheus_metrics(text: &str) -> Value {
    let mut counters = serde_json::Map::new();
    let mut gauges = serde_json::Map::new();
    let mut histograms = serde_json::Map::new();

    for line in text.lines() {
        if line.starts_with('#') || line.is_empty() {
            continue;
        }

        // Parse "metric_name value" or "metric_name{labels} value"
        if let Some(metric) = parse_metric_line(line) {
            // Categorize by BASE name patterns (before label flattening)
            // This ensures plumtree_priority_enqueued_total{priority="critical"} -> counter
            // while plumtree_total_peers -> gauge (base name doesn't end with _total)
            if metric.base_name.ends_with("_total") {
                counters.insert(metric.name, json!(metric.value));
            } else if metric.base_name.contains("_bucket")
                || metric.base_name.ends_with("_sum")
                || metric.base_name.ends_with("_count")
            {
                // Histogram components
                histograms.insert(metric.name, json!(metric.value));
            } else {
                gauges.insert(metric.name, json!(metric.value));
            }
        }
    }

    // Calculate priority queue depths (enqueued - dequeued)
    let priority_queue = calculate_priority_queue_depths(&counters);

    json!({
        "counters": counters,
        "gauges": gauges,
        "histograms": histograms,
        "priority_queue": priority_queue
    })
}

/// Calculate current priority queue depths from enqueued/dequeued counters.
fn calculate_priority_queue_depths(counters: &serde_json::Map<String, Value>) -> Value {
    let get_count = |key: &str| -> i64 {
        counters
            .get(key)
            .and_then(|v| v.as_f64())
            .map(|f| f as i64)
            .unwrap_or(0)
    };

    // Queue depth = enqueued - dequeued (clamped to 0)
    let critical = (get_count("plumtree_priority_enqueued_total_critical")
        - get_count("plumtree_priority_dequeued_total_critical"))
    .max(0);
    let high = (get_count("plumtree_priority_enqueued_total_high")
        - get_count("plumtree_priority_dequeued_total_high"))
    .max(0);
    let normal = (get_count("plumtree_priority_enqueued_total_normal")
        - get_count("plumtree_priority_dequeued_total_normal"))
    .max(0);
    let low = (get_count("plumtree_priority_enqueued_total_low")
        - get_count("plumtree_priority_dequeued_total_low"))
    .max(0);

    json!({
        "critical": critical,
        "high": high,
        "normal": normal,
        "low": low
    })
}

/// Parsed metric with base name for classification.
pub struct ParsedMetric {
    /// The final name (possibly flattened with label values)
    pub name: String,
    /// The base name (before label flattening) for classification
    pub base_name: String,
    /// The metric value
    pub value: f64,
}

/// Parse a single Prometheus metric line.
///
/// Handles:
/// - "metric_name 123"
/// - "metric_name{label=\"value\"} 123" -> flattens to "metric_name_value"
/// - "metric_name{label=\"value\",other=\"x\"} 123" -> uses first label value
pub fn parse_metric_line(line: &str) -> Option<ParsedMetric> {
    let line = line.trim();
    if line.is_empty() || line.starts_with('#') {
        return None;
    }

    // Find where the metric name ends (either at '{' or first whitespace)
    let name_end = line
        .find(|c: char| c == '{' || c.is_whitespace())
        .unwrap_or(line.len());

    let base_name = &line[..name_end];

    // Check for labels
    let final_name = if let Some(brace_start) = line.find('{') {
        if let Some(brace_end) = line.find('}') {
            let labels_str = &line[brace_start + 1..brace_end];
            if labels_str.is_empty() {
                // Empty labels: metric{} 123
                base_name.to_string()
            } else {
                // Parse first label value to flatten
                // Format: label="value" or label="value",other="x"
                if let Some(label_value) = extract_first_label_value(labels_str) {
                    format!("{}_{}", base_name, label_value)
                } else {
                    base_name.to_string()
                }
            }
        } else {
            base_name.to_string()
        }
    } else {
        base_name.to_string()
    };

    // Find the value (last whitespace-separated token)
    let value_str = line.split_whitespace().last()?;
    let value: f64 = value_str.parse().ok()?;

    Some(ParsedMetric {
        name: final_name,
        base_name: base_name.to_string(),
        value,
    })
}

/// Extract the first label value from a Prometheus labels string.
/// "priority=\"critical\"" -> "critical"
/// "priority=\"critical\",node=\"1\"" -> "critical"
fn extract_first_label_value(labels: &str) -> Option<String> {
    // Find first ="value" pattern
    let eq_pos = labels.find('=')?;
    let after_eq = &labels[eq_pos + 1..];

    // Value should be quoted
    if !after_eq.starts_with('"') {
        return None;
    }

    // Find closing quote
    let value_start = 1; // Skip opening quote
    let value_end = after_eq[value_start..].find('"')?;
    let value = &after_eq[value_start..value_start + value_end];

    Some(value.to_string())
}

/// Extract a single metric value from Prometheus text.
/// Used by web_chat_quic for inline metric extraction.
#[allow(dead_code)]
pub fn get_metric_value(prometheus_text: &str, name: &str) -> u64 {
    for line in prometheus_text.lines() {
        if line.starts_with('#') || line.is_empty() {
            continue;
        }
        // Look for lines like: metric_name 123 (global, no labels)
        // or metric_name{} 123
        if line.starts_with(name) {
            // Check if this is a global metric (no labels or empty labels)
            let has_labels = line.contains('{') && !line.contains("{}");
            if !has_labels {
                if let Some(value_str) = line.split_whitespace().last() {
                    if let Ok(v) = value_str.parse::<f64>() {
                        return v as u64;
                    }
                }
            }
        }
    }
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_metric_line() {
        let m = parse_metric_line("my_counter 42").unwrap();
        assert_eq!(m.name, "my_counter");
        assert_eq!(m.value, 42.0);

        let m = parse_metric_line("my_gauge 3.14").unwrap();
        assert_eq!(m.name, "my_gauge");
        assert_eq!(m.value, 3.14);

        // Labeled metrics get flattened
        let m = parse_metric_line("my_metric{label=\"value\"} 42").unwrap();
        assert_eq!(m.name, "my_metric_value");
        assert_eq!(m.base_name, "my_metric");
        assert_eq!(m.value, 42.0);

        // Empty labels are OK
        let m = parse_metric_line("my_metric{} 42").unwrap();
        assert_eq!(m.name, "my_metric");
        assert_eq!(m.value, 42.0);

        // Priority metrics - base_name ends with _total so classified as counter
        let m = parse_metric_line("plumtree_priority_enqueued_total{priority=\"critical\"} 19")
            .unwrap();
        assert_eq!(m.name, "plumtree_priority_enqueued_total_critical");
        assert_eq!(m.base_name, "plumtree_priority_enqueued_total");
        assert_eq!(m.value, 19.0);
        assert!(m.base_name.ends_with("_total")); // Will be classified as counter
    }

    #[test]
    fn test_parse_prometheus_metrics() {
        let text = r#"
# HELP my_counter A counter
# TYPE my_counter counter
my_counter_total 100
my_gauge 50
my_histogram_sum 1000
my_histogram_count 10
"#;
        let result = parse_prometheus_metrics(text);
        assert_eq!(result["counters"]["my_counter_total"], 100.0);
        assert_eq!(result["gauges"]["my_gauge"], 50.0);
        assert_eq!(result["histograms"]["my_histogram_sum"], 1000.0);
        assert_eq!(result["histograms"]["my_histogram_count"], 10.0);
    }

    #[test]
    fn test_priority_queue_metrics() {
        let text = r#"
plumtree_priority_enqueued_total{priority="critical"} 10
plumtree_priority_enqueued_total{priority="high"} 50
plumtree_priority_enqueued_total{priority="normal"} 100
plumtree_priority_enqueued_total{priority="low"} 5
plumtree_priority_dequeued_total{priority="critical"} 8
plumtree_priority_dequeued_total{priority="high"} 45
plumtree_priority_dequeued_total{priority="normal"} 100
plumtree_priority_dequeued_total{priority="low"} 5
"#;
        let result = parse_prometheus_metrics(text);

        // Check counters were flattened correctly
        assert_eq!(
            result["counters"]["plumtree_priority_enqueued_total_critical"],
            10.0
        );
        assert_eq!(
            result["counters"]["plumtree_priority_dequeued_total_critical"],
            8.0
        );

        // Check priority_queue calculated correctly (enqueued - dequeued)
        assert_eq!(result["priority_queue"]["critical"], 2); // 10 - 8
        assert_eq!(result["priority_queue"]["high"], 5); // 50 - 45
        assert_eq!(result["priority_queue"]["normal"], 0); // 100 - 100
        assert_eq!(result["priority_queue"]["low"], 0); // 5 - 5
    }
}

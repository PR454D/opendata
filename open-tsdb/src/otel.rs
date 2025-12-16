use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value};
use opentelemetry_proto::tonic::metrics::v1::{
    AggregationTemporality, HistogramDataPoint, Metric, metric, number_data_point,
};
use opentelemetry_proto::tonic::resource::v1::Resource;

use crate::model::{Attribute, MetricType, Sample, Temporality};
use crate::util::{OpenTsdbError, Result};

/// Collect attributes from an OTLP Resource
pub(crate) fn collect_resource_attributes(resource: Option<&Resource>) -> Vec<Attribute> {
    match resource {
        Some(resource) => key_values_to_attributes(&resource.attributes),
        None => Vec::new(),
    }
}

/// Collect attributes from an OTLP InstrumentationScope
pub(crate) fn collect_scope_attributes(scope: Option<&InstrumentationScope>) -> Vec<Attribute> {
    let mut attrs = Vec::new();
    if let Some(scope) = scope {
        if !scope.name.is_empty() {
            attrs.push(Attribute {
                key: "otel_scope_name".to_string(),
                value: scope.name.clone(),
            });
        }
        if !scope.version.is_empty() {
            attrs.push(Attribute {
                key: "otel_scope_version".to_string(),
                value: scope.version.clone(),
            });
        }
    }
    attrs
}

/// A sample with all its attributes, including the metric name
#[derive(Clone, Debug)]
pub(crate) struct SampleWithAttributes {
    pub(crate) attributes: Vec<Attribute>,
    pub(crate) sample: Sample,
}

/// Utility for converting OpenTelemetry Metrics to samples with attributes
pub(crate) struct OtelUtil;

impl OtelUtil {
    /// Returns all samples with attributes for a given metric, merging in resource and scope attributes
    pub(crate) fn samples(
        metric: &Metric,
        resource_attrs: &[Attribute],
        scope_attrs: &[Attribute],
    ) -> Vec<SampleWithAttributes> {
        let mut samples = match &metric.data {
            Some(data) => data.to_samples(&metric.name),
            None => Vec::new(),
        };

        // Merge resource and scope attributes into each sample
        for sample in &mut samples {
            // Prepend resource attrs first, then scope attrs
            // This ensures consistent ordering: resource -> scope -> metric/datapoint attrs
            let mut merged_attrs = Vec::with_capacity(
                resource_attrs.len() + scope_attrs.len() + sample.attributes.len(),
            );
            merged_attrs.extend_from_slice(resource_attrs);
            merged_attrs.extend_from_slice(scope_attrs);
            merged_attrs.append(&mut sample.attributes);
            sample.attributes = merged_attrs;
        }

        samples
    }
}

/// Trait for converting metric data types to samples
trait ToSamples {
    fn to_samples(&self, metric_name: &str) -> Vec<SampleWithAttributes>;
}

impl ToSamples for metric::Data {
    fn to_samples(&self, metric_name: &str) -> Vec<SampleWithAttributes> {
        match self {
            metric::Data::Gauge(gauge) => gauge.to_samples(metric_name),
            metric::Data::Sum(sum) => sum.to_samples(metric_name),
            metric::Data::Histogram(histogram) => histogram.to_samples(metric_name),
            metric::Data::ExponentialHistogram(exp_histogram) => {
                exp_histogram.to_samples(metric_name)
            }
            metric::Data::Summary(summary) => summary.to_samples(metric_name),
        }
    }
}

impl ToSamples for opentelemetry_proto::tonic::metrics::v1::Gauge {
    fn to_samples(&self, metric_name: &str) -> Vec<SampleWithAttributes> {
        let mut samples = Vec::new();
        for dp in &self.data_points {
            if let Some(value) = extract_number_value(&dp.value) {
                let mut attrs = key_values_to_attributes(&dp.attributes);
                attrs.push(Attribute {
                    key: "__name__".to_string(),
                    value: metric_name.to_string(),
                });
                samples.push(SampleWithAttributes {
                    attributes: attrs,
                    sample: Sample {
                        timestamp: timestamp_nanos_to_millis(dp.time_unix_nano),
                        value,
                    },
                });
            }
        }
        samples
    }
}

impl ToSamples for opentelemetry_proto::tonic::metrics::v1::Sum {
    fn to_samples(&self, metric_name: &str) -> Vec<SampleWithAttributes> {
        let mut samples = Vec::new();
        for dp in &self.data_points {
            if let Some(value) = extract_number_value(&dp.value) {
                let mut attrs = key_values_to_attributes(&dp.attributes);
                attrs.push(Attribute {
                    key: "__name__".to_string(),
                    value: metric_name.to_string(),
                });
                samples.push(SampleWithAttributes {
                    attributes: attrs,
                    sample: Sample {
                        timestamp: timestamp_nanos_to_millis(dp.time_unix_nano),
                        value,
                    },
                });
            }
        }
        samples
    }
}

impl ToSamples for opentelemetry_proto::tonic::metrics::v1::Histogram {
    fn to_samples(&self, metric_name: &str) -> Vec<SampleWithAttributes> {
        let mut samples = Vec::new();
        for dp in &self.data_points {
            samples.extend(decompose_histogram(dp, metric_name));
        }
        samples
    }
}

impl ToSamples for opentelemetry_proto::tonic::metrics::v1::ExponentialHistogram {
    fn to_samples(&self, metric_name: &str) -> Vec<SampleWithAttributes> {
        let mut samples = Vec::new();
        for dp in &self.data_points {
            samples.extend(decompose_exponential_histogram(dp, metric_name));
        }
        samples
    }
}

impl ToSamples for opentelemetry_proto::tonic::metrics::v1::Summary {
    fn to_samples(&self, metric_name: &str) -> Vec<SampleWithAttributes> {
        let mut samples = Vec::new();
        for dp in &self.data_points {
            samples.extend(decompose_summary(dp, metric_name));
        }
        samples
    }
}

/// Decomposes an OTEL histogram data point into multiple samples with attributes
fn decompose_histogram(dp: &HistogramDataPoint, metric_name: &str) -> Vec<SampleWithAttributes> {
    let mut samples = Vec::new();
    let base_attrs = key_values_to_attributes(&dp.attributes);
    let timestamp_ms = timestamp_nanos_to_millis(dp.time_unix_nano);

    // Calculate cumulative bucket counts (OTEL uses incremental, Prometheus uses cumulative)
    let mut cumulative = 0u64;
    for (i, &bucket_count) in dp.bucket_counts.iter().enumerate() {
        cumulative += bucket_count;

        // For each explicit bound, create a bucket sample
        if let Some(&bound) = dp.explicit_bounds.get(i) {
            let mut attrs = base_attrs.clone();
            attrs.push(Attribute {
                key: "le".to_string(),
                value: bound.to_string(),
            });
            attrs.push(Attribute {
                key: "__name__".to_string(),
                value: format!("{}_bucket", metric_name),
            });
            samples.push(SampleWithAttributes {
                attributes: attrs,
                sample: Sample {
                    timestamp: timestamp_ms,
                    value: cumulative as f64,
                },
            });
        }
    }

    // Add the "+Inf" bucket (total count)
    let mut inf_attrs = base_attrs.clone();
    inf_attrs.push(Attribute {
        key: "le".to_string(),
        value: "+Inf".to_string(),
    });
    inf_attrs.push(Attribute {
        key: "__name__".to_string(),
        value: format!("{}_bucket", metric_name),
    });
    samples.push(SampleWithAttributes {
        attributes: inf_attrs,
        sample: Sample {
            timestamp: timestamp_ms,
            value: dp.count as f64,
        },
    });

    // Add "_sum" sample (if present)
    if let Some(sum) = dp.sum {
        let mut attrs = base_attrs.clone();
        attrs.push(Attribute {
            key: "__name__".to_string(),
            value: format!("{}_sum", metric_name),
        });
        samples.push(SampleWithAttributes {
            attributes: attrs,
            sample: Sample {
                timestamp: timestamp_ms,
                value: sum,
            },
        });
    }

    // Add "_count" sample
    let mut count_attrs = base_attrs.clone();
    count_attrs.push(Attribute {
        key: "__name__".to_string(),
        value: format!("{}_count", metric_name),
    });
    samples.push(SampleWithAttributes {
        attributes: count_attrs,
        sample: Sample {
            timestamp: timestamp_ms,
            value: dp.count as f64,
        },
    });

    samples
}

/// Decomposes an OTEL exponential histogram data point into multiple samples with attributes
fn decompose_exponential_histogram(
    dp: &opentelemetry_proto::tonic::metrics::v1::ExponentialHistogramDataPoint,
    metric_name: &str,
) -> Vec<SampleWithAttributes> {
    let mut samples = Vec::new();
    let base_attrs = key_values_to_attributes(&dp.attributes);
    let timestamp_ms = timestamp_nanos_to_millis(dp.time_unix_nano);
    let base = 2.0_f64.powf(2.0_f64.powf(-(dp.scale as f64)));

    let mut cumulative = 0u64;

    // Process negative buckets (if present)
    if let Some(negative) = &dp.negative
        && !negative.bucket_counts.is_empty()
    {
        let mut buckets = Vec::new();
        for (i, &count) in negative.bucket_counts.iter().enumerate() {
            let index = negative.offset + i as i32;
            let boundary = base.powf(index as f64 + 1.0);
            buckets.push((boundary, count));
        }
        buckets.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());

        for (boundary, count) in buckets {
            cumulative += count;
            let mut attrs = base_attrs.clone();
            attrs.push(Attribute {
                key: "le".to_string(),
                value: format!("{}", -boundary),
            });
            attrs.push(Attribute {
                key: "__name__".to_string(),
                value: format!("{}_bucket", metric_name),
            });
            samples.push(SampleWithAttributes {
                attributes: attrs,
                sample: Sample {
                    timestamp: timestamp_ms,
                    value: cumulative as f64,
                },
            });
        }
    }

    // Add zero bucket if present
    if dp.zero_count > 0 {
        cumulative += dp.zero_count;
        let mut attrs = base_attrs.clone();
        attrs.push(Attribute {
            key: "le".to_string(),
            value: "0".to_string(),
        });
        attrs.push(Attribute {
            key: "__name__".to_string(),
            value: format!("{}_bucket", metric_name),
        });
        samples.push(SampleWithAttributes {
            attributes: attrs,
            sample: Sample {
                timestamp: timestamp_ms,
                value: cumulative as f64,
            },
        });
    }

    // Process positive buckets (if present)
    if let Some(positive) = &dp.positive {
        for (i, &count) in positive.bucket_counts.iter().enumerate() {
            cumulative += count;
            let index = positive.offset + i as i32;
            let boundary = base.powf(index as f64 + 1.0);

            let mut attrs = base_attrs.clone();
            attrs.push(Attribute {
                key: "le".to_string(),
                value: boundary.to_string(),
            });
            attrs.push(Attribute {
                key: "__name__".to_string(),
                value: format!("{}_bucket", metric_name),
            });
            samples.push(SampleWithAttributes {
                attributes: attrs,
                sample: Sample {
                    timestamp: timestamp_ms,
                    value: cumulative as f64,
                },
            });
        }
    }

    // Add the "+Inf" bucket (total count)
    let mut inf_attrs = base_attrs.clone();
    inf_attrs.push(Attribute {
        key: "le".to_string(),
        value: "+Inf".to_string(),
    });
    inf_attrs.push(Attribute {
        key: "__name__".to_string(),
        value: format!("{}_bucket", metric_name),
    });
    samples.push(SampleWithAttributes {
        attributes: inf_attrs,
        sample: Sample {
            timestamp: timestamp_ms,
            value: dp.count as f64,
        },
    });

    // Add "_sum" sample (if present)
    if let Some(sum) = dp.sum {
        let mut attrs = base_attrs.clone();
        attrs.push(Attribute {
            key: "__name__".to_string(),
            value: format!("{}_sum", metric_name),
        });
        samples.push(SampleWithAttributes {
            attributes: attrs,
            sample: Sample {
                timestamp: timestamp_ms,
                value: sum,
            },
        });
    }

    // Add "_count" sample
    let mut count_attrs = base_attrs.clone();
    count_attrs.push(Attribute {
        key: "__name__".to_string(),
        value: format!("{}_count", metric_name),
    });
    samples.push(SampleWithAttributes {
        attributes: count_attrs,
        sample: Sample {
            timestamp: timestamp_ms,
            value: dp.count as f64,
        },
    });

    samples
}

/// Decomposes an OTEL summary data point into multiple samples with attributes
fn decompose_summary(
    dp: &opentelemetry_proto::tonic::metrics::v1::SummaryDataPoint,
    metric_name: &str,
) -> Vec<SampleWithAttributes> {
    let mut samples = Vec::new();
    let base_attrs = key_values_to_attributes(&dp.attributes);
    let timestamp_ms = timestamp_nanos_to_millis(dp.time_unix_nano);

    // Create a sample for each quantile
    for quantile_value in &dp.quantile_values {
        let mut attrs = base_attrs.clone();
        attrs.push(Attribute {
            key: "quantile".to_string(),
            value: quantile_value.quantile.to_string(),
        });
        attrs.push(Attribute {
            key: "__name__".to_string(),
            value: metric_name.to_string(), // No suffix for quantiles
        });
        samples.push(SampleWithAttributes {
            attributes: attrs,
            sample: Sample {
                timestamp: timestamp_ms,
                value: quantile_value.value,
            },
        });
    }

    // Add "_sum" sample
    let mut sum_attrs = base_attrs.clone();
    sum_attrs.push(Attribute {
        key: "__name__".to_string(),
        value: format!("{}_sum", metric_name),
    });
    samples.push(SampleWithAttributes {
        attributes: sum_attrs,
        sample: Sample {
            timestamp: timestamp_ms,
            value: dp.sum,
        },
    });

    // Add "_count" sample
    let mut count_attrs = base_attrs.clone();
    count_attrs.push(Attribute {
        key: "__name__".to_string(),
        value: format!("{}_count", metric_name),
    });
    samples.push(SampleWithAttributes {
        attributes: count_attrs,
        sample: Sample {
            timestamp: timestamp_ms,
            value: dp.count as f64,
        },
    });

    samples
}

/// Converts OTEL timestamp in nanoseconds to milliseconds
fn timestamp_nanos_to_millis(nanos: u64) -> u64 {
    nanos / 1_000_000
}

/// Extracts numeric value from OTEL NumberDataPoint, converting to f64
fn extract_number_value(value: &Option<number_data_point::Value>) -> Option<f64> {
    match value {
        Some(number_data_point::Value::AsDouble(val)) => Some(*val),
        Some(number_data_point::Value::AsInt(val)) => Some(*val as f64),
        None => None,
    }
}

/// Converts OTEL KeyValue attributes to our Attribute type
fn key_values_to_attributes(attrs: &[KeyValue]) -> Vec<Attribute> {
    attrs
        .iter()
        .filter_map(|kv| {
            extract_attribute_string(kv.value.as_ref()).map(|value_str| Attribute {
                key: kv.key.clone(),
                value: value_str,
            })
        })
        .collect()
}

/// Extracts string value from OTEL AnyValue
fn extract_attribute_string(value: Option<&AnyValue>) -> Option<String> {
    value.and_then(|av| match &av.value {
        Some(any_value::Value::StringValue(s)) => Some(s.clone()),
        Some(any_value::Value::IntValue(i)) => Some(i.to_string()),
        Some(any_value::Value::DoubleValue(d)) => Some(d.to_string()),
        Some(any_value::Value::BoolValue(b)) => Some(b.to_string()),
        Some(any_value::Value::ArrayValue(_)) => None, // Arrays not supported
        Some(any_value::Value::KvlistValue(_)) => None, // KeyValue lists not supported
        Some(any_value::Value::BytesValue(b)) => {
            // Try to decode as UTF-8, fallback to hex
            String::from_utf8(b.clone()).ok()
        }
        None => None,
    })
}

/// Convert OpenTelemetry AggregationTemporality to our Temporality enum
impl From<AggregationTemporality> for Temporality {
    fn from(agg_temp: AggregationTemporality) -> Self {
        match agg_temp {
            AggregationTemporality::Unspecified => Temporality::Unspecified,
            AggregationTemporality::Delta => Temporality::Delta,
            AggregationTemporality::Cumulative => Temporality::Cumulative,
        }
    }
}

/// Convert i32 (proto enum value) to Temporality
impl From<i32> for Temporality {
    fn from(agg_temp: i32) -> Self {
        match agg_temp {
            x if x == AggregationTemporality::Unspecified as i32 => Temporality::Unspecified,
            x if x == AggregationTemporality::Delta as i32 => Temporality::Delta,
            x if x == AggregationTemporality::Cumulative as i32 => Temporality::Cumulative,
            _ => Temporality::Unspecified,
        }
    }
}

/// Convert OpenTelemetry Metric to our MetricType enum
impl TryFrom<&Metric> for MetricType {
    type Error = OpenTsdbError;

    fn try_from(metric: &Metric) -> Result<Self> {
        match &metric.data {
            Some(metric::Data::Gauge(_)) => Ok(MetricType::Gauge),
            Some(metric::Data::Sum(sum)) => {
                let monotonic = sum.is_monotonic;
                let temporality = Temporality::from(sum.aggregation_temporality);
                Ok(MetricType::Sum {
                    monotonic,
                    temporality,
                })
            }
            Some(metric::Data::Histogram(hist)) => {
                let temporality = Temporality::from(hist.aggregation_temporality);
                Ok(MetricType::Histogram { temporality })
            }
            Some(metric::Data::ExponentialHistogram(exp)) => {
                let temporality = Temporality::from(exp.aggregation_temporality);
                Ok(MetricType::ExponentialHistogram { temporality })
            }
            Some(metric::Data::Summary(_)) => Ok(MetricType::Summary),
            None => Err(OpenTsdbError::InvalidInput(
                "Metric has no data".to_string(),
            )),
        }
    }
}

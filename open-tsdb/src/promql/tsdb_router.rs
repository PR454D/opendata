use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use async_trait::async_trait;
use opendata_common::clock::SystemClock;
use promql_parser::parser::EvalStmt;

use super::evaluator::Evaluator;
use super::parser::Parseable;
use super::request::{
    FederateRequest, LabelValuesRequest, LabelsRequest, MetadataRequest, QueryRangeRequest,
    QueryRequest, SeriesRequest,
};
use super::response::{
    ErrorResponse, FederateResponse, LabelValuesResponse, LabelsResponse, MatrixSeries,
    MetadataResponse, QueryRangeResponse, QueryRangeResult, QueryResponse, QueryResult,
    SeriesResponse, VectorSeries,
};
use super::router::PromqlRouter;
use crate::tsdb::Tsdb;

#[async_trait]
impl PromqlRouter for Tsdb {
    async fn query(&self, request: QueryRequest) -> QueryResponse {
        // Parse the request to EvalStmt
        // Note: QueryRequest has a single `time` field for instant queries
        // The Parseable impl sets stmt.start == stmt.end == time
        let clock = Arc::new(SystemClock {});
        let stmt = match request.parse(clock) {
            Ok(stmt) => stmt,
            Err(e) => {
                let err = ErrorResponse::bad_data(e.to_string());
                return QueryResponse {
                    status: err.status,
                    data: None,
                    error: Some(err.error),
                    error_type: Some(err.error_type),
                };
            }
        };

        // Calculate time range for bucket discovery
        // For instant queries: query_time = stmt.start (which equals stmt.end)
        // We need buckets covering [query_time - lookback, query_time]
        let query_time = stmt.start;
        let query_time_secs = query_time
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let lookback_start_secs = query_time
            .checked_sub(stmt.lookback_delta)
            .unwrap_or(std::time::UNIX_EPOCH)
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        // Get query reader for the time range
        let reader = match self
            .query_reader(lookback_start_secs, query_time_secs)
            .await
        {
            Ok(reader) => reader,
            Err(e) => {
                let err = ErrorResponse::internal(e.to_string());
                return QueryResponse {
                    status: err.status,
                    data: None,
                    error: Some(err.error),
                    error_type: Some(err.error_type),
                };
            }
        };

        // Evaluate the query
        let evaluator = Evaluator::new(&reader);
        let samples = match evaluator.evaluate(stmt).await {
            Ok(samples) => samples,
            Err(e) => {
                let err = ErrorResponse::execution(e.to_string());
                return QueryResponse {
                    status: err.status,
                    data: None,
                    error: Some(err.error),
                    error_type: Some(err.error_type),
                };
            }
        };

        // Convert EvalSamples to VectorSeries format
        let result: Vec<VectorSeries> = samples
            .into_iter()
            .map(|sample| VectorSeries {
                metric: sample.labels,
                value: (
                    sample.timestamp_ms as f64 / 1000.0, // Convert ms to seconds
                    sample.value.to_string(),
                ),
            })
            .collect();

        // Return success response
        QueryResponse {
            status: "success".to_string(),
            data: Some(QueryResult {
                result_type: "vector".to_string(),
                result: serde_json::to_value(result).unwrap(),
            }),
            error: None,
            error_type: None,
        }
    }

    async fn query_range(&self, request: QueryRangeRequest) -> QueryRangeResponse {
        // Parse the request to get the expression
        let clock = Arc::new(SystemClock {});
        let stmt = match request.parse(clock) {
            Ok(stmt) => stmt,
            Err(e) => {
                let err = ErrorResponse::bad_data(e.to_string());
                return QueryRangeResponse {
                    status: err.status,
                    data: None,
                    error: Some(err.error),
                    error_type: Some(err.error_type),
                };
            }
        };

        // Calculate time range for bucket discovery
        // Need buckets covering [start - lookback, end]
        let start_secs = stmt
            .start
            .checked_sub(stmt.lookback_delta)
            .unwrap_or(UNIX_EPOCH)
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let end_secs = stmt.end.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;

        // Get query reader for the full time range
        let reader = match self.query_reader(start_secs, end_secs).await {
            Ok(reader) => reader,
            Err(e) => {
                let err = ErrorResponse::internal(e.to_string());
                return QueryRangeResponse {
                    status: err.status,
                    data: None,
                    error: Some(err.error),
                    error_type: Some(err.error_type),
                };
            }
        };

        // Evaluate at each step from start to end
        // Group results by metric labels -> Vec of (timestamp, value)
        // Use sorted Vec as key since HashMap doesn't implement Hash
        let mut series_map: HashMap<Vec<(String, String)>, Vec<(f64, String)>> = HashMap::new();

        let evaluator = Evaluator::new(&reader);
        let mut current_time = stmt.start;

        while current_time <= stmt.end {
            // Create instant query for this timestamp
            let instant_stmt = EvalStmt {
                expr: stmt.expr.clone(),
                start: current_time,
                end: current_time,
                interval: Duration::from_secs(0),
                lookback_delta: stmt.lookback_delta,
            };

            match evaluator.evaluate(instant_stmt).await {
                Ok(samples) => {
                    let timestamp_secs = current_time
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs_f64();

                    for sample in samples {
                        // Convert labels to sorted vec for use as key
                        let mut labels_key: Vec<(String, String)> =
                            sample.labels.into_iter().collect();
                        labels_key.sort();

                        let values = series_map.entry(labels_key).or_default();
                        values.push((timestamp_secs, sample.value.to_string()));
                    }
                }
                Err(e) => {
                    let err = ErrorResponse::execution(e.to_string());
                    return QueryRangeResponse {
                        status: err.status,
                        data: None,
                        error: Some(err.error),
                        error_type: Some(err.error_type),
                    };
                }
            }

            // Advance to next step
            current_time += stmt.interval;
        }

        // Convert to MatrixSeries format
        let result: Vec<MatrixSeries> = series_map
            .into_iter()
            .map(|(labels_vec, values)| {
                let metric: HashMap<String, String> = labels_vec.into_iter().collect();
                MatrixSeries { metric, values }
            })
            .collect();

        QueryRangeResponse {
            status: "success".to_string(),
            data: Some(QueryRangeResult {
                result_type: "matrix".to_string(),
                result,
            }),
            error: None,
            error_type: None,
        }
    }

    async fn series(&self, _request: SeriesRequest) -> SeriesResponse {
        todo!()
    }

    async fn labels(&self, _request: LabelsRequest) -> LabelsResponse {
        todo!()
    }

    async fn label_values(&self, _request: LabelValuesRequest) -> LabelValuesResponse {
        todo!()
    }

    async fn metadata(&self, _request: MetadataRequest) -> MetadataResponse {
        todo!()
    }

    async fn federate(&self, _request: FederateRequest) -> FederateResponse {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Attribute, MetricType, Sample, SampleWithAttributes, TimeBucket};
    use crate::storage::merge_operator::OpenTsdbMergeOperator;
    use opendata_common::Storage;
    use opendata_common::storage::in_memory::InMemoryStorage;
    use std::time::{Duration, UNIX_EPOCH};

    async fn create_test_storage() -> Arc<dyn Storage> {
        Arc::new(InMemoryStorage::with_merge_operator(Arc::new(
            OpenTsdbMergeOperator,
        )))
    }

    fn create_sample(
        metric_name: &str,
        labels: Vec<(&str, &str)>,
        timestamp: u64,
        value: f64,
    ) -> SampleWithAttributes {
        let mut attributes = vec![Attribute {
            key: "__name__".to_string(),
            value: metric_name.to_string(),
        }];
        for (key, val) in labels {
            attributes.push(Attribute {
                key: key.to_string(),
                value: val.to_string(),
            });
        }
        SampleWithAttributes {
            attributes,
            metric_unit: None,
            metric_type: MetricType::Gauge,
            sample: Sample { timestamp, value },
        }
    }

    #[tokio::test]
    async fn should_return_success_response_for_valid_query() {
        // given
        let storage = create_test_storage().await;
        let tsdb = Tsdb::new(storage);

        // Ingest sample data
        // Bucket at minute 60 covers seconds 3600-7199
        let bucket = TimeBucket::hour(60);
        let mini = tsdb.get_or_create_for_ingest(bucket).await.unwrap();

        // Sample at 4000 seconds = 4000000 ms
        let sample = create_sample("http_requests", vec![("env", "prod")], 4_000_000, 42.0);
        mini.ingest(vec![sample]).await.unwrap();
        tsdb.flush().await.unwrap();

        // Query time: 4100 seconds (within lookback of sample at 4000s)
        let query_time = UNIX_EPOCH + Duration::from_secs(4100);
        let request = QueryRequest {
            query: "http_requests".to_string(),
            time: Some(query_time),
            timeout: None,
        };

        // when
        let response = tsdb.query(request).await;

        // then
        assert_eq!(response.status, "success");
        assert!(response.error.is_none());
        assert!(response.data.is_some());

        let data = response.data.unwrap();
        assert_eq!(data.result_type, "vector");

        let results: Vec<VectorSeries> = serde_json::from_value(data.result).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].metric.get("__name__"),
            Some(&"http_requests".to_string())
        );
        assert_eq!(results[0].metric.get("env"), Some(&"prod".to_string()));
        assert_eq!(results[0].value.1, "42");
    }

    #[tokio::test]
    async fn should_return_error_for_invalid_query() {
        // given
        let storage = create_test_storage().await;
        let tsdb = Tsdb::new(storage);

        let request = QueryRequest {
            query: "invalid{".to_string(), // Invalid PromQL syntax
            time: None,
            timeout: None,
        };

        // when
        let response = tsdb.query(request).await;

        // then
        assert_eq!(response.status, "error");
        assert!(response.error.is_some());
        assert_eq!(response.error_type, Some("bad_data".to_string()));
        assert!(response.data.is_none());
    }

    #[tokio::test]
    async fn should_return_matrix_for_range_query() {
        // given
        let storage = create_test_storage().await;
        let tsdb = Tsdb::new(storage);

        // Ingest multiple samples at different timestamps
        // Bucket at minute 60 covers seconds 3600-7199
        let bucket = TimeBucket::hour(60);
        let mini = tsdb.get_or_create_for_ingest(bucket).await.unwrap();

        // Samples at 4000s, 4060s, 4120s (60s apart)
        let samples = vec![
            create_sample("http_requests", vec![("env", "prod")], 4_000_000, 10.0),
            create_sample("http_requests", vec![("env", "prod")], 4_060_000, 20.0),
            create_sample("http_requests", vec![("env", "prod")], 4_120_000, 30.0),
        ];
        mini.ingest(samples).await.unwrap();
        tsdb.flush().await.unwrap();

        // Query range: 4000s to 4120s with 60s step
        let request = QueryRangeRequest {
            query: "http_requests".to_string(),
            start: UNIX_EPOCH + Duration::from_secs(4000),
            end: UNIX_EPOCH + Duration::from_secs(4120),
            step: Duration::from_secs(60),
            timeout: None,
        };

        // when
        let response = tsdb.query_range(request).await;

        // then
        assert_eq!(response.status, "success");
        assert!(response.error.is_none());
        assert!(response.data.is_some());

        let data = response.data.unwrap();
        assert_eq!(data.result_type, "matrix");
        assert_eq!(data.result.len(), 1); // One series

        let series = &data.result[0];
        assert_eq!(
            series.metric.get("__name__"),
            Some(&"http_requests".to_string())
        );
        assert_eq!(series.metric.get("env"), Some(&"prod".to_string()));

        // Should have 3 values (one per step)
        assert_eq!(series.values.len(), 3);
        assert_eq!(series.values[0], (4000.0, "10".to_string()));
        assert_eq!(series.values[1], (4060.0, "20".to_string()));
        assert_eq!(series.values[2], (4120.0, "30".to_string()));
    }
}

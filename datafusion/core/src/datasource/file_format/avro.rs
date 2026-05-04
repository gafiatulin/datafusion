// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Re-exports the [`datafusion_datasource_avro::file_format`] module, and contains tests for it.

pub use datafusion_datasource_avro::file_format::*;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        datasource::file_format::test_util::scan_format, prelude::SessionContext,
    };
    use arrow::array::{Array, as_string_array};
    use datafusion_catalog::Session;
    use datafusion_common::test_util::batches_to_string;
    use datafusion_common::{
        Result,
        cast::{
            as_binary_array, as_boolean_array, as_float32_array, as_float64_array,
            as_int32_array, as_timestamp_microsecond_array,
        },
        test_util,
    };

    use datafusion_datasource_avro::AvroFormat;
    use datafusion_execution::config::SessionConfig;
    use datafusion_physical_plan::{ExecutionPlan, collect};
    use futures::StreamExt;
    use insta::assert_snapshot;

    #[tokio::test]
    async fn read_small_batches() -> Result<()> {
        let config = SessionConfig::new().with_batch_size(2);
        let session_ctx = SessionContext::new_with_config(config);
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        let projection = None;
        let exec = get_exec(&state, "alltypes_plain.avro", projection, None).await?;
        let stream = exec.execute(0, task_ctx)?;

        let tt_batches = stream
            .map(|batch| {
                let batch = batch.unwrap();
                assert_eq!(11, batch.num_columns());
                assert_eq!(2, batch.num_rows());
            })
            .fold(0, |acc, _| async move { acc + 1i32 })
            .await;

        assert_eq!(tt_batches, 4 /* 8/2 */);

        Ok(())
    }

    #[tokio::test]
    async fn read_limit() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        let projection = None;
        let exec = get_exec(&state, "alltypes_plain.avro", projection, Some(1)).await?;
        let batches = collect(exec, task_ctx).await?;
        assert_eq!(1, batches.len());
        assert_eq!(11, batches[0].num_columns());
        assert_eq!(1, batches[0].num_rows());

        Ok(())
    }

    #[tokio::test]
    async fn read_alltypes_plain_avro() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        let projection = None;
        let exec = get_exec(&state, "alltypes_plain.avro", projection, None).await?;

        let x: Vec<String> = exec
            .schema()
            .fields()
            .iter()
            .map(|f| format!("{}: {}", f.name(), f.data_type()))
            .collect();
        assert_eq!(
            vec![
                "id: Int32",
                "bool_col: Boolean",
                "tinyint_col: Int32",
                "smallint_col: Int32",
                "int_col: Int32",
                "bigint_col: Int64",
                "float_col: Float32",
                "double_col: Float64",
                "date_string_col: Binary",
                "string_col: Binary",
                "timestamp_col: Timestamp(µs, \"+00:00\")",
            ],
            x
        );

        let batches = collect(exec, task_ctx).await?;
        assert_eq!(batches.len(), 1);

        assert_snapshot!(batches_to_string(&batches),@r"
        +----+----------+-------------+--------------+---------+------------+-----------+------------+------------------+------------+----------------------+
        | id | bool_col | tinyint_col | smallint_col | int_col | bigint_col | float_col | double_col | date_string_col  | string_col | timestamp_col        |
        +----+----------+-------------+--------------+---------+------------+-----------+------------+------------------+------------+----------------------+
        | 4  | true     | 0           | 0            | 0       | 0          | 0.0       | 0.0        | 30332f30312f3039 | 30         | 2009-03-01T00:00:00Z |
        | 5  | false    | 1           | 1            | 1       | 10         | 1.1       | 10.1       | 30332f30312f3039 | 31         | 2009-03-01T00:01:00Z |
        | 6  | true     | 0           | 0            | 0       | 0          | 0.0       | 0.0        | 30342f30312f3039 | 30         | 2009-04-01T00:00:00Z |
        | 7  | false    | 1           | 1            | 1       | 10         | 1.1       | 10.1       | 30342f30312f3039 | 31         | 2009-04-01T00:01:00Z |
        | 2  | true     | 0           | 0            | 0       | 0          | 0.0       | 0.0        | 30322f30312f3039 | 30         | 2009-02-01T00:00:00Z |
        | 3  | false    | 1           | 1            | 1       | 10         | 1.1       | 10.1       | 30322f30312f3039 | 31         | 2009-02-01T00:01:00Z |
        | 0  | true     | 0           | 0            | 0       | 0          | 0.0       | 0.0        | 30312f30312f3039 | 30         | 2009-01-01T00:00:00Z |
        | 1  | false    | 1           | 1            | 1       | 10         | 1.1       | 10.1       | 30312f30312f3039 | 31         | 2009-01-01T00:01:00Z |
        +----+----------+-------------+--------------+---------+------------+-----------+------------+------------------+------------+----------------------+
        ");
        Ok(())
    }

    #[tokio::test]
    async fn read_bool_alltypes_plain_avro() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        let projection = Some(vec![1]);
        let exec = get_exec(&state, "alltypes_plain.avro", projection, None).await?;

        let batches = collect(exec, task_ctx).await?;
        assert_eq!(batches.len(), 1);
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(8, batches[0].num_rows());

        let array = as_boolean_array(batches[0].column(0))?;
        let mut values: Vec<bool> = vec![];
        for i in 0..batches[0].num_rows() {
            values.push(array.value(i));
        }

        assert_eq!(
            "[true, false, true, false, true, false, true, false]",
            format!("{values:?}")
        );

        Ok(())
    }

    #[tokio::test]
    async fn read_null_bool_alltypes_plain_avro() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        let projection = Some(vec![2]);
        let exec =
            get_exec(&state, "alltypes_nulls_plain.avro", projection, None).await?;

        let batches = collect(exec, task_ctx).await?;
        assert_eq!(batches.len(), 1);
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(1, batches[0].num_rows());

        let array = as_boolean_array(batches[0].column(0))?;

        assert!(array.is_null(0));

        Ok(())
    }

    #[tokio::test]
    async fn read_i32_alltypes_plain_avro() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        let projection = Some(vec![0]);
        let exec = get_exec(&state, "alltypes_plain.avro", projection, None).await?;

        let batches = collect(exec, task_ctx).await?;
        assert_eq!(batches.len(), 1);
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(8, batches[0].num_rows());

        let array = as_int32_array(batches[0].column(0))?;
        let mut values: Vec<i32> = vec![];
        for i in 0..batches[0].num_rows() {
            values.push(array.value(i));
        }

        assert_eq!("[4, 5, 6, 7, 2, 3, 0, 1]", format!("{values:?}"));

        Ok(())
    }

    #[tokio::test]
    async fn read_null_i32_alltypes_plain_avro() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        let projection = Some(vec![1]);
        let exec =
            get_exec(&state, "alltypes_nulls_plain.avro", projection, None).await?;

        let batches = collect(exec, task_ctx).await?;
        assert_eq!(batches.len(), 1);
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(1, batches[0].num_rows());

        let array = as_int32_array(batches[0].column(0))?;

        assert!(array.is_null(0));

        Ok(())
    }

    #[tokio::test]
    async fn read_i96_alltypes_plain_avro() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        let projection = Some(vec![10]);
        let exec = get_exec(&state, "alltypes_plain.avro", projection, None).await?;

        let batches = collect(exec, task_ctx).await?;
        assert_eq!(batches.len(), 1);
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(8, batches[0].num_rows());

        let array = as_timestamp_microsecond_array(batches[0].column(0))?;
        let mut values: Vec<i64> = vec![];
        for i in 0..batches[0].num_rows() {
            values.push(array.value(i));
        }

        assert_eq!(
            "[1235865600000000, 1235865660000000, 1238544000000000, 1238544060000000, 1233446400000000, 1233446460000000, 1230768000000000, 1230768060000000]",
            format!("{values:?}")
        );

        Ok(())
    }

    #[tokio::test]
    async fn read_f32_alltypes_plain_avro() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        let projection = Some(vec![6]);
        let exec = get_exec(&state, "alltypes_plain.avro", projection, None).await?;

        let batches = collect(exec, task_ctx).await?;
        assert_eq!(batches.len(), 1);
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(8, batches[0].num_rows());

        let array = as_float32_array(batches[0].column(0))?;
        let mut values: Vec<f32> = vec![];
        for i in 0..batches[0].num_rows() {
            values.push(array.value(i));
        }

        assert_eq!(
            "[0.0, 1.1, 0.0, 1.1, 0.0, 1.1, 0.0, 1.1]",
            format!("{values:?}")
        );

        Ok(())
    }

    #[tokio::test]
    async fn read_f64_alltypes_plain_avro() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        let projection = Some(vec![7]);
        let exec = get_exec(&state, "alltypes_plain.avro", projection, None).await?;

        let batches = collect(exec, task_ctx).await?;
        assert_eq!(batches.len(), 1);
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(8, batches[0].num_rows());

        let array = as_float64_array(batches[0].column(0))?;
        let mut values: Vec<f64> = vec![];
        for i in 0..batches[0].num_rows() {
            values.push(array.value(i));
        }

        assert_eq!(
            "[0.0, 10.1, 0.0, 10.1, 0.0, 10.1, 0.0, 10.1]",
            format!("{values:?}")
        );

        Ok(())
    }

    #[tokio::test]
    async fn read_binary_alltypes_plain_avro() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        let projection = Some(vec![9]);
        let exec = get_exec(&state, "alltypes_plain.avro", projection, None).await?;

        let batches = collect(exec, task_ctx).await?;
        assert_eq!(batches.len(), 1);
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(8, batches[0].num_rows());

        let array = as_binary_array(batches[0].column(0))?;
        let mut values: Vec<&str> = vec![];
        for i in 0..batches[0].num_rows() {
            values.push(std::str::from_utf8(array.value(i)).unwrap());
        }

        assert_eq!(
            "[\"0\", \"1\", \"0\", \"1\", \"0\", \"1\", \"0\", \"1\"]",
            format!("{values:?}")
        );

        Ok(())
    }

    #[tokio::test]
    async fn read_null_binary_alltypes_plain_avro() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        let projection = Some(vec![6]);
        let exec =
            get_exec(&state, "alltypes_nulls_plain.avro", projection, None).await?;

        let batches = collect(exec, task_ctx).await?;
        assert_eq!(batches.len(), 1);
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(1, batches[0].num_rows());

        let array = as_binary_array(batches[0].column(0))?;

        assert!(array.is_null(0));

        Ok(())
    }

    #[tokio::test]
    async fn read_null_string_alltypes_plain_avro() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        let projection = Some(vec![0]);
        let exec =
            get_exec(&state, "alltypes_nulls_plain.avro", projection, None).await?;

        let batches = collect(exec, task_ctx).await?;
        assert_eq!(batches.len(), 1);
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(1, batches[0].num_rows());

        let array = as_string_array(batches[0].column(0));

        assert!(array.is_null(0));

        Ok(())
    }

    async fn get_exec(
        state: &dyn Session,
        file_name: &str,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let testdata = test_util::arrow_test_data();
        let store_root = format!("{testdata}/avro");
        let format = AvroFormat::default();
        scan_format(
            state,
            &format,
            None,
            &store_root,
            file_name,
            projection,
            limit,
        )
        .await
    }

    /// Assert that what was read back equals what was written, modulo
    /// schema-level metadata (the Avro writer embeds the source schema JSON
    /// in `Schema.metadata` under `avro.schema`).
    fn assert_avro_roundtrip(
        written: &arrow::array::RecordBatch,
        read: Vec<arrow::array::RecordBatch>,
    ) {
        use arrow::compute::concat_batches;

        assert!(!read.is_empty(), "reader produced zero batches");
        let combined =
            concat_batches(&read[0].schema(), &read).expect("concat read batches");

        // Field-wise schema check (name + type + nullability) — surfaces
        // logical-type drift like Timestamp tz "+00:00" vs "UTC" with a clear
        // message before the value diff fires.
        let written_fields: Vec<_> = written
            .schema()
            .fields()
            .iter()
            .map(|f| (f.name().clone(), f.data_type().clone(), f.is_nullable()))
            .collect();
        let read_fields: Vec<_> = combined
            .schema()
            .fields()
            .iter()
            .map(|f| (f.name().clone(), f.data_type().clone(), f.is_nullable()))
            .collect();
        assert_eq!(
            read_fields, written_fields,
            "schema (name, type, nullable) drifted on round-trip"
        );

        assert_eq!(combined.num_rows(), written.num_rows(), "row count differs");
        for (i, (w, r)) in written.columns().iter().zip(combined.columns()).enumerate() {
            assert_eq!(
                w.as_ref(),
                r.as_ref(),
                "column {i} ({}) values differ",
                written.schema().field(i).name(),
            );
        }
    }

    /// Round-trip a small Arrow table through `DataFrame::write_avro` and
    /// back via `read_avro`. Verifies the writer wires up end-to-end and
    /// produces files the reader can decode.
    #[tokio::test]
    async fn roundtrip_avro_write_read() -> Result<()> {
        use crate::dataframe::DataFrameWriteOptions;
        use arrow::array::{Int32Array, RecordBatch, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};

        let tmp = tempfile::tempdir()?;
        let out = tmp.path().join("out.avro");
        let out_str = out.to_str().expect("non-utf8 tempdir");

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
            ],
        )?;

        let ctx = SessionContext::new();
        ctx.read_batch(batch.clone())?
            .write_avro(out_str, DataFrameWriteOptions::new(), None)
            .await?;

        let read_batches = ctx
            .read_avro(out_str, crate::prelude::AvroReadOptions::default())
            .await?
            .collect()
            .await?;
        assert_avro_roundtrip(&batch, read_batches);
        Ok(())
    }

    /// Avro **logical types** (date, time-micros, timestamp-micros with tz,
    /// decimal) must survive the write -> read round-trip. The arrow-avro
    /// writer/encoder is responsible for translating Arrow types to Avro
    /// logical types; this test guards the DataFusion-level wiring (sink +
    /// reader schema) against silently dropping the type info.
    #[tokio::test]
    async fn roundtrip_avro_logical_types() -> Result<()> {
        use crate::dataframe::DataFrameWriteOptions;
        use arrow::array::{
            Date32Array, Decimal128Array, RecordBatch, Time64MicrosecondArray,
            TimestampMicrosecondArray,
        };
        use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

        let tmp = tempfile::tempdir()?;
        let out = tmp.path().join("logical.avro");
        let out_str = out.to_str().expect("non-utf8 tempdir");

        let schema = Arc::new(Schema::new(vec![
            Field::new("d", DataType::Date32, false),
            Field::new("t", DataType::Time64(TimeUnit::Microsecond), false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
                false,
            ),
            Field::new("dec", DataType::Decimal128(10, 2), false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Date32Array::from(vec![19_000, 19_001])),
                Arc::new(Time64MicrosecondArray::from(vec![
                    12 * 3_600 * 1_000_000,
                    13 * 3_600 * 1_000_000 + 30 * 60 * 1_000_000,
                ])),
                Arc::new(
                    TimestampMicrosecondArray::from(vec![
                        1_700_000_000_000_000,
                        1_700_000_001_000_000,
                    ])
                    .with_timezone("+00:00"),
                ),
                Arc::new(
                    Decimal128Array::from(vec![12_345_i128, -67_890_i128])
                        .with_precision_and_scale(10, 2)?,
                ),
            ],
        )?;

        let ctx = SessionContext::new();
        ctx.read_batch(batch.clone())?
            .write_avro(out_str, DataFrameWriteOptions::new(), None)
            .await?;

        let read_batches = ctx
            .read_avro(out_str, crate::prelude::AvroReadOptions::default())
            .await?
            .collect()
            .await?;
        assert_avro_roundtrip(&batch, read_batches);
        Ok(())
    }

    /// `COPY ... TO ... STORED AS AVRO OPTIONS (compression '<codec>', ...)`
    /// must:
    ///   1. plumb the codec option through the SQL parser → `AvroFormatFactory`
    ///      → `AvroSink` → `arrow-avro` writer, and
    ///   2. produce a file the reader can decode back to the original batch.
    ///
    /// To catch silent option-dropping (e.g. a regression where `create()`
    /// ignored `format_options`), inspect the OCF header directly and assert
    /// `avro.codec` matches the requested codec — *before* round-tripping
    /// through the reader, which would happily decode an uncompressed file
    /// regardless of what we asked for.
    #[tokio::test]
    async fn sql_copy_to_avro_codec_roundtrip() -> Result<()> {
        use arrow::array::{Int32Array, RecordBatch, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use datafusion_datasource_avro::arrow_avro::compression::CompressionCodec;
        use datafusion_datasource_avro::arrow_avro::reader::read_header_info;
        use std::fs::File;
        use std::io::BufReader;

        // (option spelling we pass via SQL, expected variant on the wire)
        // Wire codec names live in the OCF header per the Avro spec; arrow-avro
        // accepts "zstd" as an alias on the input side but writes "zstandard"
        // into `avro.codec`. We assert at the variant level (not raw bytes) so
        // the test is robust to alias renames in either direction.
        enum ExpectedCodec {
            Uncompressed,
            Snappy,
            Deflate,
            Zstd,
        }
        let cases: &[(&str, ExpectedCodec)] = &[
            ("uncompressed", ExpectedCodec::Uncompressed),
            ("snappy", ExpectedCodec::Snappy),
            ("deflate", ExpectedCodec::Deflate),
            ("zstd", ExpectedCodec::Zstd),
        ];

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
            ],
        )?;

        for (codec_opt, expected) in cases {
            let tmp = tempfile::tempdir()?;
            let out = tmp.path().join("out.avro");
            let out_str = out.to_str().expect("non-utf8 tempdir").to_string();

            let ctx = SessionContext::new();
            ctx.register_batch("t", batch.clone())?;
            let copy_sql = format!(
                "COPY t TO '{out_str}' STORED AS AVRO OPTIONS (compression '{codec_opt}')"
            );
            ctx.sql(&copy_sql).await?.collect().await?;

            // 1. The codec option survived parse → factory → sink → writer.
            let header = read_header_info(BufReader::new(File::open(&out)?))
                .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?;
            let actual = header
                .compression()
                .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?;
            let codec_matches = matches!(
                (expected, &actual),
                (ExpectedCodec::Uncompressed, None)
                    | (ExpectedCodec::Snappy, Some(CompressionCodec::Snappy))
                    | (ExpectedCodec::Deflate, Some(CompressionCodec::Deflate(_)))
                    | (ExpectedCodec::Zstd, Some(CompressionCodec::ZStandard(_)))
            );
            assert!(
                codec_matches,
                "OCF header codec for option '{codec_opt}' did not match expected variant; got {actual:?}",
            );

            // 2. Values round-trip end-to-end (so a broken codec doesn't
            //    silently corrupt data).
            let read_batches = ctx
                .read_avro(out_str.as_str(), crate::prelude::AvroReadOptions::default())
                .await?
                .collect()
                .await?;
            assert_avro_roundtrip(&batch, read_batches);
        }
        Ok(())
    }

    /// `AvroSink` should expose `rows_written` / `bytes_written` /
    /// `elapsed_compute` via `DataSinkExec::metrics()` so `EXPLAIN ANALYZE`
    /// has something to show. This is the parity test with `ParquetSink`.
    #[tokio::test]
    async fn test_avro_sink_metrics() -> Result<()> {
        use arrow::array::{Int32Array, RecordBatch};
        use arrow::datatypes::{DataType, Field, Schema};
        use datafusion_execution::TaskContext;
        use futures::TryStreamExt;

        let metric_usize =
            |aggregated: &datafusion_physical_expr_common::metrics::MetricsSet,
             name: &str| {
                aggregated
                    .iter()
                    .find(|m| m.value().name() == name)
                    .unwrap_or_else(|| panic!("expected metric {name}"))
                    .value()
                    .as_usize()
            };

        let ctx = SessionContext::new();
        let tmp = tempfile::tempdir()?;
        let out = tmp.path().join("metrics.avro");
        let out_str = out.to_str().expect("non-utf8 tempdir");

        let schema =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from((0..100).collect::<Vec<i32>>()))],
        )?;
        ctx.register_batch("source", batch)?;

        let plan = ctx
            .sql(&format!("COPY source TO '{out_str}' STORED AS AVRO"))
            .await?
            .create_physical_plan()
            .await?;

        let task_ctx = Arc::new(TaskContext::from(&ctx.state()));
        let stream = plan.execute(0, task_ctx)?;
        let _: Vec<_> = stream.try_collect().await?;

        let metrics = plan
            .metrics()
            .expect("AvroSink should return metrics from DataSinkExec");
        let aggregated = metrics.aggregate_by_name();

        assert_eq!(metric_usize(&aggregated, "rows_written"), 100);
        let bytes = metric_usize(&aggregated, "bytes_written");
        assert!(bytes > 0, "expected bytes_written > 0, got {bytes}");
        let elapsed = metric_usize(&aggregated, "elapsed_compute");
        assert!(elapsed > 0, "expected elapsed_compute > 0, got {elapsed}");

        Ok(())
    }
}

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

//! Apache Avro [`FileFormat`] abstractions
use std::collections::HashMap;
use std::fmt;
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::read_avro_schema_from_reader;
use crate::source::AvroSource;

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow_avro::compression::{
    Bzip2Level, CompressionCodec as AvroOcfCodec, DeflateLevel, XzLevel, ZstdLevel,
};
use arrow_avro::writer::{AsyncAvroWriter, AsyncFileWriter, WriterBuilder};
use bytes::Bytes;
use datafusion_common::DEFAULT_AVRO_EXTENSION;
use datafusion_common::GetExt;
use datafusion_common::config::{AvroOptions, ConfigField, ConfigFileType};
use datafusion_common::file_options::avro_writer::{
    AvroCompressionCodec, AvroWriterOptions,
};
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::{Result, Statistics, internal_err, not_impl_err};
use datafusion_common_runtime::SpawnedTask;
use datafusion_datasource::TableSchema;
use datafusion_datasource::display::FileGroupDisplay;
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_datasource::file_format::{FileFormat, FileFormatFactory};
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_datasource::file_sink_config::{FileSink, FileSinkConfig};
use datafusion_datasource::sink::{DataSink, DataSinkExec};
use datafusion_datasource::source::DataSourceExec;
use datafusion_datasource::write::demux::DemuxedStreamReceiver;
use datafusion_datasource::write::get_writer_schema;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_expr::dml::InsertOp;
use datafusion_physical_expr_common::sort_expr::LexRequirement;
use datafusion_physical_plan::metrics::{
    ElapsedComputeFutureExt, ExecutionPlanMetricsSet, MetricBuilder, MetricCategory,
    MetricsSet,
};
use datafusion_physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};
use datafusion_session::Session;

use async_trait::async_trait;
use futures::future::BoxFuture;
use object_store::buffered::BufWriter;
use object_store::path::Path;
use object_store::{GetResultPayload, ObjectMeta, ObjectStore, ObjectStoreExt};
use tokio::io::AsyncWriteExt;
use tokio::task::JoinSet;

#[derive(Default)]
/// Factory struct used to create [`AvroFormat`]
pub struct AvroFormatFactory {
    /// Options carried by this factory
    pub options: Option<AvroOptions>,
}

impl AvroFormatFactory {
    /// Creates an instance of [`AvroFormatFactory`]
    pub fn new() -> Self {
        Self { options: None }
    }

    /// Creates an instance of [`AvroFormatFactory`] with customized default options
    pub fn new_with_options(options: AvroOptions) -> Self {
        Self {
            options: Some(options),
        }
    }
}

impl FileFormatFactory for AvroFormatFactory {
    fn create(
        &self,
        state: &dyn Session,
        format_options: &HashMap<String, String>,
    ) -> Result<Arc<dyn FileFormat>> {
        let avro_options = match &self.options {
            None => {
                let mut table_options = state.default_table_options();
                table_options.set_config_format(ConfigFileType::AVRO);
                table_options.alter_with_string_hash_map(format_options)?;
                table_options.avro
            }
            Some(avro_options) => {
                let mut avro_options = avro_options.clone();
                for (k, v) in format_options {
                    avro_options.set(k, v)?;
                }
                avro_options
            }
        };

        Ok(Arc::new(AvroFormat::default().with_options(avro_options)))
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(AvroFormat::default())
    }
}

impl Debug for AvroFormatFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AvroFormatFactory")
            .field("options", &self.options)
            .finish()
    }
}

impl GetExt for AvroFormatFactory {
    fn get_ext(&self) -> String {
        // Removes the dot, i.e. ".avro" -> "avro"
        DEFAULT_AVRO_EXTENSION[1..].to_string()
    }
}

/// Avro [`FileFormat`] implementation.
#[derive(Default, Debug)]
pub struct AvroFormat {
    options: AvroOptions,
}

impl AvroFormat {
    /// Set Avro options
    pub fn with_options(mut self, options: AvroOptions) -> Self {
        self.options = options;
        self
    }

    /// Retrieve Avro options
    pub fn options(&self) -> &AvroOptions {
        &self.options
    }
}

#[async_trait]
impl FileFormat for AvroFormat {
    fn get_ext(&self) -> String {
        AvroFormatFactory::new().get_ext()
    }

    fn get_ext_with_compression(
        &self,
        file_compression_type: &FileCompressionType,
    ) -> Result<String> {
        let ext = self.get_ext();
        match file_compression_type.get_variant() {
            CompressionTypeVariant::UNCOMPRESSED => Ok(ext),
            _ => internal_err!("Avro FileFormat does not support compression."),
        }
    }

    fn compression_type(&self) -> Option<FileCompressionType> {
        None
    }

    async fn infer_schema(
        &self,
        _state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        let mut schemas = vec![];
        for object in objects {
            let r = store.as_ref().get(&object.location).await?;
            let schema = match r.payload {
                GetResultPayload::File(mut file, _) => {
                    read_avro_schema_from_reader(&mut file)?
                }
                GetResultPayload::Stream(_) => {
                    // TODO: Fetching entire file to get schema is potentially wasteful
                    let data = r.bytes().await?;
                    read_avro_schema_from_reader(&mut data.as_ref())?
                }
            };
            schemas.push(schema);
        }
        let merged_schema = Schema::try_merge(schemas)?;
        Ok(Arc::new(merged_schema))
    }

    async fn infer_stats(
        &self,
        _state: &dyn Session,
        _store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        _object: &ObjectMeta,
    ) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&table_schema))
    }

    async fn create_physical_plan(
        &self,
        _state: &dyn Session,
        conf: FileScanConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(DataSourceExec::from_data_source(conf))
    }

    async fn create_writer_physical_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
        _state: &dyn Session,
        conf: FileSinkConfig,
        order_requirements: Option<LexRequirement>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if conf.insert_op != InsertOp::Append {
            return not_impl_err!("Overwrites are not implemented yet for Avro");
        }

        let writer_options = AvroWriterOptions::try_from(&self.options)?;
        let sink = Arc::new(AvroSink::new(conf, writer_options));

        Ok(Arc::new(DataSinkExec::new(input, sink, order_requirements)) as _)
    }

    fn file_source(&self, table_schema: TableSchema) -> Arc<dyn FileSource> {
        Arc::new(AvroSource::new(table_schema))
    }
}

/// Resolve datafusion's [`AvroCompressionCodec`] (+ optional level) into the
/// `arrow_avro` codec consumed by the writer.
fn build_ocf_codec(opts: &AvroWriterOptions) -> Result<Option<AvroOcfCodec>> {
    let map_arrow_err =
        |e: ArrowError| datafusion_common::DataFusionError::ArrowError(Box::new(e), None);
    Ok(match opts.compression {
        AvroCompressionCodec::Uncompressed => None,
        AvroCompressionCodec::Snappy => Some(AvroOcfCodec::Snappy),
        AvroCompressionCodec::Deflate => {
            let level = match opts.compression_level {
                Some(l) => DeflateLevel::try_new(l as u32).map_err(map_arrow_err)?,
                None => DeflateLevel::default(),
            };
            Some(AvroOcfCodec::Deflate(level))
        }
        AvroCompressionCodec::Zstd => {
            let level = match opts.compression_level {
                Some(l) => ZstdLevel::try_new(l).map_err(map_arrow_err)?,
                None => ZstdLevel::default(),
            };
            Some(AvroOcfCodec::ZStandard(level))
        }
        AvroCompressionCodec::Bzip2 => {
            let level = match opts.compression_level {
                Some(l) => Bzip2Level::try_new(l as u32).map_err(map_arrow_err)?,
                None => Bzip2Level::default(),
            };
            Some(AvroOcfCodec::Bzip2(level))
        }
        AvroCompressionCodec::Xz => {
            let level = match opts.compression_level {
                Some(l) => XzLevel::try_new(l as u32).map_err(map_arrow_err)?,
                None => XzLevel::default(),
            };
            Some(AvroOcfCodec::Xz(level))
        }
    })
}

/// An [`AsyncFileWriter`] that writes Avro output to an [`ObjectStore`] location
/// via multipart upload.
struct AvroObjectStoreWriter {
    inner: BufWriter,
}

impl AvroObjectStoreWriter {
    fn new(store: Arc<dyn ObjectStore>, path: Path) -> Self {
        Self {
            inner: BufWriter::new(store, path),
        }
    }
}

impl AsyncFileWriter for AvroObjectStoreWriter {
    fn write(&mut self, bs: Bytes) -> BoxFuture<'_, Result<(), ArrowError>> {
        Box::pin(async move {
            self.inner.put(bs).await.map_err(|e| {
                ArrowError::ExternalError(
                    Box::new(e) as Box<dyn std::error::Error + Send + Sync>
                )
            })
        })
    }

    fn complete(&mut self) -> BoxFuture<'_, Result<(), ArrowError>> {
        Box::pin(async move {
            self.inner.shutdown().await.map_err(|e| {
                ArrowError::IoError(
                    format!("Error finishing object store upload: {e}"),
                    e,
                )
            })
        })
    }

    fn abort(&mut self) -> BoxFuture<'_, Result<(), ArrowError>> {
        Box::pin(async move {
            self.inner.abort().await.map_err(|e| {
                ArrowError::ExternalError(
                    Box::new(e) as Box<dyn std::error::Error + Send + Sync>
                )
            })
        })
    }
}

/// An [`AsyncFileWriter`] adapter that counts bytes pushed through to the
/// underlying sink, so [`AvroSink`] can expose a `bytes_written` metric.
///
/// `bytes` reflects post-compression OCF bytes — i.e. exactly what gets
/// uploaded to the object store, including header, sync markers, and data
/// blocks.
struct ByteCountingWriter<W> {
    inner: W,
    bytes: Arc<AtomicU64>,
}

impl<W> ByteCountingWriter<W> {
    fn new(inner: W, bytes: Arc<AtomicU64>) -> Self {
        Self { inner, bytes }
    }
}

impl<W: AsyncFileWriter> AsyncFileWriter for ByteCountingWriter<W> {
    fn write(&mut self, bs: Bytes) -> BoxFuture<'_, Result<(), ArrowError>> {
        self.bytes.fetch_add(bs.len() as u64, Ordering::Relaxed);
        self.inner.write(bs)
    }

    fn complete(&mut self) -> BoxFuture<'_, Result<(), ArrowError>> {
        self.inner.complete()
    }

    fn abort(&mut self) -> BoxFuture<'_, Result<(), ArrowError>> {
        self.inner.abort()
    }
}

/// Implements [`DataSink`] for writing to an Avro Object Container File.
pub struct AvroSink {
    /// Config options for writing data
    config: FileSinkConfig,
    /// Writer options for the underlying Avro writer
    writer_options: AvroWriterOptions,
    /// Metrics for tracking write operations
    metrics: ExecutionPlanMetricsSet,
}

impl Debug for AvroSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AvroSink").finish()
    }
}

impl DisplayAs for AvroSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "AvroSink(file_groups=",)?;
                FileGroupDisplay(&self.config.file_group).fmt_as(t, f)?;
                write!(f, ")")
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "format: avro")?;
                write!(f, "file={}", self.config.original_url)
            }
        }
    }
}

impl AvroSink {
    /// Create from config.
    pub fn new(config: FileSinkConfig, writer_options: AvroWriterOptions) -> Self {
        Self {
            config,
            writer_options,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// Retrieve the writer options
    pub fn writer_options(&self) -> &AvroWriterOptions {
        &self.writer_options
    }

    /// Open a fresh async OCF writer at `path`, configured per `writer_options`,
    /// wrapped with a [`ByteCountingWriter`] so callers can observe how many
    /// bytes hit the object store.
    async fn create_async_writer(
        &self,
        path: &Path,
        object_store: Arc<dyn ObjectStore>,
        bytes: Arc<AtomicU64>,
    ) -> Result<AsyncAvroWriter<ByteCountingWriter<AvroObjectStoreWriter>>> {
        let codec = build_ocf_codec(&self.writer_options)?;
        let schema: Schema = get_writer_schema(&self.config).as_ref().clone();

        let mut builder = WriterBuilder::new(schema).with_compression(codec);
        if let Some(block_size) = self.writer_options.block_size {
            builder = builder.with_block_size(block_size);
        }

        let sink = ByteCountingWriter::new(
            AvroObjectStoreWriter::new(object_store, path.clone()),
            bytes,
        );
        let writer = builder
            .build_async::<_, arrow_avro::writer::format::AvroOcfFormat>(sink)
            .await
            .map_err(|e| {
                datafusion_common::DataFusionError::ArrowError(Box::new(e), None)
            })?;
        Ok(writer)
    }
}

#[async_trait]
impl FileSink for AvroSink {
    fn config(&self) -> &FileSinkConfig {
        &self.config
    }

    async fn spawn_writer_tasks_and_join(
        &self,
        _context: &Arc<TaskContext>,
        demux_task: SpawnedTask<Result<()>>,
        mut file_stream_rx: DemuxedStreamReceiver,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<u64> {
        let rows_written = MetricBuilder::new(&self.metrics)
            .with_category(MetricCategory::Rows)
            .global_counter("rows_written");
        let bytes_written = MetricBuilder::new(&self.metrics)
            .with_category(MetricCategory::Bytes)
            .global_counter("bytes_written");
        let elapsed_compute = MetricBuilder::new(&self.metrics).elapsed_compute(0);

        let mut file_write_tasks: JoinSet<Result<u64>> = JoinSet::new();

        while let Some((path, mut rx)) = file_stream_rx.recv().await {
            // Per-file counter; the wrapper increments it on every chunk
            // pushed to the object store. We aggregate into the global
            // bytes_written counter once the file is complete to keep the
            // semantics atomic-per-file.
            let bytes = Arc::new(AtomicU64::new(0));
            let mut writer = self
                .create_async_writer(&path, Arc::clone(&object_store), Arc::clone(&bytes))
                .await?;

            let rows_written = rows_written.clone();
            let bytes_written = bytes_written.clone();
            file_write_tasks.spawn(
                async move {
                    let mut rows: u64 = 0;
                    while let Some(batch) = rx.recv().await {
                        let n = batch.num_rows() as u64;
                        let batch: RecordBatch = batch;
                        writer.write(&batch).await.map_err(|e| {
                            datafusion_common::DataFusionError::ArrowError(
                                Box::new(e),
                                None,
                            )
                        })?;
                        rows += n;
                    }
                    let stats = writer.finish().await.map_err(|e| {
                        datafusion_common::DataFusionError::ArrowError(Box::new(e), None)
                    })?;
                    debug_assert_eq!(stats.rows_written, rows);
                    rows_written.add(rows as usize);
                    bytes_written.add(bytes.load(Ordering::Relaxed) as usize);
                    Ok(rows)
                }
                .with_elapsed_compute(elapsed_compute.clone()),
            );
        }

        let mut total: u64 = 0;
        while let Some(result) = file_write_tasks.join_next().await {
            match result {
                Ok(r) => total += r?,
                Err(e) => {
                    if e.is_panic() {
                        std::panic::resume_unwind(e.into_panic());
                    } else {
                        unreachable!()
                    }
                }
            }
        }

        demux_task.join_unwind().await.map_err(|e| {
            datafusion_common::DataFusionError::ExecutionJoin(Box::new(e))
        })??;

        Ok(total)
    }
}

#[async_trait]
impl DataSink for AvroSink {
    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn schema(&self) -> &SchemaRef {
        self.config.output_schema()
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> Result<u64> {
        FileSink::write_all(self, data, context).await
    }
}

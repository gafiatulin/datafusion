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

//! Options related to how avro files should be written

use std::str::FromStr;

use crate::config::AvroOptions;
use crate::error::{_config_err, DataFusionError, Result};

/// Avro Object Container File compression codec.
///
/// This mirrors the codecs supported by the Avro OCF spec and
/// `arrow_avro::compression::CompressionCodec`. It is held here in
/// `datafusion-common` so configuration can reach the sink without making
/// `datafusion-common` depend on `arrow-avro`. The sink resolves it into
/// `arrow_avro::compression::CompressionCodec` (with the configured level)
/// at write time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AvroCompressionCodec {
    #[default]
    Uncompressed,
    Deflate,
    Snappy,
    Zstd,
    Bzip2,
    Xz,
}

impl AvroCompressionCodec {
    /// Lower-case canonical name used in `avro.codec` and SQL options.
    pub fn as_str(self) -> &'static str {
        match self {
            AvroCompressionCodec::Uncompressed => "uncompressed",
            AvroCompressionCodec::Deflate => "deflate",
            AvroCompressionCodec::Snappy => "snappy",
            AvroCompressionCodec::Zstd => "zstd",
            AvroCompressionCodec::Bzip2 => "bzip2",
            AvroCompressionCodec::Xz => "xz",
        }
    }
}

impl FromStr for AvroCompressionCodec {
    type Err = DataFusionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "uncompressed" | "null" | "none" => Ok(AvroCompressionCodec::Uncompressed),
            "deflate" => Ok(AvroCompressionCodec::Deflate),
            "snappy" => Ok(AvroCompressionCodec::Snappy),
            "zstd" | "zstandard" => Ok(AvroCompressionCodec::Zstd),
            "bzip2" => Ok(AvroCompressionCodec::Bzip2),
            "xz" => Ok(AvroCompressionCodec::Xz),
            other => _config_err!(
                "Unsupported Avro compression codec '{other}'; expected one of \
                 uncompressed, deflate, snappy, zstd, bzip2, xz"
            ),
        }
    }
}

/// Options for writing Avro files
#[derive(Clone, Debug, Default)]
pub struct AvroWriterOptions {
    /// Compression codec applied to OCF blocks.
    pub compression: AvroCompressionCodec,
    /// Optional codec-specific level. `None` uses the codec default.
    pub compression_level: Option<i32>,
    /// Optional approximate target uncompressed block size in bytes.
    pub block_size: Option<usize>,
}

impl AvroWriterOptions {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new `AvroWriterOptions` with the specified compression codec.
    pub fn with_compression(mut self, compression: AvroCompressionCodec) -> Self {
        self.compression = compression;
        self
    }

    /// Set an optional compression level.
    pub fn with_compression_level(mut self, level: Option<i32>) -> Self {
        self.compression_level = level;
        self
    }

    /// Set an optional target block size in bytes.
    pub fn with_block_size(mut self, block_size: Option<usize>) -> Self {
        self.block_size = block_size;
        self
    }
}

impl TryFrom<&AvroOptions> for AvroWriterOptions {
    type Error = DataFusionError;

    fn try_from(value: &AvroOptions) -> Result<Self> {
        Ok(AvroWriterOptions {
            compression: AvroCompressionCodec::from_str(&value.compression)?,
            compression_level: value.compression_level,
            block_size: value.block_size,
        })
    }
}

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

use std::sync::Arc;

use crate::datasource::file_format::format_as_file_type;

use super::{
    DataFrame, DataFrameWriteOptions, DataFusionError, LogicalPlanBuilder, RecordBatch,
};

use datafusion_common::config::AvroOptions;
use datafusion_common::not_impl_err;
use datafusion_datasource_avro::file_format::AvroFormatFactory;
use datafusion_expr::dml::InsertOp;

impl DataFrame {
    /// Execute the `DataFrame` and write the results to Avro Object Container
    /// File(s).
    ///
    /// # Example
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # use std::fs;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// use datafusion::dataframe::DataFrameWriteOptions;
    /// let ctx = SessionContext::new();
    /// ctx.read_csv("tests/data/example.csv", CsvReadOptions::new())
    ///     .await?
    ///     .write_avro(
    ///         "output.avro",
    ///         DataFrameWriteOptions::new(),
    ///         None, // can also specify avro writing options here
    ///     )
    ///     .await?;
    /// # fs::remove_file("output.avro")?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn write_avro(
        self,
        path: &str,
        options: DataFrameWriteOptions,
        writer_options: Option<AvroOptions>,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        if options.insert_op != InsertOp::Append {
            return not_impl_err!(
                "{} is not implemented for DataFrame::write_avro.",
                options.insert_op
            );
        }

        let format = if let Some(avro_opts) = writer_options {
            Arc::new(AvroFormatFactory::new_with_options(avro_opts))
        } else {
            Arc::new(AvroFormatFactory::new())
        };

        let file_type = format_as_file_type(format);

        let copy_options = options.build_sink_options();

        let plan = if options.sort_by.is_empty() {
            self.plan
        } else {
            LogicalPlanBuilder::from(self.plan)
                .sort(options.sort_by)?
                .build()?
        };

        let plan = LogicalPlanBuilder::copy_to(
            plan,
            path.into(),
            file_type,
            copy_options,
            options.partition_by,
        )?
        .build()?;
        DataFrame {
            session_state: self.session_state,
            plan,
            projection_requires_validation: self.projection_requires_validation,
        }
        .collect()
        .await
    }
}

//! Contains implementation of IOx system tables:
//!
//! system.chunks
//! system.columns
//! system.chunk_columns
//! system.operations
//!
//! For example `SELECT * FROM system.chunks`

use super::catalog::Catalog;
use crate::JobRegistry;
use arrow::{array::ArrayRef, datatypes::{DataType, Field, Schema, SchemaRef}, error::Result, record_batch::RecordBatch};
use async_trait::async_trait;
use datafusion::{
    catalog::schema::SchemaProvider,
    datasource::TableProvider,
    error::{DataFusionError, Result as DataFusionResult},
    physical_plan::{memory::MemoryExec, ExecutionPlan},
};
use hashbrown::HashSet;
use std::{any::Any, sync::Arc};

mod chunks;
mod columns;
mod operations;
mod persistence;

// The IOx system schema
pub const SYSTEM_SCHEMA: &str = "system";

const CHUNKS: &str = "chunks";
const COLUMNS: &str = "columns";
const CHUNK_COLUMNS: &str = "chunk_columns";
const OPERATIONS: &str = "operations";
const PERSISTENCE_WINDOWS: &str = "persistence_windows";

pub struct SystemSchemaProvider {
    chunks: Arc<dyn TableProvider>,
    columns: Arc<dyn TableProvider>,
    chunk_columns: Arc<dyn TableProvider>,
    operations: Arc<dyn TableProvider>,
    persistence_windows: Arc<dyn TableProvider>,
}

impl std::fmt::Debug for SystemSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SystemSchemaProvider")
            .field("fields", &"...")
            .finish()
    }
}

impl SystemSchemaProvider {
    pub fn new(db_name: impl Into<String>, catalog: Arc<Catalog>, jobs: Arc<JobRegistry>) -> Self {
        let db_name = db_name.into();
        let chunks = Arc::new(SystemTableProvider {
            inner: chunks::ChunksTable::new(Arc::clone(&catalog)),
        });
        let columns = Arc::new(SystemTableProvider {
            inner: columns::ColumnsTable::new(Arc::clone(&catalog)),
        });
        let chunk_columns = Arc::new(SystemTableProvider {
            inner: columns::ChunkColumnsTable::new(Arc::clone(&catalog)),
        });
        let operations = Arc::new(SystemTableProvider {
            inner: operations::OperationsTable::new(db_name, jobs),
        });
        let persistence_windows = Arc::new(SystemTableProvider {
            inner: persistence::PersistenceWindowsTable::new(catalog),
        });
        Self {
            chunks,
            columns,
            chunk_columns,
            operations,
            persistence_windows,
        }
    }
}

const ALL_SYSTEM_TABLES: [&str; 5] = [
    CHUNKS,
    COLUMNS,
    CHUNK_COLUMNS,
    OPERATIONS,
    PERSISTENCE_WINDOWS,
];

impl SchemaProvider for SystemSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }

    fn table_names(&self) -> Vec<String> {
        ALL_SYSTEM_TABLES
            .iter()
            .map(|name| name.to_string())
            .collect()
    }

    fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        match name {
            CHUNKS => Some(Arc::clone(&self.chunks)),
            COLUMNS => Some(Arc::clone(&self.columns)),
            CHUNK_COLUMNS => Some(Arc::clone(&self.chunk_columns)),
            OPERATIONS => Some(Arc::clone(&self.operations)),
            PERSISTENCE_WINDOWS => Some(Arc::clone(&self.persistence_windows)),
            _ => None,
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        ALL_SYSTEM_TABLES
            .iter()
            .any(|&system_table| system_table == name)
    }
}

/// Controls the creation of system tables
#[derive(Debug, Default)]
struct CreationOptions {

    /// List of columns to make; If none, selects all columns
    required_columns: Option<HashSet<String>>,

    // TODO add limit and predicates to this (eventually)
}

impl CreationOptions {
    fn new() -> Self {
        Default::default()
    }

    /// Add column_name to the list of columns to be created
    fn with_column(self, column_name: impl Into<String>) -> Self {
        let Self { required_columns } = self;

        let mut required_columns = required_columns.unwrap_or_else(|| HashSet::new());
        required_columns.insert(column_name.into());

        Self { required_columns: Some(required_columns) }
    }

    /// returns true of this column is needed, false otherwise
    fn needs_column(&self, column_name: impl AsRef<str>) -> bool {
        self.required_columns.as_ref()
            .map(|required_columns| {
                required_columns.contains(column_name.as_ref())
            })
            .unwrap_or(true)
    }

}

/// A function that creates an arrow array
type CreationFunction = dyn Fn () -> ArrayRef + Send + Sync;


/// Creates a system table
#[derive(Default)]
struct SystemTableBuilder {
    name: String,
    columns: Vec<(Field, Box<CreationFunction>)>,
}

impl SystemTableBuilder {
    fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ..Default::default()
        }
    }

    /// Register a column and a function that can create it
    fn with_column<F>(mut self, name: &str, data_type: DataType, nullable: bool, creation: F) -> Self
    where
        F: Fn () -> ArrayRef + Send + Sync + 'static
    {
        let creation = Box::new(creation);
        self.columns.push((Field::new(name, data_type, nullable), creation));
        self
    }

    fn build(self) -> SystemTable {
        let Self { name, columns} = self;
        let (fields, creation_functions): (Vec<_>, Vec<_>) = columns.into_iter().unzip();

        let schema = Arc::new(Schema::new(fields));
        SystemTable {
            name,
            schema,
            creation_functions
        }
    }
}

/// Creates a system table
struct SystemTable {
    name: String,
    schema: SchemaRef,
    creation_functions: Vec<Box<CreationFunction>>
}

/// TODO remove this trait and use the struct directly
impl IoxSystemTable for SystemTable {

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    /// Get the contents of the system table as a single RecordBatch
    fn batch(&self, options: CreationOptions) -> Result<RecordBatch> {
        let field_iter = self.schema.fields().iter();
        let func_iter = self.creation_functions.iter();

        let (fields, columns): (Vec<_>, Vec<_>) = field_iter.zip(func_iter)
            .filter_map(|(field, func)| {
                if options.needs_column(field.name()) {
                    Some((field.clone(), func()))
                } else {
                    None
                }
            })
            .unzip();

        let schema = Arc::new(Schema::new(fields));
        RecordBatch::try_new(schema, columns)
    }
}


/// The minimal thing that a system table needs to implement
trait IoxSystemTable: Send + Sync {
    /// Produce the schema from this system table
    fn schema(&self) -> SchemaRef;

    /// Get the contents of the system table as a single RecordBatch
    fn batch(&self, options: CreationOptions) -> Result<RecordBatch>;
}

/// Adapter that makes any `IoxSystemTable` a DataFusion `TableProvider`
struct SystemTableProvider<T>
where
    T: IoxSystemTable,
{
    inner: T,
}

#[async_trait]
impl<T> TableProvider for SystemTableProvider<T>
where
    T: IoxSystemTable + 'static,
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    async fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        _batch_size: usize,
        // It would be cool to push projection and limit down
        _filters: &[datafusion::logical_plan::Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {

        let schema = self.schema();

        let options = match projection {
            None => CreationOptions::new(),
            Some(projection) => {
                projection
                    .iter()
                    .try_fold(CreationOptions::new(),
                              |options, &i| {
                                  if i < schema.fields().len() {
                                      let field = schema.field(i);
                                      Ok(options.with_column(field.name()))
                                  } else {
                                      Err(DataFusionError::Internal(format!(
                                          "Projection index out of range in SystemTableProvder: {}",
                                          i
                                      )))
                                  }
                              }
                    )?
            }
        };
        let batch = self.inner.batch(options)?;
        Ok(Arc::new(MemoryExec::try_new(&[vec![batch]], schema, None)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, UInt64Array};
    use arrow_util::assert_batches_eq;
    use test_helpers::assert_contains;

    fn seq_array(start: u64, end: u64) -> ArrayRef {
        Arc::new(UInt64Array::from_iter_values(start..end))
    }

    #[tokio::test]
    async fn test_scan_batch_no_projection() {
        let system_table = SystemTableBuilder::new("foo")
            .with_column("col1", DataType::UInt64, false, || seq_array(0, 3))
            .with_column("col2", DataType::UInt64, false, || seq_array(1, 4))
            .with_column("col3", DataType::UInt64, false, || seq_array(2, 5))
            .with_column("col4", DataType::UInt64, false, || seq_array(3, 6))
            .build();

        let options = CreationOptions::new();

        let collected = vec![system_table.batch(options).unwrap()];

        let expected = vec![
            "+------+------+------+------+",
            "| col1 | col2 | col3 | col4 |",
            "+------+------+------+------+",
            "| 0    | 1    | 2    | 3    |",
            "| 1    | 2    | 3    | 4    |",
            "| 2    | 3    | 4    | 5    |",
            "+------+------+------+------+",
        ];

        assert_batches_eq!(&expected, &collected);
    }

    #[tokio::test]
    async fn test_scan_batch_good_projection() {
        let system_table = SystemTableBuilder::new("foo")
            .with_column("col1", DataType::UInt64, false, || seq_array(0, 3))
            .with_column("col2", DataType::UInt64, false, || seq_array(1, 4))
            .with_column("col3", DataType::UInt64, false, || seq_array(2, 5))
            .with_column("col4", DataType::UInt64, false, || seq_array(3, 6))
            .build();

        let options = CreationOptions::new()
            .with_column("col4")
            .with_column("col2");

        let collected = vec![system_table.batch(options).unwrap()];

        let expected = vec![
            "+------+------+",
            "| col4 | col2 |",
            "+------+------+",
            "| 3    | 1    |",
            "| 4    | 2    |",
            "| 5    | 3    |",
            "+------+------+",
        ];

        assert_batches_eq!(&expected, &collected);
    }

    #[tokio::test]
    async fn test_scan_batch_bad_projection() {
        let system_table = SystemTableBuilder::new("foo")
            .with_column("col1", DataType::UInt64, false, || seq_array(0, 3))
            .with_column("col2", DataType::UInt64, false, || seq_array(1, 4))
            .with_column("col3", DataType::UInt64, false, || seq_array(2, 5))
            .with_column("col4", DataType::UInt64, false, || seq_array(3, 6))
            .build();

        // no column "col5"
        let options = CreationOptions::new()
            .with_column("col4")
            .with_column("col5");

        let result = system_table.batch(options).unwrap_err().to_string();
        assert_contains!(result, "Internal error: Projection index out of range in ChunksProvider: 5");
    }
}

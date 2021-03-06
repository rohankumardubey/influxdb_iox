use crate::{write_buffer::WriteBufferConnection, DatabaseName};
use snafu::Snafu;
use std::{
    num::{NonZeroU32, NonZeroU64, NonZeroUsize},
    time::Duration,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error in {}: {}", source_module, source))]
    PassThrough {
        source_module: &'static str,
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    #[snafu(display("No sharding rule matches table: {}", table))]
    NoShardingRuleMatches { table: String },

    #[snafu(display("No shards defined"))]
    NoShardsDefined,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// `DatabaseRules` contains the rules for replicating data, sending data to
/// subscribers, and querying data for a single database. This information is
/// provided by and exposed to operators.
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct DatabaseRules {
    /// The name of the database
    pub name: DatabaseName<'static>,

    /// Template that generates a partition key for each row inserted into the
    /// db
    pub partition_template: PartitionTemplate,

    /// Configure how data flows through the system
    pub lifecycle_rules: LifecycleRules,

    /// Duration for which the cleanup loop should sleep on average.
    /// Defaults to 500 seconds.
    pub worker_cleanup_avg_sleep: Duration,

    /// An optional connection string to a write buffer for either writing or reading.
    pub write_buffer_connection: Option<WriteBufferConnection>,
}

impl DatabaseRules {
    pub fn new(name: DatabaseName<'static>) -> Self {
        Self {
            name,
            partition_template: Default::default(),
            lifecycle_rules: Default::default(),
            worker_cleanup_avg_sleep: Duration::from_secs(500),
            write_buffer_connection: None,
        }
    }

    pub fn db_name(&self) -> &str {
        self.name.as_str()
    }
}

pub const DEFAULT_WORKER_BACKOFF_MILLIS: u64 = 1_000;
pub const DEFAULT_CATALOG_TRANSACTIONS_UNTIL_CHECKPOINT: u64 = 100;
pub const DEFAULT_CATALOG_TRANSACTION_PRUNE_AGE: Duration = Duration::from_secs(24 * 60 * 60);
pub const DEFAULT_MUB_ROW_THRESHOLD: usize = 100_000;
pub const DEFAULT_PERSIST_ROW_THRESHOLD: usize = 1_000_000;
pub const DEFAULT_PERSIST_AGE_THRESHOLD_SECONDS: u32 = 30 * 60;
pub const DEFAULT_LATE_ARRIVE_WINDOW_SECONDS: u32 = 5 * 60;

/// Configures how data automatically flows through the system
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct LifecycleRules {
    /// Once the total amount of buffered data in memory reaches this size start
    /// dropping data from memory
    pub buffer_size_soft: Option<NonZeroUsize>,

    /// Once the amount of data in memory reaches this size start
    /// rejecting writes
    pub buffer_size_hard: Option<NonZeroUsize>,

    /// Persists chunks to object storage.
    pub persist: bool,

    /// Do not allow writing new data to this database
    pub immutable: bool,

    /// If the background worker doesn't find anything to do it
    /// will sleep for this many milliseconds before looking again
    pub worker_backoff_millis: NonZeroU64,

    /// The maximum number of permitted concurrently executing compactions.
    pub max_active_compactions: MaxActiveCompactions,

    /// After how many transactions should IOx write a new checkpoint?
    pub catalog_transactions_until_checkpoint: NonZeroU64,

    /// Prune catalog transactions older than the given age.
    ///
    /// Keeping old transaction can be useful for debugging.
    pub catalog_transaction_prune_age: Duration,

    /// Once a partition hasn't received a write for this period of time,
    /// it will be compacted and, if set, persisted. Writers will generally
    /// have this amount of time to send late arriving writes or this could
    /// be their clock skew.
    pub late_arrive_window_seconds: NonZeroU32,

    /// Maximum number of rows before triggering persistence
    pub persist_row_threshold: NonZeroUsize,

    /// Maximum age of a write before triggering persistence
    pub persist_age_threshold_seconds: NonZeroU32,

    /// Maximum number of rows to buffer in a MUB chunk before compacting it
    pub mub_row_threshold: NonZeroUsize,

    /// Use up to this amount of space in bytes for caching Parquet files. None
    /// will disable Parquet file caching.
    pub parquet_cache_limit: Option<NonZeroU64>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum MaxActiveCompactions {
    /// The maximum number of permitted concurrently executing compactions.
    /// It is not currently possible to set a limit that disables compactions
    /// entirely, nor is it possible to set an "unlimited" value.
    MaxActiveCompactions(NonZeroU32),

    // The maximum number of concurrent active compactions that can run
    // expressed as a fraction of the available cpus (rounded to the next smallest non-zero integer).
    MaxActiveCompactionsCpuFraction {
        fraction: f32,
        effective: NonZeroU32,
    },
}

impl MaxActiveCompactions {
    pub fn new(fraction: f32) -> Self {
        let cpus = num_cpus::get() as f32 * fraction;
        let effective = (cpus as u32).saturating_sub(1) + 1;
        let effective = NonZeroU32::new(effective).unwrap();
        Self::MaxActiveCompactionsCpuFraction {
            fraction,
            effective,
        }
    }

    pub fn get(&self) -> u32 {
        match self {
            Self::MaxActiveCompactions(effective) => effective,
            Self::MaxActiveCompactionsCpuFraction { effective, .. } => effective,
        }
        .get()
    }
}

// Defaults to number of CPUs.
impl Default for MaxActiveCompactions {
    fn default() -> Self {
        Self::new(1.0)
    }
}

// Required because database rules must be Eq but cannot derive Eq for Self
// since f32 is not Eq.
impl Eq for MaxActiveCompactions {}

impl LifecycleRules {
    /// The max timestamp skew across concurrent writers before persisted chunks might overlap
    pub fn late_arrive_window(&self) -> Duration {
        Duration::from_secs(self.late_arrive_window_seconds.get() as u64)
    }
}

impl Default for LifecycleRules {
    fn default() -> Self {
        Self {
            buffer_size_soft: None,
            buffer_size_hard: None,
            persist: false,
            immutable: false,
            worker_backoff_millis: NonZeroU64::new(DEFAULT_WORKER_BACKOFF_MILLIS).unwrap(),
            max_active_compactions: Default::default(),
            catalog_transactions_until_checkpoint: NonZeroU64::new(
                DEFAULT_CATALOG_TRANSACTIONS_UNTIL_CHECKPOINT,
            )
            .unwrap(),
            catalog_transaction_prune_age: DEFAULT_CATALOG_TRANSACTION_PRUNE_AGE,
            late_arrive_window_seconds: NonZeroU32::new(DEFAULT_LATE_ARRIVE_WINDOW_SECONDS)
                .unwrap(),
            persist_row_threshold: NonZeroUsize::new(DEFAULT_PERSIST_ROW_THRESHOLD).unwrap(),
            persist_age_threshold_seconds: NonZeroU32::new(DEFAULT_PERSIST_AGE_THRESHOLD_SECONDS)
                .unwrap(),
            mub_row_threshold: NonZeroUsize::new(DEFAULT_MUB_ROW_THRESHOLD).unwrap(),
            parquet_cache_limit: None,
        }
    }
}

/// `PartitionTemplate` is used to compute the partition key of each row that
/// gets written. It can consist of the table name, a column name and its value,
/// a formatted time, or a string column and regex captures of its value. For
/// columns that do not appear in the input row, a blank value is output.
///
/// The key is constructed in order of the template parts; thus ordering changes
/// what partition key is generated.
#[derive(Debug, Default, Eq, PartialEq, Clone)]
pub struct PartitionTemplate {
    pub parts: Vec<TemplatePart>,
}

/// `TemplatePart` specifies what part of a row should be used to compute this
/// part of a partition key.
#[derive(Debug, Eq, PartialEq, Clone)]
pub enum TemplatePart {
    /// The name of a table
    Table,
    /// The value in a named column
    Column(String),
    /// Applies a  `strftime` format to the "time" column.
    ///
    /// For example, a time format of "%Y-%m-%d %H:%M:%S" will produce
    /// partition key parts such as "2021-03-14 12:25:21" and
    /// "2021-04-14 12:24:21"
    TimeFormat(String),
    /// Applies a regex to the value in a string column
    RegexCapture(RegexCapture),
    /// Applies a `strftime` pattern to some column other than "time"
    StrftimeColumn(StrftimeColumn),
}

/// `RegexCapture` is for pulling parts of a string column into the partition
/// key.
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct RegexCapture {
    pub column: String,
    pub regex: String,
}

/// [`StrftimeColumn`] is used to create a time based partition key off some
/// column other than the builtin `time` column.
///
/// The value of the named column is formatted using a `strftime`
/// style string.
///
/// For example, a time format of "%Y-%m-%d %H:%M:%S" will produce
/// partition key parts such as "2021-03-14 12:25:21" and
/// "2021-04-14 12:24:21"
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct StrftimeColumn {
    pub column: String,
    pub format: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_max_active_compactions_cpu_fraction() {
        let n = MaxActiveCompactions::new(1.0);
        let cpus = n.get();

        let n = MaxActiveCompactions::new(0.5);
        let half_cpus = n.get();

        assert_eq!(half_cpus, cpus / 2);

        let n = MaxActiveCompactions::new(0.0);
        let non_zero = n.get();

        assert_eq!(non_zero, 1);
    }
}

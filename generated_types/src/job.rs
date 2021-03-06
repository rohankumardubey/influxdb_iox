use crate::influxdata::iox::management::v1 as management;
use data_types::job::Job;

impl management::operation_metadata::Job {
    /// Return the db_name for this job
    pub fn db_name(&self) -> &str {
        match self {
            Self::Dummy(management::Dummy { db_name, .. }) => db_name,
            Self::WriteChunk(management::WriteChunk { db_name, .. }) => db_name,
            Self::WipePreservedCatalog(management::WipePreservedCatalog { db_name, .. }) => db_name,
            Self::CompactChunks(management::CompactChunks { db_name, .. }) => db_name,
            Self::PersistChunks(management::PersistChunks { db_name, .. }) => db_name,
            Self::DropChunk(management::DropChunk { db_name, .. }) => db_name,
            Self::DropPartition(management::DropPartition { db_name, .. }) => db_name,
            Self::LoadReadBufferChunk(management::LoadReadBufferChunk { db_name, .. }) => db_name,
            Self::RebuildPreservedCatalog(management::RebuildPreservedCatalog {
                db_name, ..
            }) => db_name,
            Self::CompactObjectStoreChunks(management::CompactObjectStoreChunks {
                db_name,
                ..
            }) => db_name,
            Self::CompactObjectStorePartition(management::CompactObjectStorePartition {
                db_name,
                ..
            }) => db_name,
        }
    }
}

impl From<Job> for management::operation_metadata::Job {
    fn from(job: Job) -> Self {
        match job {
            Job::Dummy { nanos, db_name } => Self::Dummy(management::Dummy {
                nanos,
                db_name: db_name.map(|x| x.to_string()).unwrap_or_default(),
            }),
            Job::WriteChunk { chunk } => Self::WriteChunk(management::WriteChunk {
                db_name: chunk.db_name.to_string(),
                partition_key: chunk.partition_key.to_string(),
                table_name: chunk.table_name.to_string(),
                chunk_id: chunk.chunk_id.into(),
            }),
            Job::WipePreservedCatalog { db_name } => {
                Self::WipePreservedCatalog(management::WipePreservedCatalog {
                    db_name: db_name.to_string(),
                })
            }
            Job::CompactChunks { partition, chunks } => {
                Self::CompactChunks(management::CompactChunks {
                    db_name: partition.db_name.to_string(),
                    partition_key: partition.partition_key.to_string(),
                    table_name: partition.table_name.to_string(),
                    chunks: chunks.into_iter().map(|chunk_id| chunk_id.into()).collect(),
                })
            }
            Job::CompactObjectStoreChunks { partition, chunks } => {
                Self::CompactObjectStoreChunks(management::CompactObjectStoreChunks {
                    db_name: partition.db_name.to_string(),
                    partition_key: partition.partition_key.to_string(),
                    table_name: partition.table_name.to_string(),
                    chunks: chunks.into_iter().map(|chunk_id| chunk_id.into()).collect(),
                })
            }
            Job::PersistChunks { partition, chunks } => {
                Self::PersistChunks(management::PersistChunks {
                    db_name: partition.db_name.to_string(),
                    partition_key: partition.partition_key.to_string(),
                    table_name: partition.table_name.to_string(),
                    chunks: chunks.into_iter().map(|chunk_id| chunk_id.into()).collect(),
                })
            }
            Job::DropChunk { chunk } => Self::DropChunk(management::DropChunk {
                db_name: chunk.db_name.to_string(),
                partition_key: chunk.partition_key.to_string(),
                table_name: chunk.table_name.to_string(),
                chunk_id: chunk.chunk_id.into(),
            }),
            Job::DropPartition { partition } => Self::DropPartition(management::DropPartition {
                db_name: partition.db_name.to_string(),
                partition_key: partition.partition_key.to_string(),
                table_name: partition.table_name.to_string(),
            }),
            Job::LoadReadBufferChunk { chunk } => {
                Self::LoadReadBufferChunk(management::LoadReadBufferChunk {
                    db_name: chunk.db_name.to_string(),
                    partition_key: chunk.partition_key.to_string(),
                    table_name: chunk.table_name.to_string(),
                    chunk_id: chunk.chunk_id.into(),
                })
            }
            Job::RebuildPreservedCatalog { db_name } => {
                Self::RebuildPreservedCatalog(management::RebuildPreservedCatalog {
                    db_name: db_name.to_string(),
                })
            }
        }
    }
}

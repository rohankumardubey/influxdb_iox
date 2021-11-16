//! This module contains the code that splits and persist chunks

use super::{
    error::{ChunksNotContiguous, ChunksNotInPartition, EmptyChunks},
    LockableCatalogChunk, LockableCatalogPartition, Result,
};
use crate::db::{
    catalog::{chunk::CatalogChunk, partition::Partition},
    lifecycle::{collect_rub, merge_schemas, write::write_chunk_to_object_store},
    DbChunk,
};
use data_types::{chunk_metadata::ChunkOrder, delete_predicate::DeletePredicate, job::Job};
use lifecycle::{LifecycleWriteGuard, LockableChunk, LockablePartition};
use observability_deps::tracing::info;
use persistence_windows::persistence_windows::FlushHandle;
use query::{compute_sort_key, exec::ExecutorType, frontend::reorg::ReorgPlanner, QueryChunkMeta};
use std::{
    collections::{BTreeSet, HashSet},
    future::Future,
    sync::Arc,
};
use time::Time;
use tracker::{TaskTracker, TrackedFuture, TrackedFutureExt};

/// Split and then persist the provided chunks
///
/// `flush_handle` describes both what to persist and also acts as a transaction
/// on the persistence windows
///
/// TODO: Replace low-level locks with transaction object
pub fn persist_chunks(
    partition: LifecycleWriteGuard<'_, Partition, LockableCatalogPartition>,
    chunks: Vec<LifecycleWriteGuard<'_, CatalogChunk, LockableCatalogChunk>>,
    flush_handle: FlushHandle,
) -> Result<(
    TaskTracker<Job>,
    TrackedFuture<impl Future<Output = Result<Option<Arc<DbChunk>>>> + Send>,
)> {
    let now = std::time::Instant::now(); // time persist duration.
    let db = Arc::clone(&partition.data().db);
    let addr = partition.addr().clone();
    let chunk_ids: Vec<_> = chunks.iter().map(|x| x.id()).collect();

    info!(%addr, ?chunk_ids, "splitting and persisting chunks");

    let max_persistable_timestamp = flush_handle.timestamp();
    let flush_timestamp = max_persistable_timestamp.timestamp_nanos();

    let (tracker, registration) = db.jobs.register(Job::PersistChunks {
        partition: partition.addr().clone(),
        chunks: chunk_ids.clone(),
    });

    // Mark and snapshot chunks, then drop locks
    let mut input_rows = 0;
    let mut time_of_first_write: Option<Time> = None;
    let mut time_of_last_write: Option<Time> = None;
    let mut query_chunks = vec![];
    let mut delete_predicates_before: HashSet<Arc<DeletePredicate>> = HashSet::new();
    let mut min_order = ChunkOrder::MAX;
    for mut chunk in chunks {
        // Sanity-check
        assert!(Arc::ptr_eq(&db, &chunk.data().db));
        assert_eq!(chunk.table_name().as_ref(), addr.table_name.as_ref());
        assert_eq!(chunk.key(), addr.partition_key.as_ref());

        input_rows += chunk.table_summary().total_count();

        let candidate_first = chunk.time_of_first_write();
        time_of_first_write = time_of_first_write
            .map(|prev_first| prev_first.min(candidate_first))
            .or(Some(candidate_first));

        let candidate_last = chunk.time_of_last_write();
        time_of_last_write = time_of_last_write
            .map(|prev_last| prev_last.max(candidate_last))
            .or(Some(candidate_last));

        delete_predicates_before.extend(chunk.delete_predicates().iter().cloned());

        min_order = min_order.min(chunk.order());

        chunk.set_writing_to_object_store(&registration)?;
        query_chunks.push(DbChunk::snapshot(&*chunk));
    }

    // drop partition lock guard
    let partition = partition.into_data().partition;

    let metric_registry = Arc::clone(&db.metric_registry);
    let ctx = db.exec.new_context(ExecutorType::Reorg);

    let fut = async move {
        if query_chunks.is_empty() {
            partition
                .write()
                .persistence_windows_mut()
                .unwrap()
                .flush(flush_handle);

            return Ok(None);
        }

        let time_of_first_write =
            time_of_first_write.expect("Should have had a first write somewhere");

        let time_of_last_write =
            time_of_last_write.expect("Should have had a last write somewhere");

        let key = compute_sort_key(query_chunks.iter().map(|x| x.summary()));
        let key_str = format!("\"{}\"", key); // for logging

        // build schema
        let schema = merge_schemas(&query_chunks);

        // Cannot move query_chunks as the sort key borrows the column names
        let (schema, plan) = ReorgPlanner::new().split_plan(
            schema,
            query_chunks.iter().map(Arc::clone),
            key,
            flush_timestamp,
        )?;

        let physical_plan = ctx.prepare_plan(&plan).await?;
        assert_eq!(
            physical_plan.output_partitioning().partition_count(),
            2,
            "Expected split plan to produce exactly 2 partitions"
        );

        let to_persist_stream = ctx
            .execute_stream_partitioned(Arc::clone(&physical_plan), 0)
            .await?;
        let remainder_stream = ctx.execute_stream_partitioned(physical_plan, 1).await?;

        let (to_persist, remainder) = futures::future::try_join(
            collect_rub(to_persist_stream, &addr, metric_registry.as_ref()),
            collect_rub(remainder_stream, &addr, metric_registry.as_ref()),
        )
        .await?;

        let persisted_rows = to_persist.as_ref().map(|p| p.rows()).unwrap_or(0);
        let remainder_rows = remainder.as_ref().map(|r| r.rows()).unwrap_or(0);

        let persist_fut = {
            let partition = LockableCatalogPartition::new(Arc::clone(&db), partition);
            let mut partition_write = partition.write();
            let mut delete_predicates_after: HashSet<Arc<DeletePredicate>> = HashSet::new();
            for id in &chunk_ids {
                let chunk = partition_write.force_drop_chunk(*id).expect(
                    "There was a lifecycle action attached to this chunk, who deleted it?!",
                );

                let chunk = chunk.read();
                for pred in chunk.delete_predicates() {
                    if !delete_predicates_before.contains(pred) {
                        delete_predicates_after.insert(Arc::clone(pred));
                    }
                }
            }

            let delete_predicates = {
                let mut tmp: Vec<_> = delete_predicates_after.into_iter().collect();
                tmp.sort();
                tmp
            };

            // Upsert remainder to catalog if any
            if let Some(remainder) = remainder {
                partition_write.create_rub_chunk(
                    remainder,
                    time_of_first_write,
                    time_of_last_write,
                    Arc::clone(&schema),
                    delete_predicates.clone(),
                    min_order,
                    None,
                );
            }

            let to_persist = match to_persist {
                Some(to_persist) => to_persist,
                None => {
                    info!(%addr, ?chunk_ids, "no rows to persist, no chunk created");
                    partition_write
                        .persistence_windows_mut()
                        .unwrap()
                        .flush(flush_handle);
                    return Ok(None);
                }
            };

            let (new_chunk_id, new_chunk) = partition_write.create_rub_chunk(
                to_persist,
                time_of_first_write,
                time_of_last_write,
                schema,
                delete_predicates,
                min_order,
                db.persisted_chunk_id_override.lock().as_ref().cloned(),
            );
            let to_persist = LockableCatalogChunk {
                db,
                chunk: Arc::clone(new_chunk),
                id: new_chunk_id,
                order: min_order,
            };
            let to_persist = to_persist.write();

            write_chunk_to_object_store(partition_write, to_persist, flush_handle)?.1
        };

        // Wait for write operation to complete
        let persisted_chunk = persist_fut.await??;

        let elapsed = now.elapsed();
        // input rows per second
        let throughput = (input_rows as u128 * 1_000_000_000) / elapsed.as_nanos();

        info!(input_chunks=query_chunks.len(),
              input_rows, persisted_rows, remainder_rows,
              sort_key=%key_str, compaction_took = ?elapsed,
              ?max_persistable_timestamp,
              rows_per_sec=?throughput,  "chunk(s) persisted");

        Ok(Some(persisted_chunk))
    };

    Ok((tracker, fut.track(registration)))
}

// Compact the provided object store chunks into a single object store chunk,
/// returning the newly created chunk
///
/// The function will error if
///    . No chunks are provided
///    . provided chunk(s) not belong to the provided partition
///    . not all provided chunks are persisted
///    . the provided chunks are not contiguous
/// Todo: update steps here when they are finalized
///       This is a long complicated function so document will help
pub(crate) fn compact_object_store_chunks(
    partition: LifecycleWriteGuard<'_, Partition, LockableCatalogPartition>,
    chunks: Vec<LifecycleWriteGuard<'_, CatalogChunk, LockableCatalogChunk>>,
    flush_handle: FlushHandle,
) -> Result<(
    TaskTracker<Job>,
    TrackedFuture<impl Future<Output = Result<Option<Arc<DbChunk>>>> + Send>,
)> {
    // no chunks provided
    if chunks.is_empty() {
        return EmptyChunks {}.fail();
    }

    // tracking compaction duration
    let now = std::time::Instant::now();

    let db = Arc::clone(&partition.data().db);
    let partition_addr = partition.addr().clone();
    let chunk_ids: Vec<_> = chunks.iter().map(|x| x.id()).collect();
    info!(%partition_addr, ?chunk_ids, "compacting object store chunks");

    let (tracker, registration) = db.jobs.register(Job::CompactObjectStoreChunks {
        partition: partition.addr().clone(),
        chunks: chunk_ids.clone(),
    });

    // Mark and snapshot chunks, then drop locks
    let mut input_rows = 0;
    let mut time_of_first_write: Option<Time> = None;
    let mut time_of_last_write: Option<Time> = None;
    let mut delete_predicates_before: HashSet<Arc<DeletePredicate>> = HashSet::new();
    let mut min_order = ChunkOrder::MAX;
    let mut chunk_orders = BTreeSet::new();
    let query_chunks = chunks
        .into_iter()
        .map(|mut chunk| {
            // Sanity-check
            assert!(Arc::ptr_eq(&db, &chunk.data().db));
            assert_eq!(
                chunk.table_name().as_ref(),
                partition_addr.table_name.as_ref()
            );

            // provided chunks not in the provided partition
            if chunk.key() != partition_addr.partition_key.as_ref() {
                return ChunksNotInPartition {}.fail();
            }

            input_rows += chunk.table_summary().total_count();

            let candidate_first = chunk.time_of_first_write();
            time_of_first_write = time_of_first_write
                .map(|prev_first| prev_first.min(candidate_first))
                .or(Some(candidate_first));

            let candidate_last = chunk.time_of_last_write();
            time_of_last_write = time_of_last_write
                .map(|prev_last| prev_last.max(candidate_last))
                .or(Some(candidate_last));

            delete_predicates_before.extend(chunk.delete_predicates().iter().cloned());

            min_order = min_order.min(chunk.order());
            chunk_orders.insert(chunk.order());

            // Set chunk in the right action which is compacting object store
            // This function will also error out if the chunk is not yet persisted
            chunk.set_compacting_object_store(&registration)?;
            Ok(DbChunk::snapshot(&*chunk))
        })
        .collect::<Result<Vec<_>>>()?;

    // Verify if all the provided chunks are contiguous
    if !partition.contiguous_object_store_chunks(&chunk_orders) {
        return ChunksNotContiguous {}.fail();
    }

    // drop partition lock
    let partition = partition.into_data().partition;

    let time_of_first_write = time_of_first_write.expect("Should have had a first write somewhere");
    let time_of_last_write = time_of_last_write.expect("Should have had a last write somewhere");
    // Tracking metric
    let metric_registry = Arc::clone(&db.metric_registry);
    let ctx = db.exec.new_context(ExecutorType::Reorg);

    // Now let start compacting
    let fut = async move {
        let fut_now = std::time::Instant::now();

        // Compute the sorted output of the compacting result
        let key = compute_sort_key(query_chunks.iter().map(|x| x.summary()));
        let key_str = format!("\"{}\"", key); // for logging

        // Merge schema of the compacting chunks
        let schema = merge_schemas(&query_chunks);

        // Compacting query plan
        let (schema, plan) =
            ReorgPlanner::new().compact_plan(schema, query_chunks.iter().map(Arc::clone), key)?;
        let physical_plan = ctx.prepare_plan(&plan).await?;
        // run the plan
        let stream = ctx.execute_stream(physical_plan).await?;

        // create a read buffer chunk for persisting the compacted one
        // todo: after the code works well, this will be improved to pass this RUB pass
        let persisting_chunk =
            collect_rub(stream, &partition_addr, metric_registry.as_ref()).await?;

        // number of rows to be persisted to log when it is done
        let persisted_rows = persisting_chunk.as_ref().map(|p| p.rows()).unwrap_or(0);

        let persist_fut = {
            let partition = LockableCatalogPartition::new(Arc::clone(&db), partition);

            // Drop in-memory compacted chunks
            let mut partition = partition.write();
            let mut delete_predicates_after = HashSet::new();
            for id in &chunk_ids {
                let chunk = partition.force_drop_chunk(*id).expect(
                    "There was a lifecycle action attached to this chunk, who deleted it?!",
                );
                // Keep the delete predicates newly added during the compacting process
                let chunk = chunk.read();
                for pred in chunk.delete_predicates() {
                    if !delete_predicates_before.contains(pred) {
                        delete_predicates_after.insert(Arc::clone(pred));
                    }
                }
            }

            let delete_predicates = {
                let mut tmp: Vec<_> = delete_predicates_after.into_iter().collect();
                tmp.sort();
                tmp
            };

            // nothing persisted if all rows are hard deleted  while compacting
            let persisting_chunk = match persisting_chunk {
                Some(persisting_chunk) => persisting_chunk,
                None => {
                    info!(%partition_addr, ?chunk_ids, "no rows after compacting to get persisted , no chunk created");
                    partition
                        .persistence_windows_mut()
                        .unwrap()
                        .flush(flush_handle);

                    // Todo: Need a way to drop compacted chunks here before returning
                    // We cannot call the code below because rust does not know that we no longer need to use partition
                    // for id in chunk_ids {
                    //     db.drop_chunk(&*partition_addr.table_name, &*partition_addr.partition_key, id).await;
                    // }

                    return Ok(None);
                }
            };

            // Create a RUB chunk before persisting
            // This RUB will be unloaded as needed during the lifecycle's maybe_free_memory
            // todo: this step will be improved to avoid creating RUB if needed
            let (new_chunk_id, new_chunk) = partition.create_rub_chunk(
                persisting_chunk,
                time_of_first_write,
                time_of_last_write,
                schema,
                delete_predicates,
                min_order,
                db.persisted_chunk_id_override.lock().as_ref().cloned(),
            );
            let persisting_chunk = LockableCatalogChunk {
                db: Arc::clone(&db),
                chunk: Arc::clone(new_chunk),
                id: new_chunk_id,
                order: min_order,
            };

            // Now write the chunk to object store
            let persisting_chunk = persisting_chunk.write();
            write_chunk_to_object_store(partition, persisting_chunk, flush_handle)?.1
        };

        // Wait for write operation to complete
        let persisted_chunk = persist_fut.await??;

        // Now drop compacted chunks from preserved catalog before returning
        for id in &chunk_ids {
            // todo: return context for error
            let _result = db
                .drop_chunk(
                    &*partition_addr.table_name,
                    &*partition_addr.partition_key,
                    *id,
                )
                .await;
        }

        // input rows per second
        let elapsed = now.elapsed();
        let throughput = (input_rows as u128 * 1_000_000_000) / elapsed.as_nanos();

        info!(input_chunks=chunk_ids.len(),
            %input_rows, %persisted_rows,
            sort_key=%key_str, compaction_took = ?elapsed,
            fut_execution_duration= ?fut_now.elapsed(),
            rows_per_sec=?throughput,  "object store chunk(s) compacted");

        Ok(Some(persisted_chunk))
    };

    Ok((tracker, fut.track(registration)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        db::{catalog::Catalog, load::load_or_create_preserved_catalog, test_helpers::write_lp},
        utils::TestDb,
        Db,
    };

    use data_types::{
        chunk_metadata::ChunkStorage,
        database_rules::LifecycleRules,
        delete_predicate::{DeleteExpr, Op, Scalar},
        server_id::ServerId,
        timestamp::TimestampRange,
    };
    use lifecycle::{LockableChunk, LockablePartition};
    use object_store::ObjectStore;
    use query::{QueryChunk, QueryDatabase};
    use std::{
        convert::TryFrom,
        num::{NonZeroU32, NonZeroU64},
        time::Duration,
    };
    use time::Time;

    async fn test_db() -> (Arc<Db>, Arc<time::MockProvider>) {
        let time_provider = Arc::new(time::MockProvider::new(Time::from_timestamp(3409, 45)));
        let test_db = TestDb::builder()
            .lifecycle_rules(LifecycleRules {
                late_arrive_window_seconds: NonZeroU32::new(1).unwrap(),
                // Disable lifecycle manager - TODO: Better way to do this, as this will still run the loop once
                worker_backoff_millis: NonZeroU64::new(u64::MAX).unwrap(),
                ..Default::default()
            })
            .time_provider(Arc::<time::MockProvider>::clone(&time_provider))
            .build()
            .await;

        (test_db.db, time_provider)
    }

    #[tokio::test]
    async fn test_flush_overlapping() {
        let (db, time) = test_db().await;
        write_lp(db.as_ref(), "cpu,tag1=cupcakes bar=1 10").await;

        let partition_keys = db.partition_keys().unwrap();
        assert_eq!(partition_keys.len(), 1);
        let db_partition = db.partition("cpu", &partition_keys[0]).unwrap();

        // Close window
        time.inc(Duration::from_secs(2));

        write_lp(db.as_ref(), "cpu,tag1=lagged bar=1 10").await;

        let partition = LockableCatalogPartition::new(Arc::clone(&db), Arc::clone(&db_partition));
        let partition = partition.read();

        let chunks = LockablePartition::chunks(&partition);
        let chunks = chunks.iter().map(|x| x.read());

        let mut partition = partition.upgrade();

        let handle = LockablePartition::prepare_persist(&mut partition, false)
            .unwrap()
            .0;

        assert_eq!(handle.timestamp(), Time::from_timestamp_nanos(10));
        let chunks: Vec<_> = chunks.map(|x| x.upgrade()).collect();

        persist_chunks(partition, chunks, handle)
            .unwrap()
            .1
            .await
            .unwrap()
            .unwrap();

        assert!(db_partition
            .read()
            .persistence_windows()
            .unwrap()
            .minimum_unpersisted_age()
            .is_none());
    }

    #[tokio::test]
    async fn test_persist_delete_all() {
        let (db, time) = test_db().await;

        let late_arrival = Duration::from_secs(1);

        time.inc(Duration::from_secs(32));
        write_lp(db.as_ref(), "cpu,tag1=cupcakes bar=1 10").await;

        time.inc(late_arrival);
        write_lp(db.as_ref(), "cpu,tag1=cupcakes bar=3 23").await;

        let partition_keys = db.partition_keys().unwrap();
        assert_eq!(partition_keys.len(), 1);
        let partition_key = partition_keys.into_iter().next().unwrap();
        let partition = db.partition("cpu", partition_key.as_str()).unwrap();

        // Delete first row
        let predicate = Arc::new(DeletePredicate {
            range: TimestampRange { start: 0, end: 20 },
            exprs: vec![],
        });

        db.delete("cpu", predicate).await.unwrap();

        // Try to persist first write but it has been deleted
        let maybe_chunk = db
            .persist_partition("cpu", partition_key.as_str(), false)
            .await
            .unwrap();

        assert!(maybe_chunk.is_none());

        let chunks: Vec<_> = partition.read().chunk_summaries().collect();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].storage, ChunkStorage::ReadBuffer);
        assert_eq!(chunks[0].row_count, 1);

        assert_eq!(
            partition
                .read()
                .persistence_windows()
                .unwrap()
                .minimum_unpersisted_timestamp()
                .unwrap(),
            Time::from_timestamp_nanos(23)
        );

        // Add a second set of writes one of which overlaps the above chunk
        time.inc(late_arrival * 10);
        write_lp(db.as_ref(), "cpu,tag1=foo bar=2 23").await;
        write_lp(db.as_ref(), "cpu,tag1=cupcakes bar=2 26").await;

        // Persist second write but not third
        let maybe_chunk = db
            .persist_partition("cpu", partition_key.as_str(), false)
            .await
            .unwrap();
        assert!(maybe_chunk.is_some());

        assert_eq!(
            partition
                .read()
                .persistence_windows()
                .unwrap()
                .minimum_unpersisted_timestamp()
                .unwrap(),
            // The persistence windows only know that all rows <= 23 have been persisted
            // They do not know that the remaining row has timestamp 26, only that
            // it is in the range 24..=26
            Time::from_timestamp_nanos(24)
        );

        let mut chunks: Vec<_> = partition.read().chunk_summaries().collect();
        chunks.sort_by_key(|c| c.storage);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].storage, ChunkStorage::ReadBuffer);
        assert_eq!(chunks[0].row_count, 1);
        assert_eq!(chunks[1].storage, ChunkStorage::ReadBufferAndObjectStore);
        assert_eq!(chunks[1].row_count, 2);

        // Delete everything
        let predicate = Arc::new(DeletePredicate {
            range: TimestampRange {
                start: 0,
                end: 1_000,
            },
            exprs: vec![],
        });

        db.delete("cpu", predicate).await.unwrap();

        // Try to persist third set of writes
        time.inc(late_arrival);
        let maybe_chunk = db
            .persist_partition("cpu", partition_key.as_str(), false)
            .await
            .unwrap();

        assert!(maybe_chunk.is_none());

        // The already persisted chunk should remain
        let chunks: Vec<_> = partition.read().chunk_summaries().collect();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].storage, ChunkStorage::ReadBufferAndObjectStore);
        assert_eq!(chunks[0].row_count, 2);

        assert!(partition.read().persistence_windows().unwrap().is_empty());
    }

    #[tokio::test]
    async fn persist_compacted_deletes() {
        let (db, time) = test_db().await;

        let late_arrival = Duration::from_secs(1);
        write_lp(db.as_ref(), "cpu,tag1=cupcakes bar=1 10").await;

        let partition_keys = db.partition_keys().unwrap();
        assert_eq!(partition_keys.len(), 1);
        let partition_key = partition_keys.into_iter().next().unwrap();

        let partition = db.partition("cpu", partition_key.as_str()).unwrap();

        // Cannot simply use empty predicate (#2687)
        let predicate = Arc::new(DeletePredicate {
            range: TimestampRange {
                start: 0,
                end: 1_000,
            },
            exprs: vec![],
        });

        // Delete everything
        db.delete("cpu", predicate).await.unwrap();

        // Compact deletes away
        let chunk = db
            .compact_partition("cpu", partition_key.as_str())
            .await
            .unwrap();

        assert!(chunk.is_none());

        // Persistence windows unaware rows have been deleted
        assert!(!partition.read().persistence_windows().unwrap().is_empty());

        time.inc(late_arrival);
        let maybe_chunk = db
            .persist_partition("cpu", partition_key.as_str(), false)
            .await
            .unwrap();

        assert!(maybe_chunk.is_none());
        assert!(partition.read().persistence_windows().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_delete_predicate_propagation() {
        // setup DB
        let object_store = Arc::new(ObjectStore::new_in_memory());
        let server_id = ServerId::try_from(1).unwrap();
        let db_name = "delete_predicate_propagation";
        let test_db = TestDb::builder()
            .object_store(Arc::clone(&object_store))
            .server_id(server_id)
            .db_name(db_name)
            .lifecycle_rules(LifecycleRules {
                // do not create checkpoints
                catalog_transactions_until_checkpoint: NonZeroU64::new(u64::MAX).unwrap(),
                late_arrive_window_seconds: NonZeroU32::new(1).unwrap(),
                // Disable lifecycle manager - TODO: Better way to do this, as this will still run the loop once
                worker_backoff_millis: NonZeroU64::new(u64::MAX).unwrap(),
                ..Default::default()
            })
            .build()
            .await;
        let db = test_db.db;

        // | foo | delete before persist | delete during persist |
        // | --- | --------------------- | --------------------- |
        // |   1 |                   yes |                    no |
        // |   2 |                   yes |                   yes |
        // |   3 |                    no |                   yes |
        // |   4 |                    no |                    no |
        write_lp(db.as_ref(), "cpu foo=1 10").await;
        write_lp(db.as_ref(), "cpu foo=2 20").await;
        write_lp(db.as_ref(), "cpu foo=3 20").await;
        write_lp(db.as_ref(), "cpu foo=4 20").await;

        let range = TimestampRange {
            start: 0,
            end: 1_000,
        };
        let pred1 = Arc::new(DeletePredicate {
            range,
            exprs: vec![DeleteExpr::new("foo".to_string(), Op::Eq, Scalar::I64(1))],
        });
        let pred2 = Arc::new(DeletePredicate {
            range,
            exprs: vec![DeleteExpr::new("foo".to_string(), Op::Eq, Scalar::I64(2))],
        });
        let pred3 = Arc::new(DeletePredicate {
            range,
            exprs: vec![DeleteExpr::new("foo".to_string(), Op::Eq, Scalar::I64(3))],
        });
        db.delete("cpu", Arc::clone(&pred1)).await.unwrap();
        db.delete("cpu", Arc::clone(&pred2)).await.unwrap();

        // start persistence job (but don't poll the future yet)
        let partition_keys = db.partition_keys().unwrap();
        assert_eq!(partition_keys.len(), 1);
        let db_partition = db.partition("cpu", &partition_keys[0]).unwrap();

        // Wait for the persistence window to be closed
        let partition = LockableCatalogPartition::new(Arc::clone(&db), Arc::clone(&db_partition));
        let partition = partition.read();

        let chunks = LockablePartition::chunks(&partition);
        let chunks = chunks.iter().map(|x| x.read());

        let mut partition = partition.upgrade();

        let handle = LockablePartition::prepare_persist(&mut partition, true)
            .unwrap()
            .0;

        assert_eq!(handle.timestamp(), Time::from_timestamp_nanos(20));
        let chunks: Vec<_> = chunks.map(|x| x.upgrade()).collect();

        let (_, fut) = persist_chunks(partition, chunks, handle).unwrap();

        // add more delete predicates
        db.delete("cpu", Arc::clone(&pred2)).await.unwrap();
        db.delete("cpu", Arc::clone(&pred3)).await.unwrap();

        // finish future
        tokio::spawn(fut).await.unwrap().unwrap().unwrap();

        // check in-mem delete predicates
        let check_closure = |catalog: &Catalog| {
            let chunks = catalog.chunks();
            assert_eq!(chunks.len(), 1);
            let chunk = &chunks[0];
            let chunk = chunk.read();
            let actual = chunk.delete_predicates();
            let expected = vec![Arc::clone(&pred3)];
            assert_eq!(actual, &expected);
        };
        check_closure(&db.catalog);

        // check object store delete predicates
        let metric_registry = Arc::new(metric::Registry::new());
        let (_preserved_catalog, catalog, _replay_plan) = load_or_create_preserved_catalog(
            db_name,
            Arc::clone(&db.iox_object_store),
            metric_registry,
            Arc::clone(&db.time_provider),
            false,
            false,
        )
        .await
        .unwrap();
        check_closure(&catalog);
    }

    #[tokio::test]
    async fn test_compact_os_negative() {
        // Tests that nothing will get compacted

        test_helpers::maybe_start_logging();

        let (db, time) = test_db().await;
        let late_arrival = Duration::from_secs(1);
        write_lp(db.as_ref(), "cpu,tag1=cupcakes bar=1 10").await;
        // todo: ask Marco why we need this time.inc function? it closes what?
        time.inc(late_arrival);

        let partition_keys = db.partition_keys().unwrap();
        assert_eq!(partition_keys.len(), 1);
        let db_partition = db.partition("cpu", &partition_keys[0]).unwrap();

        // Test 1: no chunks provided
        let partition = LockableCatalogPartition::new(Arc::clone(&db), Arc::clone(&db_partition));
        let mut partition = partition.read().upgrade();
        let handle = LockablePartition::prepare_persist(&mut partition, false)
            .unwrap()
            .0;
        let compact_no_chunks = compact_object_store_chunks(partition, vec![], handle);
        assert!(compact_no_chunks.is_err());

        // test 2: persisted non persisted chunks
        let partition = LockableCatalogPartition::new(Arc::clone(&db), Arc::clone(&db_partition));
        let partition = partition.read();
        let chunks = LockablePartition::chunks(&partition);
        assert_eq!(chunks.len(), 1);
        let mut partition = partition.upgrade();
        let handle = LockablePartition::prepare_persist(&mut partition, false)
            .unwrap()
            .0;
        let chunk = chunks[0].read();
        let compact_non_persisted_chunks =
            compact_object_store_chunks(partition, vec![chunk.upgrade()], handle);
        assert!(compact_non_persisted_chunks.is_err());

        // test 3: persisted non-contiguous chunks
        // persist chunk 1
        db.persist_partition("cpu", partition_keys[0].as_str(), true)
            .await
            .unwrap()
            .unwrap()
            .id();
        //
        // persist chunk 2
        write_lp(db.as_ref(), "cpu,tag1=chunk2,tag2=a bar=2 10").await;
        db.persist_partition("cpu", partition_keys[0].as_str(), true)
            .await
            .unwrap()
            .unwrap()
            .id();
        //
        // persist chunk 3
        write_lp(db.as_ref(), "cpu,tag1=chunk3,tag2=a bar=2 30").await;
        db.persist_partition("cpu", partition_keys[0].as_str(), true)
            .await
            .unwrap()
            .unwrap()
            .id();
        //
        // Add a MUB
        write_lp(db.as_ref(), "cpu,tag1=chunk4,tag2=a bar=2 40").await;
        // todo: Need to ask Marco why there is no handle created here
        time.inc(Duration::from_secs(40));
        //
        // let compact 2 non contiguous chunk 1 and chunk 3
        let partition = LockableCatalogPartition::new(Arc::clone(&db), Arc::clone(&db_partition));
        let partition = partition.read();
        let chunks = LockablePartition::chunks(&partition);
        assert_eq!(chunks.len(), 4);
        let mut partition = partition.upgrade();
        let handle = LockablePartition::prepare_persist(&mut partition, false)
            .unwrap()
            .0;
        let chunk1 = chunks[0].read();
        let chunk3 = chunks[2].read();
        let compact_non_persisted_chunks = compact_object_store_chunks(
            partition,
            vec![chunk1.upgrade(), chunk3.upgrade()],
            handle,
        );
        assert!(compact_non_persisted_chunks.is_err());
    }

    #[tokio::test]
    async fn test_compact_os_chunk_1_2() {
        test_helpers::maybe_start_logging();

        let (db, time) = test_db().await;
        write_lp(db.as_ref(), "cpu,tag1=cupcakes bar=1 10").await;
        let partition_keys = db.partition_keys().unwrap();
        assert_eq!(partition_keys.len(), 1);
        let db_partition = db.partition("cpu", &partition_keys[0]).unwrap();

        // persist chunk 1
        let chunk_id_1 = db
            .persist_partition("cpu", partition_keys[0].as_str(), true)
            .await
            .unwrap()
            .unwrap()
            .id();

        // persist chunk 2
        write_lp(db.as_ref(), "cpu,tag1=cookies bar=2 20").await;
        let chunk_id_2 = db
            .persist_partition("cpu", partition_keys[0].as_str(), true)
            .await
            .unwrap()
            .unwrap()
            .id();

        // persist chunk 3
        write_lp(db.as_ref(), "cpu,tag1=cookies bar=2 20").await;
        let chunk_id_3 = db
            .persist_partition("cpu", partition_keys[0].as_str(), true)
            .await
            .unwrap()
            .unwrap()
            .id();

        // drop RUBs but keep the OSs
        db.unload_read_buffer("cpu", partition_keys[0].as_str(), chunk_id_1)
            .unwrap();
        db.unload_read_buffer("cpu", partition_keys[0].as_str(), chunk_id_2)
            .unwrap();
        db.unload_read_buffer("cpu", partition_keys[0].as_str(), chunk_id_3)
            .unwrap();

        // Add MUB
        write_lp(db.as_ref(), "cpu,tag1=brownies bar=2 30").await;
        time.inc(Duration::from_secs(40));

        // Verify results before OS compacting
        let partition = LockableCatalogPartition::new(Arc::clone(&db), Arc::clone(&db_partition));
        let partition = partition.read();
        let chunks = LockablePartition::chunks(&partition);
        assert_eq!(chunks.len(), 4);
        // ensure all RUBs are unloaded
        let mut summary_chunks: Vec<_> = partition.chunk_summaries().collect();
        assert_eq!(summary_chunks.len(), 4);
        summary_chunks.sort_by_key(|c| c.storage);
        assert_eq!(summary_chunks[0].storage, ChunkStorage::OpenMutableBuffer);
        assert_eq!(summary_chunks[0].row_count, 1);
        assert_eq!(summary_chunks[1].storage, ChunkStorage::ObjectStoreOnly);
        assert_eq!(summary_chunks[1].row_count, 1);
        assert_eq!(summary_chunks[2].storage, ChunkStorage::ObjectStoreOnly);
        assert_eq!(summary_chunks[2].row_count, 1);
        assert_eq!(summary_chunks[3].storage, ChunkStorage::ObjectStoreOnly);
        assert_eq!(summary_chunks[3].row_count, 1);

        // let compact OS chunks 1 and 2
        let mut partition = partition.upgrade();
        let handle = LockablePartition::prepare_persist(&mut partition, false)
            .unwrap()
            .0;
        let chunk1 = chunks[0].read();
        let chunk2 = chunks[1].read();
        compact_object_store_chunks(partition, vec![chunk1.upgrade(), chunk2.upgrade()], handle)
            .unwrap()
            .1
            .await
            .unwrap()
            .unwrap();

        // verify results
        let partition = db.partition("cpu", partition_keys[0].as_str()).unwrap();
        let mut summary_chunks: Vec<_> = partition.read().chunk_summaries().collect();
        summary_chunks.sort_by_key(|c| c.storage);
        assert_eq!(summary_chunks.len(), 3);
        // MUB
        assert_eq!(summary_chunks[0].storage, ChunkStorage::OpenMutableBuffer);
        assert_eq!(summary_chunks[0].row_count, 1);
        // the result of compacting 2 persisted chunks
        assert_eq!(
            summary_chunks[1].storage,
            ChunkStorage::ReadBufferAndObjectStore
        );
        assert_eq!(summary_chunks[1].row_count, 2);
        // OS (chunk_id_3 above) but not compacted in compacting_os
        assert_eq!(summary_chunks[2].storage, ChunkStorage::ObjectStoreOnly);
        assert_eq!(summary_chunks[2].row_count, 1);
    }

    #[tokio::test]
    async fn test_compact_os_for_hard_delete() {
        test_helpers::maybe_start_logging();

        let (db, time) = test_db().await;
        write_lp(db.as_ref(), "cpu,tag1=cupcakes bar=1 10").await;
        write_lp(db.as_ref(), "cpu,tag1=cookies bar=2 10").await; // delete
        let partition_keys = db.partition_keys().unwrap();
        assert_eq!(partition_keys.len(), 1);
        let db_partition = db.partition("cpu", &partition_keys[0]).unwrap();

        // persist chunk 1
        let _chunk_id_1 = db
            .persist_partition("cpu", partition_keys[0].as_str(), true)
            .await
            .unwrap()
            .unwrap()
            .id();

        // persist chunk 2
        write_lp(db.as_ref(), "cpu,tag1=cookies bar=2 20").await; // delete
        write_lp(db.as_ref(), "cpu,tag1=cookies bar=3 30").await; // duplicate & delete
        write_lp(db.as_ref(), "cpu,tag1=cupcakes bar=2 20").await;
        let chunk_id_2 = db
            .persist_partition("cpu", partition_keys[0].as_str(), true)
            .await
            .unwrap()
            .unwrap()
            .id();

        // persist chunk 3
        write_lp(db.as_ref(), "cpu,tag1=cookies bar=2 20").await; // delete
        let _chunk_id_3 = db
            .persist_partition("cpu", partition_keys[0].as_str(), true)
            .await
            .unwrap()
            .unwrap()
            .id();

        // drop chunk_id_2 RUB and keep the other 2
        db.unload_read_buffer("cpu", partition_keys[0].as_str(), chunk_id_2)
            .unwrap();

        // Delete all cookies
        let predicate = Arc::new(DeletePredicate {
            range: TimestampRange { start: 0, end: 30 },
            exprs: vec![DeleteExpr::new(
                "tag1".to_string(),
                data_types::delete_predicate::Op::Eq,
                data_types::delete_predicate::Scalar::String("cookies".to_string()),
            )],
        });
        db.delete("cpu", predicate).await.unwrap();

        // Add a MUB
        write_lp(db.as_ref(), "cpu,tag1=brownies bar=2 30").await;
        time.inc(Duration::from_secs(40));

        // Verify results before OS compacting
        let partition = LockableCatalogPartition::new(Arc::clone(&db), Arc::clone(&db_partition));
        let partition = partition.read();
        let chunks = LockablePartition::chunks(&partition);
        assert_eq!(chunks.len(), 4);
        // ensure all RUBs are unloaded
        let mut summary_chunks: Vec<_> = partition.chunk_summaries().collect();
        assert_eq!(summary_chunks.len(), 4);
        summary_chunks.sort_by_key(|c| c.storage);
        // MUB
        assert_eq!(summary_chunks[0].storage, ChunkStorage::OpenMutableBuffer);
        assert_eq!(summary_chunks[0].row_count, 1);
        // RUB_and_OS chunk_id_1
        assert_eq!(
            summary_chunks[1].storage,
            ChunkStorage::ReadBufferAndObjectStore
        );
        assert_eq!(summary_chunks[1].row_count, 2);
        // RUB_and_OS chunk_id_3
        assert_eq!(
            summary_chunks[2].storage,
            ChunkStorage::ReadBufferAndObjectStore
        );
        assert_eq!(summary_chunks[2].row_count, 1);
        // OS chunk_id_2
        assert_eq!(summary_chunks[3].storage, ChunkStorage::ObjectStoreOnly);
        assert_eq!(summary_chunks[3].row_count, 3);

        // let compact chunks 1, 2 and 3
        let mut partition = partition.upgrade();
        let handle = LockablePartition::prepare_persist(&mut partition, false)
            .unwrap()
            .0;
        let chunk1 = chunks[0].read();
        let chunk2 = chunks[1].read();
        let chunk3 = chunks[2].read();
        compact_object_store_chunks(
            partition,
            vec![chunk1.upgrade(), chunk2.upgrade(), chunk3.upgrade()],
            handle,
        )
        .unwrap()
        .1
        .await
        .unwrap()
        .unwrap();

        // verify results
        let partition = db.partition("cpu", partition_keys[0].as_str()).unwrap();
        let mut summary_chunks: Vec<_> = partition.read().chunk_summaries().collect();
        summary_chunks.sort_by_key(|c| c.storage);
        assert_eq!(summary_chunks.len(), 2);
        // MUB
        assert_eq!(summary_chunks[0].storage, ChunkStorage::OpenMutableBuffer);
        assert_eq!(summary_chunks[0].row_count, 1);
        // the result of compacting 2 persisted chunks
        assert_eq!(
            summary_chunks[1].storage,
            ChunkStorage::ReadBufferAndObjectStore
        );
        // 2 rows left because 4 duplicated & deleted rows have been removed
        assert_eq!(summary_chunks[1].row_count, 2);
    }
}

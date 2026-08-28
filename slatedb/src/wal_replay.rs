use crate::block_iterator::DataBlockIterator;
use crate::config::WalReplaySettings;
use crate::error::SlateDBError;
use crate::format::block::Block;
use crate::iter::{IterationOrder, RowEntryIterator};
use crate::manifest::ManifestCore;
use crate::mem_table::WritableKVTable;
use crate::tablestore::{DecodedWalSst, TableStore};
use crate::types::RowEntry;
use crate::utils::panic_string;
use async_trait::async_trait;
use bytes::Bytes;
use log::{error, info};
use std::collections::{BTreeMap, VecDeque};
use std::ops::Range;
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;

pub(crate) struct WalReplayOptions {
    /// Limits concurrent full-object WAL prefetch.
    pub(crate) prefetch: WalReplaySettings,

    /// The target maximum number of bytes in each returned table. WAL replay only
    /// splits between complete WAL SSTs, so a returned table may exceed this if a
    /// single WAL SST is larger.
    pub(crate) max_memtable_bytes: usize,

    /// The minimum seq number to replay. If unset, will replay all
    /// entries after `last_l0_seq` in the manifest.
    pub(crate) min_seq: Option<u64>,
}

impl Default for WalReplayOptions {
    fn default() -> Self {
        Self {
            prefetch: WalReplaySettings::default(),
            max_memtable_bytes: 64 * 1024 * 1024,
            min_seq: None,
        }
    }
}

pub(crate) struct ReplayedMemtable {
    pub(crate) table: WritableKVTable,
    pub(crate) last_tick: i64,
    pub(crate) last_seq: u64,
    pub(crate) last_wal_id: u64,
}

struct WalIdAndIter {
    wal_id: u64,
    iter: Box<dyn RowEntryIterator + 'static>,
    _permit: OwnedSemaphorePermit,
}

struct WalObjectPlan {
    wal_id: u64,
    expected_size: Option<u64>,
}

struct FetchedWal {
    wal_id: u64,
    bytes: Bytes,
    permit: OwnedSemaphorePermit,
}

struct PendingWal {
    wal_id: u64,
    handle: JoinHandle<Result<FetchedWal, SlateDBError>>,
}

struct WalBlocksIterator {
    format_version: u16,
    blocks: VecDeque<Block>,
    current: Option<DataBlockIterator<Block>>,
}

impl WalBlocksIterator {
    fn new(format_version: u16, blocks: VecDeque<Block>) -> Self {
        Self {
            format_version,
            blocks,
            current: None,
        }
    }
}

#[async_trait]
impl RowEntryIterator for WalBlocksIterator {
    async fn init(&mut self) -> Result<(), SlateDBError> {
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        loop {
            if let Some(current) = &mut self.current {
                if let Some(entry) = current.next().await? {
                    return Ok(Some(entry));
                }
            }
            let Some(block) = self.blocks.pop_front() else {
                return Ok(None);
            };
            self.current = Some(DataBlockIterator::new(
                block,
                self.format_version,
                IterationOrder::Ascending,
            )?);
        }
    }

    async fn seek(&mut self, _next_key: &[u8]) -> Result<(), SlateDBError> {
        Err(SlateDBError::InvalidDBState)
    }
}

struct IteratorHolder<T> {
    initialized: bool,
    current_iter: Option<T>,
}

impl<T> IteratorHolder<T> {
    fn new() -> Self {
        Self {
            initialized: false,
            current_iter: None,
        }
    }

    fn is_finished(&self) -> bool {
        self.initialized && self.current_iter.is_none()
    }

    fn advance(&mut self, iterator: Option<T>) {
        self.initialized = true;
        self.current_iter = iterator;
    }

    fn reset(&mut self) {
        self.initialized = false;
        self.current_iter = None;
    }
}

pub(crate) struct WalReplayIterator {
    options: WalReplayOptions,
    wal_id_range: Range<u64>,
    table_store: Arc<TableStore>,
    current_iter: IteratorHolder<WalIdAndIter>,
    pending_plans: VecDeque<WalObjectPlan>,
    pending_fetches: VecDeque<PendingWal>,
    byte_semaphore: Arc<Semaphore>,
    last_tick: i64,
    last_seq: u64,
    min_seq: u64,
    fetched_objects: u64,
    fetched_bytes: u64,
    decoded_objects: u64,
    peak_concurrent_objects: usize,
    peak_reserved_bytes: usize,
    completion_logged: bool,
}

impl WalReplayIterator {
    pub(crate) async fn range(
        wal_id_range: Range<u64>,
        db_state: &ManifestCore,
        options: WalReplayOptions,
        table_store: Arc<TableStore>,
    ) -> Result<Self, SlateDBError> {
        options.prefetch.validate()?;

        // load the last seq number from manifest, and use it as the starting seq number to avoid
        // replaying the entries that are already in the L0 SST. while replaying the WALs, we'll
        // update the last seq number to the max seq number, and this final `last_seq` will be passed
        // to the db_state for the further writes.
        let min_seq = options.min_seq.unwrap_or(db_state.last_l0_seq);
        let last_seq = db_state.last_l0_seq;
        let last_tick = db_state.last_l0_clock_tick;
        let listed_sizes = table_store
            .list_wal_ssts_for_replay(wal_id_range.clone())
            .await?
            .into_iter()
            .map(|metadata| (metadata.id.unwrap_wal_id(), metadata.metadata.size))
            .collect::<BTreeMap<_, _>>();
        let pending_plans = wal_id_range
            .clone()
            .map(|wal_id| WalObjectPlan {
                wal_id,
                expected_size: listed_sizes.get(&wal_id).copied(),
            })
            .collect();
        let byte_semaphore = Arc::new(Semaphore::new(options.prefetch.max_inflight_bytes));

        let mut replay_iter = WalReplayIterator {
            options,
            wal_id_range,
            table_store: Arc::clone(&table_store),
            current_iter: IteratorHolder::new(),
            pending_plans,
            pending_fetches: VecDeque::new(),
            byte_semaphore,
            last_tick,
            last_seq,
            min_seq,
            fetched_objects: 0,
            fetched_bytes: 0,
            decoded_objects: 0,
            peak_concurrent_objects: 0,
            peak_reserved_bytes: 0,
            completion_logged: false,
        };

        replay_iter.fill_prefetch();
        info!(
            "SlateDB WAL full-object replay initialized [replay_start_wal_id={}, replay_end_wal_id={}, replay_wal_count={}, listed_wal_count={}, missing_size_count={}, max_concurrent_objects={}, max_inflight_bytes={}]",
            replay_iter.wal_id_range.start,
            replay_iter.wal_id_range.end,
            replay_iter
                .wal_id_range
                .end
                .saturating_sub(replay_iter.wal_id_range.start),
            listed_sizes.len(),
            replay_iter
                .wal_id_range
                .end
                .saturating_sub(replay_iter.wal_id_range.start)
                .saturating_sub(listed_sizes.len() as u64),
            replay_iter.options.prefetch.max_concurrent_objects,
            replay_iter.options.prefetch.max_inflight_bytes,
        );

        Ok(replay_iter)
    }

    fn fill_prefetch(&mut self) {
        while self.pending_fetches.len() < self.options.prefetch.max_concurrent_objects {
            let Some(plan) = self.pending_plans.front() else {
                break;
            };
            let byte_limit = self.options.prefetch.max_inflight_bytes;
            let reserved_bytes = plan
                .expected_size
                .and_then(|size| usize::try_from(size).ok())
                .unwrap_or(byte_limit)
                .clamp(1, byte_limit);
            let permit_count = u32::try_from(reserved_bytes)
                .expect("validated WAL replay byte limit must fit in u32");
            let Ok(permit) = Arc::clone(&self.byte_semaphore).try_acquire_many_owned(permit_count)
            else {
                break;
            };
            let plan = self
                .pending_plans
                .pop_front()
                .expect("front plan must exist");
            let table_store = Arc::clone(&self.table_store);
            let handle = tokio::spawn(async move {
                let bytes = table_store.read_wal_sst_bytes(plan.wal_id).await?;
                if let Some(expected_size) = plan.expected_size {
                    let actual_size = u64::try_from(bytes.len()).map_err(|err| {
                        SlateDBError::WalDataError(Arc::new(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            err,
                        )))
                    })?;
                    if actual_size != expected_size {
                        return Err(SlateDBError::WalDataError(Arc::new(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!(
                                "WAL {} size changed between listing and GET: expected {}, got {}",
                                plan.wal_id, expected_size, actual_size
                            ),
                        ))));
                    }
                }
                Ok(FetchedWal {
                    wal_id: plan.wal_id,
                    bytes,
                    permit,
                })
            });
            self.pending_fetches.push_back(PendingWal {
                wal_id: plan.wal_id,
                handle,
            });
            self.peak_concurrent_objects =
                self.peak_concurrent_objects.max(self.pending_fetches.len());
            self.peak_reserved_bytes = self.peak_reserved_bytes.max(
                self.options
                    .prefetch
                    .max_inflight_bytes
                    .saturating_sub(self.byte_semaphore.available_permits()),
            );
        }
    }

    async fn advance_current_iter(&mut self) -> Result<(), SlateDBError> {
        self.current_iter.current_iter.take();
        self.fill_prefetch();
        let next_iter = if let Some(pending) = self.pending_fetches.pop_front() {
            let fetched = match pending.handle.await {
                Ok(Ok(fetched)) => fetched,
                Ok(Err(slate_err)) => return Err(slate_err),
                Err(join_err) => {
                    let task_name = format!("wal_replay[{:?}]", self.wal_id_range);
                    if let Ok(panic_err) = join_err.try_into_panic() {
                        error!(
                            "wal_replay task panicked unexpectedly. [task_name={}, panic={}]",
                            task_name,
                            panic_string(&panic_err),
                        );
                        return Err(SlateDBError::BackgroundTaskPanic(task_name));
                    }
                    return Err(SlateDBError::BackgroundTaskCancelled(task_name));
                }
            };
            assert_eq!(pending.wal_id, fetched.wal_id);
            self.fetched_objects = self.fetched_objects.saturating_add(1);
            self.fetched_bytes = self
                .fetched_bytes
                .saturating_add(u64::try_from(fetched.bytes.len()).unwrap_or(u64::MAX));
            let decoded = self
                .table_store
                .decode_wal_sst(fetched.wal_id, fetched.bytes)
                .await?;
            self.decoded_objects = self.decoded_objects.saturating_add(1);
            let iter: Box<dyn RowEntryIterator + 'static> = match decoded {
                DecodedWalSst::Fence => Box::new(WalBlocksIterator::new(0, VecDeque::new())),
                DecodedWalSst::Data {
                    format_version,
                    blocks,
                } => Box::new(WalBlocksIterator::new(format_version, blocks)),
            };
            Some(WalIdAndIter {
                wal_id: fetched.wal_id,
                iter,
                _permit: fetched.permit,
            })
        } else {
            None
        };
        self.current_iter.advance(next_iter);
        self.fill_prefetch();
        Ok(())
    }

    /// Get the next table replayed from the WAL. Replay accumulates complete WAL
    /// SSTs until the returned table reaches [`WalReplayOptions::max_memtable_bytes`],
    /// unless it is the final table replayed from the WAL. The final table may even
    /// be empty since writers use an empty WAL to fence zombie writers. The empty
    /// table must still be returned so that replay logic can account for the latest
    /// WAL ID.
    ///
    /// The returned table may exceed [`WalReplayOptions::max_memtable_bytes`] when
    /// a complete WAL SST is larger than the configured target, because replay
    /// must not split a WAL SST across replayed memtables.
    pub(crate) async fn next(&mut self) -> Result<Option<ReplayedMemtable>, SlateDBError> {
        if self.current_iter.is_finished() {
            self.log_completion();
            return Ok(None);
        }

        if !self.current_iter.initialized {
            self.advance_current_iter().await?;
        }

        let table = WritableKVTable::new();
        let mut last_wal_id = 0;

        while !self.current_iter.is_finished() {
            if let Some(wal_id_and_iter) = &mut self.current_iter.current_iter {
                while let Some(row_entry) = wal_id_and_iter.iter.next().await? {
                    // skip the entries that are already in the L0 SST.
                    if row_entry.seq <= self.min_seq {
                        continue;
                    }

                    if let Some(ts) = row_entry.create_ts {
                        self.last_tick = self.last_tick.max(ts);
                    }
                    self.last_seq = self.last_seq.max(row_entry.seq);
                    table.put(row_entry);
                }

                last_wal_id = wal_id_and_iter.wal_id;
                let replayed_wal_count = last_wal_id
                    .saturating_sub(self.wal_id_range.start)
                    .saturating_add(1);
                if replayed_wal_count.is_multiple_of(128) {
                    let metadata = table.metadata();
                    info!(
                        "SlateDB WAL replay progress [replay_start_wal_id={}, replay_end_wal_id={}, replay_wal_count={}, last_replayed_wal_id={}, replayed_wal_count={}, replayed_entries={}, replayed_bytes={}]",
                        self.wal_id_range.start,
                        self.wal_id_range.end,
                        self.wal_id_range
                            .end
                            .saturating_sub(self.wal_id_range.start),
                        last_wal_id,
                        replayed_wal_count,
                        metadata.entry_num,
                        metadata.entries_size_in_bytes
                    );
                }

                let meta = table.metadata();
                let estimated_bytes = self
                    .table_store
                    .estimate_encoded_size_compacted(meta.entry_num, meta.entries_size_in_bytes);
                if !table.is_empty() && estimated_bytes >= self.options.max_memtable_bytes {
                    self.current_iter.reset();
                    break;
                }
            }

            self.advance_current_iter().await?
        }

        if last_wal_id > 0 {
            Ok(Some(ReplayedMemtable {
                table,
                last_tick: self.last_tick,
                last_seq: self.last_seq,
                last_wal_id,
            }))
        } else {
            self.log_completion();
            Ok(None)
        }
    }

    fn log_completion(&mut self) {
        if self.completion_logged {
            return;
        }
        self.completion_logged = true;
        info!(
            "SlateDB WAL full-object replay completed [replay_start_wal_id={}, replay_end_wal_id={}, fetched_objects={}, fetched_bytes={}, decoded_objects={}, peak_concurrent_objects={}, peak_reserved_bytes={}]",
            self.wal_id_range.start,
            self.wal_id_range.end,
            self.fetched_objects,
            self.fetched_bytes,
            self.decoded_objects,
            self.peak_concurrent_objects,
            self.peak_reserved_bytes,
        );
    }
}

impl Drop for WalReplayIterator {
    fn drop(&mut self) {
        for pending in &self.pending_fetches {
            pending.handle.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{WalReplayIterator, WalReplayOptions};
    use crate::block_cache_policy::BlockCachePolicy;
    use crate::bytes_range::BytesRange;
    use crate::config::WalReplaySettings;
    use crate::db_state::SsTableId;
    use crate::format::sst::SsTableFormat;
    use crate::iter::{IterationOrder, RowEntryIterator};
    use crate::manifest::ManifestCore;
    use crate::mem_table::WritableKVTable;
    use crate::object_stores::ObjectStores;
    use crate::proptest_util::{rng, sample};
    use crate::tablestore::{TableStore, TableStoreKind};
    use crate::test_utils::{GatedObjectStore, RecordingObjectStore};
    use crate::types::RowEntry;
    use crate::{error::SlateDBError, test_utils};
    use bytes::Bytes;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{ObjectStore, ObjectStoreExt};
    use proptest::test_runner::TestRng;
    use rand::Rng;
    use std::cmp::min;
    use std::collections::btree_map::Iter;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    impl WalReplayIterator {
        async fn all_wal_ids(
            db_state: &ManifestCore,
            options: WalReplayOptions,
            table_store: Arc<TableStore>,
        ) -> Result<Self, SlateDBError> {
            let wal_id_start = db_state.replay_after_wal_id + 1;
            let wal_id_end = table_store
                .last_seen_wal_id(db_state.replay_after_wal_id)
                .await?;
            let wal_id_range = wal_id_start..(wal_id_end + 1);
            Self::range(wal_id_range, db_state, options, table_store).await
        }
    }

    #[tokio::test]
    async fn should_replay_empty_wal() {
        let table_store = test_table_store();
        write_empty_wal(1, Arc::clone(&table_store)).await.unwrap();
        let mut replay_iter = WalReplayIterator::all_wal_ids(
            &ManifestCore::new(),
            WalReplayOptions::default(),
            Arc::clone(&table_store),
        )
        .await
        .unwrap();

        let Some(table) = replay_iter.next().await.unwrap() else {
            panic!("Expected empty table to be returned from iterator")
        };

        assert_eq!(table.last_wal_id, 1);
        assert_eq!(table.last_seq, 0);
        assert!(table.table.is_empty());
        assert_eq!(table.last_tick, i64::MIN);
        assert!(replay_iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn should_replay_zero_byte_wal_fence() {
        let table_store = test_table_store();
        table_store.write_wal_fence(1).await.unwrap();
        let mut replay_iter = WalReplayIterator::all_wal_ids(
            &ManifestCore::new(),
            WalReplayOptions::default(),
            Arc::clone(&table_store),
        )
        .await
        .unwrap();

        let Some(table) = replay_iter.next().await.unwrap() else {
            panic!("Expected empty table to be returned from iterator")
        };

        assert_eq!(table.last_wal_id, 1);
        assert_eq!(table.last_seq, 0);
        assert!(table.table.is_empty());
        assert_eq!(table.last_tick, i64::MIN);
        assert!(replay_iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn should_replay_zero_byte_wal_fence_before_real_wal() {
        let table_store = test_table_store();
        table_store.write_wal_fence(1).await.unwrap();

        let row = RowEntry::new_value(b"key", b"value", 1);
        let mut builder = table_store.wal_table_builder();
        builder.add(row.clone()).await.unwrap();
        let encoded_sst = builder.build().await.unwrap();
        table_store
            .write_sst(&SsTableId::Wal(2), &encoded_sst)
            .await
            .unwrap();

        let mut replay_iter = WalReplayIterator::all_wal_ids(
            &ManifestCore::new(),
            WalReplayOptions::default(),
            Arc::clone(&table_store),
        )
        .await
        .unwrap();

        let Some(replayed_table) = replay_iter.next().await.unwrap() else {
            panic!("Expected table to be returned from iterator")
        };
        assert_eq!(replayed_table.last_wal_id, 2);
        assert_eq!(replayed_table.last_seq, 1);

        let mut iter = replayed_table.table.table().iter();
        test_utils::assert_iterator(&mut iter, vec![row]).await;
        assert!(replay_iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn should_replay_all_entries() {
        let table_store = test_table_store();
        let mut rng = rng::new_test_rng(None);
        let entries = sample::table(&mut rng, 1000, 10);
        let next_wal_id = write_wals(&entries, 1, &mut rng, 200, Arc::clone(&table_store))
            .await
            .unwrap();

        let mut replay_iter = WalReplayIterator::all_wal_ids(
            &ManifestCore::new(),
            WalReplayOptions::default(),
            Arc::clone(&table_store),
        )
        .await
        .unwrap();

        let Some(replayed_table) = replay_iter.next().await.unwrap() else {
            panic!("Expected table to be returned from iterator")
        };
        assert_eq!(replayed_table.last_wal_id + 1, next_wal_id);

        let mut imm_table_iter = replayed_table.table.table().iter();
        test_utils::assert_ranged_kv_scan(
            &entries,
            &BytesRange::from(..),
            IterationOrder::Ascending,
            &mut imm_table_iter,
        )
        .await;
        assert!(replay_iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn should_issue_one_full_get_per_wal() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let recording = Arc::new(RecordingObjectStore::new(inner));
        let table_store = test_table_store_with_object_store(recording.clone());
        for wal_id in 1..=3 {
            let mut builder = table_store.wal_table_builder();
            builder
                .add(RowEntry::new_value(
                    format!("key-{wal_id}").as_bytes(),
                    b"value",
                    wal_id,
                ))
                .await
                .unwrap();
            let encoded = builder.build().await.unwrap();
            table_store
                .write_sst(&SsTableId::Wal(wal_id), &encoded)
                .await
                .unwrap();
        }
        recording.clear();

        let mut replay_iter = WalReplayIterator::range(
            1..4,
            &ManifestCore::new(),
            WalReplayOptions::default(),
            table_store,
        )
        .await
        .unwrap();
        while replay_iter.next().await.unwrap().is_some() {}

        assert_eq!(recording.get_kinds(false).len(), 3);
        assert!(recording.get_kinds(true).is_empty());
        assert_eq!(
            recording.get_sst_types(false),
            vec![Some(crate::db_state::SstType::Wal); 3]
        );
        assert_eq!(recording.get_retries(false), vec![None; 3]);
    }

    #[tokio::test]
    async fn should_refetch_the_full_wal_once_after_checksum_failure() {
        let inner = Arc::new(InMemory::new());
        let recording = Arc::new(RecordingObjectStore::new(inner.clone()));
        let table_store = test_table_store_with_object_store(recording.clone());
        let mut builder = table_store.wal_table_builder();
        builder
            .add(RowEntry::new_value(b"key", b"value", 1))
            .await
            .unwrap();
        let encoded = builder.build().await.unwrap();
        table_store
            .write_sst(&SsTableId::Wal(1), &encoded)
            .await
            .unwrap();
        let metadata = table_store
            .list_wal_ssts_for_replay(1..2)
            .await
            .unwrap()
            .pop()
            .unwrap();
        let mut corrupted = inner
            .get(&metadata.metadata.location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .to_vec();
        corrupted[0] ^= 1;
        inner
            .put(&metadata.metadata.location, corrupted.into())
            .await
            .unwrap();
        recording.clear();

        let mut replay_iter = WalReplayIterator::range(
            1..2,
            &ManifestCore::new(),
            WalReplayOptions::default(),
            table_store,
        )
        .await
        .unwrap();
        assert!(matches!(
            replay_iter.next().await,
            Err(SlateDBError::ChecksumMismatch { .. })
        ));
        assert_eq!(recording.get_kinds(false).len(), 2);
        assert_eq!(recording.get_retries(false)[0], None);
        assert!(recording.get_retries(false)[1].is_some());
    }

    #[tokio::test]
    async fn should_not_skip_a_missing_wal_id() {
        let table_store = test_table_store();
        for wal_id in [1, 3] {
            let mut builder = table_store.wal_table_builder();
            builder
                .add(RowEntry::new_value(
                    format!("key-{wal_id}").as_bytes(),
                    b"value",
                    wal_id,
                ))
                .await
                .unwrap();
            let encoded = builder.build().await.unwrap();
            table_store
                .write_sst(&SsTableId::Wal(wal_id), &encoded)
                .await
                .unwrap();
        }

        let mut replay_iter = WalReplayIterator::range(
            1..4,
            &ManifestCore::new(),
            WalReplayOptions::default(),
            table_store,
        )
        .await
        .unwrap();
        let Err(error) = replay_iter.next().await else {
            panic!("missing WAL must fail replay");
        };
        assert!(error.has_object_store_not_found());
    }

    #[tokio::test]
    async fn should_prefetch_up_to_the_object_limit() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let gated = Arc::new(GatedObjectStore::new(inner));
        let table_store = test_table_store_with_object_store(gated.clone());
        for wal_id in 1..=4 {
            let mut builder = table_store.wal_table_builder();
            builder
                .add(RowEntry::new_value(
                    format!("key-{wal_id}").as_bytes(),
                    b"value",
                    wal_id,
                ))
                .await
                .unwrap();
            let encoded = builder.build().await.unwrap();
            table_store
                .write_sst(&SsTableId::Wal(wal_id), &encoded)
                .await
                .unwrap();
        }
        gated.get_opts_gate.close();

        let mut replay_iter = WalReplayIterator::range(
            1..5,
            &ManifestCore::new(),
            WalReplayOptions {
                prefetch: WalReplaySettings {
                    max_concurrent_objects: 2,
                    max_inflight_bytes: 1024 * 1024,
                },
                ..WalReplayOptions::default()
            },
            table_store,
        )
        .await
        .unwrap();
        gated.get_opts_gate.wait_for_arrivals(2).await;
        assert_eq!(gated.get_opts_gate.arrivals(), 2);

        gated.get_opts_gate.release();
        while replay_iter.next().await.unwrap().is_some() {}
        assert_eq!(gated.get_opts_gate.arrivals(), 4);
    }

    #[tokio::test]
    async fn should_hold_byte_permits_until_the_wal_is_consumed() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let gated = Arc::new(GatedObjectStore::new(inner));
        let table_store = test_table_store_with_object_store(gated.clone());
        for wal_id in 1..=2 {
            let mut builder = table_store.wal_table_builder();
            builder
                .add(RowEntry::new_value(
                    format!("key-{wal_id}").as_bytes(),
                    &[b'x'; 128],
                    wal_id,
                ))
                .await
                .unwrap();
            let encoded = builder.build().await.unwrap();
            table_store
                .write_sst(&SsTableId::Wal(wal_id), &encoded)
                .await
                .unwrap();
        }
        gated.get_opts_gate.close();

        let mut replay_iter = WalReplayIterator::range(
            1..3,
            &ManifestCore::new(),
            WalReplayOptions {
                prefetch: WalReplaySettings {
                    max_concurrent_objects: 2,
                    max_inflight_bytes: 1,
                },
                max_memtable_bytes: 1,
                ..WalReplayOptions::default()
            },
            table_store,
        )
        .await
        .unwrap();
        gated.get_opts_gate.wait_for_arrivals(1).await;
        for _ in 0..100 {
            tokio::task::yield_now().await;
        }
        assert_eq!(gated.get_opts_gate.arrivals(), 1);

        gated.get_opts_gate.release();
        assert!(replay_iter.next().await.unwrap().is_some());
        assert!(replay_iter.next().await.unwrap().is_some());
        assert!(replay_iter.next().await.unwrap().is_none());
        assert_eq!(gated.get_opts_gate.arrivals(), 2);
    }

    #[tokio::test]
    async fn should_enforce_max_memtable_bytes() {
        let table_store = test_table_store();
        let mut rng = rng::new_test_rng(None);
        let num_entries = 5000;
        let entries = sample::table(&mut rng, num_entries, 10);
        let next_wal_id = write_wals(&entries, 1, &mut rng, 200, Arc::clone(&table_store))
            .await
            .unwrap();

        let max_memtable_bytes = 1024;
        let mut replay_iter = WalReplayIterator::all_wal_ids(
            &ManifestCore::new(),
            WalReplayOptions {
                max_memtable_bytes,
                ..WalReplayOptions::default()
            },
            Arc::clone(&table_store),
        )
        .await
        .unwrap();

        let full_replayed_table = WritableKVTable::new();
        let mut last_wal_id = 0;
        let mut replayed_entry_count = 0;

        while let Some(replayed_table) = replay_iter.next().await.unwrap() {
            last_wal_id = replayed_table.last_wal_id;
            let metadata = replayed_table.table.metadata();
            replayed_entry_count += metadata.entry_num;

            // The last table may be less than `max_memtable_bytes`.
            if replayed_entry_count < num_entries {
                let estimated_bytes = table_store.estimate_encoded_size_compacted(
                    metadata.entry_num,
                    metadata.entries_size_in_bytes,
                );
                assert!(estimated_bytes >= max_memtable_bytes);
            }

            let mut iter = replayed_table.table.table().iter();
            while let Some(next) = iter.next().await.unwrap() {
                full_replayed_table.put(next);
            }
        }
        assert_eq!(last_wal_id + 1, next_wal_id);

        let mut full_replayed_iter = full_replayed_table.table().iter();
        test_utils::assert_ranged_kv_scan(
            &entries,
            &BytesRange::from(..),
            IterationOrder::Ascending,
            &mut full_replayed_iter,
        )
        .await;
    }

    #[tokio::test]
    async fn should_apply_max_memtable_bytes_at_wal_boundaries() {
        let table_store = test_table_store();
        let wal_entries = [
            vec![RowEntry::new_value(b"key_001", &[b'x'; 128], 1)],
            vec![RowEntry::new_value(b"key_002", &[b'x'; 128], 2)],
            vec![RowEntry::new_value(b"key_003", &[b'x'; 128], 3)],
        ];
        let single_row_size = wal_entries[0][0].estimated_size();
        let max_memtable_bytes =
            table_store.estimate_encoded_size_compacted(1, single_row_size) + 1;

        for (wal_id, entries) in wal_entries.into_iter().enumerate() {
            let mut builder = table_store.wal_table_builder();
            for entry in entries {
                builder.add(entry).await.unwrap();
            }
            let encoded_sst = builder.build().await.unwrap();
            table_store
                .write_sst(&SsTableId::Wal(wal_id as u64 + 1), &encoded_sst)
                .await
                .unwrap();
        }

        let mut replay_iter = WalReplayIterator::all_wal_ids(
            &ManifestCore::new(),
            WalReplayOptions {
                max_memtable_bytes,
                ..WalReplayOptions::default()
            },
            Arc::clone(&table_store),
        )
        .await
        .unwrap();

        let mut replayed_last_wal_ids = Vec::new();
        let mut replayed_table_sizes = Vec::new();
        let mut replayed_seqs = Vec::new();

        while let Some(replayed_table) = replay_iter.next().await.unwrap() {
            replayed_last_wal_ids.push(replayed_table.last_wal_id);
            let metadata = replayed_table.table.metadata();
            replayed_table_sizes.push(table_store.estimate_encoded_size_compacted(
                metadata.entry_num,
                metadata.entries_size_in_bytes,
            ));
            let mut iter = replayed_table.table.table().iter();
            while let Some(next) = iter.next().await.unwrap() {
                replayed_seqs.push(next.seq);
            }
        }

        assert_eq!(replayed_last_wal_ids, vec![2, 3]);
        assert!(
            replayed_table_sizes[0] > max_memtable_bytes,
            "first replayed table should exceed the target rather than split a WAL SST"
        );
        assert_eq!(replayed_seqs, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn should_not_split_one_commit_seq_across_replayed_memtables() {
        let table_store = test_table_store();
        let commit_seq = 42;

        // Simulate one committed write batch. Every row gets the same commit
        // sequence, which means replay must not split these rows into separate
        // memtable layers.
        let entries = (0..8)
            .map(|i| {
                RowEntry::new_value(format!("key_{i:03}").as_bytes(), &[b'x'; 128], commit_seq)
            })
            .collect::<Vec<_>>();

        // Size replayed memtables so one real row fits, but the second row
        // overflows into the next replayed memtable.
        let max_memtable_bytes =
            table_store.estimate_encoded_size_compacted(1, entries[0].estimated_size());

        // Use the real WAL SST builder so the fixture matches WAL flushes.
        let mut builder = table_store.wal_table_builder();
        for entry in entries {
            builder.add(entry).await.unwrap();
        }
        let encoded_sst = builder.build().await.unwrap();
        table_store
            .write_sst(&SsTableId::Wal(1), &encoded_sst)
            .await
            .unwrap();

        // Replay the single WAL SST into in-memory tables. If the replay code
        // can split a single commit sequence, it will do so here.
        let mut replay_iter = WalReplayIterator::all_wal_ids(
            &ManifestCore::new(),
            WalReplayOptions {
                max_memtable_bytes,
                ..WalReplayOptions::default()
            },
            Arc::clone(&table_store),
        )
        .await
        .unwrap();

        let mut replayed_seq_ranges = Vec::new();
        while let Some(replayed_table) = replay_iter.next().await.unwrap() {
            let metadata = replayed_table.table.metadata();
            replayed_seq_ranges.push((metadata.first_seq, metadata.last_seq));
        }

        // This guards against producing multiple replayed memtables with the same
        // sequence range, which can make later replay logic treat part of the write
        // batch as already committed.
        assert_eq!(
            replayed_seq_ranges,
            vec![(commit_seq, commit_seq)],
            "WAL replay split one commit seq across replayed memtables: {replayed_seq_ranges:?}"
        );
    }

    #[tokio::test]
    async fn should_replay_memtables_in_sequence_order() {
        let table_store = test_table_store();

        // Write one WAL with entries whose sequence numbers do not match key
        // order. Replay must not expose a later memtable whose sequence range
        // starts before the previous memtable's sequence range ends.
        let entries = vec![
            RowEntry::new_value(b"key_000", &[b'x'; 128], 100),
            RowEntry::new_value(b"key_001", &[b'x'; 128], 10),
            RowEntry::new_value(b"key_002", &[b'x'; 128], 110),
        ];

        // Size replayed memtables so one real row fits, but the second row
        // overflows into the next replayed memtable.
        let max_memtable_bytes =
            table_store.estimate_encoded_size_compacted(1, entries[0].estimated_size());

        // Use the real WAL SST builder so replay sees the same entry order as a
        // flushed WAL.
        let mut builder = table_store.wal_table_builder();
        for entry in entries {
            builder.add(entry).await.unwrap();
        }
        let encoded_sst = builder.build().await.unwrap();
        table_store
            .write_sst(&SsTableId::Wal(1), &encoded_sst)
            .await
            .unwrap();

        // Replay the single WAL SST into in-memory tables.
        let mut replay_iter = WalReplayIterator::all_wal_ids(
            &ManifestCore::new(),
            WalReplayOptions {
                max_memtable_bytes,
                ..WalReplayOptions::default()
            },
            Arc::clone(&table_store),
        )
        .await
        .unwrap();

        let mut replayed_seq_ranges = Vec::new();
        while let Some(replayed_table) = replay_iter.next().await.unwrap() {
            let metadata = replayed_table.table.metadata();
            replayed_seq_ranges.push((metadata.first_seq, metadata.last_seq));
        }

        // This guards against returning the seq=10 row in a later replayed
        // memtable after already returning seq=100.
        for adjacent in replayed_seq_ranges.windows(2) {
            let previous_last_seq = adjacent[0].1;
            let later_first_seq = adjacent[1].0;
            assert!(
                later_first_seq >= previous_last_seq,
                "WAL replay returned out-of-order memtable sequence ranges: {replayed_seq_ranges:?}"
            );
        }
    }

    #[tokio::test]
    async fn should_only_replay_wals_after_last_l0_flushed_wal_id() {
        let table_store = test_table_store();
        let mut rng = rng::new_test_rng(None);
        let compacted_entries = sample::table(&mut rng, 1000, 10);
        let mut next_wal_id = 1;

        next_wal_id = write_wals(
            &compacted_entries,
            next_wal_id,
            &mut rng,
            200,
            Arc::clone(&table_store),
        )
        .await
        .unwrap();

        let replay_after_wal_id = next_wal_id - 1;
        let non_compacted_entries = sample::table(&mut rng, 1000, 10);
        next_wal_id = write_wals(
            &non_compacted_entries,
            next_wal_id,
            &mut rng,
            200,
            Arc::clone(&table_store),
        )
        .await
        .unwrap();

        let mut db_state = ManifestCore::new();
        db_state.replay_after_wal_id = replay_after_wal_id;
        db_state.next_wal_sst_id = replay_after_wal_id + 1;

        let mut replay_iter = WalReplayIterator::all_wal_ids(
            &db_state,
            WalReplayOptions::default(),
            Arc::clone(&table_store),
        )
        .await
        .unwrap();

        let Some(replayed_table) = replay_iter.next().await.unwrap() else {
            panic!("Expected table to be returned from iterator")
        };
        assert_eq!(replayed_table.last_wal_id + 1, next_wal_id);

        let mut imm_table_iter = replayed_table.table.table().iter();
        test_utils::assert_ranged_kv_scan(
            &non_compacted_entries,
            &BytesRange::from(..),
            IterationOrder::Ascending,
            &mut imm_table_iter,
        )
        .await;
        assert!(replay_iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn should_replay_wals_after_min_seq() {
        let table_store = test_table_store();
        let mut rng = rng::new_test_rng(None);
        let entries = sample::table(&mut rng, 1000, 10);
        let next_wal_id = write_wals(&entries, 1, &mut rng, 200, Arc::clone(&table_store))
            .await
            .unwrap();

        // Set min_seq to skip the first half of entries
        let min_seq = 500;
        let mut db_state = ManifestCore::new();
        db_state.last_l0_seq = min_seq;
        db_state.last_l0_clock_tick = 0;

        let mut replay_iter = WalReplayIterator::all_wal_ids(
            &db_state,
            WalReplayOptions::default(),
            Arc::clone(&table_store),
        )
        .await
        .unwrap();

        let Some(replayed_table) = replay_iter.next().await.unwrap() else {
            panic!("Expected table to be returned from iterator")
        };
        assert_eq!(replayed_table.last_wal_id + 1, next_wal_id);

        // Verify that only entries with seq > min_seq are replayed
        let mut imm_table_iter = replayed_table.table.table().iter();
        let mut replayed_entries = BTreeMap::new();
        let mut total = 0;
        while let Some(entry) = imm_table_iter.next().await.unwrap() {
            assert!(entry.seq > min_seq);
            replayed_entries.insert(entry.key.clone(), entry.value);
            total += 1;
        }
        assert_eq!(total, 500);
    }

    fn test_table_store() -> Arc<TableStore> {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        test_table_store_with_object_store(object_store)
    }

    fn test_table_store_with_object_store(object_store: Arc<dyn ObjectStore>) -> Arc<TableStore> {
        let path = Path::from("/tmp/test_kv_store");
        Arc::new(TableStore::new(
            ObjectStores::new(object_store, None),
            SsTableFormat::default(),
            path,
            None,
            TableStoreKind::Main,
            BlockCachePolicy::default(),
        ))
    }

    /// Write a sequence of WALs with a random (bounded) number of entries.
    /// Return the ID of the next WAL.
    async fn write_wals(
        entries: &BTreeMap<Bytes, Bytes>,
        next_wal_id: u64,
        rng: &mut TestRng,
        max_wal_entries: usize,
        table_store: Arc<TableStore>,
    ) -> Result<u64, SlateDBError> {
        let mut iter = entries.iter();
        let mut next_seq = 1;
        let mut total_wal_entries = 0;
        let mut next_wal_id = next_wal_id;

        while total_wal_entries < entries.len() {
            let wal_entries = min(
                entries.len() - total_wal_entries,
                rng.random_range(0..max_wal_entries),
            );
            next_seq = write_wal(
                next_wal_id,
                next_seq,
                &mut iter,
                wal_entries,
                Arc::clone(&table_store),
            )
            .await?;
            next_wal_id += 1;
            total_wal_entries += wal_entries;
        }
        Ok(next_wal_id)
    }

    async fn write_empty_wal(
        wal_id: u64,
        table_store: Arc<TableStore>,
    ) -> Result<(), SlateDBError> {
        let empty_entries = BTreeMap::new();
        let mut empty_iter = empty_entries.iter();
        let _ = write_wal(wal_id, 0, &mut empty_iter, 0, table_store).await?;
        Ok(())
    }

    async fn write_wal(
        wal_id: u64,
        next_seq: u64,
        entries: &mut Iter<'_, Bytes, Bytes>,
        max_entries: usize,
        table_store: Arc<TableStore>,
    ) -> Result<u64, SlateDBError> {
        let mut writer = table_store.table_writer(SsTableId::Wal(wal_id));
        let mut next_seq = next_seq;
        while next_seq < next_seq + (max_entries as u64) {
            let Some((key, value)) = entries.next() else {
                break;
            };
            writer
                .add(RowEntry::new_value(key, value, next_seq))
                .await?;
            next_seq += 1;
        }
        writer.close().await?;
        Ok(next_seq)
    }
}

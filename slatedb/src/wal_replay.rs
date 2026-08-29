use crate::block_iterator::DataBlockIterator;
use crate::config::WalReplaySettings;
use crate::error::SlateDBError;
use crate::iter::{IterationOrder, RowEntryIterator};
use crate::manifest::ManifestCore;
use crate::mem_table::WritableKVTable;
use crate::tablestore::{
    DecodedWalSst, DecodedWalSstData, RangedWalSst, RangedWalSstData, TableStore,
};
use crate::types::RowEntry;
use crate::utils::panic_string;
use async_trait::async_trait;
use bytes::Bytes;
use log::{error, info};
use std::collections::VecDeque;
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
    _permit: Option<OwnedSemaphorePermit>,
}

struct WalObjectPlan {
    wal_id: u64,
    expected_size: Option<u64>,
}

enum FetchedWal {
    Full {
        wal_id: u64,
        bytes: Bytes,
        permit: OwnedSemaphorePermit,
    },
    Ranged {
        wal_id: u64,
        expected_size: u64,
    },
}

impl FetchedWal {
    fn wal_id(&self) -> u64 {
        match self {
            Self::Full { wal_id, .. } | Self::Ranged { wal_id, .. } => *wal_id,
        }
    }
}

struct PendingWal {
    wal_id: u64,
    handle: JoinHandle<Result<FetchedWal, SlateDBError>>,
}

struct WalBlocksIterator {
    table_store: Arc<TableStore>,
    wal: Option<WalBlockSource>,
    next_block: usize,
    current: Option<CurrentWalBlock>,
    encoded_byte_limit: usize,
    working_memory_limit: usize,
    block_working_memory_limit: usize,
    validation_retried: bool,
    yielded_rows: bool,
    #[cfg(test)]
    block_lifetime_probe: Option<Arc<BlockLifetimeProbe>>,
}

struct CurrentWalBlock {
    iterator: DataBlockIterator<crate::format::block::Block>,
    #[cfg(test)]
    lifetime_probe: Option<Arc<BlockLifetimeProbe>>,
}

impl CurrentWalBlock {
    fn new(
        iterator: DataBlockIterator<crate::format::block::Block>,
        #[cfg(test)] lifetime_probe: Option<Arc<BlockLifetimeProbe>>,
    ) -> Self {
        #[cfg(test)]
        if let Some(probe) = &lifetime_probe {
            let previous = probe
                .active
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            probe
                .peak
                .fetch_max(previous + 1, std::sync::atomic::Ordering::SeqCst);
        }
        Self {
            iterator,
            #[cfg(test)]
            lifetime_probe,
        }
    }
}

#[cfg(test)]
impl Drop for CurrentWalBlock {
    fn drop(&mut self) {
        if let Some(probe) = &self.lifetime_probe {
            let previous = probe
                .active
                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            assert_eq!(previous, 1, "decoded WAL blocks overlapped");
        }
    }
}

#[cfg(test)]
#[derive(Default)]
struct BlockLifetimeProbe {
    active: std::sync::atomic::AtomicUsize,
    peak: std::sync::atomic::AtomicUsize,
}

enum WalBlockSource {
    Full(DecodedWalSstData),
    Ranged(RangedWalSstData),
}

impl WalBlockSource {
    fn block_count(&self) -> usize {
        match self {
            Self::Full(wal) => wal.index.borrow().block_meta().len(),
            Self::Ranged(wal) => wal.index.borrow().block_meta().len(),
        }
    }

    fn format_version(&self) -> u16 {
        match self {
            Self::Full(wal) => wal.format_version,
            Self::Ranged(wal) => wal.format_version,
        }
    }

    fn retained_decode_bytes(&self) -> usize {
        match self {
            Self::Full(wal) => wal.retained_decode_bytes,
            Self::Ranged(wal) => wal.retained_decode_bytes,
        }
    }
}

impl WalBlocksIterator {
    fn new(
        table_store: Arc<TableStore>,
        wal: DecodedWalSstData,
        encoded_byte_limit: usize,
        working_memory_limit: usize,
    ) -> Result<Self, SlateDBError> {
        Self::from_source(
            table_store,
            WalBlockSource::Full(wal),
            encoded_byte_limit,
            working_memory_limit,
        )
    }

    fn new_ranged(
        table_store: Arc<TableStore>,
        wal: RangedWalSstData,
        encoded_byte_limit: usize,
        working_memory_limit: usize,
    ) -> Result<Self, SlateDBError> {
        Self::from_source(
            table_store,
            WalBlockSource::Ranged(wal),
            encoded_byte_limit,
            working_memory_limit,
        )
    }

    fn from_source(
        table_store: Arc<TableStore>,
        wal: WalBlockSource,
        encoded_byte_limit: usize,
        working_memory_limit: usize,
    ) -> Result<Self, SlateDBError> {
        let block_working_memory_limit = (working_memory_limit / 2).min(
            working_memory_limit
                .checked_sub(wal.retained_decode_bytes())
                .ok_or(SlateDBError::WalReplayMemoryLimitExceeded {
                    kind: "WAL metadata, index, and block",
                    required_bytes: wal.retained_decode_bytes(),
                    limit_bytes: working_memory_limit,
                })?,
        );
        Ok(Self {
            table_store,
            wal: Some(wal),
            next_block: 0,
            current: None,
            encoded_byte_limit,
            working_memory_limit,
            block_working_memory_limit,
            validation_retried: false,
            yielded_rows: false,
            #[cfg(test)]
            block_lifetime_probe: None,
        })
    }

    #[cfg(test)]
    fn observe_block_lifetimes_with(mut self, probe: Arc<BlockLifetimeProbe>) -> Self {
        self.block_lifetime_probe = Some(probe);
        self
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
                if let Some(entry) = current.iterator.next().await? {
                    self.yielded_rows = true;
                    return Ok(Some(entry));
                }
            }
            self.current.take();
            let wal = self.wal.as_ref().ok_or(SlateDBError::InvalidDBState)?;
            let block_count = wal.block_count();
            if self.next_block >= block_count {
                return Ok(None);
            }
            debug_assert!(
                self.current.is_none(),
                "exhausted WAL block must be released before decoding the next block"
            );
            let decoded = match wal {
                WalBlockSource::Full(wal) => {
                    self.table_store
                        .decode_wal_block(wal, self.next_block, self.block_working_memory_limit)
                        .await
                }
                WalBlockSource::Ranged(wal) => {
                    self.table_store
                        .read_ranged_wal_block(
                            wal,
                            self.next_block,
                            self.block_working_memory_limit,
                        )
                        .await
                }
            };
            let block = match decoded {
                Ok(block) => block,
                Err(error) => {
                    let Some(reason) = error.maybe_validation_retry_reason() else {
                        return Err(error);
                    };
                    if self.validation_retried || self.yielded_rows {
                        return Err(error);
                    }
                    let old_wal = self.wal.take().ok_or(SlateDBError::InvalidDBState)?;
                    let WalBlockSource::Full(old_wal) = old_wal else {
                        return Err(error);
                    };
                    let wal_id = old_wal.wal_id;
                    let expected_size = old_wal.object_bytes.len();
                    drop(old_wal);
                    self.validation_retried = true;
                    let replacement = self
                        .table_store
                        .refetch_wal_sst_after_validation(
                            wal_id,
                            expected_size,
                            self.encoded_byte_limit,
                            self.working_memory_limit,
                            reason,
                        )
                        .await?;
                    let DecodedWalSst::Data(replacement) = replacement else {
                        return Err(SlateDBError::CorruptSst {
                            reason: "nonempty WAL changed into a fence during validation retry",
                            path: None,
                        });
                    };
                    self.block_working_memory_limit = (self.working_memory_limit / 2).min(
                        self.working_memory_limit
                            .checked_sub(replacement.retained_decode_bytes)
                            .ok_or(SlateDBError::WalReplayMemoryLimitExceeded {
                                kind: "WAL metadata, index, and block",
                                required_bytes: replacement.retained_decode_bytes,
                                limit_bytes: self.working_memory_limit,
                            })?,
                    );
                    self.wal = Some(WalBlockSource::Full(*replacement));
                    continue;
                }
            };
            self.next_block += 1;
            let wal = self.wal.as_ref().ok_or(SlateDBError::InvalidDBState)?;
            self.current = Some(CurrentWalBlock::new(
                DataBlockIterator::new(block, wal.format_version(), IterationOrder::Ascending)?,
                #[cfg(test)]
                self.block_lifetime_probe.clone(),
            ));
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

/// Replays one exact, contiguous WAL-ID range.
///
/// Safety invariants:
/// - every ID is fetched directly, including IDs omitted by object listing;
/// - fetched objects are consumed strictly in WAL-ID order;
/// - encoded-byte permits remain owned until the corresponding WAL is consumed;
/// - an error permanently fails the iterator and aborts every outstanding fetch;
/// - the replay cursor is lazy, so memory does not grow with a sparse ID span.
pub(crate) struct WalReplayIterator {
    options: WalReplayOptions,
    wal_id_range: Range<u64>,
    table_store: Arc<TableStore>,
    current_iter: IteratorHolder<WalIdAndIter>,
    next_wal_id: u64,
    listed_sizes: VecDeque<(u64, u64)>,
    pending_fetches: VecDeque<PendingWal>,
    byte_semaphore: Arc<Semaphore>,
    encoded_byte_limit: usize,
    working_memory_limit: usize,
    last_tick: i64,
    last_seq: u64,
    min_seq: u64,
    fetched_objects: u64,
    fetched_bytes: u64,
    decoded_objects: u64,
    ranged_objects: u64,
    peak_concurrent_objects: usize,
    peak_reserved_bytes: usize,
    completion_logged: bool,
    failed: Option<SlateDBError>,
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
        let listed = table_store
            .list_wal_ssts_for_replay(wal_id_range.clone())
            .await?;
        let listed_wal_count = listed.len();
        let mut listed_sizes = VecDeque::with_capacity(listed_wal_count);
        let mut previous_listed_wal_id = None;
        for metadata in listed {
            let wal_id = metadata.id.unwrap_wal_id();
            if !wal_id_range.contains(&wal_id)
                || previous_listed_wal_id.is_some_and(|previous| wal_id <= previous)
            {
                return Err(SlateDBError::InvalidDBState);
            }
            previous_listed_wal_id = Some(wal_id);
            listed_sizes.push_back((wal_id, metadata.metadata.size));
        }
        let working_memory_limit = options.prefetch.working_memory_limit();
        let encoded_byte_limit = options.prefetch.encoded_byte_limit()?;
        let byte_semaphore = Arc::new(Semaphore::new(encoded_byte_limit));
        let next_wal_id = wal_id_range.start;

        let mut replay_iter = WalReplayIterator {
            options,
            wal_id_range,
            table_store: Arc::clone(&table_store),
            current_iter: IteratorHolder::new(),
            next_wal_id,
            listed_sizes,
            pending_fetches: VecDeque::new(),
            byte_semaphore,
            encoded_byte_limit,
            working_memory_limit,
            last_tick,
            last_seq,
            min_seq,
            fetched_objects: 0,
            fetched_bytes: 0,
            decoded_objects: 0,
            ranged_objects: 0,
            peak_concurrent_objects: 0,
            peak_reserved_bytes: 0,
            completion_logged: false,
            failed: None,
        };

        replay_iter.fill_prefetch();
        info!(
            "SlateDB WAL replay initialized [replay_start_wal_id={}, replay_end_wal_id={}, replay_wal_count={}, listed_wal_count={}, missing_size_count={}, max_concurrent_objects={}, max_inflight_bytes={}]",
            replay_iter.wal_id_range.start,
            replay_iter.wal_id_range.end,
            replay_iter
                .wal_id_range
                .end
                .saturating_sub(replay_iter.wal_id_range.start),
            listed_wal_count,
            replay_iter
                .wal_id_range
                .end
                .saturating_sub(replay_iter.wal_id_range.start)
                .saturating_sub(listed_wal_count as u64),
            replay_iter.options.prefetch.max_concurrent_objects,
            replay_iter.options.prefetch.max_inflight_bytes,
        );

        Ok(replay_iter)
    }

    fn fill_prefetch(&mut self) {
        while self.pending_fetches.len() < self.options.prefetch.max_concurrent_objects {
            if self.next_wal_id >= self.wal_id_range.end {
                break;
            }
            while self
                .listed_sizes
                .front()
                .is_some_and(|(wal_id, _)| *wal_id < self.next_wal_id)
            {
                self.listed_sizes.pop_front();
            }
            let plan = WalObjectPlan {
                wal_id: self.next_wal_id,
                expected_size: self
                    .listed_sizes
                    .front()
                    .filter(|(wal_id, _)| *wal_id == self.next_wal_id)
                    .map(|(_, size)| *size),
            };
            let byte_limit = self.encoded_byte_limit;
            let reserved_bytes = plan
                .expected_size
                .and_then(|size| usize::try_from(size).ok())
                .unwrap_or(byte_limit)
                .max(1);
            if reserved_bytes > byte_limit {
                let wal_id = plan.wal_id;
                let expected_size = plan
                    .expected_size
                    .expect("an oversized planned WAL must have a listed size");
                self.listed_sizes.pop_front();
                self.next_wal_id += 1;
                self.pending_fetches.push_back(PendingWal {
                    wal_id,
                    handle: tokio::spawn(async move {
                        Ok(FetchedWal::Ranged {
                            wal_id,
                            expected_size,
                        })
                    }),
                });
                self.peak_concurrent_objects =
                    self.peak_concurrent_objects.max(self.pending_fetches.len());
                continue;
            }
            let permit_count = u32::try_from(reserved_bytes)
                .expect("validated WAL replay byte limit must fit in u32");
            let Ok(permit) = Arc::clone(&self.byte_semaphore).try_acquire_many_owned(permit_count)
            else {
                break;
            };
            if plan.expected_size.is_some() {
                self.listed_sizes.pop_front();
            }
            self.next_wal_id += 1;
            let table_store = Arc::clone(&self.table_store);
            let handle = tokio::spawn(async move {
                match table_store
                    .read_wal_sst_bytes(plan.wal_id, plan.expected_size, byte_limit)
                    .await
                {
                    Ok(bytes) => Ok(FetchedWal::Full {
                        wal_id: plan.wal_id,
                        bytes,
                        permit,
                    }),
                    Err(SlateDBError::WalReplayMemoryLimitExceeded {
                        kind: "encoded WAL object",
                        required_bytes,
                        limit_bytes,
                    }) if required_bytes > limit_bytes => {
                        drop(permit);
                        Ok(FetchedWal::Ranged {
                            wal_id: plan.wal_id,
                            expected_size: u64::try_from(required_bytes).map_err(|_| {
                                SlateDBError::WalReplayMemoryLimitExceeded {
                                    kind: "encoded WAL object",
                                    required_bytes,
                                    limit_bytes,
                                }
                            })?,
                        })
                    }
                    Err(error) => Err(error),
                }
            });
            self.pending_fetches.push_back(PendingWal {
                wal_id: plan.wal_id,
                handle,
            });
            self.peak_concurrent_objects =
                self.peak_concurrent_objects.max(self.pending_fetches.len());
            self.peak_reserved_bytes = self.peak_reserved_bytes.max(
                self.encoded_byte_limit
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
                Ok(Err(slate_err)) => return Err(self.fail(slate_err)),
                Err(join_err) => {
                    let task_name = format!("wal_replay[{:?}]", self.wal_id_range);
                    if let Ok(panic_err) = join_err.try_into_panic() {
                        error!(
                            "wal_replay task panicked unexpectedly. [task_name={}, panic={}]",
                            task_name,
                            panic_string(&panic_err),
                        );
                        return Err(self.fail(SlateDBError::BackgroundTaskPanic(task_name)));
                    }
                    return Err(self.fail(SlateDBError::BackgroundTaskCancelled(task_name)));
                }
            };
            if pending.wal_id != fetched.wal_id() {
                return Err(self.fail(SlateDBError::InvalidDBState));
            }
            let (wal_id, iter, permit): (_, Box<dyn RowEntryIterator + 'static>, _) = match fetched
            {
                FetchedWal::Full {
                    wal_id,
                    bytes,
                    permit,
                } => {
                    self.fetched_objects = self.fetched_objects.saturating_add(1);
                    self.fetched_bytes = self
                        .fetched_bytes
                        .saturating_add(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
                    let expected_size = bytes.len();
                    let initial_decode = self
                        .table_store
                        .decode_wal_sst(wal_id, bytes, self.working_memory_limit)
                        .await;
                    let decoded = match initial_decode {
                        Ok(decoded) => decoded,
                        Err(error) => {
                            let Some(reason) = error.maybe_validation_retry_reason() else {
                                return Err(self.fail(error));
                            };
                            match self
                                .table_store
                                .refetch_wal_sst_after_validation(
                                    wal_id,
                                    expected_size,
                                    self.encoded_byte_limit,
                                    self.working_memory_limit,
                                    reason,
                                )
                                .await
                            {
                                Ok(decoded) => decoded,
                                Err(error) => return Err(self.fail(error)),
                            }
                        }
                    };
                    let iter: Box<dyn RowEntryIterator + 'static> = match decoded {
                        DecodedWalSst::Fence => Box::new(crate::iter::EmptyIterator::new()),
                        DecodedWalSst::Data(wal) => Box::new(
                            WalBlocksIterator::new(
                                Arc::clone(&self.table_store),
                                *wal,
                                self.encoded_byte_limit,
                                self.working_memory_limit,
                            )
                            .map_err(|error| self.fail(error))?,
                        ),
                    };
                    (wal_id, iter, Some(permit))
                }
                FetchedWal::Ranged {
                    wal_id,
                    expected_size,
                } => {
                    self.ranged_objects = self.ranged_objects.saturating_add(1);
                    let decoded = match self
                        .table_store
                        .open_ranged_wal_sst(wal_id, expected_size, self.working_memory_limit)
                        .await
                    {
                        Ok(decoded) => decoded,
                        Err(error) => return Err(self.fail(error)),
                    };
                    let iter: Box<dyn RowEntryIterator + 'static> = match decoded {
                        RangedWalSst::Fence => Box::new(crate::iter::EmptyIterator::new()),
                        RangedWalSst::Data(wal) => Box::new(
                            WalBlocksIterator::new_ranged(
                                Arc::clone(&self.table_store),
                                *wal,
                                self.encoded_byte_limit,
                                self.working_memory_limit,
                            )
                            .map_err(|error| self.fail(error))?,
                        ),
                    };
                    (wal_id, iter, None)
                }
            };
            self.decoded_objects = self.decoded_objects.saturating_add(1);
            Some(WalIdAndIter {
                wal_id,
                iter,
                _permit: permit,
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
        if let Some(error) = &self.failed {
            return Err(error.clone());
        }
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
                loop {
                    let row_entry = match wal_id_and_iter.iter.next().await {
                        Ok(Some(row_entry)) => row_entry,
                        Ok(None) => break,
                        Err(error) => return Err(self.fail(error)),
                    };
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
            "SlateDB WAL replay completed [replay_start_wal_id={}, replay_end_wal_id={}, fetched_objects={}, fetched_bytes={}, ranged_objects={}, decoded_objects={}, peak_concurrent_objects={}, peak_reserved_bytes={}]",
            self.wal_id_range.start,
            self.wal_id_range.end,
            self.fetched_objects,
            self.fetched_bytes,
            self.ranged_objects,
            self.decoded_objects,
            self.peak_concurrent_objects,
            self.peak_reserved_bytes,
        );
    }

    fn fail(&mut self, error: SlateDBError) -> SlateDBError {
        for pending in self.pending_fetches.drain(..) {
            pending.handle.abort();
        }
        self.current_iter.current_iter.take();
        self.failed = Some(error.clone());
        error
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
    use crate::format::sst::{BlockTransformer, SsTableFormat};
    use crate::iter::{IterationOrder, RowEntryIterator};
    use crate::manifest::ManifestCore;
    use crate::mem_table::WritableKVTable;
    use crate::object_stores::ObjectStores;
    use crate::proptest_util::{rng, sample};
    use crate::tablestore::{DecodedWalSst, TableStore, TableStoreKind};
    use crate::test_utils::{GatedObjectStore, RecordingObjectStore};
    use crate::types::{RowEntry, ValueDeletable};
    use crate::{error::SlateDBError, test_utils};
    use async_trait::async_trait;
    use bytes::Bytes;
    use futures::stream::BoxStream;
    use futures::FutureExt;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
        ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
    };
    use proptest::prelude::*;
    use proptest::test_runner::TestRng;
    use rand::Rng;
    use std::cmp::min;
    use std::collections::btree_map::Iter;
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::Notify;

    struct HistoricalXorTransformer;

    #[async_trait]
    impl BlockTransformer for HistoricalXorTransformer {
        fn max_decoded_len(&self, encoded_len: usize) -> Option<usize> {
            Some(encoded_len)
        }

        async fn encode(&self, data: Bytes) -> Result<Bytes, crate::Error> {
            Ok(Bytes::from(
                data.iter().map(|byte| byte ^ 0xa5).collect::<Vec<_>>(),
            ))
        }

        async fn decode(&self, data: Bytes) -> Result<Bytes, crate::Error> {
            self.encode(data).await
        }
    }

    struct UnboundedTransformer {
        decode_called: Arc<AtomicBool>,
    }

    #[async_trait]
    impl BlockTransformer for UnboundedTransformer {
        fn max_decoded_len(&self, _encoded_len: usize) -> Option<usize> {
            None
        }

        async fn encode(&self, data: Bytes) -> Result<Bytes, crate::Error> {
            Ok(data)
        }

        async fn decode(&self, data: Bytes) -> Result<Bytes, crate::Error> {
            self.decode_called.store(true, Ordering::SeqCst);
            Ok(data)
        }
    }

    #[cfg(test)]
    use crate::sst_builder::BlockFormat;

    #[derive(Debug)]
    struct PausedFirstWalGetStore {
        inner: Arc<dyn ObjectStore>,
        first_path: Path,
        panic_path: Option<Path>,
        first_started: Arc<AtomicBool>,
        later_get_returned: Arc<AtomicBool>,
        first_cancelled: Arc<AtomicBool>,
        release_first: Arc<Notify>,
    }

    impl PausedFirstWalGetStore {
        fn new(inner: Arc<dyn ObjectStore>, first_path: Path) -> Self {
            Self {
                inner,
                first_path,
                panic_path: None,
                first_started: Arc::new(AtomicBool::new(false)),
                later_get_returned: Arc::new(AtomicBool::new(false)),
                first_cancelled: Arc::new(AtomicBool::new(false)),
                release_first: Arc::new(Notify::new()),
            }
        }

        fn panic_on(mut self, path: Path) -> Self {
            self.panic_path = Some(path);
            self
        }

        async fn wait_until(flag: &AtomicBool) {
            while !flag.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        }
    }

    impl std::fmt::Display for PausedFirstWalGetStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "PausedFirstWalGetStore({})", self.inner)
        }
    }

    struct PendingGetGuard {
        cancelled: Arc<AtomicBool>,
        completed: bool,
    }

    impl Drop for PendingGetGuard {
        fn drop(&mut self) {
            if !self.completed {
                self.cancelled.store(true, Ordering::Release);
            }
        }
    }

    #[async_trait]
    impl ObjectStore for PausedFirstWalGetStore {
        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            if !options.head && self.panic_path.as_ref() == Some(location) {
                panic!("injected WAL fetch panic for {location}");
            }
            if !options.head && location == &self.first_path {
                let mut guard = PendingGetGuard {
                    cancelled: Arc::clone(&self.first_cancelled),
                    completed: false,
                };
                self.first_started.store(true, Ordering::Release);
                self.release_first.notified().await;
                let result = self.inner.get_opts(location, options).await;
                guard.completed = true;
                return result;
            }
            let result = self.inner.get_opts(location, options).await;
            if location != &self.first_path {
                self.later_get_returned.store(true, Ordering::Release);
            }
            result
        }

        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            options: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(location, payload, options).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            options: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        fn list_with_offset(
            &self,
            prefix: Option<&Path>,
            offset: &Path,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list_with_offset(prefix, offset)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }

        async fn rename_opts(
            &self,
            from: &Path,
            to: &Path,
            options: RenameOptions,
        ) -> object_store::Result<()> {
            self.inner.rename_opts(from, to, options).await
        }
    }

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
    async fn should_replay_zero_byte_wal_fences_between_and_after_data_wals() {
        let table_store = test_table_store();
        let first = RowEntry::new_value(b"first", b"value-1", 1);
        let second = RowEntry::new_value(b"second", b"value-2", 2);

        for (wal_id, row) in [(1, first.clone()), (3, second.clone())] {
            let mut builder = table_store.wal_table_builder();
            builder.add(row).await.unwrap();
            let encoded = builder.build().await.unwrap();
            table_store
                .write_sst(&SsTableId::Wal(wal_id), &encoded)
                .await
                .unwrap();
        }
        table_store.write_wal_fence(2).await.unwrap();
        table_store.write_wal_fence(4).await.unwrap();

        let mut replay_iter = WalReplayIterator::range(
            1..5,
            &ManifestCore::new(),
            WalReplayOptions::default(),
            Arc::clone(&table_store),
        )
        .await
        .unwrap();

        let Some(replayed) = replay_iter.next().await.unwrap() else {
            panic!("expected replayed WALs");
        };
        assert_eq!(replayed.last_wal_id, 4);
        assert_eq!(replayed.last_seq, 2);
        let mut iter = replayed.table.table().iter();
        test_utils::assert_iterator(&mut iter, vec![first, second]).await;
        assert!(replay_iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn should_replay_legacy_v1_wal_encoding() {
        let format = SsTableFormat {
            block_format: Some(BlockFormat::V1),
            ..SsTableFormat::default()
        };
        let table_store = test_table_store_with_format(format);
        let rows = vec![
            RowEntry::new_value(b"first", b"value-1", 1),
            RowEntry::new_value(b"second", b"value-2", 2),
        ];
        let mut builder = table_store.table_builder();
        for row in &rows {
            builder.add(row.clone()).await.unwrap();
        }
        let encoded = builder.build().await.unwrap();
        table_store
            .write_sst(&SsTableId::Wal(1), &encoded)
            .await
            .unwrap();

        let mut replay_iter = WalReplayIterator::range(
            1..2,
            &ManifestCore::new(),
            WalReplayOptions::default(),
            Arc::clone(&table_store),
        )
        .await
        .unwrap();
        let Some(replayed) = replay_iter.next().await.unwrap() else {
            panic!("expected legacy WAL to replay");
        };
        let mut iter = replayed.table.table().iter();
        test_utils::assert_iterator(&mut iter, rows).await;
        assert_eq!(replayed.last_wal_id, 1);
        assert_eq!(replayed.last_seq, 2);
    }

    #[tokio::test]
    async fn historical_wal_goldens_match_range_and_full_object_decoders() {
        for (name, bytes) in historical_wal_fixtures() {
            let mut format = SsTableFormat::default();
            if name.ends_with("-xor") {
                format.block_transformer = Some(Arc::new(HistoricalXorTransformer));
            }
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let table_store =
                test_table_store_with_format_and_object_store(format, Arc::clone(&store));
            let path = Path::from("/tmp/test_kv_store/wal/00000000000000000001.sst");
            store.put(&path, bytes.clone().into()).await.unwrap();

            let handle = table_store.open_sst(&SsTableId::Wal(1)).await.unwrap();
            let index = table_store.read_index(&handle, false).await.unwrap();
            let block_count = index.borrow().block_meta().len();
            let range_blocks = table_store
                .read_blocks_using_index(&handle, index, 0..block_count, false)
                .await
                .unwrap();
            let mut range_rows = Vec::new();
            for block in range_blocks {
                let mut iterator = crate::block_iterator::DataBlockIterator::new(
                    block,
                    handle.format_version,
                    IterationOrder::Ascending,
                )
                .unwrap();
                while let Some(row) = iterator.next().await.unwrap() {
                    range_rows.push(row);
                }
            }

            let decoded = table_store
                .decode_wal_sst(1, bytes, 64 * 1024 * 1024)
                .await
                .unwrap();
            let DecodedWalSst::Data(wal) = decoded else {
                panic!("historical fixture {name} decoded as a fence");
            };
            let mut iterator = super::WalBlocksIterator::new(
                Arc::clone(&table_store),
                *wal,
                192 * 1024 * 1024,
                64 * 1024 * 1024,
            )
            .unwrap();
            let mut full_rows = Vec::new();
            while let Some(row) = iterator.next().await.unwrap() {
                full_rows.push(row);
            }

            let ranged = table_store
                .open_ranged_wal_sst(1, store.head(&path).await.unwrap().size, 64 * 1024 * 1024)
                .await
                .unwrap();
            let super::RangedWalSst::Data(wal) = ranged else {
                panic!("historical fixture {name} range-decoded as a fence");
            };
            let mut iterator = super::WalBlocksIterator::new_ranged(
                Arc::clone(&table_store),
                *wal,
                192 * 1024 * 1024,
                64 * 1024 * 1024,
            )
            .unwrap();
            let mut bounded_range_rows = Vec::new();
            while let Some(row) = iterator.next().await.unwrap() {
                bounded_range_rows.push(row);
            }

            assert_eq!(full_rows, range_rows, "fixture {name}");
            assert_eq!(bounded_range_rows, range_rows, "fixture {name}");
            assert_historical_rows(name, &full_rows);

            let expected_filtered = range_rows
                .iter()
                .filter(|row| row.seq > 8)
                .cloned()
                .collect::<Vec<_>>();
            let mut replay = WalReplayIterator::range(
                1..2,
                &ManifestCore::new(),
                WalReplayOptions {
                    min_seq: Some(8),
                    ..WalReplayOptions::default()
                },
                Arc::clone(&table_store),
            )
            .await
            .unwrap();
            let replayed = replay.next().await.unwrap().unwrap();
            let mut replayed_rows = Vec::new();
            let mut replayed_iter = replayed.table.table().iter();
            while let Some(row) = replayed_iter.next().await.unwrap() {
                replayed_rows.push(row);
            }
            assert_eq!(replayed_rows, expected_filtered, "filtered fixture {name}");
            assert!(replay.next().await.unwrap().is_none());
        }
    }

    #[tokio::test]
    async fn historical_wal_goldens_report_identical_checksum_errors() {
        for (name, bytes) in historical_wal_fixtures() {
            let mut format = SsTableFormat::default();
            if name.ends_with("-xor") {
                format.block_transformer = Some(Arc::new(HistoricalXorTransformer));
            }
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let table_store =
                test_table_store_with_format_and_object_store(format, Arc::clone(&store));
            let path = Path::from("/tmp/test_kv_store/wal/00000000000000000001.sst");
            let mut corrupted = bytes.to_vec();
            corrupted[0] ^= 1;
            let corrupted = Bytes::from(corrupted);
            store.put(&path, corrupted.clone().into()).await.unwrap();

            let handle = table_store.open_sst(&SsTableId::Wal(1)).await.unwrap();
            let index = table_store.read_index(&handle, false).await.unwrap();
            let range_error = table_store
                .read_blocks_using_index(
                    &handle,
                    index.clone(),
                    0..index.borrow().block_meta().len(),
                    false,
                )
                .await;
            let Err(range_error) = range_error else {
                panic!("corrupted historical fixture {name} passed range decoding");
            };

            let decoded = table_store
                .decode_wal_sst(1, corrupted, 64 * 1024 * 1024)
                .await
                .unwrap();
            let DecodedWalSst::Data(wal) = decoded else {
                panic!("corrupted historical fixture {name} decoded as a fence");
            };
            let mut full_iterator = super::WalBlocksIterator::new(
                Arc::clone(&table_store),
                *wal,
                192 * 1024 * 1024,
                64 * 1024 * 1024,
            )
            .unwrap();
            let full_error = full_iterator.next().await.unwrap_err();

            assert_eq!(
                full_error.maybe_validation_retry_reason(),
                range_error.maybe_validation_retry_reason(),
                "fixture {name}"
            );
            assert_eq!(
                full_error.maybe_validation_retry_reason(),
                Some(crate::error::RetryReason::CrcMismatch),
                "fixture {name}"
            );
        }
    }

    #[cfg(feature = "snappy")]
    #[tokio::test]
    async fn should_replay_compressed_wal() {
        let format = SsTableFormat {
            compression_codec: Some(crate::config::CompressionCodec::Snappy),
            ..SsTableFormat::default()
        };
        let table_store = test_table_store_with_format(format);
        let row = RowEntry::new_value(b"key", &[b'x'; 16 * 1024], 1);
        let mut builder = table_store.wal_table_builder();
        builder.add(row.clone()).await.unwrap();
        let encoded = builder.build().await.unwrap();
        table_store
            .write_sst(&SsTableId::Wal(1), &encoded)
            .await
            .unwrap();

        let mut replay_iter = WalReplayIterator::range(
            1..2,
            &ManifestCore::new(),
            WalReplayOptions::default(),
            Arc::clone(&table_store),
        )
        .await
        .unwrap();
        let Some(replayed) = replay_iter.next().await.unwrap() else {
            panic!("expected compressed WAL to replay");
        };
        let mut iter = replayed.table.table().iter();
        test_utils::assert_iterator(&mut iter, vec![row]).await;
    }

    #[tokio::test]
    async fn should_finish_empty_replay_range_without_object_io() {
        let recording = Arc::new(RecordingObjectStore::new(Arc::new(InMemory::new())));
        let table_store = test_table_store_with_object_store(recording.clone());
        let mut replay_iter = WalReplayIterator::range(
            7..7,
            &ManifestCore::new(),
            WalReplayOptions::default(),
            table_store,
        )
        .await
        .unwrap();

        assert!(replay_iter.next().await.unwrap().is_none());
        assert!(recording.get_kinds(false).is_empty());
        assert!(recording.get_kinds(true).is_empty());
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
    async fn should_fetch_replay_objects_only_from_dedicated_wal_store() {
        let main = Arc::new(RecordingObjectStore::new(Arc::new(InMemory::new())));
        let wal = Arc::new(RecordingObjectStore::new(Arc::new(InMemory::new())));
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(main.clone(), Some(wal.clone())),
            SsTableFormat::default(),
            Path::from("/tmp/test_kv_store"),
            None,
            TableStoreKind::Main,
            BlockCachePolicy::default(),
        ));
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
        main.clear();
        wal.clear();

        let mut replay_iter = WalReplayIterator::range(
            1..2,
            &ManifestCore::new(),
            WalReplayOptions::default(),
            table_store,
        )
        .await
        .unwrap();
        while replay_iter.next().await.unwrap().is_some() {}

        assert!(main.get_kinds(false).is_empty());
        assert!(main.get_kinds(true).is_empty());
        assert_eq!(wal.get_kinds(false), vec![Some(TableStoreKind::Main)]);
        assert!(wal.get_kinds(true).is_empty());
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
    async fn should_recover_when_validation_refetch_returns_valid_wal() {
        let inner = Arc::new(InMemory::new());
        let recording = Arc::new(RecordingObjectStore::new(inner));
        let table_store = test_table_store_with_object_store(recording.clone());
        let mut builder = table_store.wal_table_builder();
        builder
            .add(RowEntry::new_value(b"key", b"value", 1))
            .await
            .unwrap();
        let encoded = builder.build().await.unwrap();
        let valid = encoded.remaining_as_bytes();
        table_store
            .write_sst(&SsTableId::Wal(1), &encoded)
            .await
            .unwrap();
        recording.clear();

        let mut corrupted = valid.to_vec();
        corrupted[0] ^= 1;
        let initially_decoded = table_store
            .decode_wal_sst(1, Bytes::from(corrupted), 64 * 1024 * 1024)
            .await
            .unwrap();
        let DecodedWalSst::Data(initially_decoded) = initially_decoded else {
            panic!("nonempty WAL decoded as a fence");
        };
        let Err(initial_error) = table_store
            .decode_wal_block(&initially_decoded, 0, 64 * 1024 * 1024)
            .await
        else {
            panic!("corrupt initial WAL block unexpectedly decoded");
        };
        drop(initially_decoded);
        let reason = initial_error
            .maybe_validation_retry_reason()
            .expect("corruption must be classified for a validation retry");
        let decoded = table_store
            .refetch_wal_sst_after_validation(
                1,
                valid.len(),
                192 * 1024 * 1024,
                64 * 1024 * 1024,
                reason,
            )
            .await
            .unwrap();

        assert!(matches!(decoded, DecodedWalSst::Data(_)));
        assert_eq!(recording.get_kinds(false).len(), 1);
        assert!(recording.get_retries(false)[0].is_some());
    }

    #[tokio::test]
    async fn should_fail_without_panicking_for_every_truncated_wal_prefix() {
        let inner = Arc::new(InMemory::new());
        let table_store = test_table_store_with_object_store(inner.clone());
        let mut builder = table_store.wal_table_builder();
        for seq in 1..=8 {
            builder
                .add(RowEntry::new_value(
                    format!("key-{seq}").as_bytes(),
                    &[b'x'; 128],
                    seq,
                ))
                .await
                .unwrap();
        }
        let encoded = builder.build().await.unwrap();
        let valid = encoded.remaining_as_bytes();
        let location = Path::from("/tmp/test_kv_store/wal/00000000000000000001.sst");

        for prefix_len in 1..valid.len() {
            let truncated = valid.slice(..prefix_len);
            inner
                .put(&location, truncated.clone().into())
                .await
                .unwrap();
            let result = std::panic::AssertUnwindSafe(table_store.decode_wal_sst(
                1,
                truncated,
                64 * 1024 * 1024,
            ))
            .catch_unwind()
            .await;
            let decode = result
                .unwrap_or_else(|_| panic!("WAL decoder panicked for prefix length {prefix_len}"));
            assert!(
                decode.is_err(),
                "truncated WAL prefix {prefix_len} decoded successfully"
            );
        }
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
    async fn should_remain_failed_after_a_replay_error() {
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
        let Err(first) = replay_iter.next().await else {
            panic!("missing WAL must fail replay");
        };
        let Err(second) = replay_iter.next().await else {
            panic!("failed replay iterator must remain failed");
        };
        assert!(first.has_object_store_not_found());
        assert!(second.has_object_store_not_found());
    }

    #[tokio::test]
    async fn should_construct_huge_sparse_range_without_materializing_every_wal_id() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let gated = Arc::new(GatedObjectStore::new(inner));
        gated.get_opts_gate.close();
        let table_store = test_table_store_with_object_store(gated.clone());

        let replay_iter = tokio::time::timeout(
            Duration::from_secs(1),
            WalReplayIterator::range(
                1..u64::MAX,
                &ManifestCore::new(),
                WalReplayOptions {
                    prefetch: WalReplaySettings {
                        max_concurrent_objects: 2,
                        max_inflight_bytes: 2,
                    },
                    ..WalReplayOptions::default()
                },
                table_store,
            ),
        )
        .await
        .expect("replay range construction allocated proportional to the WAL ID span")
        .unwrap();

        gated.get_opts_gate.wait_for_arrivals(1).await;
        assert_eq!(gated.get_opts_gate.arrivals(), 1);
        drop(replay_iter);
    }

    #[tokio::test]
    async fn should_fail_if_wal_size_changes_after_listing() {
        let inner = Arc::new(InMemory::new());
        let gated = Arc::new(GatedObjectStore::new(inner.clone()));
        let table_store = test_table_store_with_object_store(gated.clone());
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
        gated.get_opts_gate.close();

        let mut replay_iter = WalReplayIterator::range(
            1..2,
            &ManifestCore::new(),
            WalReplayOptions::default(),
            table_store,
        )
        .await
        .unwrap();
        gated.get_opts_gate.wait_for_arrivals(1).await;
        let location = Path::from("/tmp/test_kv_store/wal/00000000000000000001.sst");
        inner
            .put(&location, Bytes::from_static(b"changed").into())
            .await
            .unwrap();
        gated.get_opts_gate.release();

        let Err(error) = replay_iter.next().await else {
            panic!("changed WAL size must fail replay");
        };
        assert!(matches!(error, SlateDBError::WalDataError(_)));
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

    #[cfg(any(
        feature = "snappy",
        feature = "zlib",
        feature = "lz4",
        feature = "zstd"
    ))]
    #[tokio::test]
    async fn compressed_wals_cannot_expand_past_total_replay_memory_budget() {
        let mut codecs = Vec::new();
        #[cfg(feature = "snappy")]
        codecs.push(crate::config::CompressionCodec::Snappy);
        #[cfg(feature = "zlib")]
        codecs.push(crate::config::CompressionCodec::Zlib);
        #[cfg(feature = "lz4")]
        codecs.push(crate::config::CompressionCodec::Lz4);
        #[cfg(feature = "zstd")]
        codecs.push(crate::config::CompressionCodec::Zstd);

        for codec in codecs {
            let inner = Arc::new(InMemory::new());
            let recording = Arc::new(RecordingObjectStore::new(inner));
            let format = SsTableFormat {
                compression_codec: Some(codec),
                ..SsTableFormat::default()
            };
            let table_store =
                test_table_store_with_format_and_object_store(format, recording.clone());
            let mut builder = table_store.wal_table_builder();
            builder
                .add(RowEntry::new_value(b"large", &vec![b'x'; 1024 * 1024], 1))
                .await
                .unwrap();
            let encoded = builder.build().await.unwrap();
            table_store
                .write_sst(&SsTableId::Wal(1), &encoded)
                .await
                .unwrap();
            recording.clear();

            let total_budget = 256 * 1024;
            let mut replay = WalReplayIterator::range(
                1..2,
                &ManifestCore::new(),
                WalReplayOptions {
                    prefetch: WalReplaySettings {
                        max_concurrent_objects: 64,
                        max_inflight_bytes: total_budget,
                    },
                    ..WalReplayOptions::default()
                },
                table_store,
            )
            .await
            .unwrap();
            assert!(replay.peak_reserved_bytes + replay.working_memory_limit <= total_budget);
            let Err(first_error) = replay.next().await else {
                panic!("{codec:?} WAL expanded past its memory budget");
            };
            let Err(second_error) = replay.next().await else {
                panic!("{codec:?} memory-limit failure was not sticky");
            };
            assert!(matches!(
                first_error,
                SlateDBError::WalReplayMemoryLimitExceeded { .. }
            ));
            assert!(matches!(
                second_error,
                SlateDBError::WalReplayMemoryLimitExceeded { .. }
            ));
            assert_eq!(recording.get_kinds(false).len(), 1, "{codec:?}");
        }
    }

    #[tokio::test]
    async fn oversized_encoded_wal_uses_bounded_range_replay() {
        let inner = Arc::new(InMemory::new());
        let recording = Arc::new(RecordingObjectStore::new(inner));
        let table_store = test_table_store_with_object_store(recording.clone());
        let mut builder = table_store.wal_table_builder();
        for seq in 1..=64 {
            builder
                .add(RowEntry::new_value(
                    format!("large-{seq:03}").as_bytes(),
                    &[b'x'; 2048],
                    seq,
                ))
                .await
                .unwrap();
        }
        let encoded = builder.build().await.unwrap();
        table_store
            .write_sst(&SsTableId::Wal(1), &encoded)
            .await
            .unwrap();
        recording.clear();

        let mut replay = WalReplayIterator::range(
            1..2,
            &ManifestCore::new(),
            WalReplayOptions {
                prefetch: WalReplaySettings {
                    max_concurrent_objects: 64,
                    max_inflight_bytes: 64 * 1024,
                },
                ..WalReplayOptions::default()
            },
            table_store,
        )
        .await
        .unwrap();
        let replayed = replay
            .next()
            .await
            .expect("oversized WAL should remain recoverable")
            .expect("oversized WAL should produce a replayed table");
        let mut rows = replayed.table.table().iter();
        let mut row_count = 0;
        while let Some(row) = rows.next().await.unwrap() {
            assert_eq!(row.value.len(), 2048);
            row_count += 1;
        }
        assert_eq!(row_count, 64);
        assert!(replay.next().await.unwrap().is_none());
        assert!(recording.get_kinds(false).len() > 1);
        assert!(
            recording
                .get_range_sizes()
                .into_iter()
                .all(|range_bytes| range_bytes <= 8 * 1024),
            "range replay must not fetch a section larger than its working partition"
        );
    }

    #[tokio::test]
    async fn oversized_range_replay_rejects_large_metadata_before_fetching_it() {
        let inner = Arc::new(InMemory::new());
        let recording = Arc::new(RecordingObjectStore::new(inner.clone()));
        let table_store = test_table_store_with_object_store(recording.clone());
        let mut builder = table_store.wal_table_builder();
        for seq in 1..=64 {
            builder
                .add(RowEntry::new_value(
                    format!("large-metadata-{seq:03}").as_bytes(),
                    &[b'x'; 2048],
                    seq,
                ))
                .await
                .unwrap();
        }
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
            .unwrap()
            .metadata;
        let mut bytes = inner
            .get(&metadata.location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .to_vec();
        let footer_start = bytes.len() - 10;
        bytes[footer_start..footer_start + 8].copy_from_slice(&0_u64.to_be_bytes());
        inner
            .put(&metadata.location, Bytes::from(bytes).into())
            .await
            .unwrap();
        recording.clear();

        let mut replay = WalReplayIterator::range(
            1..2,
            &ManifestCore::new(),
            WalReplayOptions {
                prefetch: WalReplaySettings {
                    max_concurrent_objects: 4,
                    max_inflight_bytes: 64 * 1024,
                },
                ..WalReplayOptions::default()
            },
            table_store,
        )
        .await
        .unwrap();
        let Err(error) = replay.next().await else {
            panic!("oversized metadata was not rejected");
        };

        assert!(matches!(
            error,
            SlateDBError::WalReplayMemoryLimitExceeded {
                kind: "encoded and decoded WAL metadata",
                ..
            }
        ));
        assert!(
            recording
                .get_range_sizes()
                .into_iter()
                .all(|range_bytes| range_bytes <= 8 * 1024),
            "metadata validation must reject before an oversized range GET"
        );
    }

    #[tokio::test]
    async fn oversized_range_replay_rejects_large_index_before_fetching_it() {
        let inner = Arc::new(InMemory::new());
        let recording = Arc::new(RecordingObjectStore::new(inner));
        let format = SsTableFormat {
            block_size: 128,
            ..SsTableFormat::default()
        };
        let table_store = test_table_store_with_format_and_object_store(format, recording.clone());
        let mut builder = table_store.wal_table_builder();
        for seq in 1..=1024 {
            builder
                .add(RowEntry::new_value(
                    format!("large-index-{seq:04}-{}", "k".repeat(64)).as_bytes(),
                    &[b'x'; 128],
                    seq,
                ))
                .await
                .unwrap();
        }
        let encoded = builder.build().await.unwrap();
        let index_len = usize::try_from(encoded.info.index_len).unwrap();
        let total_budget = 64 * 1024;
        let metadata_memory_limit = total_budget / 8;
        assert!(encoded.remaining_len() > total_budget * 3 / 4);
        assert!(index_len > metadata_memory_limit);
        assert!(encoded.unconsumed_blocks.len() > 100);
        table_store
            .write_sst(&SsTableId::Wal(1), &encoded)
            .await
            .unwrap();
        recording.clear();

        let mut replay = WalReplayIterator::range(
            1..2,
            &ManifestCore::new(),
            WalReplayOptions {
                prefetch: WalReplaySettings {
                    max_concurrent_objects: 4,
                    max_inflight_bytes: total_budget,
                },
                ..WalReplayOptions::default()
            },
            table_store,
        )
        .await
        .unwrap();
        let Err(error) = replay.next().await else {
            panic!("oversized WAL index was not rejected");
        };

        assert!(matches!(
            error,
            SlateDBError::WalReplayMemoryLimitExceeded {
                kind: "encoded and decoded WAL index",
                ..
            }
        ));
        assert!(
            recording
                .get_range_sizes()
                .into_iter()
                .all(|range_bytes| range_bytes <= metadata_memory_limit as u64),
            "index validation must reject before the oversized index range GET"
        );
    }

    #[tokio::test]
    async fn oversized_range_replay_supports_codecs_and_block_transformations() {
        let mut codecs = Vec::with_capacity(5);
        codecs.push(None);
        #[cfg(feature = "snappy")]
        codecs.push(Some(crate::config::CompressionCodec::Snappy));
        #[cfg(feature = "zlib")]
        codecs.push(Some(crate::config::CompressionCodec::Zlib));
        #[cfg(feature = "lz4")]
        codecs.push(Some(crate::config::CompressionCodec::Lz4));
        #[cfg(feature = "zstd")]
        codecs.push(Some(crate::config::CompressionCodec::Zstd));

        for codec in codecs {
            for transformed in [false, true] {
                let inner = Arc::new(InMemory::new());
                let recording = Arc::new(RecordingObjectStore::new(inner));
                let format = SsTableFormat {
                    compression_codec: codec,
                    block_transformer: transformed
                        .then(|| Arc::new(HistoricalXorTransformer) as Arc<dyn BlockTransformer>),
                    ..SsTableFormat::default()
                };
                let table_store =
                    test_table_store_with_format_and_object_store(format, recording.clone());
                let mut builder = table_store.wal_table_builder();
                let mut test_rng = rng::new_test_rng(None);
                for seq in 1..=64 {
                    let mut value = vec![0_u8; 2048];
                    test_rng.fill(value.as_mut_slice());
                    builder
                        .add(RowEntry::new_value(
                            format!("codec-{seq:03}").as_bytes(),
                            &value,
                            seq,
                        ))
                        .await
                        .unwrap();
                }
                let encoded = builder.build().await.unwrap();
                assert!(encoded.remaining_len() > 96 * 1024, "{codec:?}");
                table_store
                    .write_sst(&SsTableId::Wal(1), &encoded)
                    .await
                    .unwrap();
                recording.clear();

                let mut replay = WalReplayIterator::range(
                    1..2,
                    &ManifestCore::new(),
                    WalReplayOptions {
                        prefetch: WalReplaySettings {
                            max_concurrent_objects: 4,
                            max_inflight_bytes: 128 * 1024,
                        },
                        ..WalReplayOptions::default()
                    },
                    table_store,
                )
                .await
                .unwrap();
                let replayed = replay.next().await.unwrap().unwrap();
                assert_eq!(replayed.table.metadata().entry_num, 64, "{codec:?}");
                assert!(replay.next().await.unwrap().is_none());
                assert!(
                    recording.get_kinds(false).len() > 1,
                    "{codec:?}, transformed={transformed}"
                );
            }
        }
    }

    #[tokio::test]
    async fn exhausted_block_is_dropped_before_decoding_the_next_block() {
        let format = SsTableFormat {
            block_size: 64,
            ..SsTableFormat::default()
        };
        let table_store = test_table_store_with_format(format);
        let mut builder = table_store.wal_table_builder();
        for seq in 1..=3 {
            builder
                .add(RowEntry::new_value(
                    format!("key-{seq}").as_bytes(),
                    &[b'x'; 48],
                    seq,
                ))
                .await
                .unwrap();
        }
        let encoded = builder.build().await.unwrap();
        table_store
            .write_sst(&SsTableId::Wal(1), &encoded)
            .await
            .unwrap();
        let bytes = table_store
            .read_wal_sst_bytes(1, None, encoded.remaining_len())
            .await
            .unwrap();
        let DecodedWalSst::Data(wal) = table_store
            .decode_wal_sst(1, bytes, 1024 * 1024)
            .await
            .unwrap()
        else {
            panic!("data WAL decoded as a fence");
        };
        let probe = Arc::new(super::BlockLifetimeProbe::default());
        let mut iterator =
            super::WalBlocksIterator::new(Arc::clone(&table_store), *wal, 1024 * 1024, 1024 * 1024)
                .unwrap()
                .observe_block_lifetimes_with(Arc::clone(&probe));

        while iterator.next().await.unwrap().is_some() {}

        assert_eq!(probe.active.load(Ordering::SeqCst), 0);
        assert_eq!(probe.peak.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn cancelling_wal_block_iteration_releases_the_current_block() {
        let format = SsTableFormat {
            block_size: 64,
            ..SsTableFormat::default()
        };
        let table_store = test_table_store_with_format(format);
        let mut builder = table_store.wal_table_builder();
        builder
            .add(RowEntry::new_value(b"key", &[b'x'; 48], 1))
            .await
            .unwrap();
        let encoded = builder.build().await.unwrap();
        table_store
            .write_sst(&SsTableId::Wal(1), &encoded)
            .await
            .unwrap();
        let bytes = table_store
            .read_wal_sst_bytes(1, None, encoded.remaining_len())
            .await
            .unwrap();
        let DecodedWalSst::Data(wal) = table_store
            .decode_wal_sst(1, bytes, 1024 * 1024)
            .await
            .unwrap()
        else {
            panic!("data WAL decoded as a fence");
        };
        let probe = Arc::new(super::BlockLifetimeProbe::default());
        let mut iterator =
            super::WalBlocksIterator::new(table_store, *wal, 1024 * 1024, 1024 * 1024)
                .unwrap()
                .observe_block_lifetimes_with(Arc::clone(&probe));
        assert!(iterator.next().await.unwrap().is_some());
        assert_eq!(probe.active.load(Ordering::SeqCst), 1);

        drop(iterator);

        assert_eq!(probe.active.load(Ordering::SeqCst), 0);
        assert_eq!(probe.peak.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn decoding_failure_drops_the_exhausted_block_first() {
        let format = SsTableFormat {
            block_size: 64,
            ..SsTableFormat::default()
        };
        let table_store = test_table_store_with_format(format);
        let mut builder = table_store.wal_table_builder();
        for seq in 1..=3 {
            builder
                .add(RowEntry::new_value(
                    format!("key-{seq}").as_bytes(),
                    &[b'x'; 48],
                    seq,
                ))
                .await
                .unwrap();
        }
        let encoded = builder.build().await.unwrap();
        let original = encoded.remaining_as_bytes();
        let DecodedWalSst::Data(original_wal) = table_store
            .decode_wal_sst(1, original.clone(), 1024 * 1024)
            .await
            .unwrap()
        else {
            panic!("data WAL decoded as a fence");
        };
        let second_block_offset = original_wal.index.borrow().block_meta().get(1).offset() as usize;
        let mut corrupted = original.to_vec();
        corrupted[second_block_offset] ^= 1;
        let DecodedWalSst::Data(wal) = table_store
            .decode_wal_sst(1, Bytes::from(corrupted), 1024 * 1024)
            .await
            .unwrap()
        else {
            panic!("data WAL decoded as a fence");
        };
        let probe = Arc::new(super::BlockLifetimeProbe::default());
        let mut iterator =
            super::WalBlocksIterator::new(table_store, *wal, 1024 * 1024, 1024 * 1024)
                .unwrap()
                .observe_block_lifetimes_with(Arc::clone(&probe));

        assert!(iterator.next().await.unwrap().is_some());
        let Err(error) = iterator.next().await else {
            panic!("corrupt second block did not fail");
        };

        assert!(matches!(error, SlateDBError::ChecksumMismatch { .. }));
        assert_eq!(probe.active.load(Ordering::SeqCst), 0);
        assert_eq!(probe.peak.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn oversized_range_replay_never_exposes_a_partial_wal_after_late_corruption() {
        let inner = Arc::new(InMemory::new());
        let table_store = test_table_store_with_object_store(inner.clone());
        let mut builder = table_store.wal_table_builder();
        for seq in 1..=64 {
            builder
                .add(RowEntry::new_value(
                    format!("corrupt-{seq:03}").as_bytes(),
                    &[b'x'; 2048],
                    seq,
                ))
                .await
                .unwrap();
        }
        let encoded = builder.build().await.unwrap();
        assert!(encoded.remaining_len() > 96 * 1024);
        table_store
            .write_sst(&SsTableId::Wal(1), &encoded)
            .await
            .unwrap();
        let handle = table_store.open_sst(&SsTableId::Wal(1)).await.unwrap();
        let index = table_store.read_index(&handle, false).await.unwrap();
        let second_block_offset = index.borrow().block_meta().get(1).offset() as usize;
        let metadata = table_store
            .list_wal_ssts_for_replay(1..2)
            .await
            .unwrap()
            .pop()
            .unwrap()
            .metadata;
        let mut bytes = inner
            .get(&metadata.location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .to_vec();
        bytes[second_block_offset] ^= 1;
        inner
            .put(&metadata.location, Bytes::from(bytes).into())
            .await
            .unwrap();

        let mut replay = WalReplayIterator::range(
            1..2,
            &ManifestCore::new(),
            WalReplayOptions {
                prefetch: WalReplaySettings {
                    max_concurrent_objects: 4,
                    max_inflight_bytes: 128 * 1024,
                },
                ..WalReplayOptions::default()
            },
            table_store,
        )
        .await
        .unwrap();
        let Err(first) = replay.next().await else {
            panic!("corrupt oversized WAL was partially exposed");
        };
        let Err(second) = replay.next().await else {
            panic!("corrupt oversized WAL failure was not sticky");
        };
        assert!(matches!(first, SlateDBError::ChecksumMismatch { .. }));
        assert!(matches!(second, SlateDBError::ChecksumMismatch { .. }));
    }

    #[tokio::test]
    async fn unbounded_transform_is_rejected_before_decode() {
        let decode_called = Arc::new(AtomicBool::new(false));
        let format = SsTableFormat {
            block_transformer: Some(Arc::new(UnboundedTransformer {
                decode_called: Arc::clone(&decode_called),
            })),
            ..SsTableFormat::default()
        };
        let table_store = test_table_store_with_format(format);
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

        let mut replay = WalReplayIterator::range(
            1..2,
            &ManifestCore::new(),
            WalReplayOptions::default(),
            table_store,
        )
        .await
        .unwrap();
        assert!(matches!(
            replay.next().await,
            Err(SlateDBError::BlockTransformError)
        ));
        assert!(!decode_called.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn later_checksum_valid_structural_corruption_returns_no_partial_wal() {
        let inner = Arc::new(InMemory::new());
        let recording = Arc::new(RecordingObjectStore::new(inner.clone()));
        let table_store = test_table_store_with_object_store(recording.clone());
        let mut builder = table_store.wal_table_builder();
        for seq in 1..=12 {
            builder
                .add(RowEntry::new_value(
                    format!("key-{seq:02}").as_bytes(),
                    &[b'x'; 1024],
                    seq,
                ))
                .await
                .unwrap();
        }
        let encoded = builder.build().await.unwrap();
        let mut bytes = encoded.remaining_as_bytes().to_vec();
        let decoded = table_store
            .decode_wal_sst(1, Bytes::copy_from_slice(&bytes), 64 * 1024 * 1024)
            .await
            .unwrap();
        let DecodedWalSst::Data(wal) = decoded else {
            panic!("data WAL decoded as a fence");
        };
        let block_meta = wal.index.borrow().block_meta();
        assert!(block_meta.len() >= 2);
        let block_index = block_meta.len() - 1;
        let start = block_meta.get(block_index).offset() as usize;
        let end = wal.info.filter_offset as usize;
        drop(wal);
        let payload_end = end - crate::format::sst::CHECKSUM_SIZE;
        bytes[payload_end - 2..payload_end].copy_from_slice(&0u16.to_be_bytes());
        let checksum = crc32fast::hash(&bytes[start..payload_end]);
        bytes[payload_end..end].copy_from_slice(&checksum.to_be_bytes());
        let path = Path::from("/tmp/test_kv_store/wal/00000000000000000001.sst");
        inner.put(&path, Bytes::from(bytes).into()).await.unwrap();
        recording.clear();

        let mut replay = WalReplayIterator::range(
            1..2,
            &ManifestCore::new(),
            WalReplayOptions::default(),
            table_store,
        )
        .await
        .unwrap();
        assert!(replay.next().await.is_err());
        assert!(replay.next().await.is_err());
        assert_eq!(recording.get_kinds(false).len(), 1);
    }

    #[tokio::test]
    async fn checksum_valid_metadata_index_block_and_row_mutations_never_panic() {
        let table_store = test_table_store();
        let mut builder = table_store.wal_table_builder();
        for seq in 1..=16 {
            builder
                .add(RowEntry::new(
                    format!("key-{seq:02}").into(),
                    if seq % 3 == 0 {
                        ValueDeletable::Merge(Bytes::from_static(b"merge"))
                    } else if seq % 5 == 0 {
                        ValueDeletable::Tombstone
                    } else {
                        ValueDeletable::Value(Bytes::from(vec![seq as u8; 256]))
                    },
                    seq,
                    (seq % 2 == 0).then_some(seq as i64),
                    (seq % 4 == 0).then_some((seq * 10) as i64),
                ))
                .await
                .unwrap();
        }
        let encoded = builder.build().await.unwrap().remaining_as_bytes();
        let decoded = table_store
            .decode_wal_sst(1, encoded.clone(), 64 * 1024 * 1024)
            .await
            .unwrap();
        let DecodedWalSst::Data(wal) = decoded else {
            panic!("data WAL decoded as a fence");
        };
        let metadata_start = u64::from_be_bytes(
            encoded[encoded.len() - 10..encoded.len() - 2]
                .try_into()
                .unwrap(),
        ) as usize;
        let metadata_end = encoded.len() - 10;
        let index_start = wal.info.index_offset as usize;
        let index_end = index_start + wal.info.index_len as usize;
        let block_meta = wal.index.borrow().block_meta();
        let block_start = block_meta.get(0).offset() as usize;
        let block_end = if block_meta.len() > 1 {
            block_meta.get(1).offset() as usize
        } else {
            wal.info.filter_offset as usize
        };
        drop(wal);

        let sections = [
            ("metadata", metadata_start, metadata_end),
            ("index", index_start, index_end),
            ("block-and-row", block_start, block_end),
        ];
        for (section_name, start, end) in sections {
            let payload_end = end - crate::format::sst::CHECKSUM_SIZE;
            assert!(payload_end > start);
            let mut structural_errors = 0;
            for mutation in 0..256usize {
                let mut mutated = encoded.to_vec();
                let position = start + mutation % (payload_end - start);
                mutated[position] ^= 1u8 << (mutation % 8);
                let checksum = crc32fast::hash(&mutated[start..payload_end]);
                mutated[payload_end..end].copy_from_slice(&checksum.to_be_bytes());
                let table_store = Arc::clone(&table_store);
                let decode = std::panic::AssertUnwindSafe(async move {
                    let decoded = table_store
                        .decode_wal_sst(1, Bytes::from(mutated), 64 * 1024 * 1024)
                        .await?;
                    let DecodedWalSst::Data(wal) = decoded else {
                        return Err(SlateDBError::CorruptSst {
                            reason: "mutated data WAL decoded as a fence",
                            path: None,
                        });
                    };
                    let mut iterator = super::WalBlocksIterator::new(
                        Arc::clone(&table_store),
                        *wal,
                        192 * 1024 * 1024,
                        64 * 1024 * 1024,
                    )?;
                    while iterator.next().await?.is_some() {}
                    Ok::<(), SlateDBError>(())
                })
                .catch_unwind()
                .await;
                let result = decode.unwrap_or_else(|_| {
                    panic!("checksum-valid {section_name} mutation {mutation} panicked")
                });
                if result.is_err() {
                    structural_errors += 1;
                }
            }
            assert!(
                structural_errors > 0,
                "{section_name} mutations never reached a structural error"
            );
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]

        #[test]
        fn checksum_valid_wal_structural_fuzz_never_panics(
            section in 0usize..3,
            mutations in prop::collection::vec((any::<usize>(), any::<u8>()), 1..=16),
        ) {
            let (_, encoded) = historical_wal_fixtures()
                .into_iter()
                .find(|(name, _)| *name == "v2-none")
                .unwrap();
            let table_store = test_table_store();
            let decoded = futures::executor::block_on(
                table_store.decode_wal_sst(1, encoded.clone(), 64 * 1024 * 1024),
            )
            .unwrap();
            let DecodedWalSst::Data(wal) = decoded else {
                panic!("historical data WAL decoded as a fence");
            };
            let metadata_start = u64::from_be_bytes(
                encoded[encoded.len() - 10..encoded.len() - 2]
                    .try_into()
                    .unwrap(),
            ) as usize;
            let metadata_end = encoded.len() - 10;
            let index_start = wal.info.index_offset as usize;
            let index_end = index_start + wal.info.index_len as usize;
            let block_meta = wal.index.borrow().block_meta();
            let block_start = block_meta.get(0).offset() as usize;
            let block_end = if block_meta.len() > 1 {
                block_meta.get(1).offset() as usize
            } else {
                wal.info.filter_offset as usize
            };
            let sections = [
                (metadata_start, metadata_end),
                (index_start, index_end),
                (block_start, block_end),
            ];
            let (start, end) = sections[section];
            let payload_end = end - crate::format::sst::CHECKSUM_SIZE;
            let mut mutated = encoded.to_vec();
            for (offset, value) in mutations {
                let position = start + offset % (payload_end - start);
                mutated[position] ^= value.max(1);
            }
            let checksum = crc32fast::hash(&mutated[start..payload_end]);
            mutated[payload_end..end].copy_from_slice(&checksum.to_be_bytes());

            let decode = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                futures::executor::block_on(async {
                    let decoded = table_store
                        .decode_wal_sst(1, Bytes::from(mutated), 64 * 1024 * 1024)
                        .await?;
                    let DecodedWalSst::Data(wal) = decoded else {
                        return Err(SlateDBError::CorruptSst {
                            reason: "mutated data WAL decoded as a fence",
                            path: None,
                        });
                    };
                    let mut iterator = super::WalBlocksIterator::new(
                        Arc::clone(&table_store),
                        *wal,
                        192 * 1024 * 1024,
                        64 * 1024 * 1024,
                    )?;
                    while iterator.next().await?.is_some() {}
                    Ok::<(), SlateDBError>(())
                })
            }));
            prop_assert!(decode.is_ok());
        }
    }

    #[tokio::test]
    async fn should_apply_wals_in_id_order_when_later_get_returns_first() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let first_path = Path::from("/tmp/test_kv_store/wal/00000000000000000001.sst");
        let reordered = Arc::new(PausedFirstWalGetStore::new(inner, first_path));
        let table_store = test_table_store_with_object_store(reordered.clone());
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

        let mut replay_iter = WalReplayIterator::range(
            1..3,
            &ManifestCore::new(),
            WalReplayOptions {
                prefetch: WalReplaySettings {
                    max_concurrent_objects: 2,
                    max_inflight_bytes: 1024 * 1024,
                },
                max_memtable_bytes: 1,
                ..WalReplayOptions::default()
            },
            table_store,
        )
        .await
        .unwrap();
        PausedFirstWalGetStore::wait_until(&reordered.first_started).await;
        PausedFirstWalGetStore::wait_until(&reordered.later_get_returned).await;

        let (result_tx, mut result_rx) = tokio::sync::mpsc::unbounded_channel();
        let replay_task = tokio::spawn(async move {
            while let Some(replayed) = replay_iter.next().await.unwrap() {
                result_tx.send(replayed.last_wal_id).unwrap();
            }
        });
        for _ in 0..100 {
            tokio::task::yield_now().await;
        }
        assert!(matches!(
            result_rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));

        reordered.release_first.notify_one();
        assert_eq!(result_rx.recv().await, Some(1));
        assert_eq!(result_rx.recv().await, Some(2));
        replay_task.await.unwrap();
    }

    #[tokio::test]
    async fn should_abort_pending_fetches_when_replay_iterator_is_dropped() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let first_path = Path::from("/tmp/test_kv_store/wal/00000000000000000001.sst");
        let paused = Arc::new(PausedFirstWalGetStore::new(inner, first_path));
        let table_store = test_table_store_with_object_store(paused.clone());
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

        let replay_iter = WalReplayIterator::range(
            1..2,
            &ManifestCore::new(),
            WalReplayOptions::default(),
            table_store,
        )
        .await
        .unwrap();
        PausedFirstWalGetStore::wait_until(&paused.first_started).await;
        drop(replay_iter);

        tokio::time::timeout(
            Duration::from_secs(1),
            PausedFirstWalGetStore::wait_until(&paused.first_cancelled),
        )
        .await
        .expect("pending WAL fetch was detached instead of aborted");
    }

    #[tokio::test]
    async fn should_abort_later_fetches_after_a_replay_error() {
        let inner = Arc::new(InMemory::new());
        let paused_path = Path::from("/tmp/test_kv_store/wal/00000000000000000002.sst");
        let paused = Arc::new(PausedFirstWalGetStore::new(inner.clone(), paused_path));
        let table_store = test_table_store_with_object_store(paused.clone());
        let mut first_wal_location = None;
        for wal_id in 1..=2 {
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
            if wal_id == 1 {
                first_wal_location = Some(
                    table_store
                        .list_wal_ssts_for_replay(1..2)
                        .await
                        .unwrap()
                        .pop()
                        .unwrap()
                        .metadata
                        .location,
                );
            }
        }
        let first_wal_location = first_wal_location.unwrap();
        let mut corrupted = inner
            .get(&first_wal_location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .to_vec();
        corrupted[0] ^= 1;
        inner
            .put(&first_wal_location, corrupted.into())
            .await
            .unwrap();

        let mut replay_iter = WalReplayIterator::range(
            1..3,
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
        PausedFirstWalGetStore::wait_until(&paused.first_started).await;

        assert!(matches!(
            replay_iter.next().await,
            Err(SlateDBError::ChecksumMismatch { .. })
        ));
        tokio::time::timeout(
            Duration::from_secs(1),
            PausedFirstWalGetStore::wait_until(&paused.first_cancelled),
        )
        .await
        .expect("a replay error did not abort a later in-flight WAL fetch");
    }

    #[tokio::test]
    async fn should_fail_terminally_and_abort_later_fetches_after_a_fetch_panic() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let first_path = Path::from("/tmp/test_kv_store/wal/00000000000000000001.sst");
        let paused_path = Path::from("/tmp/test_kv_store/wal/00000000000000000002.sst");
        let store = Arc::new(PausedFirstWalGetStore::new(inner, paused_path).panic_on(first_path));
        let table_store = test_table_store_with_object_store(store.clone());
        for wal_id in 1..=2 {
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
            1..3,
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
        PausedFirstWalGetStore::wait_until(&store.first_started).await;

        let Err(first) = replay_iter.next().await else {
            panic!("panicking WAL fetch must fail replay");
        };
        let Err(second) = replay_iter.next().await else {
            panic!("failed replay iterator must remain failed");
        };
        assert!(matches!(first, SlateDBError::BackgroundTaskPanic(_)));
        assert!(matches!(second, SlateDBError::BackgroundTaskPanic(_)));
        tokio::time::timeout(
            Duration::from_secs(1),
            PausedFirstWalGetStore::wait_until(&store.first_cancelled),
        )
        .await
        .expect("fetch panic did not abort a later in-flight WAL fetch");
    }

    #[tokio::test]
    async fn should_fail_terminally_if_a_fetch_task_is_cancelled() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let gated = Arc::new(GatedObjectStore::new(inner));
        let table_store = test_table_store_with_object_store(gated.clone());
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
        gated.get_opts_gate.close();

        let mut replay_iter = WalReplayIterator::range(
            1..2,
            &ManifestCore::new(),
            WalReplayOptions::default(),
            table_store,
        )
        .await
        .unwrap();
        gated.get_opts_gate.wait_for_arrivals(1).await;
        replay_iter.pending_fetches.front().unwrap().handle.abort();

        let Err(first) = replay_iter.next().await else {
            panic!("cancelled WAL fetch must fail replay");
        };
        let Err(second) = replay_iter.next().await else {
            panic!("failed replay iterator must remain failed");
        };
        assert!(matches!(first, SlateDBError::BackgroundTaskCancelled(_)));
        assert!(matches!(second, SlateDBError::BackgroundTaskCancelled(_)));
    }

    #[tokio::test]
    async fn should_hold_byte_permits_until_the_wal_is_consumed() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let gated = Arc::new(GatedObjectStore::new(inner));
        let table_store = test_table_store_with_format_and_object_store(
            SsTableFormat {
                block_size: 256,
                ..SsTableFormat::default()
            },
            gated.clone(),
        );
        for wal_id in 1..=2 {
            let mut builder = table_store.wal_table_builder();
            for row_id in 0..16 {
                builder
                    .add(RowEntry::new_value(
                        format!("key-{wal_id}-{row_id:02}").as_bytes(),
                        &[b'x'; 128],
                        wal_id * 16 + row_id,
                    ))
                    .await
                    .unwrap();
            }
            let encoded = builder.build().await.unwrap();
            table_store
                .write_sst(&SsTableId::Wal(wal_id), &encoded)
                .await
                .unwrap();
        }
        let listed = table_store.list_wal_ssts_for_replay(1..3).await.unwrap();
        let largest_object = listed
            .iter()
            .map(|entry| usize::try_from(entry.metadata.size).unwrap())
            .max()
            .unwrap();
        let total_budget = largest_object.checked_mul(2).unwrap();
        let working_budget = total_budget / 4;
        let encoded_budget = total_budget - working_budget;
        assert!(encoded_budget >= largest_object);
        assert!(encoded_budget < largest_object * 2);
        gated.get_opts_gate.close();

        let mut replay_iter = WalReplayIterator::range(
            1..3,
            &ManifestCore::new(),
            WalReplayOptions {
                prefetch: WalReplaySettings {
                    max_concurrent_objects: 2,
                    max_inflight_bytes: total_budget,
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

    fn historical_wal_fixtures() -> Vec<(&'static str, Bytes)> {
        let source = include_str!("../testdata/historical-wal-fixtures.hex");
        source
            .lines()
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
            .filter_map(|line| {
                let (name, encoded) = line.split_once(' ')?;
                let feature_enabled = match name.rsplit_once('-').map(|(_, suffix)| suffix) {
                    Some("snappy") => cfg!(feature = "snappy"),
                    Some("zlib") => cfg!(feature = "zlib"),
                    Some("lz4") => cfg!(feature = "lz4"),
                    Some("zstd") => cfg!(feature = "zstd"),
                    _ => true,
                };
                feature_enabled.then(|| (name, Bytes::from(decode_hex(encoded))))
            })
            .collect()
    }

    fn decode_hex(encoded: &str) -> Vec<u8> {
        assert!(encoded.len().is_multiple_of(2));
        encoded
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| {
                let high = (pair[0] as char).to_digit(16).unwrap();
                let low = (pair[1] as char).to_digit(16).unwrap();
                ((high << 4) | low) as u8
            })
            .collect()
    }

    fn assert_historical_rows(name: &str, rows: &[RowEntry]) {
        assert_eq!(rows.len(), 5, "fixture {name}");
        assert_eq!(
            rows.iter().map(|row| row.seq).collect::<Vec<_>>(),
            vec![7, 8, 9, 10, 11]
        );
        assert_eq!(rows[0].key, Bytes::from_static(b"alpha"));
        assert_eq!(
            rows[0].value,
            ValueDeletable::Value(Bytes::from_static(b"value-a"))
        );
        assert_eq!(
            (rows[0].create_ts, rows[0].expire_ts),
            (Some(100), Some(200))
        );
        assert_eq!(rows[1].key, Bytes::from_static(b"dup"));
        assert_eq!(
            rows[1].value,
            ValueDeletable::Merge(Bytes::from_static(b"merge-1"))
        );
        assert_eq!(rows[2].key, Bytes::from_static(b"dup"));
        assert_eq!(rows[3].value, ValueDeletable::Tombstone);
        assert_eq!(rows[4].value.len(), 20);
    }

    fn test_table_store() -> Arc<TableStore> {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        test_table_store_with_object_store(object_store)
    }

    fn test_table_store_with_object_store(object_store: Arc<dyn ObjectStore>) -> Arc<TableStore> {
        test_table_store_with_format_and_object_store(SsTableFormat::default(), object_store)
    }

    fn test_table_store_with_format(format: SsTableFormat) -> Arc<TableStore> {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        test_table_store_with_format_and_object_store(format, object_store)
    }

    fn test_table_store_with_format_and_object_store(
        format: SsTableFormat,
        object_store: Arc<dyn ObjectStore>,
    ) -> Arc<TableStore> {
        let path = Path::from("/tmp/test_kv_store");
        Arc::new(TableStore::new(
            ObjectStores::new(object_store, None),
            format,
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

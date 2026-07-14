use crate::bytes_range::BytesRange;
use crate::db_state::{SortedRun, SsTableView};
use crate::db_stats::DbStats;
use crate::error::SlateDBError;
use crate::iter::{IterationOrder, RowEntryIterator};
use crate::sst_iter::{SstIterator, SstIteratorOptions, SstView};
use crate::tablestore::TableStore;
use crate::types::RowEntry;
use async_trait::async_trait;
use bytes::Bytes;
use std::cmp::Reverse;
use std::collections::VecDeque;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

#[derive(Debug)]
enum SortedRunView<'a> {
    Owned(VecDeque<SsTableView>, BytesRange),
    Borrowed(
        VecDeque<&'a SsTableView>,
        (Bound<&'a [u8]>, Bound<&'a [u8]>),
    ),
}

impl<'a> SortedRunView<'a> {
    /// Pops the next table, restricting the iteration range to the table's
    /// view range. Projected tables (e.g. in cloned manifests) may have a
    /// `visible_range` narrower than the requested range; tables whose view
    /// range does not intersect the requested range are skipped entirely.
    fn pop_sst(&mut self) -> Option<SstView<'a>> {
        match self {
            SortedRunView::Owned(tables, range) => loop {
                let table = tables.pop_front()?;
                let Some(view_range) = table.calculate_view_range(range.clone()) else {
                    continue;
                };
                return Some(SstView::Owned(Box::new(table), view_range));
            },
            SortedRunView::Borrowed(tables, range) => loop {
                let table = tables.pop_front()?;
                let Some(view_range) = table.calculate_view_range(BytesRange::from_slice(*range))
                else {
                    continue;
                };
                return Some(SstView::Borrowed(table, view_range));
            },
        }
    }

    async fn build_next_iter(
        &mut self,
        table_store: Arc<TableStore>,
        sst_iterator_options: SstIteratorOptions,
        db_stats: Option<DbStats>,
    ) -> Result<Option<SstIterator<'a>>, SlateDBError> {
        let Some(view) = self.pop_sst() else {
            return Ok(None);
        };
        Ok(Some(SstIterator::new_with_stats(
            view,
            table_store,
            sst_iterator_options,
            db_stats,
        )?))
    }

    fn peek_next_table(&self) -> Option<&SsTableView> {
        match self {
            SortedRunView::Owned(tables, _) => tables.front(),
            SortedRunView::Borrowed(tables, _) => tables.front().copied(),
        }
    }
}

enum SortedRunIteratorState<'a> {
    Uninitialized(IterationOrder, Option<SstIterator<'a>>),
    Ascending(Option<SstIterator<'a>>),
    Descending {
        current: Option<SstIterator<'a>>,
        buffered_entries: VecDeque<RowEntry>,
        pending_entry: Option<RowEntry>,
        pending_seek: Option<Bytes>,
    },
}

pub(crate) struct SortedRunIterator<'a> {
    table_store: Arc<TableStore>,
    sst_iter_options: SstIteratorOptions,
    db_stats: Option<DbStats>,
    view: SortedRunView<'a>,
    state: SortedRunIteratorState<'a>,
}

impl<'a> SortedRunIterator<'a> {
    async fn new(
        mut view: SortedRunView<'a>,
        table_store: Arc<TableStore>,
        sst_iter_options: SstIteratorOptions,
        db_stats: Option<DbStats>,
    ) -> Result<Self, SlateDBError> {
        let order = sst_iter_options.order;
        if matches!(order, IterationOrder::Descending) {
            match &mut view {
                SortedRunView::Owned(tables, _) => tables.make_contiguous().reverse(),
                SortedRunView::Borrowed(tables, _) => tables.make_contiguous().reverse(),
            }
        }
        let mut iter = Self {
            table_store,
            sst_iter_options,
            db_stats,
            view,
            state: SortedRunIteratorState::Uninitialized(order, None),
        };
        iter.advance_table().await?;
        Ok(iter)
    }

    pub(crate) async fn new_owned<T: RangeBounds<Bytes>>(
        range: T,
        sorted_run: SortedRun,
        table_store: Arc<TableStore>,
        sst_iter_options: SstIteratorOptions,
        db_stats: Option<DbStats>,
    ) -> Result<Self, SlateDBError> {
        let range = BytesRange::from(range);
        let tables = sorted_run.into_tables_covering_range(&range);
        let view = SortedRunView::Owned(tables, range);
        SortedRunIterator::new(view, table_store, sst_iter_options, db_stats).await
    }

    #[allow(dead_code)]
    pub(crate) async fn new_owned_initialized<T: RangeBounds<Bytes>>(
        range: T,
        sorted_run: SortedRun,
        table_store: Arc<TableStore>,
        sst_iter_options: SstIteratorOptions,
    ) -> Result<Self, SlateDBError> {
        SortedRunIterator::new_owned_initialized_with_stats(
            range,
            sorted_run,
            table_store,
            sst_iter_options,
            None,
        )
        .await
    }

    pub(crate) async fn new_owned_initialized_with_stats<T: RangeBounds<Bytes>>(
        range: T,
        sorted_run: SortedRun,
        table_store: Arc<TableStore>,
        sst_iter_options: SstIteratorOptions,
        db_stats: Option<DbStats>,
    ) -> Result<Self, SlateDBError> {
        let mut iter = SortedRunIterator::new_owned(
            range,
            sorted_run,
            table_store,
            sst_iter_options,
            db_stats,
        )
        .await?;
        iter.init().await?;
        Ok(iter)
    }

    pub(crate) async fn new_borrowed<T: RangeBounds<&'a [u8]>>(
        range: T,
        sorted_run: &'a SortedRun,
        table_store: Arc<TableStore>,
        sst_iter_options: SstIteratorOptions,
    ) -> Result<Self, SlateDBError> {
        Self::new_borrowed_with_stats(range, sorted_run, table_store, sst_iter_options, None).await
    }

    pub(crate) async fn new_borrowed_with_stats<T: RangeBounds<&'a [u8]>>(
        range: T,
        sorted_run: &'a SortedRun,
        table_store: Arc<TableStore>,
        sst_iter_options: SstIteratorOptions,
        db_stats: Option<DbStats>,
    ) -> Result<Self, SlateDBError> {
        let range = (range.start_bound().cloned(), range.end_bound().cloned());
        let tables = sorted_run.tables_covering_range(BytesRange::from_slice(range));
        let view = SortedRunView::Borrowed(tables, range);
        SortedRunIterator::new(view, table_store, sst_iter_options, db_stats).await
    }

    #[cfg(test)]
    pub(crate) async fn new_borrowed_initialized<T: RangeBounds<&'a [u8]>>(
        range: T,
        sorted_run: &'a SortedRun,
        table_store: Arc<TableStore>,
        sst_iter_options: SstIteratorOptions,
    ) -> Result<Self, SlateDBError> {
        let mut iter =
            SortedRunIterator::new_borrowed(range, sorted_run, table_store, sst_iter_options)
                .await?;
        iter.init().await?;
        Ok(iter)
    }

    async fn advance_table(&mut self) -> Result<(), SlateDBError> {
        let current = self
            .view
            .build_next_iter(
                self.table_store.clone(),
                self.sst_iter_options.clone(),
                self.db_stats.clone(),
            )
            .await?;

        match &mut self.state {
            SortedRunIteratorState::Uninitialized(_, slot) => *slot = current,
            SortedRunIteratorState::Ascending(slot) => {
                *slot = current;
                if let Some(current) = slot {
                    current.init().await?;
                }
            }
            SortedRunIteratorState::Descending {
                current: slot,
                pending_seek,
                ..
            } => {
                *slot = current;
                if let Some(current) = slot {
                    current.init().await?;
                    if let Some(next_key) = pending_seek {
                        current.seek(next_key).await?;
                    }
                }
            }
        }
        Ok(())
    }

    async fn fill_descending_buffer(&mut self, first_entry: RowEntry) -> Result<(), SlateDBError> {
        let target_key = first_entry.key.clone();
        let mut entries = vec![first_entry];
        let mut crossed_table_boundary = false;

        loop {
            let next = match &mut self.state {
                SortedRunIteratorState::Descending { current, .. } => {
                    if let Some(current) = current {
                        current.next().await?
                    } else {
                        None
                    }
                }
                _ => return Err(SlateDBError::IteratorNotInitialized),
            };

            match next {
                Some(entry) if entry.key == target_key => entries.push(entry),
                Some(entry) => {
                    if let SortedRunIteratorState::Descending { pending_entry, .. } =
                        &mut self.state
                    {
                        *pending_entry = Some(entry);
                    }
                    break;
                }
                None if self.view.peek_next_table().is_some_and(|table| {
                    table.compacted_effective_range().contains(&target_key)
                }) =>
                {
                    self.advance_table().await?;
                    crossed_table_boundary = true;
                }
                None => break,
            }
        }

        if crossed_table_boundary {
            entries.sort_by_key(|entry| Reverse(entry.seq));
        }
        if let SortedRunIteratorState::Descending {
            buffered_entries, ..
        } = &mut self.state
        {
            buffered_entries.extend(entries);
        }
        Ok(())
    }

    async fn seek_descending(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        if let SortedRunIteratorState::Descending {
            buffered_entries,
            pending_entry,
            pending_seek,
            ..
        } = &mut self.state
        {
            buffered_entries.clear();
            *pending_entry = None;
            *pending_seek = None;
        }

        while self
            .view
            .peek_next_table()
            .map(
                |table| match table.compacted_effective_range().end_bound() {
                    Bound::Included(end) | Bound::Excluded(end) => end.as_ref() > next_key,
                    Bound::Unbounded => true,
                },
            )
            .unwrap_or(false)
        {
            self.advance_table().await?;
        }

        loop {
            let result = match &mut self.state {
                SortedRunIteratorState::Descending { current, .. } => {
                    let Some(current) = current else {
                        break;
                    };
                    current.seek(next_key).await
                }
                _ => return Err(SlateDBError::IteratorNotInitialized),
            };
            match result {
                Ok(()) => break,
                Err(SlateDBError::SeekKeyOutOfKeyRange { .. }) => self.advance_table().await?,
                Err(error) => return Err(error),
            }
        }
        if let SortedRunIteratorState::Descending { pending_seek, .. } = &mut self.state {
            *pending_seek = Some(Bytes::copy_from_slice(next_key));
        }
        Ok(())
    }
}

#[async_trait]
impl RowEntryIterator for SortedRunIterator<'_> {
    async fn init(&mut self) -> Result<(), SlateDBError> {
        let transition = match &mut self.state {
            SortedRunIteratorState::Uninitialized(order, current) => {
                if let Some(current) = current {
                    current.init().await?;
                }
                Some((*order, current.take()))
            }
            SortedRunIteratorState::Ascending(_) | SortedRunIteratorState::Descending { .. } => {
                None
            }
        };

        if let Some((order, current)) = transition {
            self.state = match order {
                IterationOrder::Ascending => SortedRunIteratorState::Ascending(current),
                IterationOrder::Descending => SortedRunIteratorState::Descending {
                    current,
                    buffered_entries: VecDeque::new(),
                    pending_entry: None,
                    pending_seek: None,
                },
            };
        }
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        loop {
            let (next, descending) = match &mut self.state {
                SortedRunIteratorState::Uninitialized(_, _) => {
                    return Err(SlateDBError::IteratorNotInitialized);
                }
                SortedRunIteratorState::Ascending(Some(current)) => (current.next().await?, false),
                SortedRunIteratorState::Ascending(None) => return Ok(None),
                SortedRunIteratorState::Descending {
                    buffered_entries,
                    pending_entry,
                    current,
                    ..
                } => {
                    if let Some(entry) = buffered_entries.pop_front() {
                        return Ok(Some(entry));
                    }
                    let next = if let Some(entry) = pending_entry.take() {
                        Some(entry)
                    } else if let Some(current) = current {
                        current.next().await?
                    } else {
                        return Ok(None);
                    };
                    (next, true)
                }
            };

            if let Some(entry) = next {
                if descending
                    && self
                        .view
                        .peek_next_table()
                        .is_some_and(|table| table.compacted_effective_range().contains(&entry.key))
                {
                    self.fill_descending_buffer(entry).await?;
                    continue;
                }
                return Ok(Some(entry));
            }
            self.advance_table().await?;
        }
    }

    async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        match self.state {
            SortedRunIteratorState::Uninitialized(_, _) => {
                Err(SlateDBError::IteratorNotInitialized)
            }
            SortedRunIteratorState::Ascending(_) => {
                while self
                    .view
                    .peek_next_table()
                    .is_some_and(|table| table.compacted_effective_start_key() < next_key)
                {
                    self.advance_table().await?;
                }
                if let SortedRunIteratorState::Ascending(Some(current)) = &mut self.state {
                    current.seek(next_key).await?;
                }
                Ok(())
            }
            SortedRunIteratorState::Descending { .. } => self.seek_descending(next_key).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bytes_generator::OrderedBytesGenerator;
    use crate::db_state::{SsTableHandle, SsTableId};
    use crate::format::sst::SsTableFormat;
    use crate::proptest_util;
    use crate::proptest_util::sample;
    use crate::tablestore::TableStoreKind;
    use crate::test_utils::assert_kv;
    use crate::types::KeyValue;

    use crate::object_stores::ObjectStores;
    use bytes::{BufMut, BytesMut};
    use object_store::path::Path;
    use object_store::{memory::InMemory, ObjectStore};
    use proptest::test_runner::TestRng;
    use rand::distr::uniform::SampleRange;
    use rand::Rng;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    #[tokio::test]
    async fn should_require_initialization() {
        let mut iter = SortedRunIterator::new_owned(
            ..,
            SortedRun {
                id: 0,
                sst_views: vec![],
            },
            build_test_table_store(),
            descending_options(),
            None,
        )
        .await
        .unwrap();
        let error = iter.next().await.unwrap_err();
        assert!(matches!(error, SlateDBError::IteratorNotInitialized));
        let error = iter.seek(b"a").await.unwrap_err();
        assert!(matches!(error, SlateDBError::IteratorNotInitialized));
    }

    #[tokio::test]
    async fn test_one_sst_sr_iter() {
        let root_path = Path::from("");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let format = SsTableFormat {
            min_filter_keys: 3,
            ..SsTableFormat::default()
        };
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(object_store, None),
            format,
            root_path.clone(),
            None,
            TableStoreKind::Main,
        ));
        let mut builder = table_store.table_builder();
        builder
            .add_value(b"key1", b"value1", Some(1), None)
            .await
            .unwrap();
        builder
            .add_value(b"key2", b"value2", Some(2), None)
            .await
            .unwrap();
        builder
            .add_value(b"key3", b"value3", Some(3), None)
            .await
            .unwrap();
        let encoded = builder.build().await.unwrap();
        let id = SsTableId::Compacted(ulid::Ulid::new());
        let handle = table_store.write_sst(&id, &encoded, false).await.unwrap();
        let sr = SortedRun {
            id: 0,
            sst_views: vec![SsTableView::identity(handle)],
        };

        let mut iter = SortedRunIterator::new_owned_initialized(
            ..,
            sr,
            table_store,
            SstIteratorOptions::default(),
        )
        .await
        .unwrap();

        let kv: KeyValue = iter.next().await.unwrap().unwrap().into();
        assert_eq!(kv.key, b"key1".as_slice());
        assert_eq!(kv.value, b"value1".as_slice());
        iter.init().await.unwrap();
        let kv: KeyValue = iter.next().await.unwrap().unwrap().into();
        assert_eq!(kv.key, b"key2".as_slice());
        assert_eq!(kv.value, b"value2".as_slice());
        let kv: KeyValue = iter.next().await.unwrap().unwrap().into();
        assert_eq!(kv.key, b"key3".as_slice());
        assert_eq!(kv.value, b"value3".as_slice());
        let kv = iter.next().await.unwrap().map(KeyValue::from);
        assert!(kv.is_none());
    }

    #[tokio::test]
    async fn test_many_sst_sr_iter() {
        let root_path = Path::from("");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let format = SsTableFormat {
            min_filter_keys: 3,
            ..SsTableFormat::default()
        };
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(object_store, None),
            format,
            root_path.clone(),
            None,
            TableStoreKind::Main,
        ));
        let mut builder = table_store.table_builder();
        builder
            .add_value(b"key1", b"value1", Some(1), None)
            .await
            .unwrap();
        builder
            .add_value(b"key2", b"value2", Some(2), None)
            .await
            .unwrap();
        let encoded = builder.build().await.unwrap();
        let id1 = SsTableId::Compacted(ulid::Ulid::new());
        let handle1 = table_store.write_sst(&id1, &encoded, false).await.unwrap();
        let mut builder = table_store.table_builder();
        builder
            .add_value(b"key3", b"value3", Some(3), None)
            .await
            .unwrap();
        let encoded = builder.build().await.unwrap();
        let id2 = SsTableId::Compacted(ulid::Ulid::new());
        let handle2 = table_store.write_sst(&id2, &encoded, false).await.unwrap();
        let sr = SortedRun {
            id: 0,
            sst_views: vec![
                SsTableView::identity(handle1),
                SsTableView::identity(handle2),
            ],
        };

        let mut iter = SortedRunIterator::new_owned_initialized(
            ..,
            sr,
            table_store.clone(),
            SstIteratorOptions::default(),
        )
        .await
        .unwrap();

        let kv: KeyValue = iter.next().await.unwrap().unwrap().into();
        assert_eq!(kv.key, b"key1".as_slice());
        assert_eq!(kv.value, b"value1".as_slice());
        let kv: KeyValue = iter.next().await.unwrap().unwrap().into();
        assert_eq!(kv.key, b"key2".as_slice());
        assert_eq!(kv.value, b"value2".as_slice());
        let kv: KeyValue = iter.next().await.unwrap().unwrap().into();
        assert_eq!(kv.key, b"key3".as_slice());
        assert_eq!(kv.value, b"value3".as_slice());
        let kv = iter.next().await.unwrap().map(KeyValue::from);
        assert!(kv.is_none());
    }

    #[tokio::test]
    async fn should_iterate_multi_sst_sorted_run_in_descending_order() {
        // given: two non-overlapping SSTs ordered by ascending key range
        let root_path = Path::from("");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(object_store, None),
            SsTableFormat::default(),
            root_path,
            None,
            TableStoreKind::Main,
        ));
        let first_sst = build_sst(&table_store, &[(b"a", b"value-a"), (b"b", b"value-b")]).await;
        let second_sst = build_sst(&table_store, &[(b"c", b"value-c"), (b"d", b"value-d")]).await;
        let sorted_run = SortedRun {
            id: 0,
            sst_views: vec![
                SsTableView::identity(first_sst),
                SsTableView::identity(second_sst),
            ],
        };

        // when: iterating over the complete sorted run in descending order
        let options = SstIteratorOptions {
            order: IterationOrder::Descending,
            ..Default::default()
        };
        let mut iter =
            SortedRunIterator::new_owned_initialized(.., sorted_run, table_store, options)
                .await
                .unwrap();

        let mut actual = Vec::new();
        while let Some(entry) = iter.next().await.unwrap() {
            actual.push(entry.key);
        }

        // then: keys should be globally descending across SST boundaries
        assert_eq!(
            actual,
            vec![
                Bytes::from_static(b"d"),
                Bytes::from_static(b"c"),
                Bytes::from_static(b"b"),
                Bytes::from_static(b"a"),
            ]
        );
    }

    #[tokio::test]
    async fn should_support_descending_iteration_across_sst_boundaries() {
        let table_store = build_test_table_store();
        let sorted_run = sorted_run_from_ssts([
            build_sst_from_entries(
                &table_store,
                &[(b"a".as_slice(), 110), (b"c", 100), (b"c", 90)],
            )
            .await,
            build_sst_from_entries(
                &table_store,
                &[(b"c".as_slice(), 80), (b"d", 70), (b"e", 60)],
            )
            .await,
            build_sst_from_entries(
                &table_store,
                &[(b"g".as_slice(), 50), (b"g", 40), (b"h", 30)],
            )
            .await,
        ]);
        let all = [
            (b"h".as_slice(), 30),
            (b"g".as_slice(), 50),
            (b"g".as_slice(), 40),
            (b"e".as_slice(), 60),
            (b"d".as_slice(), 70),
            (b"c".as_slice(), 100),
            (b"c".as_slice(), 90),
            (b"c".as_slice(), 80),
            (b"a".as_slice(), 110),
        ];

        let mut borrowed = SortedRunIterator::new_borrowed_initialized(
            ..,
            &sorted_run,
            table_store.clone(),
            descending_options(),
        )
        .await
        .unwrap();
        assert_entries(&collect_entries(&mut borrowed).await, &all);

        let mut bounded = SortedRunIterator::new_owned_initialized(
            Bytes::from_static(b"b")..Bytes::from_static(b"h"),
            sorted_run.clone(),
            table_store.clone(),
            descending_options(),
        )
        .await
        .unwrap();
        assert_entries(&collect_entries(&mut bounded).await, &all[1..8]);

        let mut exact = SortedRunIterator::new_owned_initialized(
            ..,
            sorted_run.clone(),
            table_store.clone(),
            descending_options(),
        )
        .await
        .unwrap();
        exact.seek(b"c").await.unwrap();
        assert_entries(&collect_entries(&mut exact).await, &all[5..]);

        let mut gap = SortedRunIterator::new_owned_initialized(
            ..,
            sorted_run,
            table_store,
            descending_options(),
        )
        .await
        .unwrap();
        assert_eq!(gap.next().await.unwrap().unwrap().key, b"h".as_slice());
        gap.seek(b"f").await.unwrap();
        assert_entries(&collect_entries(&mut gap).await, &all[3..]);
    }

    #[tokio::test]
    async fn test_sr_iter_respects_visible_range() {
        // given: a sorted run whose views carry visible_range restrictions,
        // as produced by manifest projection (e.g. range-restricted clones)
        let root_path = Path::from("");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let format = SsTableFormat {
            min_filter_keys: 3,
            ..SsTableFormat::default()
        };
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(object_store, None),
            format,
            root_path.clone(),
            None,
            TableStoreKind::Main,
        ));
        let mut builder = table_store.table_builder();
        for i in 1..=4 {
            let key = format!("key{i}");
            let value = format!("value{i}");
            builder
                .add_value(key.as_bytes(), value.as_bytes(), Some(i), None)
                .await
                .unwrap();
        }
        let encoded = builder.build().await.unwrap();
        let id1 = SsTableId::Compacted(ulid::Ulid::new());
        let handle1 = table_store.write_sst(&id1, &encoded, false).await.unwrap();
        let mut builder = table_store.table_builder();
        for i in 5..=8 {
            let key = format!("key{i}");
            let value = format!("value{i}");
            builder
                .add_value(key.as_bytes(), value.as_bytes(), Some(i), None)
                .await
                .unwrap();
        }
        let encoded = builder.build().await.unwrap();
        let id2 = SsTableId::Compacted(ulid::Ulid::new());
        let handle2 = table_store.write_sst(&id2, &encoded, false).await.unwrap();
        let sr = SortedRun {
            id: 0,
            sst_views: vec![
                SsTableView::new_projected(
                    ulid::Ulid::new(),
                    handle1,
                    Some(BytesRange::from_ref("key2".."key4")),
                ),
                SsTableView::new_projected(
                    ulid::Ulid::new(),
                    handle2,
                    Some(BytesRange::from_ref("key5".."key7")),
                ),
            ],
        };

        // when: iterating the full range, then: only visible keys appear
        let mut iter = SortedRunIterator::new_borrowed_initialized(
            ..,
            &sr,
            table_store.clone(),
            SstIteratorOptions::default(),
        )
        .await
        .unwrap();
        for i in [2, 3, 5, 6] {
            let kv: KeyValue = iter.next().await.unwrap().unwrap().into();
            assert_eq!(kv.key.as_ref(), format!("key{i}").as_bytes());
        }
        assert!(iter.next().await.unwrap().is_none());

        // when: iterating a sub-range, then: the narrower bound applies and
        // tables disjoint with the query range are skipped entirely
        let mut iter = SortedRunIterator::new_owned_initialized(
            BytesRange::from_ref("key6"..),
            sr,
            table_store.clone(),
            SstIteratorOptions::default(),
        )
        .await
        .unwrap();
        let kv: KeyValue = iter.next().await.unwrap().unwrap().into();
        assert_eq!(kv.key.as_ref(), b"key6");
        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_sr_iter_from_key() {
        let root_path = Path::from("");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let format = SsTableFormat {
            min_filter_keys: 3,
            ..SsTableFormat::default()
        };
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(object_store, None),
            format,
            root_path.clone(),
            None,
            TableStoreKind::Main,
        ));
        let key_gen = OrderedBytesGenerator::new_with_byte_range(&[b'a'; 16], b'a', b'z');
        let mut test_case_key_gen = key_gen.clone();
        let val_gen = OrderedBytesGenerator::new_with_byte_range(&[0u8; 16], 0u8, 26u8);
        let mut test_case_val_gen = val_gen.clone();
        let sr = build_sr_with_ssts(table_store.clone(), 3, 10, key_gen, val_gen).await;

        for i in 0..30 {
            let mut expected_key_gen = test_case_key_gen.clone();
            let mut expected_val_gen = test_case_val_gen.clone();
            let from_key = test_case_key_gen.next();
            _ = test_case_val_gen.next();
            let mut iter = SortedRunIterator::new_borrowed_initialized(
                from_key.as_ref()..,
                &sr,
                table_store.clone(),
                SstIteratorOptions::default(),
            )
            .await
            .unwrap();
            for _ in 0..30 - i {
                assert_kv(
                    &iter.next().await.unwrap().unwrap().into(),
                    expected_key_gen.next().as_ref(),
                    expected_val_gen.next().as_ref(),
                );
            }
            assert!(iter.next().await.unwrap().is_none());
        }
    }

    #[tokio::test]
    async fn test_sr_iter_from_key_lower_than_range() {
        let root_path = Path::from("");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let format = SsTableFormat {
            min_filter_keys: 3,
            ..SsTableFormat::default()
        };
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(object_store, None),
            format,
            root_path.clone(),
            None,
            TableStoreKind::Main,
        ));
        let key_gen = OrderedBytesGenerator::new_with_byte_range(&[b'a'; 16], b'a', b'z');
        let mut expected_key_gen = key_gen.clone();
        let val_gen = OrderedBytesGenerator::new_with_byte_range(&[0u8; 16], 0u8, 26u8);
        let mut expected_val_gen = val_gen.clone();
        let sr = build_sr_with_ssts(table_store.clone(), 3, 10, key_gen, val_gen).await;
        let mut iter = SortedRunIterator::new_borrowed_initialized(
            [b'a', 10].as_ref()..,
            &sr,
            table_store.clone(),
            SstIteratorOptions::default(),
        )
        .await
        .unwrap();

        for _ in 0..30 {
            assert_kv(
                &iter.next().await.unwrap().unwrap().into(),
                expected_key_gen.next().as_ref(),
                expected_val_gen.next().as_ref(),
            );
        }
        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_sr_iter_from_key_higher_than_range() {
        let root_path = Path::from("");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let format = SsTableFormat {
            min_filter_keys: 3,
            ..SsTableFormat::default()
        };
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(object_store, None),
            format,
            root_path.clone(),
            None,
            TableStoreKind::Main,
        ));
        let key_gen = OrderedBytesGenerator::new_with_byte_range(&[b'a'; 16], b'a', b'z');
        let val_gen = OrderedBytesGenerator::new_with_byte_range(&[0u8; 16], 0u8, 26u8);
        let sr = build_sr_with_ssts(table_store.clone(), 3, 10, key_gen, val_gen).await;

        let mut iter = SortedRunIterator::new_borrowed_initialized(
            [b'z', 30].as_ref()..,
            &sr,
            table_store.clone(),
            SstIteratorOptions::default(),
        )
        .await
        .unwrap();

        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_seek_through_sorted_run() {
        let root_path = Path::from("");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(object_store, None),
            SsTableFormat::default(),
            root_path.clone(),
            None,
            TableStoreKind::Main,
        ));

        let mut rng = proptest_util::rng::new_test_rng(None);
        let table = sample::table(&mut rng, 400, 10);
        let max_entries_per_sst = 20u64;
        let entries_per_sst = 1..max_entries_per_sst;
        let sr =
            build_sorted_run_from_table(&table, table_store.clone(), entries_per_sst, &mut rng)
                .await;
        let mut sr_iter = SortedRunIterator::new_owned_initialized(
            ..,
            sr,
            table_store.clone(),
            SstIteratorOptions::default(),
        )
        .await
        .unwrap();
        let mut table_iter = table.iter();
        loop {
            let skip = rng.random::<u64>() % (max_entries_per_sst * 2);
            let run = rng.random::<u64>() % (max_entries_per_sst * 2);

            let Some((k, _)) = table_iter.nth(skip as usize) else {
                break;
            };
            let seek_key = increment_length(k);
            sr_iter.seek(&seek_key).await.unwrap();

            for (key, value) in table_iter.by_ref().take(run as usize) {
                let kv: KeyValue = sr_iter.next().await.unwrap().unwrap().into();
                assert_eq!(*key, kv.key);
                assert_eq!(*value, kv.value);
            }
        }
    }

    fn increment_length(b: &[u8]) -> Bytes {
        let mut buf = BytesMut::from(b);
        buf.put_u8(u8::MIN);
        buf.freeze()
    }

    fn build_test_table_store() -> Arc<TableStore> {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        Arc::new(TableStore::new(
            ObjectStores::new(object_store, None),
            SsTableFormat::default(),
            Path::from(""),
            None,
            TableStoreKind::Main,
        ))
    }

    fn descending_options() -> SstIteratorOptions {
        SstIteratorOptions {
            order: IterationOrder::Descending,
            ..Default::default()
        }
    }

    fn sorted_run_from_ssts(ssts: impl IntoIterator<Item = SsTableHandle>) -> SortedRun {
        SortedRun {
            id: 0,
            sst_views: ssts.into_iter().map(SsTableView::identity).collect(),
        }
    }

    async fn collect_entries(iter: &mut SortedRunIterator<'_>) -> Vec<RowEntry> {
        let mut entries = Vec::new();
        while let Some(entry) = iter.next().await.unwrap() {
            entries.push(entry);
        }
        entries
    }

    fn assert_entries(actual: &[RowEntry], expected: &[(&[u8], u64)]) {
        assert_eq!(actual.len(), expected.len());
        for (entry, (key, seq)) in actual.iter().zip(expected) {
            assert_eq!((entry.key.as_ref(), entry.seq), (*key, *seq));
        }
    }

    async fn build_sst(
        table_store: &Arc<TableStore>,
        keys_and_values: &[(&[u8], &[u8])],
    ) -> SsTableHandle {
        let mut builder = table_store.table_builder();
        for (key, value) in keys_and_values {
            builder.add_value(key, value, Some(0), None).await.unwrap();
        }
        let encoded = builder.build().await.unwrap();
        let id = SsTableId::Compacted(ulid::Ulid::new());
        table_store.write_sst(&id, &encoded, false).await.unwrap()
    }

    async fn build_sst_from_entries(
        table_store: &Arc<TableStore>,
        entries: &[(&[u8], u64)],
    ) -> SsTableHandle {
        let mut writer = table_store.table_writer(SsTableId::Compacted(ulid::Ulid::new()));
        for (key, seq) in entries {
            writer
                .add(RowEntry::new_value(key, b"value", *seq))
                .await
                .unwrap();
        }
        writer.close().await.unwrap()
    }

    async fn build_sorted_run_from_table<R: SampleRange<u64> + Clone>(
        table: &BTreeMap<Bytes, Bytes>,
        table_store: Arc<TableStore>,
        entries_per_sst: R,
        rng: &mut TestRng,
    ) -> SortedRun {
        let mut ssts = Vec::new();
        let mut entries = table.iter();
        loop {
            let sst_len = rng.random_range(entries_per_sst.clone());
            let mut builder = table_store.table_builder();

            let sst_kvs: Vec<(&Bytes, &Bytes)> = entries.by_ref().take(sst_len as usize).collect();
            if sst_kvs.is_empty() {
                break;
            }

            for (key, value) in sst_kvs {
                builder.add_value(key, value, Some(0), None).await.unwrap();
            }

            let encoded = builder.build().await.unwrap();
            let id = SsTableId::Compacted(ulid::Ulid::new());
            let handle = table_store.write_sst(&id, &encoded, false).await.unwrap();
            ssts.push(SsTableView::identity(handle));
        }

        SortedRun {
            id: 0,
            sst_views: ssts,
        }
    }

    async fn build_sr_with_ssts(
        table_store: Arc<TableStore>,
        n: usize,
        keys_per_sst: usize,
        mut key_gen: OrderedBytesGenerator,
        mut val_gen: OrderedBytesGenerator,
    ) -> SortedRun {
        let mut ssts = Vec::<SsTableView>::new();
        for _ in 0..n {
            let mut writer = table_store.table_writer(SsTableId::Compacted(ulid::Ulid::new()));
            for _ in 0..keys_per_sst {
                let entry =
                    RowEntry::new_value(key_gen.next().as_ref(), val_gen.next().as_ref(), 0);
                writer.add(entry).await.unwrap();
            }
            let sst = writer.close().await.unwrap();
            ssts.push(SsTableView::identity(sst));
        }
        SortedRun {
            id: 0,
            sst_views: ssts,
        }
    }

    mod mixed_version_tests {
        use super::*;
        use crate::sst_builder::BlockFormat;

        async fn build_sst_v1(
            table_store: &Arc<TableStore>,
            keys_and_values: &[(&[u8], &[u8])],
        ) -> SsTableHandle {
            let mut builder = table_store
                .table_builder()
                .with_block_format(BlockFormat::V1);
            for (key, value) in keys_and_values {
                builder.add_value(key, value, Some(0), None).await.unwrap();
            }
            let encoded = builder.build().await.unwrap();
            let id = SsTableId::Compacted(ulid::Ulid::new());
            table_store.write_sst(&id, &encoded, false).await.unwrap()
        }

        async fn build_sst_v2(
            table_store: &Arc<TableStore>,
            keys_and_values: &[(&[u8], &[u8])],
        ) -> SsTableHandle {
            // V2 is now the default, so no need to explicitly set block format
            let mut builder = table_store.table_builder();
            for (key, value) in keys_and_values {
                builder.add_value(key, value, Some(0), None).await.unwrap();
            }
            let encoded = builder.build().await.unwrap();
            let id = SsTableId::Compacted(ulid::Ulid::new());
            table_store.write_sst(&id, &encoded, false).await.unwrap()
        }

        #[tokio::test]
        async fn should_iterate_sorted_run_with_mixed_v1_and_v2_ssts() {
            // given: a sorted run with alternating v1 and v2 SSTs
            let root_path = Path::from("");
            let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let format = SsTableFormat {
                min_filter_keys: 10,
                ..SsTableFormat::default()
            };
            let table_store = Arc::new(TableStore::new(
                ObjectStores::new(object_store, None),
                format,
                root_path,
                None,
                TableStoreKind::Main,
            ));

            // Build a sorted run with v1, v2, v1, v2 SSTs
            let sst1_v1 = build_sst_v1(
                &table_store,
                &[(b"key01", b"value01"), (b"key02", b"value02")],
            )
            .await;
            let sst2_v2 = build_sst_v2(
                &table_store,
                &[(b"key03", b"value03"), (b"key04", b"value04")],
            )
            .await;
            let sst3_v1 = build_sst_v1(
                &table_store,
                &[(b"key05", b"value05"), (b"key06", b"value06")],
            )
            .await;
            let sst4_v2 = build_sst_v2(
                &table_store,
                &[(b"key07", b"value07"), (b"key08", b"value08")],
            )
            .await;

            let sorted_run = SortedRun {
                id: 0,
                sst_views: vec![
                    SsTableView::identity(sst1_v1),
                    SsTableView::identity(sst2_v2),
                    SsTableView::identity(sst3_v1),
                    SsTableView::identity(sst4_v2),
                ],
            };

            // when: iterating over the sorted run
            let mut iter = SortedRunIterator::new_owned_initialized(
                ..,
                sorted_run,
                table_store.clone(),
                SstIteratorOptions::default(),
            )
            .await
            .unwrap();

            // then: all keys should be returned in order across both v1 and v2 SSTs
            for i in 1..=8 {
                let kv: KeyValue = iter.next().await.unwrap().unwrap().into();
                let expected_key = format!("key{:02}", i);
                let expected_value = format!("value{:02}", i);
                assert_eq!(kv.key.as_ref(), expected_key.as_bytes());
                assert_eq!(kv.value.as_ref(), expected_value.as_bytes());
            }

            let kv = iter.next().await.unwrap().map(KeyValue::from);
            assert!(kv.is_none());
        }

        #[tokio::test]
        async fn should_seek_through_mixed_v1_and_v2_ssts() {
            // given: a sorted run with alternating v1 and v2 SSTs
            let root_path = Path::from("");
            let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let format = SsTableFormat {
                min_filter_keys: 10,
                ..SsTableFormat::default()
            };
            let table_store = Arc::new(TableStore::new(
                ObjectStores::new(object_store, None),
                format,
                root_path,
                None,
                TableStoreKind::Main,
            ));

            // Build a sorted run with v1, v2, v1, v2 SSTs
            let sst1_v1 = build_sst_v1(
                &table_store,
                &[(b"key01", b"value01"), (b"key02", b"value02")],
            )
            .await;
            let sst2_v2 = build_sst_v2(
                &table_store,
                &[(b"key03", b"value03"), (b"key04", b"value04")],
            )
            .await;
            let sst3_v1 = build_sst_v1(
                &table_store,
                &[(b"key05", b"value05"), (b"key06", b"value06")],
            )
            .await;
            let sst4_v2 = build_sst_v2(
                &table_store,
                &[(b"key07", b"value07"), (b"key08", b"value08")],
            )
            .await;

            let sorted_run = SortedRun {
                id: 0,
                sst_views: vec![
                    SsTableView::identity(sst1_v1),
                    SsTableView::identity(sst2_v2),
                    SsTableView::identity(sst3_v1),
                    SsTableView::identity(sst4_v2),
                ],
            };

            let mut iter = SortedRunIterator::new_owned_initialized(
                ..,
                sorted_run,
                table_store.clone(),
                SstIteratorOptions::default(),
            )
            .await
            .unwrap();

            // when: seeking to key05 (which is in a v1 SST after a v2 SST)
            iter.seek(b"key05").await.unwrap();

            // then: we should get key05 and subsequent keys
            let kv: KeyValue = iter.next().await.unwrap().unwrap().into();
            assert_eq!(kv.key.as_ref(), b"key05");
            assert_eq!(kv.value.as_ref(), b"value05");

            let kv: KeyValue = iter.next().await.unwrap().unwrap().into();
            assert_eq!(kv.key.as_ref(), b"key06");
            assert_eq!(kv.value.as_ref(), b"value06");

            // Seek again to a v2 SST
            iter.seek(b"key07").await.unwrap();

            let kv: KeyValue = iter.next().await.unwrap().unwrap().into();
            assert_eq!(kv.key.as_ref(), b"key07");
            assert_eq!(kv.value.as_ref(), b"value07");
        }
    }
}

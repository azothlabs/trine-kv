use super::{Bound, Iter, KeyRange, KeyValue, Result, Value, decode_branch_value};

/// One row a merge source yields: its key and either a value or a tombstone
/// (`None`, meaning the level deletes the key).
type MergeRow = Result<(Vec<u8>, Option<Value>)>;

/// A lazy k-way merge of a branch's read chain — the branch's own writes, each
/// ancestor branch, and the root — yielding the resolved rows in key order. The
/// nearest level holding a key wins; a tombstone there hides the key entirely.
/// Returned by [`Branch::range_sync`](crate::Branch::range_sync).
pub struct BranchRange {
    inner: AsyncBranchRange,
}

impl BranchRange {
    pub(super) const fn new(inner: AsyncBranchRange) -> Self {
        Self { inner }
    }
}

pub(super) enum AsyncMergeRows {
    Items(std::vec::IntoIter<MergeRow>),
    Database {
        rows: Iter,
        decode_branch_values: bool,
    },
}

impl AsyncMergeRows {
    async fn next(&mut self) -> MergeRowOption {
        match self {
            Self::Items(rows) => rows.next(),
            Self::Database {
                rows,
                decode_branch_values,
            } => rows.next().await.transpose().map(|row| {
                row.and_then(|row| {
                    if *decode_branch_values {
                        decode_branch_value(&row.value).map(|value| (row.key, value))
                    } else {
                        Ok((row.key, Some(row.value)))
                    }
                })
            }),
        }
    }
}

type MergeRowOption = Option<MergeRow>;

pub(super) struct AsyncMergeSource {
    rows: AsyncMergeRows,
    head: MergeRowOption,
}

impl AsyncMergeSource {
    pub(super) async fn new(mut rows: AsyncMergeRows) -> Self {
        let head = rows.next().await;
        Self { rows, head }
    }

    fn key(&self) -> Option<&[u8]> {
        match &self.head {
            Some(Ok((key, _))) => Some(key),
            _ => None,
        }
    }

    fn is_err(&self) -> bool {
        matches!(self.head, Some(Err(_)))
    }

    async fn take(&mut self) -> MergeRowOption {
        let row = self.head.take();
        self.head = self.rows.next().await;
        row
    }
}

/// Asynchronous lazy merge of a branch's own writes, its ancestors, and the
/// root snapshot.
///
/// Rows are pulled from storage only when [`AsyncBranchRange::next`] is
/// awaited. This preserves backend-native asynchronous reads for table blocks
/// and large values.
pub struct AsyncBranchRange {
    pub(super) sources: Vec<AsyncMergeSource>,
}

impl AsyncBranchRange {
    /// Returns the next visible branch row.
    ///
    /// The nearest branch layer wins when several layers contain the same key;
    /// a tombstone in that layer hides the key.
    ///
    /// # Errors
    ///
    /// Returns a storage, format, or closed-database error from the source that
    /// supplies the next row. An error consumes that source's failing head, so
    /// callers should normally stop iteration after receiving it.
    pub async fn next(&mut self) -> Result<Option<KeyValue>> {
        loop {
            for source_index in 0..self.sources.len() {
                if self.sources[source_index].is_err()
                    && let Some(Err(error)) = self.sources[source_index].take().await
                {
                    return Err(error);
                }
            }

            let Some(key) = self
                .sources
                .iter()
                .filter_map(AsyncMergeSource::key)
                .min()
                .map(<[u8]>::to_vec)
            else {
                return Ok(None);
            };

            let mut chosen = None;
            for source in &mut self.sources {
                if source.key() == Some(key.as_slice())
                    && let Some(Ok((_, value))) = source.take().await
                    && chosen.is_none()
                {
                    chosen = Some(value);
                }
            }
            if let Some(Some(value)) = chosen {
                return Ok(Some(KeyValue::new(key, value)));
            }
        }
    }
}

impl Iterator for BranchRange {
    type Item = Result<KeyValue>;

    fn next(&mut self) -> Option<Self::Item> {
        match futures::executor::block_on(self.inner.next()) {
            Ok(Some(row)) => Some(Ok(row)),
            Ok(None) => None,
            Err(error) => Some(Err(error)),
        }
    }
}

/// Whether `key` falls within `range`.
pub(super) fn range_contains(range: &KeyRange, key: &[u8]) -> bool {
    let after_start = match &range.start {
        Bound::Unbounded => true,
        Bound::Included(start) => key >= start.as_slice(),
        Bound::Excluded(start) => key > start.as_slice(),
    };
    let before_end = match &range.end {
        Bound::Unbounded => true,
        Bound::Included(end) => key <= end.as_slice(),
        Bound::Excluded(end) => key < end.as_slice(),
    };
    after_start && before_end
}

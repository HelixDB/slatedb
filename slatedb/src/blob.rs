use std::ops::Range;

use bytes::Bytes;

use crate::error::SlateDBError;

pub(crate) trait ReadOnlyBlob {
    async fn len(&self) -> Result<u64, SlateDBError>;

    async fn read_range(&self, range: Range<u64>) -> Result<Bytes, SlateDBError>;

    #[allow(dead_code)]
    async fn read(&self) -> Result<Bytes, SlateDBError>;
}

/// An immutable object held entirely in memory.
pub(crate) struct BytesBlob {
    bytes: Bytes,
}

impl BytesBlob {
    #[allow(dead_code)]
    pub(crate) fn new(bytes: Bytes) -> Self {
        Self { bytes }
    }
}

impl ReadOnlyBlob for BytesBlob {
    async fn len(&self) -> Result<u64, SlateDBError> {
        u64::try_from(self.bytes.len()).map_err(|err| {
            SlateDBError::WalDataError(std::sync::Arc::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                err,
            )))
        })
    }

    async fn read_range(&self, range: Range<u64>) -> Result<Bytes, SlateDBError> {
        let start = usize::try_from(range.start).ok();
        let end = usize::try_from(range.end).ok();
        let Some((start, end)) = start.zip(end) else {
            return Err(invalid_range(range, self.bytes.len()));
        };
        if start > end || end > self.bytes.len() {
            return Err(invalid_range(range, self.bytes.len()));
        }
        Ok(self.bytes.slice(start..end))
    }

    async fn read(&self) -> Result<Bytes, SlateDBError> {
        Ok(self.bytes.clone())
    }
}

fn invalid_range(range: Range<u64>, len: usize) -> SlateDBError {
    SlateDBError::WalDataError(std::sync::Arc::new(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!("invalid in-memory WAL range {range:?} for object length {len}"),
    )))
}

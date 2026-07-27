use super::{
    ContentUploadOptions, Db, Error, Result, SealedContent, UploadId, UploadSessionState,
    encode_chunk, fmt, mem,
};

/// In-progress sequential upload with memory bounded by its configured chunk.
///
/// Calls to [`write`](Self::write) may be any size and are split into fixed
/// chunks. `seal` consumes the upload and publishes the fixed-size descriptor
/// only after all chunks have been stored and the complete identity verified.
/// Dropping an unsealed upload never publishes content. Its durable
/// [`UploadId`] can be resumed or explicitly aborted later; cleanup of uploads
/// that are never resumed or aborted is a maintenance concern.
pub struct ContentUpload {
    db: Db,
    upload_id: UploadId,
    options: ContentUploadOptions,
    buffer: Vec<u8>,
    length: u64,
    complete_chunks: u64,
    revision: u64,
    failed: bool,
}

impl fmt::Debug for ContentUpload {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ContentUpload")
            .field("upload_id", &self.upload_id)
            .field("chunk_bytes", &self.options.chunk_bytes)
            .field("buffered_bytes", &self.buffer.len())
            .field("length", &self.length)
            .field("complete_chunks", &self.complete_chunks)
            .field("revision", &self.revision)
            .field("failed", &self.failed)
            .finish_non_exhaustive()
    }
}

impl ContentUpload {
    pub(crate) fn new(
        db: Db,
        upload_id: UploadId,
        options: ContentUploadOptions,
        buffer: Vec<u8>,
        length: u64,
        complete_chunks: u64,
        revision: u64,
    ) -> Self {
        Self {
            db,
            upload_id,
            options,
            buffer,
            length,
            complete_chunks,
            revision,
            failed: false,
        }
    }

    /// Returns this temporary upload identity.
    #[must_use]
    pub const fn upload_id(&self) -> UploadId {
        self.upload_id
    }

    /// Returns the number of original bytes accepted so far.
    #[must_use]
    pub const fn len(&self) -> u64 {
        self.length
    }

    /// Returns whether no original bytes have been accepted.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.length == 0
    }

    /// Returns bytes currently retained before the next chunk write.
    ///
    /// This value never exceeds [`ContentUploadOptions::chunk_bytes`]. It is
    /// exposed so callers and benchmarks can verify the memory boundary.
    #[must_use]
    pub fn buffered_bytes(&self) -> usize {
        self.buffer.len()
    }

    /// Accepts more original bytes and stores each completed chunk.
    ///
    /// Empty writes are no-ops. If a storage write fails, this in-memory writer
    /// becomes unusable because the failure may have happened between the chunk
    /// and session writes. Resume or abort its [`UploadId`] through [`Db`] to
    /// recover from the last durable session revision.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Closed`] or [`Error::ReadOnly`] when the database cannot
    /// write, [`Error::InvalidOptions`] on length overflow, or a backend storage
    /// error while persisting a completed chunk.
    pub async fn write(&mut self, mut bytes: &[u8]) -> Result<()> {
        let db = self.db.clone();
        let _activity = db.begin_activity()?;
        self.ensure_active()?;
        if bytes.is_empty() {
            return Ok(());
        }

        let _upload = db.lock_content_upload(self.upload_id).await;
        let durable = db.require_upload_state(self.upload_id).await?;
        durable.require_open_revision(self.revision)?;

        let incoming = u64::try_from(bytes.len())
            .map_err(|_| Error::invalid_options("content write length exceeds u64"))?;
        let next_length = self
            .length
            .checked_add(incoming)
            .ok_or_else(|| Error::invalid_options("content length overflow"))?;
        if let Some(expected) = self.options.expected_length()
            && next_length > expected
        {
            return Err(Error::ContentLengthMismatch {
                expected,
                actual: next_length,
            });
        }
        let desired_reservation = self.options.expected_length().unwrap_or(next_length);
        db.reserve_content_upload_bytes(&durable, desired_reservation)
            .await?;
        self.length = next_length;
        let next_revision = self
            .revision
            .checked_add(1)
            .ok_or_else(|| Error::invalid_options("content upload revision overflow"))?;

        while !bytes.is_empty() {
            let available = self.options.chunk_bytes() - self.buffer.len();
            let take = available.min(bytes.len());
            self.buffer.extend_from_slice(&bytes[..take]);
            bytes = &bytes[take..];
            if self.buffer.len() == self.options.chunk_bytes()
                && let Err(error) = self.flush_full_chunk().await
            {
                self.failed = true;
                return Err(error);
            }
        }

        if !self.buffer.is_empty() {
            let frame = encode_chunk(self.upload_id, self.complete_chunks, &self.buffer)?;
            if let Err(error) = self
                .db
                .write_content_partial_chunk(
                    self.upload_id,
                    self.complete_chunks,
                    next_revision,
                    frame,
                )
                .await
            {
                self.failed = true;
                return Err(error);
            }
        }
        let state = UploadSessionState::open(
            self.upload_id,
            next_revision,
            self.options,
            self.length,
            self.complete_chunks,
            u32::try_from(self.buffer.len())
                .map_err(|_| Error::invalid_options("content partial chunk exceeds u32"))?,
            durable.upload_token(),
        )?;
        if let Err(error) = self.db.write_upload_state(&state).await {
            self.failed = true;
            return Err(error);
        }
        self.revision = next_revision;
        Ok(())
    }

    /// Seals this upload and publishes its immutable descriptor.
    ///
    /// Seal is the visibility boundary: chunk objects written before this call
    /// are not openable by `ContentId`. The returned length and identity describe
    /// original bytes, not framed storage bytes. Existing identical content is
    /// reused and the redundant upload chunks are removed.
    ///
    /// # Errors
    ///
    /// Returns a typed length or digest mismatch and aborts the session when an
    /// expectation fails,
    /// [`Error::InvalidOptions`] when the upload previously failed, or a storage
    /// error if the final chunk or descriptor cannot be made durable. A failed
    /// descriptor write does not publish the `ContentObject`. If descriptor
    /// publication succeeds but the sealed session write fails, retry
    /// [`Db::seal_content_upload`] with the same [`UploadId`].
    pub async fn seal(self) -> Result<SealedContent> {
        self.ensure_active()?;
        self.db
            .seal_content_upload_at(self.upload_id, Some(self.revision))
            .await
    }

    /// Aborts the upload and makes every staging chunk unreachable.
    ///
    /// No descriptor is published. The durable session enters `aborting`
    /// before chunk cleanup and ends as a permanent retired-ID marker, so a
    /// crash cannot reopen it as writable state.
    ///
    /// # Errors
    ///
    /// Returns a conflict if another writer advanced the session, or the
    /// backend error that prevented deletion of the durable session.
    pub async fn abort(self) -> Result<()> {
        self.ensure_active()?;
        self.db
            .abort_content_upload_at(self.upload_id, Some(self.revision))
            .await
    }

    async fn flush_full_chunk(&mut self) -> Result<()> {
        let payload = mem::replace(
            &mut self.buffer,
            Vec::with_capacity(self.options.chunk_bytes()),
        );
        let frame = encode_chunk(self.upload_id, self.complete_chunks, &payload)?;
        self.db
            .write_content_chunk(self.upload_id, self.complete_chunks, frame)
            .await?;
        self.complete_chunks = self
            .complete_chunks
            .checked_add(1)
            .ok_or_else(|| Error::invalid_options("content chunk count overflow"))?;
        Ok(())
    }

    fn ensure_active(&self) -> Result<()> {
        if self.failed {
            Err(Error::invalid_options(
                "content upload previously failed and cannot continue",
            ))
        } else {
            Ok(())
        }
    }
}

use std::time::Duration;

use trine_kv::{
    ContentAttachmentScope, ContentUploadOptions, Db, OwnerScopeId, SealedContent, StorageDomainId,
};

pub(crate) fn content_scope() -> ContentAttachmentScope {
    ContentAttachmentScope::new(
        StorageDomainId::from_bytes([11; 16]),
        OwnerScopeId::from_bytes([12; 16]),
    )
}

pub(crate) async fn seal_bytes(db: &Db, value: &[u8]) -> trine_kv::Result<SealedContent> {
    let mut upload = db
        .begin_content_upload(ContentUploadOptions::new(
            content_scope(),
            Duration::from_hours(1),
        ))
        .await?;
    upload.write(value).await?;
    upload.seal().await
}

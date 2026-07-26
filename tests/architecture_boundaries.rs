use trine_kv::{
    AsyncBranchRange, Branch, BranchRange, CommitInfo, Db, KeyRange, ReadVersion, Result,
    TransactionOptions,
};

#[test]
fn branch_public_contracts_are_type_checked() {
    async fn async_contract(
        db: &Db,
        name: &str,
        version: ReadVersion,
        range: &KeyRange,
    ) -> Result<()> {
        db.create_branch(name, version).await?;
        let branch: Branch<'_> = db.open_branch(name).await?;
        let mut rows: AsyncBranchRange = branch.range("", range).await?;
        while rows.next().await?.is_some() {}
        db.delete_branch(name).await
    }

    fn sync_contract(db: &Db, name: &str, version: ReadVersion, range: &KeyRange) -> Result<()> {
        db.create_branch_sync(name, version)?;
        let branch: Branch<'_> = db.open_branch_sync(name)?;
        let rows: BranchRange = branch.range_sync("", range)?;
        for row in rows {
            row?;
        }
        db.delete_branch_sync(name)
    }

    async fn transaction_contract(db: &Db, range: KeyRange) -> Result<CommitInfo> {
        let mut transaction = db.transaction(TransactionOptions::default());
        let _: Option<Vec<u8>> = transaction.get(b"key").await?;
        transaction.read_range(range).await?;
        transaction.put(b"key", b"value");
        transaction.commit().await
    }

    let _ = async_contract;
    let _ = sync_contract;
    let _ = transaction_contract;
}

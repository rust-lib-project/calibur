use rocksdb_rs::AsyncFileSystem;
use rocksdb_rs::DBOptions;
use rocksdb_rs::Engine;
use rocksdb_rs::ReadOptions;
use rocksdb_rs::WriteBatch;
use std::sync::Arc;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    // Open engine
    let dir = tempfile::Builder::new()
        .prefix("db_simple_example")
        .tempdir()?;
    let mut db_options = DBOptions::default();
    db_options.fs = Arc::new(AsyncFileSystem::new(2));
    db_options.db_path = dir.path().to_str().unwrap().to_string();

    let mut engine = Engine::open(db_options.clone(), vec![], None).await?;

    // Put key value
    let mut wb = WriteBatch::new();
    wb.put(b"key1", b"value1");
    engine.write(&mut wb).await?;

    // Get value
    let opts = ReadOptions::default();
    let v = engine.get(&opts, 0, b"key1").await?;
    assert!(v.is_some());
    assert_eq!(v.unwrap(), b"value1".to_vec());

    // Atomically apply a set of updates
    let mut wb = WriteBatch::new();
    wb.put(b"key2", b"value");
    wb.delete(b"key1");
    engine.write(&mut wb).await?;

    let v = engine.get(&opts, 0, b"key1").await?;
    assert!(v.is_none(), "value is {:?}", v);

    let v = engine.get(&opts, 0, b"key2").await?;
    assert!(v.is_some());
    assert_eq!(v.unwrap(), b"value".to_vec());

    Ok(())
}

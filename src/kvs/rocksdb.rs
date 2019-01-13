use crate::kvs::KeyValueStore;
use crate::task::Existence;
use crate::Result;
use rocksdb::{DBVector, DB};
use std::path::Path;

#[derive(Debug)]
pub struct RocksDb {
    db: DB,
}
impl RocksDb {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let db = track_any_err!(DB::open_default(path))?;
        Ok(RocksDb { db })
    }
}
impl KeyValueStore for RocksDb {
    type OwnedValue = DBVector;

    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<Existence> {
        track_any_err!(self.db.put(key, value))?;
        Ok(Existence::unknown())
    }

    fn get(&mut self, key: &[u8]) -> Result<Option<Self::OwnedValue>> {
        let value = track_any_err!(self.db.get(key))?;
        Ok(value)
    }

    fn delete(&mut self, key: &[u8]) -> Result<Existence> {
        track_any_err!(self.db.delete(key))?;
        Ok(Existence::unknown())
    }
}

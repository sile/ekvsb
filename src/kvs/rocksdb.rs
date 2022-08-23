use crate::kvs::KeyValueStore;
use crate::task::Existence;
use crate::Result;
use rocksdb::{Options, DB};
use std::path::Path;

#[derive(Debug)]
pub struct RocksDb {
    db: DB,
}
impl RocksDb {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let db = track_any_err!(DB::open_default(path))?;
        Ok(Self { db })
    }

    pub fn with_options<P: AsRef<Path>>(path: P, mut options: Options) -> Result<Self> {
        options.create_if_missing(true);
        let db = track_any_err!(DB::open(&options, path))?;
        Ok(Self { db })
    }
}
impl KeyValueStore for RocksDb {
    type OwnedValue = Vec<u8>;

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

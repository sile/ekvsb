use std::collections::{BTreeMap, HashMap};

use Result;

use task::Existence;

pub use self::fs::FileSystemKvs;
pub use self::rocksdb::RocksDb;
pub use self::sled::SledTree;

mod fs;
mod rocksdb;
mod sled;

pub trait KeyValueStore {
    type OwnedValue;

    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<Existence>;
    fn get(&mut self, key: &[u8]) -> Result<Option<Self::OwnedValue>>;
    fn delete(&mut self, key: &[u8]) -> Result<Existence>;
}

impl KeyValueStore for HashMap<Vec<u8>, Vec<u8>> {
    type OwnedValue = Vec<u8>;

    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<Existence> {
        let exists = self.insert(key.to_vec(), value.to_vec()).is_some();
        Ok(Existence::new(exists))
    }

    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(HashMap::get(self, key).cloned())
    }

    fn delete(&mut self, key: &[u8]) -> Result<Existence> {
        let exists = self.remove(key).is_some();
        Ok(Existence::new(exists))
    }
}

impl KeyValueStore for BTreeMap<Vec<u8>, Vec<u8>> {
    type OwnedValue = Vec<u8>;

    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<Existence> {
        let exists = self.insert(key.to_vec(), value.to_vec()).is_some();
        Ok(Existence::new(exists))
    }

    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(BTreeMap::get(self, key).cloned())
    }

    fn delete(&mut self, key: &[u8]) -> Result<Existence> {
        let exists = self.remove(key).is_some();
        Ok(Existence::new(exists))
    }
}

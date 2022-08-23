use crate::kvs::KeyValueStore;
use crate::task::Existence;
use crate::Result;
use sled::{Config, Db, IVec};
use std::path::Path;

// #[derive(Debug)]
pub struct SledTree {
    tree: Db,
}
impl SledTree {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let tree = track_any_err!(Config::new().path(path).open())?;
        Ok(SledTree { tree })
    }
}
impl KeyValueStore for SledTree {
    type OwnedValue = IVec;

    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<Existence> {
        track_any_err!(self.tree.insert(key.to_vec(), value.to_vec()))?;
        Ok(Existence::unknown())
    }

    fn get(&mut self, key: &[u8]) -> Result<Option<Self::OwnedValue>> {
        let value = track_any_err!(self.tree.get(key))?;
        Ok(value)
    }

    fn delete(&mut self, key: &[u8]) -> Result<Existence> {
        let exists = track_any_err!(self.tree.remove(key))?.is_some();
        Ok(Existence::new(exists))
    }
}

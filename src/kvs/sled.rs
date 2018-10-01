use sled::{ConfigBuilder, Tree};
use std::path::Path;

use kvs::KeyValueStore;
use task::Existence;
use Result;

#[derive(Debug)]
pub struct SledTree {
    tree: Tree,
}
impl SledTree {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let config = ConfigBuilder::new().path(path).build();
        let tree = track_any_err!(Tree::start(config))?;
        Ok(SledTree { tree })
    }
}
impl KeyValueStore for SledTree {
    type OwnedValue = Vec<u8>;

    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<Existence> {
        track_any_err!(self.tree.set(key.to_vec(), value.to_vec()))?;
        Ok(Existence::unknown())
    }

    fn get(&mut self, key: &[u8]) -> Result<Option<Self::OwnedValue>> {
        let value = track_any_err!(self.tree.get(key))?;
        Ok(value)
    }

    fn delete(&mut self, key: &[u8]) -> Result<Existence> {
        let exists = track_any_err!(self.tree.del(key))?.is_some();
        Ok(Existence::new(exists))
    }
}

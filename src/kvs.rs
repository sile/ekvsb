use std::collections::{BTreeMap, HashMap};

use Result;

pub trait KeyValueStore {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<bool>;
    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    fn delete(&mut self, key: &[u8]) -> Result<bool>;
}

impl KeyValueStore for HashMap<Vec<u8>, Vec<u8>> {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<bool> {
        let exists = self.insert(key.to_vec(), value.to_vec()).is_some();
        Ok(exists)
    }

    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(HashMap::get(self, key).cloned())
    }

    fn delete(&mut self, key: &[u8]) -> Result<bool> {
        let exists = self.remove(key).is_some();
        Ok(exists)
    }
}

impl KeyValueStore for BTreeMap<Vec<u8>, Vec<u8>> {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<bool> {
        let exists = self.insert(key.to_vec(), value.to_vec()).is_some();
        Ok(exists)
    }

    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(BTreeMap::get(self, key).cloned())
    }

    fn delete(&mut self, key: &[u8]) -> Result<bool> {
        let exists = self.remove(key).is_some();
        Ok(exists)
    }
}

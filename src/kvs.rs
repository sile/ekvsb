use std::collections::{BTreeMap, HashMap};

use Result;

pub trait KeyValueStore {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()>;
    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    fn delete(&mut self, key: &[u8]) -> Result<()>;
}

impl KeyValueStore for HashMap<Vec<u8>, Vec<u8>> {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(HashMap::get(self, key).cloned())
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.remove(key);
        Ok(())
    }
}

impl KeyValueStore for BTreeMap<Vec<u8>, Vec<u8>> {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(BTreeMap::get(self, key).cloned())
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.remove(key);
        Ok(())
    }
}

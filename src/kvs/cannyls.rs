use crate::kvs::KeyValueStore;
use crate::task::Existence;
use crate::Result;
use cannyls;
use cannyls::deadline::Deadline;
use cannyls::device::{Device, DeviceBuilder};
use cannyls::lump::{LumpData, LumpId};
use cannyls::nvm::FileNvm;
use cannyls::storage::{Storage, StorageBuilder};
use futures::{Async, Future};
use std::path::Path;
use std::thread;
use std::time::Duration;
use trackable::error::{ErrorKindExt, Failed, Failure};

#[derive(Debug)]
pub struct CannyLsOptions {
    pub capacity: u64,
    pub journal_sync_interval: usize,
}
impl Default for CannyLsOptions {
    fn default() -> Self {
        CannyLsOptions {
            capacity: 1024 * 1024 * 1024,
            journal_sync_interval: 4096,
        }
    }
}

#[derive(Debug)]
pub struct CannyLsDevice {
    device: Device,
}
impl CannyLsDevice {
    pub fn new<P: AsRef<Path>>(lusf_file: P, options: &CannyLsOptions) -> Result<Self> {
        let (nvm, created) =
            track!(FileNvm::create_if_absent(lusf_file, options.capacity).map_err(into_failure))?;

        let mut storage = StorageBuilder::new();
        storage.journal_sync_interval(options.journal_sync_interval);
        let storage = if created {
            track!(storage.create(nvm).map_err(into_failure))?
        } else {
            track!(storage.open(nvm).map_err(into_failure))?
        };

        let device = DeviceBuilder::new().spawn(|| Ok(storage));
        let device = track!(wait(device.wait_for_running()))?;
        Ok(CannyLsDevice { device })
    }
}
impl KeyValueStore for CannyLsDevice {
    type OwnedValue = LumpData;

    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<Existence> {
        let id = track!(bytes_to_lump_id(key))?;
        let data = track!(self
            .device
            .handle()
            .allocate_lump_data_with_bytes(value)
            .map_err(into_failure))?;
        let new = track!(wait(self.device.handle().request().put(id, data)))?;
        Ok(Existence::new(!new))
    }

    fn get(&mut self, key: &[u8]) -> Result<Option<Self::OwnedValue>> {
        let id = track!(bytes_to_lump_id(key))?;
        let data = track!(wait(self.device.handle().request().get(id)))?;
        Ok(data)
    }

    fn delete(&mut self, key: &[u8]) -> Result<Existence> {
        let id = track!(bytes_to_lump_id(key))?;
        let exists = track!(wait(self.device.handle().request().delete(id)))?;
        Ok(Existence::new(exists))
    }
}
impl Drop for CannyLsDevice {
    fn drop(&mut self) {
        self.device.stop(Deadline::Immediate);
        let _ = wait(&mut self.device);
    }
}

fn wait<F>(mut future: F) -> Result<F::Item>
where
    F: Future<Error = cannyls::Error>,
{
    loop {
        if let Async::Ready(item) = future.poll().map_err(into_failure)? {
            return Ok(item);
        }
        thread::sleep(Duration::from_micros(100));
    }
}

#[derive(Debug)]
pub struct CannyLsStorage {
    storage: Storage<FileNvm>,
}
impl CannyLsStorage {
    pub fn new<P: AsRef<Path>>(lusf_file: P, options: &CannyLsOptions) -> Result<Self> {
        let (nvm, created) =
            track!(FileNvm::create_if_absent(lusf_file, options.capacity).map_err(into_failure))?;

        let mut storage = StorageBuilder::new();
        storage.journal_sync_interval(options.journal_sync_interval);
        let storage = if created {
            track!(storage.create(nvm).map_err(into_failure))?
        } else {
            track!(storage.open(nvm).map_err(into_failure))?
        };
        Ok(CannyLsStorage { storage })
    }
}
impl KeyValueStore for CannyLsStorage {
    type OwnedValue = LumpData;

    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<Existence> {
        let id = track!(bytes_to_lump_id(key))?;
        let data = track!(self
            .storage
            .allocate_lump_data_with_bytes(value)
            .map_err(into_failure))?;
        let new = track!(self.storage.put(&id, &data).map_err(into_failure))?;
        Ok(Existence::new(!new))
    }

    fn get(&mut self, key: &[u8]) -> Result<Option<Self::OwnedValue>> {
        let id = track!(bytes_to_lump_id(key))?;
        let data = track!(self.storage.get(&id).map_err(into_failure))?;
        Ok(data)
    }

    fn delete(&mut self, key: &[u8]) -> Result<Existence> {
        let id = track!(bytes_to_lump_id(key))?;
        let exists = track!(self.storage.delete(&id).map_err(into_failure))?;
        Ok(Existence::new(exists))
    }
}

fn bytes_to_lump_id(bytes: &[u8]) -> Result<LumpId> {
    track_assert!(bytes.len() <= 16, Failed; bytes.len());
    let mut id = 0;
    for b in bytes {
        id = (id << 8) + u128::from(*b);
    }
    Ok(LumpId::new(id))
}

fn into_failure(e: cannyls::Error) -> Failure {
    Failed.takes_over(e).into()
}

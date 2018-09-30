use percent_encoding::{percent_encode, DEFAULT_ENCODE_SET};
use siphasher::sip::SipHasher13;
use std::fs::{self, File, OpenOptions};
use std::hash::{Hash, Hasher};
use std::io::{ErrorKind, Read, Write};
use std::path::{Path, PathBuf};
use trackable::error::Failed;

use kvs::KeyValueStore;
use task::Existence;
use Result;

#[derive(Debug)]
pub struct FileSystemKvs {
    root_dir: PathBuf,
}
impl FileSystemKvs {
    pub fn new<P: AsRef<Path>>(root_dir: P) -> Result<Self> {
        track_any_err!(fs::create_dir_all(&root_dir))?;
        Ok(FileSystemKvs {
            root_dir: root_dir.as_ref().to_path_buf(),
        })
    }

    fn key_to_path(&self, key: &[u8]) -> PathBuf {
        let name = percent_encode(key, DEFAULT_ENCODE_SET).to_string();

        let mut hasher = SipHasher13::new();
        name.hash(&mut hasher);
        let file = format!("{:04x}/{}", hasher.finish() as u16, name);

        self.root_dir.join(file)
    }
}
impl KeyValueStore for FileSystemKvs {
    type OwnedValue = Vec<u8>;

    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<Existence> {
        let path = self.key_to_path(key);
        track_any_err!(fs::create_dir_all(track_assert_some!(
            path.parent(),
            Failed
        )))?;
        let mut file = track_any_err!(
            OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(path)
        )?;
        track_any_err!(file.write_all(value))?;
        Ok(Existence::unknown())
    }

    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let path = self.key_to_path(key);
        match File::open(path) {
            Err(e) => {
                if e.kind() != ErrorKind::NotFound {
                    track_any_err!(Err(e))?;
                }
                Ok(None)
            }
            Ok(mut file) => {
                let mut buf = Vec::new();
                track_any_err!(file.read_to_end(&mut buf))?;
                Ok(Some(buf))
            }
        }
    }

    fn delete(&mut self, key: &[u8]) -> Result<Existence> {
        let path = self.key_to_path(key);
        track_any_err!(fs::remove_file(path))?;
        Ok(Existence::unknown())
    }
}

use rand::{self, RngCore};
use std::cmp::Ordering;
use std::time::Duration;
use trackable::error::Failure;

use Result;

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Task {
    Put { key: Key, value: ValueSpec },
    Get { key: Key },
    Delete { key: Key },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Key(String);
impl Key {
    pub fn new(s: String) -> Self {
        Key(s)
    }

    pub fn from_utf8(b: Vec<u8>) -> Result<Self> {
        let s = track_any_err!(String::from_utf8(b))?;
        Ok(Key(s))
    }
}
impl AsRef<[u8]> for Key {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ValueSpec {
    Random { size: usize },
}
impl ValueSpec {
    pub fn generate(&self) -> Vec<u8> {
        let ValueSpec::Random { size } = self;
        let mut value = vec![0; *size];
        rand::thread_rng().fill_bytes(&mut value);
        value
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Method {
    Put,
    Get,
    Delete,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TaskResult {
    pub seqno: usize,
    pub key: Key,
    pub method: Method,
    pub start_time: Seconds,
    pub elapsed: Seconds,
    pub exists: Existence,
    pub error: Option<Failure>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Existence(Option<bool>);
impl Existence {
    pub fn new(exists: bool) -> Self {
        Existence(Some(exists))
    }

    pub fn unknown() -> Self {
        Existence(None)
    }

    pub fn exists(self) -> Option<bool> {
        self.0
    }
}

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize, PartialOrd, PartialEq)]
pub struct Seconds(f64);
impl Seconds {
    pub fn new(duration: Duration) -> Self {
        let x = duration.as_secs() as f64 + f64::from(duration.subsec_nanos()) / 1_000_000_000.0;
        Seconds(x)
    }

    pub fn as_f64(self) -> f64 {
        self.0
    }
}
impl Ord for Seconds {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.partial_cmp(&other.0).expect("Never fails")
    }
}
impl Eq for Seconds {}

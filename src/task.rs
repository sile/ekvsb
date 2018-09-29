use rand::{self, RngCore};
use std::time::Duration;
use trackable::error::Failure;

use Result;

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Task {
    Put {
        key: Key,
        value: ValueSpec,
        #[serde(default)]
        priority: usize,
    },
    Get {
        key: Key,
        #[serde(default)]
        priority: usize,
    },
    Delete {
        key: Key,
        #[serde(default)]
        priority: usize,
    },
}
impl Task {
    pub fn priority(&self) -> usize {
        match self {
            Task::Put { priority, .. } => *priority,
            Task::Get { priority, .. } => *priority,
            Task::Delete { priority, .. } => *priority,
        }
    }
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
    pub exists: bool,
    pub error: Option<Failure>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Seconds(f64);
impl Seconds {
    pub fn new(duration: Duration) -> Self {
        let x = duration.as_secs() as f64 + duration.subsec_micros() as f64 / 1_000_000.0;
        Seconds(x)
    }
}

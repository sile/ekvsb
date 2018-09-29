extern crate percent_encoding;
extern crate rand;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate siphasher;
#[macro_use]
extern crate trackable;

pub use kvs::KeyValueStore;

pub mod fs;
pub mod task;
pub mod workload;

mod kvs;

pub type Result<T> = std::result::Result<T, trackable::error::Failure>;

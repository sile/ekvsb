extern crate percent_encoding;
extern crate siphasher;
#[macro_use]
extern crate trackable;

pub use kvs::KeyValueStore;

pub mod fs;

mod kvs;

pub type Result<T> = std::result::Result<T, trackable::error::Failure>;

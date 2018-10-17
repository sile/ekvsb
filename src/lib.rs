extern crate cannyls;
extern crate futures;
extern crate gnuplot;
extern crate percent_encoding;
extern crate rand;
extern crate rocksdb;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate siphasher;
extern crate sled;
#[macro_use]
extern crate trackable;

pub mod kvs;
pub mod plot;
pub mod task;
pub mod workload;

pub type Result<T> = std::result::Result<T, trackable::error::Failure>;

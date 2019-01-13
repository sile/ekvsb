#![allow(clippy::new_ret_no_self)]

#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate trackable;

pub mod kvs;
pub mod plot;
pub mod task;
pub mod workload;

pub type Result<T> = std::result::Result<T, trackable::error::Failure>;

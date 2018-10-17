ekvsb
=====

[![Crates.io: ekvsb](https://img.shields.io/crates/v/ekvsb.svg)](https://crates.io/crates/ekvsb)
[![Documentation](https://docs.rs/ekvsb/badge.svg)](https://docs.rs/ekvsb)
[![Build Status](https://travis-ci.org/sile/ekvsb.svg?branch=master)](https://travis-ci.org/sile/ekvsb)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Benchmark Tool for Embedded Key-Value Stores available in Rust

Supported Key-Value Stores
--------------------------

- [HashMap](https://doc.rust-lang.org/std/collections/struct.HashMap.html) (volatile)
- [BTreeMap](https://doc.rust-lang.org/std/collections/struct.BTreeMap.html) (volatile)
- [FileSystem](https://docs.rs/ekvsb/0/ekvsb/kvs/struct.FileSystemKvs.html)
- [RocksDB](https://crates.io/crates/rocksdb)
- [Sled](https://crates.io/crates/sled)
- [CannyLS](https://crates.io/crates/cannyls)

Examples
--------

```console
$ ekvsb workload PUT --count 100000 --value-size 1KiB | ekvsb run rocksdb /tmp/rocksdb | ekvsb summary
{
  "oks": 100000,
  "errors": 0,
  "elapsed": 1.5015379999996445,
  "ops": 66598.38112656734,
  "latency": {
    "min": 9e-6,
    "median": 0.000013,
    "p95": 0.000023,
    "p99": 0.000055,
    "max": 0.004956
  }
}
```

ekvsb
=====

Benchmark Tool for Embedded Key-Value Stores available in Rust

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

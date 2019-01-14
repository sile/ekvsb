#[macro_use]
extern crate clap;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate trackable;

use byte_unit::Byte;
use clap::{App, Arg, ArgMatches, SubCommand};
use ekvsb::kvs::{self, KeyValueStore};
use ekvsb::task::{Key, Seconds, Task, TaskResult, ValueSpec};
use ekvsb::workload::{Workload, WorkloadExecutor};
use ekvsb::Result;
use indicatif::ProgressBar;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use rocksdb;
use std::collections::{BTreeMap, HashMap};
use std::io::{BufReader, BufWriter, Read, Write};
use trackable::error::{ErrorKindExt, Failed};

fn main() -> trackable::result::MainResult {
    let matches = app_from_crate!()
        .subcommand(
            SubCommand::with_name("run")
                .arg(
                    Arg::with_name("MEMORY_LOAD_SIZE")
                        .long("memory-load")
                        .takes_value(true)
                        .default_value("0GiB"),
                )
                .subcommand(
                    SubCommand::with_name("builtin::fs")
                        .arg(Arg::with_name("DIR").index(1).required(true)),
                )
                .subcommand(SubCommand::with_name("builtin::hashmap"))
                .subcommand(SubCommand::with_name("builtin::btreemap"))
                .subcommand(
                    SubCommand::with_name("cannyls")
                        .arg(Arg::with_name("FILE").index(1).required(true))
                        .arg(
                            Arg::with_name("CAPACITY")
                                .long("capacity")
                                .takes_value(true)
                                .default_value("1GiB"),
                        )
                        .arg(
                            Arg::with_name("JOURNAL_SYNC_INTERVAL")
                                .long("journal-sync-interval")
                                .takes_value(true)
                                .default_value("4096"),
                        )
                        .arg(Arg::with_name("WITHOUT_DEVICE").long("without-device")),
                )
                .subcommand(
                    SubCommand::with_name("rocksdb")
                        .arg(Arg::with_name("DIR").index(1).required(true))
                        .arg(
                            Arg::with_name("PARALLELISM")
                                .long("parallelism")
                                .takes_value(true),
                        )
                        .arg(
                            Arg::with_name("OPTIMIZE_LEVEL_STYLE_COMPACTION")
                                .long("optimize-level-style-compaction")
                                .takes_value(true)
                                .value_name("MEMTABLE_MEMORY_BUDGET"),
                        )
                        .arg(
                            Arg::with_name("COMPACTION_READAHEAD_SIZE")
                                .takes_value(true)
                                .long("compaction-readahead-size"),
                        )
                        .arg(
                            Arg::with_name("OPTIMIZE_FOR_POINT_LOOKUP")
                                .long("optimize-for-point-lookup")
                                .takes_value(true)
                                .value_name("CACHE_SIZE"),
                        )
                        .arg(Arg::with_name("USE_FSYNC").long("use-fsync"))
                        .arg(
                            Arg::with_name("BYTES_PER_SYNC")
                                .long("bytes-per-sync")
                                .takes_value(true),
                        )
                        .arg(
                            Arg::with_name("ALLOW_CONCURRENT_MEMTABLE_WRITE")
                                .long("allow-concurrent-memtable-write"),
                        )
                        .arg(Arg::with_name("USE_DIRECT_READS").long("use-direct-reads"))
                        .arg(
                            Arg::with_name("USE_DIRECT_IO_FOR_FLUSH_AND_COMPACTION")
                                .long("use-direct-io-for-flush-and-compaction"),
                        )
                        .arg(
                            Arg::with_name("TABLE_CACHE_NUM_SHARD_BITS")
                                .long("table-cache-num-shard-bits")
                                .takes_value(true),
                        )
                        .arg(
                            Arg::with_name("MIN_WRITE_BUFFER_NUMBER")
                                .long("min-write-buffer-number")
                                .takes_value(true),
                        )
                        .arg(
                            Arg::with_name("MAX_WRITE_BUFFER_NUMBER")
                                .long("max-write-buffer-number")
                                .takes_value(true),
                        )
                        .arg(
                            Arg::with_name("WRITE_BUFFER_SIZE")
                                .long("write-buffer-size")
                                .takes_value(true),
                        )
                        .arg(
                            Arg::with_name("MAX_BYTES_FOR_LEVEL_BASE")
                                .long("max-bytes-for-level-base")
                                .takes_value(true),
                        )
                        .arg(
                            Arg::with_name("MAX_BYTES_FOR_LEVEL_MULTIPLIER")
                                .long("max-bytes-for-level-multiplier")
                                .takes_value(true),
                        )
                        .arg(
                            Arg::with_name("MAX_MANIFEST_FILE_SIZE")
                                .long("max-manifest-file-size")
                                .takes_value(true),
                        )
                        .arg(
                            Arg::with_name("TARGET_FILE_SIZE_BASE")
                                .long("target-file-size-base")
                                .takes_value(true),
                        )
                        .arg(
                            Arg::with_name("MIN_WRITE_BUFFER_NUMBER_TO_MERGE")
                                .long("min-write-buffer-number-to-merge")
                                .takes_value(true),
                        )
                        .arg(
                            Arg::with_name("LEVEL_ZERO_FILE_NUM_COMPACTION_TRIGGER")
                                .long("level-zero-file-num-compaction-trigger")
                                .takes_value(true),
                        )
                        .arg(
                            Arg::with_name("LEVEL_ZERO_SLOWDOWN_WRITES_TRIGGER")
                                .long("level-zero-slowdown-writes-trigger")
                                .takes_value(true),
                        )
                        .arg(
                            Arg::with_name("LEVEL_ZERO_STOP_WRITES_TRIGGER")
                                .long("level-zero-stop-writes-trigger")
                                .takes_value(true),
                        )
                        .arg(
                            Arg::with_name("COMPACTION_STYLE")
                                .long("compaction-style")
                                .takes_value(true)
                                .possible_values(&["LEVEL", "UNIVERSAL", "FIFO"]),
                        )
                        .arg(
                            Arg::with_name("MAX_BACKGROUND_COMPACTIONS")
                                .long("max-background-compactions")
                                .takes_value(true),
                        )
                        .arg(
                            Arg::with_name("MAX_BACKGROUND_FLUSHES")
                                .long("max-background-flushes")
                                .takes_value(true),
                        )
                        .arg(
                            Arg::with_name("DISABLE_AUTO_COMPACTIONS")
                                .long("disable-auto-compactions"),
                        )
                        .arg(Arg::with_name("ADVISE_RANDOM_ON_OPEN").long("advise-random-on-open"))
                        .arg(
                            Arg::with_name("NUM_LEVELS")
                                .long("num-levels")
                                .takes_value(true),
                        )
                        .arg(
                            Arg::with_name("MEMTABLE_PREFIX_BLOOM_RATIO")
                                .long("memtable-prefix-bloom-ratio")
                                .takes_value(true),
                        ),
                )
                .subcommand(
                    SubCommand::with_name("sled")
                        .arg(Arg::with_name("DIR").index(1).required(true)),
                ),
        )
        .subcommand(
            SubCommand::with_name("workload")
                .subcommand(
                    workload_subcommand("PUT").arg(
                        Arg::with_name("VALUE_SIZE")
                            .long("value-size")
                            .takes_value(true)
                            .default_value("1KiB"),
                    ),
                )
                .subcommand(workload_subcommand("GET"))
                .subcommand(workload_subcommand("DELETE")),
        )
        .subcommand(SubCommand::with_name("summary"))
        .subcommand(
            SubCommand::with_name("plot")
                .subcommand(plot_subcommand("text"))
                .subcommand(
                    plot_subcommand("png")
                        .arg(Arg::with_name("OUTPUT_FILE").index(1).required(true))
                        .arg(
                            Arg::with_name("WIDTH")
                                .long("width")
                                .takes_value(true)
                                .default_value("1200"),
                        )
                        .arg(
                            Arg::with_name("HEIGHT")
                                .long("height")
                                .takes_value(true)
                                .default_value("800"),
                        ),
                ),
        )
        .get_matches();
    if let Some(matches) = matches.subcommand_matches("run") {
        track!(handle_run_subcommand(matches))?;
    } else if let Some(matches) = matches.subcommand_matches("workload") {
        track!(handle_workload_subcommand(matches))?;
    } else if let Some(matches) = matches.subcommand_matches("summary") {
        track!(handle_summary_subcommand(matches))?;
    } else if let Some(matches) = matches.subcommand_matches("plot") {
        track!(handle_plot_subcommand(matches))?;
    } else {
        eprintln!("Usage: {}", matches.usage());
        std::process::exit(1);
    }
    Ok(())
}

fn workload_subcommand(name: &'static str) -> App<'static, 'static> {
    SubCommand::with_name(name)
        .arg(
            Arg::with_name("COUNT")
                .long("count")
                .takes_value(true)
                .default_value("1000"),
        )
        .arg(
            Arg::with_name("POPULATION_SIZE")
                .long("population-size")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("KEY_SIZE")
                .long("key-size")
                .takes_value(true)
                .default_value("10"),
        )
        .arg(Arg::with_name("SEED").long("seed").takes_value(true))
        .arg(
            Arg::with_name("SHUFFLE")
                .long("shuffle")
                .takes_value(true)
                .value_name("SHUFFLE_SEED"),
        )
}

fn plot_subcommand(name: &'static str) -> App<'static, 'static> {
    SubCommand::with_name(name)
        .arg(Arg::with_name("TITLE").long("title").takes_value(true))
        .arg(
            Arg::with_name("SAMPLING_RATE")
                .long("sampling-rate")
                .takes_value(true)
                .default_value("1.0"),
        )
        .arg(Arg::with_name("Y_MAX").long("y-max").takes_value(true))
        .arg(Arg::with_name("LOGSCALE").long("logscale"))
}

fn handle_run_subcommand(matches: &ArgMatches) -> Result<()> {
    let memory_load_size = track!(parse_size(
        matches.value_of("MEMORY_LOAD_SIZE").expect("never fails")
    ))?;
    let _reserved_memory: Vec<u8> = vec![1; memory_load_size];

    let workload: Workload = track_any_err!(
        serde_json::from_reader(stdin()),
        "Malformed input workload JSON"
    )?;

    if let Some(matches) = matches.subcommand_matches("builtin::fs") {
        let dir = matches.value_of("DIR").expect("never fails");
        let kvs = track!(kvs::FileSystemKvs::new(dir))?;
        track!(execute(kvs, workload))?;
    } else if let Some(_matches) = matches.subcommand_matches("builtin::hashmap") {
        let kvs = HashMap::new();
        track!(execute(kvs, workload))?;
    } else if let Some(_matches) = matches.subcommand_matches("builtin::btreemap") {
        let kvs = BTreeMap::new();
        track!(execute(kvs, workload))?;
    } else if let Some(matches) = matches.subcommand_matches("cannyls") {
        let file = matches.value_of("FILE").expect("never fails");
        let capacity = track!(parse_size_u64(
            matches.value_of("CAPACITY").expect("never fails")
        ))?;
        let journal_sync_interval = track_any_err!(matches
            .value_of("JOURNAL_SYNC_INTERVAL")
            .expect("never fails")
            .parse())?;
        let options = kvs::CannyLsOptions {
            capacity,
            journal_sync_interval,
        };
        if matches.is_present("WITHOUT_DEVICE") {
            let kvs = track!(kvs::CannyLsStorage::new(file, &options))?;
            track!(execute(kvs, workload))?;
        } else {
            let kvs = track!(kvs::CannyLsDevice::new(file, &options))?;
            track!(execute(kvs, workload))?;
        }
    } else if let Some(matches) = matches.subcommand_matches("rocksdb") {
        let dir = matches.value_of("DIR").expect("never fails");
        let options = track!(parse_rocksdb_options(matches))?;
        let kvs = track!(kvs::RocksDb::with_options(dir, options))?;
        track!(execute(kvs, workload))?;
    } else if let Some(matches) = matches.subcommand_matches("sled") {
        let dir = matches.value_of("DIR").expect("never fails");
        let kvs = track!(kvs::SledTree::new(dir))?;
        track!(execute(kvs, workload))?;
    } else {
        eprintln!("Usage: {}", matches.usage());
        std::process::exit(1);
    }
    Ok(())
}

fn execute<T: KeyValueStore>(kvs: T, workload: Workload) -> Result<()> {
    let pb = ProgressBar::new(workload.len() as u64);
    let executor = WorkloadExecutor::new(kvs, workload);

    println!("[");
    for (i, result) in executor.enumerate() {
        if i != 0 {
            print!(",\n  ");
        } else {
            print!("  ");
        }
        pb.inc(1);
        track_any_err!(serde_json::to_writer(stdout(), &result))?;
    }
    println!("\n]");
    Ok(())
}

fn handle_workload_subcommand(matches: &ArgMatches) -> Result<()> {
    let tasks = if let Some(matches) = matches.subcommand_matches("PUT") {
        let value_size = track!(parse_size(
            matches.value_of("VALUE_SIZE").expect("never fails")
        ))?;
        track!(generate_tasks(matches, |key| Task::Put {
            key,
            value: ValueSpec::Random { size: value_size },
        }))?
    } else if let Some(matches) = matches.subcommand_matches("GET") {
        track!(generate_tasks(matches, |key| Task::Get { key }))?
    } else if let Some(matches) = matches.subcommand_matches("DELETE") {
        track!(generate_tasks(matches, |key| Task::Delete { key }))?
    } else {
        unreachable!();
    };
    track_any_err!(serde_json::to_writer(stdout(), &tasks))?;
    Ok(())
}

fn generate_tasks<F>(matches: &ArgMatches, f: F) -> Result<Vec<Task>>
where
    F: Fn(Key) -> Task,
{
    let count: usize = track_any_err!(matches.value_of("COUNT").expect("never fails").parse())?;
    let key_size = track!(parse_size(
        matches.value_of("KEY_SIZE").expect("never fails")
    ))?;
    let seed = matches.value_of("SEED");
    let mut population_size = count;
    if let Some(size) = matches.value_of("POPULATION_SIZE") {
        population_size = track_any_err!(size.parse())?;
        track_assert!(count <= population_size, Failed; count, population_size);
    }

    let mut rng = if let Some(seed) = seed {
        track_assert!(seed.len() <= 32, Failed; seed.len());
        let mut seed_bytes = [0; 32];
        for (i, b) in seed.bytes().enumerate() {
            seed_bytes[i] = b;
        }
        StdRng::from_seed(seed_bytes)
    } else {
        StdRng::from_seed(rand::thread_rng().gen())
    };

    const CHARS: &[u8; 62] = b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    let mut tasks = Vec::new();
    let mut key = vec![0u8; key_size];
    for _ in 0..population_size {
        for b in &mut key {
            *b = *CHARS.choose(&mut rng).expect("never fails");
        }
        tasks.push(f(track!(Key::from_utf8(key.clone()))?));
    }

    if let Some(seed) = matches.value_of("SHUFFLE") {
        track_assert!(seed.len() <= 32, Failed; seed.len());
        let mut seed_bytes = [0; 32];
        for (i, b) in seed.bytes().enumerate() {
            seed_bytes[i] = b;
        }

        let mut shuffle_rng = StdRng::from_seed(seed_bytes);
        tasks.shuffle(&mut shuffle_rng);
    }

    tasks.truncate(count);

    Ok(tasks)
}

fn handle_summary_subcommand(_matches: &ArgMatches) -> Result<()> {
    let results: Vec<TaskResult> = track_any_err!(
        serde_json::from_reader(stdin()),
        "Malformed run result JSON"
    )?;

    let errors = results.iter().filter(|r| r.error.is_some()).count();
    let oks = results.len() - errors;
    let elapsed = results.iter().map(|r| r.elapsed.as_f64()).sum();
    let ops = results.len() as f64 / elapsed;
    let existence = Existence::new(&results);
    let latency = Latency::new(&results);
    let summary = Summary {
        oks,
        errors,
        existence,
        elapsed,
        ops,
        latency,
    };
    track_any_err!(serde_json::to_writer_pretty(stdout(), &summary))?;
    println!();

    Ok(())
}

#[derive(Serialize)]
struct Summary {
    oks: usize,
    errors: usize,
    existence: Existence,
    elapsed: f64,
    ops: f64,
    latency: Latency,
}

#[derive(Serialize)]
struct Existence {
    exists: u64,
    absents: u64,
    unknowns: u64,
}
impl Existence {
    fn new(results: &[TaskResult]) -> Self {
        let mut exists = 0;
        let mut absents = 0;
        let mut unknowns = 0;
        for r in results {
            match r.exists.exists() {
                None => unknowns += 1,
                Some(false) => absents += 1,
                Some(true) => exists += 1,
            }
        }
        Existence {
            exists,
            absents,
            unknowns,
        }
    }
}

#[derive(Serialize)]
struct Latency {
    min: Seconds,
    median: Seconds,
    p95: Seconds,
    p99: Seconds,
    max: Seconds,
}
impl Latency {
    fn new(results: &[TaskResult]) -> Self {
        let mut times = results.iter().map(|r| r.elapsed).collect::<Vec<_>>();
        times.sort();
        Latency {
            min: times.iter().min().cloned().unwrap_or_default(),
            median: times.get(times.len() / 2).cloned().unwrap_or_default(),
            p95: times
                .get(times.len() * 95 / 100)
                .cloned()
                .unwrap_or_default(),
            p99: times
                .get(times.len() * 99 / 100)
                .cloned()
                .unwrap_or_default(),
            max: times.iter().max().cloned().unwrap_or_default(),
        }
    }
}

fn handle_plot_subcommand(matches: &ArgMatches) -> Result<()> {
    let mut options = ekvsb::plot::PlotOptions::new();

    let matches = if let Some(matches) = matches.subcommand_matches("text") {
        options.terminal = "dumb".to_owned();
        matches
    } else if let Some(matches) = matches.subcommand_matches("png") {
        let width = matches.value_of("WIDTH").expect("never fails");
        let height = matches.value_of("HEIGHT").expect("never fails");
        options.terminal = format!("pngcairo size {}, {}", width, height);

        let output_file = matches.value_of("OUTPUT_FILE").expect("never fails");
        options.output_file = output_file.to_string();

        matches
    } else {
        eprintln!("Usage: {}", matches.usage());
        std::process::exit(1);
    };
    options.sampling_rate = track_any_err!(matches
        .value_of("SAMPLING_RATE")
        .expect("never fails")
        .parse())?;
    options.logscale = matches.is_present("LOGSCALE");
    if let Some(title) = matches.value_of("TITLE") {
        options.title = title.to_string();
    }
    if let Some(y_max) = matches.value_of("Y_MAX") {
        options.y_max = Some(track_any_err!(y_max.parse())?);
    }

    let results: Vec<TaskResult> = track_any_err!(
        serde_json::from_reader(stdin()),
        "Malformed run result JSON"
    )?;
    track!(options.plot(&results))?;
    Ok(())
}

fn parse_size(s: &str) -> Result<usize> {
    let size = Byte::from_string(s)
        .map_err(|e| track!(Failed.cause(format!("Parse Error: {:?} ({:?})", s, e))))?;
    Ok(size.get_bytes() as usize)
}

fn parse_size_u64(s: &str) -> Result<u64> {
    let size = Byte::from_string(s)
        .map_err(|e| track!(Failed.cause(format!("Parse Error: {:?} ({:?})", s, e))))?;
    Ok(size.get_bytes() as u64)
}

fn stdin() -> impl Read {
    BufReader::new(std::io::stdin())
}

fn stdout() -> impl Write {
    BufWriter::new(std::io::stdout())
}

#[allow(clippy::cyclomatic_complexity)]
fn parse_rocksdb_options(matches: &ArgMatches) -> Result<rocksdb::Options> {
    let mut options = rocksdb::Options::default();
    if matches.is_present("ADVISE_RANDOM_ON_OPEN") {
        options.set_advise_random_on_open(true);
    }
    if matches.is_present("ALLOW_CONCURRENT_MEMTABLE_WRITE") {
        options.set_allow_concurrent_memtable_write(true);
    }
    if matches.is_present("DISABLE_AUTO_COMPACTIONS") {
        options.set_disable_auto_compactions(true);
    }
    if matches.is_present("USE_DIRECT_IO_FOR_FLUSH_AND_COMPACTION") {
        options.set_use_direct_io_for_flush_and_compaction(true);
    }
    if matches.is_present("USE_DIRECT_READS") {
        options.set_use_direct_reads(true);
    }
    if matches.is_present("USE_FSYNC") {
        options.set_use_fsync(true);
    }
    if let Some(v) = matches.value_of("BYTES_PER_SYNC") {
        options.set_bytes_per_sync(track_any_err!(v.parse())?);
    }
    if let Some(v) = matches.value_of("COMPACTION_READAHEAD_SIZE") {
        options.set_compaction_readahead_size(track_any_err!(v.parse())?);
    }
    if let Some(v) = matches.value_of("COMPACTION_STYLE") {
        let style = match v {
            "LEVEL" => rocksdb::DBCompactionStyle::Level,
            "UNIVERSAL" => rocksdb::DBCompactionStyle::Universal,
            "FIFO" => rocksdb::DBCompactionStyle::Fifo,
            _ => unreachable!(),
        };
        options.set_compaction_style(style);
    }
    if let Some(v) = matches.value_of("PARALLELISM") {
        options.increase_parallelism(track_any_err!(v.parse())?);
    }
    if let Some(v) = matches.value_of("LEVEL_ZERO_FILE_NUM_COMPACTION_TRIGGER") {
        options.set_level_zero_file_num_compaction_trigger(track_any_err!(v.parse())?);
    }
    if let Some(v) = matches.value_of("LEVEL_ZERO_SLOWDOWN_WRITES_TRIGGER") {
        options.set_level_zero_slowdown_writes_trigger(track_any_err!(v.parse())?);
    }
    if let Some(v) = matches.value_of("LEVEL_ZERO_STOP_WRITES_TRIGGER") {
        options.set_level_zero_stop_writes_trigger(track_any_err!(v.parse())?);
    }
    if let Some(v) = matches.value_of("MAX_BACKGROUND_COMPACTIONS") {
        options.set_max_background_compactions(track_any_err!(v.parse())?);
    }
    if let Some(v) = matches.value_of("MAX_BACKGROUND_FLUSHES") {
        options.set_max_background_flushes(track_any_err!(v.parse())?);
    }
    if let Some(v) = matches.value_of("MAX_BYTES_FOR_LEVEL_BASE") {
        options.set_max_bytes_for_level_base(track_any_err!(v.parse())?);
    }
    if let Some(v) = matches.value_of("MAX_BYTES_FOR_LEVEL_MULTIPLIER") {
        options.set_max_bytes_for_level_multiplier(track_any_err!(v.parse())?);
    }
    if let Some(v) = matches.value_of("MAX_MANIFEST_FILE_SIZE") {
        options.set_max_manifest_file_size(track_any_err!(v.parse())?);
    }
    if let Some(v) = matches.value_of("MAX_WRITE_BUFFER_NUMBER") {
        options.set_max_write_buffer_number(track_any_err!(v.parse())?);
    }
    if let Some(v) = matches.value_of("MEMTABLE_PREFIX_BLOOM_RATIO") {
        options.set_memtable_prefix_bloom_ratio(track_any_err!(v.parse())?);
    }
    if let Some(v) = matches.value_of("MIN_WRITE_BUFFER_NUMBER") {
        options.set_min_write_buffer_number(track_any_err!(v.parse())?);
    }
    if let Some(v) = matches.value_of("MIN_WRITE_BUFFER_NUMBER_TO_MERGE") {
        options.set_min_write_buffer_number_to_merge(track_any_err!(v.parse())?);
    }
    if let Some(v) = matches.value_of("NUM_LEVELS") {
        options.set_num_levels(track_any_err!(v.parse())?);
    }
    if let Some(v) = matches.value_of("OPTIMIZE_FOR_POINT_LOOKUP") {
        options.optimize_for_point_lookup(track_any_err!(v.parse())?);
    }
    if let Some(v) = matches.value_of("OPTIMIZE_LEVEL_STYLE_COMPACTION") {
        options.optimize_level_style_compaction(track_any_err!(v.parse())?);
    }
    if let Some(v) = matches.value_of("TABLE_CACHE_NUM_SHARD_BITS") {
        options.set_table_cache_num_shard_bits(track_any_err!(v.parse())?);
    }
    if let Some(v) = matches.value_of("TARGET_FILE_SIZE_BASE") {
        options.set_target_file_size_base(track_any_err!(v.parse())?);
    }
    if let Some(v) = matches.value_of("WRITE_BUFFER_SIZE") {
        options.set_write_buffer_size(track_any_err!(v.parse())?);
    }

    Ok(options)
}

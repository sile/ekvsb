extern crate byte_unit;
#[macro_use]
extern crate clap;
extern crate ekvsb;
extern crate indicatif;
extern crate rand;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
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
use rand::{Rng, SeedableRng};
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
                        .arg(Arg::with_name("DIR").index(1).required(true)),
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
        let mut options = kvs::CannyLsOptions {
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
        let kvs = track!(kvs::RocksDb::new(dir))?;
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
            *b = *rng.choose(&CHARS[..]).expect("never fails");
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
        shuffle_rng.shuffle(&mut tasks);
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

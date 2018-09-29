extern crate byte_unit;
#[macro_use]
extern crate clap;
extern crate ekvsb;
extern crate rand;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
#[macro_use]
extern crate trackable;

use byte_unit::Byte;
use clap::{Arg, ArgMatches, SubCommand};
use ekvsb::task::{Key, Seconds, Task, TaskResult, ValueSpec};
use ekvsb::workload::{Workload, WorkloadExecutor};
use ekvsb::{KeyValueStore, Result};
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
                ).subcommand(
                    SubCommand::with_name("builtin::fs")
                        .arg(Arg::with_name("DIR").index(1).required(true)),
                ).subcommand(SubCommand::with_name("builtin::hashmap"))
                .subcommand(SubCommand::with_name("builtin::btreemap")),
        ).subcommand(
            SubCommand::with_name("workload")
                .subcommand(
                    SubCommand::with_name("PUT")
                        .arg(
                            Arg::with_name("COUNT")
                                .long("count")
                                .takes_value(true)
                                .default_value("1000"),
                        ).arg(
                            Arg::with_name("KEY_SIZE")
                                .long("key-size")
                                .takes_value(true)
                                .default_value("10"),
                        ).arg(
                            Arg::with_name("VALUE_SIZE")
                                .long("value-size")
                                .takes_value(true)
                                .default_value("1KiB"),
                        ).arg(Arg::with_name("SEED").long("seed").takes_value(true))
                        .arg(
                            Arg::with_name("PRIORITY_GROUP_SIZE")
                                .long("priority-group-size")
                                .takes_value(true)
                                .default_value("1"),
                        ),
                ).subcommand(
                    SubCommand::with_name("GET")
                        .arg(
                            Arg::with_name("COUNT")
                                .long("count")
                                .takes_value(true)
                                .default_value("1000"),
                        ).arg(
                            Arg::with_name("KEY_SIZE")
                                .long("key-size")
                                .takes_value(true)
                                .default_value("10"),
                        ).arg(Arg::with_name("SEED").long("seed").takes_value(true))
                        .arg(
                            Arg::with_name("PRIORITY_GROUP_SIZE")
                                .long("priority-group-size")
                                .takes_value(true)
                                .default_value("1"),
                        ),
                ).subcommand(
                    SubCommand::with_name("DELETE")
                        .arg(
                            Arg::with_name("COUNT")
                                .long("count")
                                .takes_value(true)
                                .default_value("1000"),
                        ).arg(
                            Arg::with_name("KEY_SIZE")
                                .long("key-size")
                                .takes_value(true)
                                .default_value("10"),
                        ).arg(Arg::with_name("SEED").long("seed").takes_value(true))
                        .arg(
                            Arg::with_name("PRIORITY_GROUP_SIZE")
                                .long("priority-group-size")
                                .takes_value(true)
                                .default_value("1"),
                        ),
                ),
        ).subcommand(SubCommand::with_name("summary"))
        .get_matches();
    if let Some(matches) = matches.subcommand_matches("run") {
        track!(handle_run_subcommand(matches))?;
    } else if let Some(matches) = matches.subcommand_matches("workload") {
        track!(handle_workload_subcommand(matches))?;
    } else if let Some(matches) = matches.subcommand_matches("summary") {
        track!(handle_summary_subcommand(matches))?;
    } else {
        unreachable!();
    }
    Ok(())
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
        let kvs = track!(ekvsb::fs::FileSystemKvs::new(dir))?;
        track!(execute(kvs, workload))?;
    } else if let Some(_matches) = matches.subcommand_matches("builtin::hashmap") {
        let kvs = HashMap::new();
        track!(execute(kvs, workload))?;
    } else if let Some(_matches) = matches.subcommand_matches("builtin::btreemap") {
        let kvs = BTreeMap::new();
        track!(execute(kvs, workload))?;
    } else {
        unreachable!();
    }
    Ok(())
}

fn execute<T: KeyValueStore>(kvs: T, workload: Workload) -> Result<()> {
    let executor = WorkloadExecutor::new(kvs, workload);

    println!("[");
    for (i, result) in executor.enumerate() {
        if i != 0 {
            print!(",\n  ");
        } else {
            print!("  ");
        }
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
        track!(generate_tasks(matches, |key, priority| Task::Put {
            key,
            value: ValueSpec::Random { size: value_size },
            priority,
        }))?
    } else if let Some(matches) = matches.subcommand_matches("GET") {
        track!(generate_tasks(matches, |key, priority| Task::Get {
            key,
            priority
        }))?
    } else if let Some(matches) = matches.subcommand_matches("DELETE") {
        track!(generate_tasks(matches, |key, priority| Task::Delete {
            key,
            priority
        }))?
    } else {
        unreachable!();
    };
    track_any_err!(serde_json::to_writer(stdout(), &tasks))?;
    Ok(())
}

fn generate_tasks<F>(matches: &ArgMatches, f: F) -> Result<Vec<Task>>
where
    F: Fn(Key, usize) -> Task,
{
    let count: usize = track_any_err!(matches.value_of("COUNT").expect("never fails").parse())?;
    let key_size = track!(parse_size(
        matches.value_of("KEY_SIZE").expect("never fails")
    ))?;
    let seed = matches.value_of("SEED");
    let priority_group_size: usize = track_any_err!(
        matches
            .value_of("PRIORITY_GROUP_SIZE")
            .expect("never fails")
            .parse()
    )?;

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
    for i in 0..count {
        for b in &mut key {
            *b = *rng.choose(&CHARS[..]).expect("never fails");
        }
        let priority = i / priority_group_size;
        tasks.push(f(track!(Key::from_utf8(key.clone()))?, priority));
    }
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
    let latency = Latency::new(&results);
    let summary = Summary {
        oks,
        errors,
        elapsed,
        ops,
        latency,
    };
    track_any_err!(serde_json::to_writer_pretty(stdout(), &summary))?;

    Ok(())
}

#[derive(Serialize)]
struct Summary {
    oks: usize,
    errors: usize,
    elapsed: f64,
    ops: f64,
    latency: Latency,
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

fn parse_size(s: &str) -> Result<usize> {
    let size = Byte::from_string(s)
        .map_err(|e| track!(Failed.cause(format!("Parse Error: {:?} ({:?})", s, e))))?;
    Ok(size.get_bytes() as usize)
}

fn stdin() -> impl Read {
    BufReader::new(std::io::stdin())
}

fn stdout() -> impl Write {
    BufWriter::new(std::io::stdout())
}

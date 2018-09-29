extern crate byte_unit;
#[macro_use]
extern crate clap;
extern crate ekvsb;
#[macro_use]
extern crate trackable;
extern crate serde;
extern crate serde_json;

use byte_unit::Byte;
use clap::{Arg, ArgMatches, SubCommand};
use ekvsb::workload::{Workload, WorkloadExecutor};
use ekvsb::{KeyValueStore, Result};
use std::collections::{BTreeMap, HashMap};
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
                .subcommand(SubCommand::with_name("PUT"))
                .subcommand(SubCommand::with_name("GET"))
                .subcommand(SubCommand::with_name("DELETE")),
        ).get_matches();
    if let Some(matches) = matches.subcommand_matches("run") {
        track!(handle_run_subcommand(matches))?;
    } else if let Some(matches) = matches.subcommand_matches("workload") {
        track!(handle_workload_subcommand(matches))?;
    } else {
        unreachable!();
    }
    Ok(())
}

fn handle_run_subcommand(matches: &ArgMatches) -> Result<()> {
    let memory_load_size = matches.value_of("MEMORY_LOAD_SIZE").expect("never fails");
    let memory_load_size = Byte::from_string(&memory_load_size).map_err(|e| {
        track!(Failed.cause(format!("Parse Error: {:?} ({:?})", memory_load_size, e)))
    })?;
    let _reserved_memory: Vec<u8> = vec![1; memory_load_size.get_bytes() as usize];

    let workload: Workload = track_any_err!(
        serde_json::from_reader(std::io::stdin()),
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

fn handle_workload_subcommand(matches: &ArgMatches) -> Result<()> {
    Ok(())
}

fn execute<T: KeyValueStore>(kvs: T, workload: Workload) -> Result<()> {
    let executor = WorkloadExecutor::new(kvs, workload);

    println!("{{");
    for (i, result) in executor.enumerate() {
        if i != 0 {
            print!(",\n  ");
        } else {
            print!("  ");
        }
        track_any_err!(serde_json::to_writer(std::io::stdout(), &result))?;
    }
    println!("\n}}");
    Ok(())
}

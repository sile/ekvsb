#[macro_use]
extern crate clap;
extern crate ekvsb;
#[macro_use]
extern crate trackable;
extern crate serde;
extern crate serde_json;

use clap::{Arg, SubCommand};
use ekvsb::workload::{Workload, WorkloadExecutor};
use ekvsb::{KeyValueStore, Result};
use std::collections::{BTreeMap, HashMap};

fn main() -> trackable::result::MainResult {
    let matches = app_from_crate!()
        .subcommand(
            SubCommand::with_name("run")
                .subcommand(
                    SubCommand::with_name("builtin::fs")
                        .arg(Arg::with_name("DIR").index(1).required(true)),
                ).subcommand(SubCommand::with_name("builtin::hashmap"))
                .subcommand(SubCommand::with_name("builtin::btreemap")),
        ).get_matches();
    if let Some(matches) = matches.subcommand_matches("run") {
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
    } else {
        unreachable!();
    }
    Ok(())
}

fn execute<T: KeyValueStore>(kvs: T, workload: Workload) -> Result<()> {
    let executor = WorkloadExecutor::new(kvs, workload);

    println!("{{");
    for (i, result) in executor.enumerate() {
        if i != 0 {
            println!(",");
        }
        track_any_err!(serde_json::to_writer(std::io::stdout(), &result))?;
    }
    println!("\n}}");
    Ok(())
}

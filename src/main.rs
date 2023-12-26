#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate trackable;

use byte_unit::Byte;
use clap::Parser;
use ekvsb::kvs::{self, KeyValueStore};
use ekvsb::task::{Key, Seconds, Task, TaskResult, ValueSpec};
use ekvsb::workload::{Workload, WorkloadExecutor};
use ekvsb::Result;
use indicatif::ProgressBar;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use rocksdb::{self, Cache};
use std::collections::{BTreeMap, HashMap};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::PathBuf;
use trackable::error::{ErrorKindExt, Failed};

#[derive(Debug, Parser)]
struct Opt {
    #[clap(long, default_value = "0GiB", value_parser = parse_size)]
    memory_load: usize,

    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, clap::Subcommand)]
#[allow(clippy::large_enum_variant)]
enum Command {
    #[clap(about = "Executes a benchmark", subcommand)]
    Run(RunCommand),

    #[clap(about = "Generates a benchmark workload", subcommand)]
    Workload(WorkloadCommand),

    #[clap(about = "Shows summary of a benchmark result")]
    Summary,

    #[clap(about = "Plots a benchmark result", subcommand)]
    Plot(PlotCommand),
}

#[derive(Debug, clap::Subcommand)]
#[allow(clippy::large_enum_variant)]
enum RunCommand {
    #[clap(name = "builtin::fs", about = "FileSystem")]
    Fs { dir: PathBuf },

    #[clap(name = "builtin::hashmap", about = "HashMap")]
    HashMap,

    #[clap(name = "builtin::btreemap", about = "BTreeMap")]
    BTreeMap,

    #[clap(name = "cannyls", about = "CannyLS")]
    CannyLs {
        file: PathBuf,

        #[clap(long, default_value = "1GiB", value_parser = parse_size_u64)]
        capacity: u64,

        #[clap(long, default_value = "4096")]
        journal_sync_interval: usize,

        #[clap(long)]
        without_device: bool,
    },

    #[clap(name = "rocksdb", about = "RocksDB")]
    RocksDb(RocksDbOpt),

    #[clap(name = "sled", about = "Sled")]
    Sled { dir: PathBuf },
}

#[derive(Debug, clap::Args)]
struct RocksDbOpt {
    dir: PathBuf,

    #[clap(long)]
    force_default: bool,

    #[clap(long)]
    parallelism: Option<i32>,

    #[clap(long)]
    optimize_level_style_compaction: Option<usize>,

    #[clap(long)]
    compaction_readahead_size: Option<usize>,

    #[clap(long)]
    optimize_for_point_lookup: Option<u64>,

    #[clap(long)]
    use_fsync: bool,

    #[clap(long)]
    bytes_per_sync: Option<u64>,

    #[clap(long)]
    disable_concurrent_memtable_write: bool,

    #[clap(long)]
    use_direct_reads: bool,

    #[clap(long)]
    use_direct_io_for_flush_and_compaction: bool,

    #[clap(long)]
    table_cache_num_shard_bits: Option<i32>,

    #[clap(long)]
    min_write_buffer_number: Option<i32>,

    #[clap(long)]
    max_write_buffer_number: Option<i32>,

    #[clap(long)]
    write_buffer_size: Option<usize>,

    #[clap(long)]
    max_bytes_for_level_base: Option<u64>,

    #[clap(long)]
    max_bytes_for_level_multiplier: Option<f64>,

    #[clap(long)]
    max_manifest_file_size: Option<usize>,

    #[clap(long)]
    target_file_size_base: Option<u64>,

    #[clap(long)]
    min_write_buffer_number_to_merge: Option<i32>,

    #[clap(long)]
    level_zero_file_num_compaction_trigger: Option<i32>,

    #[clap(long)]
    level_zero_slowdown_writes_trigger: Option<i32>,

    #[clap(long)]
    level_zero_stop_writes_trigger: Option<i32>,

    #[clap(long)]
    compaction_style: Option<CompactionStyle>,

    #[clap(long)]
    disable_auto_compactions: bool,

    #[clap(long)]
    disable_advise_random_on_open: bool,

    #[clap(long)]
    num_levels: Option<i32>,

    #[clap(long)]
    memtable_prefix_bloom_ratio: Option<f64>,

    #[clap(long)]
    memtable_factory_vector: bool,

    #[clap(long)]
    memtable_factory_hashskiplist_bucket_count: Option<usize>,

    #[clap(long)]
    memtable_factory_hashskiplist_height: Option<i32>,

    #[clap(long)]
    memtable_factory_hashskiplist_branching_factor: Option<i32>,

    #[clap(long)]
    memtable_factory_hashlinklist_bucket_count: Option<usize>,

    // https://github.com/facebook/rocksdb/blob/2670fe8c73c66db6dad64bdf875e3342494e8ef2/include/rocksdb/table.h
    #[clap(long)]
    block_opt_block_size: Option<usize>,

    #[clap(long)]
    block_opt_lru_cache: Option<usize>,

    #[clap(long)]
    block_opt_disable_cache: bool,

    #[clap(long)]
    block_opt_bloom_filter_bits_per_key: Option<f64>,

    #[clap(long)]
    block_opt_bloom_filter_block_based: bool,

    #[clap(long)]
    block_opt_cache_index_and_filter_blocks: bool,

    #[clap(long)]
    block_opt_index_type: Option<BlockBasedIndexType>,
}

#[derive(Debug, clap::Subcommand)]
enum WorkloadCommand {
    #[clap(about = "PUT workload")]
    Put {
        #[clap(long, default_value = "1000")]
        count: usize,

        #[clap(long)]
        population_size: Option<usize>,

        #[clap(long, default_value = "10")]
        key_size: usize,

        #[clap(long, default_value = "1KiB", value_parser = parse_size)]
        value_size: usize,

        #[clap(long)]
        seed: Option<String>,

        #[clap(long)]
        shuffle: Option<String>,
    },

    #[clap(about = "GET workload")]
    Get {
        #[clap(long, default_value = "1000")]
        count: usize,

        #[clap(long)]
        population_size: Option<usize>,

        #[clap(long, default_value = "10")]
        key_size: usize,

        #[clap(long)]
        seed: Option<String>,

        #[clap(long)]
        shuffle: Option<String>,
    },

    #[clap(about = "DELETE workload")]
    Delete {
        #[clap(long, default_value = "1000")]
        count: usize,

        #[clap(long)]
        population_size: Option<usize>,

        #[clap(long, default_value = "10")]
        key_size: usize,

        #[clap(long)]
        seed: Option<String>,

        #[clap(long)]
        shuffle: Option<String>,
    },
}
impl WorkloadCommand {
    fn count(&self) -> usize {
        match self {
            WorkloadCommand::Put { count, .. }
            | WorkloadCommand::Get { count, .. }
            | WorkloadCommand::Delete { count, .. } => *count,
        }
    }

    fn population_size(&self) -> Option<usize> {
        match self {
            WorkloadCommand::Put {
                population_size, ..
            }
            | WorkloadCommand::Get {
                population_size, ..
            }
            | WorkloadCommand::Delete {
                population_size, ..
            } => *population_size,
        }
    }

    fn key_size(&self) -> usize {
        match self {
            WorkloadCommand::Put { key_size, .. }
            | WorkloadCommand::Get { key_size, .. }
            | WorkloadCommand::Delete { key_size, .. } => *key_size,
        }
    }

    fn seed(&self) -> Option<&str> {
        match self {
            WorkloadCommand::Put { seed, .. }
            | WorkloadCommand::Get { seed, .. }
            | WorkloadCommand::Delete { seed, .. } => seed.as_ref().map(String::as_str),
        }
    }

    fn shuffle(&self) -> Option<&str> {
        match self {
            WorkloadCommand::Put { shuffle, .. }
            | WorkloadCommand::Get { shuffle, .. }
            | WorkloadCommand::Delete { shuffle, .. } => shuffle.as_ref().map(String::as_str),
        }
    }
}

#[derive(Debug, clap::Subcommand)]
enum PlotCommand {
    #[clap(name = "text", about = "TEXT")]
    Text {
        #[clap(long)]
        title: Option<String>,

        #[clap(long, default_value = "1.0")]
        sampling_rate: f64,

        #[clap(long)]
        y_max: Option<f64>,

        #[clap(long)]
        logscale: bool,
    },

    #[clap(name = "png", about = "PNG")]
    Png {
        output_file: PathBuf,

        #[clap(long)]
        title: Option<String>,

        #[clap(long, default_value = "1.0")]
        sampling_rate: f64,

        #[clap(long)]
        y_max: Option<f64>,

        #[clap(long)]
        logscale: bool,

        #[clap(long, default_value = "1200")]
        width: usize,

        #[clap(long, default_value = "800")]
        height: usize,
    },
}
impl PlotCommand {
    fn title(&self) -> Option<&str> {
        match self {
            PlotCommand::Text { title, .. } | PlotCommand::Png { title, .. } => {
                title.as_ref().map(String::as_str)
            }
        }
    }

    fn sampling_rate(&self) -> f64 {
        match self {
            PlotCommand::Text { sampling_rate, .. } | PlotCommand::Png { sampling_rate, .. } => {
                *sampling_rate
            }
        }
    }

    fn y_max(&self) -> Option<f64> {
        match self {
            PlotCommand::Text { y_max, .. } | PlotCommand::Png { y_max, .. } => *y_max,
        }
    }

    fn logscale(&self) -> bool {
        match self {
            PlotCommand::Text { logscale, .. } | PlotCommand::Png { logscale, .. } => *logscale,
        }
    }
}

#[derive(Debug, Clone, clap::ValueEnum)]
enum CompactionStyle {
    Level,
    Universal,
    Fifo,
}

#[derive(Debug, Clone, clap::ValueEnum)]
#[allow(clippy::enum_variant_names)]
enum BlockBasedIndexType {
    BinarySearch,
    HashSearch,
    TwoLevelIndexSearch,
}

fn main() -> trackable::result::MainResult {
    let opt = Opt::parse();

    match opt.command {
        Command::Run(ref command) => {
            track!(handle_run_subcommand(&opt, command))?;
        }
        Command::Workload(ref command) => {
            track!(handle_workload_subcommand(command))?;
        }
        Command::Summary => {
            track!(handle_summary_subcommand())?;
        }
        Command::Plot(ref command) => {
            track!(handle_plot_subcommand(command))?;
        }
    }
    Ok(())
}

fn handle_run_subcommand(opt: &Opt, command: &RunCommand) -> Result<()> {
    let _reserved_memory: Vec<u8> = vec![1; opt.memory_load];

    let workload: Workload = track_any_err!(
        serde_json::from_reader(stdin()),
        "Malformed input workload JSON"
    )?;

    match command {
        RunCommand::Fs { dir } => {
            let kvs = track!(kvs::FileSystemKvs::new(dir))?;
            track!(execute(kvs, workload))?;
        }
        RunCommand::HashMap => {
            let kvs = HashMap::new();
            track!(execute(kvs, workload))?;
        }
        RunCommand::BTreeMap => {
            let kvs = BTreeMap::new();
            track!(execute(kvs, workload))?;
        }
        RunCommand::CannyLs {
            file,
            capacity,
            journal_sync_interval,
            without_device,
        } => {
            let options = kvs::CannyLsOptions {
                capacity: *capacity,
                journal_sync_interval: *journal_sync_interval,
            };
            if *without_device {
                let kvs = track!(kvs::CannyLsStorage::new(file, &options))?;
                track!(execute(kvs, workload))?;
            } else {
                let kvs = track!(kvs::CannyLsDevice::new(file, &options))?;
                track!(execute(kvs, workload))?;
            }
        }
        RunCommand::RocksDb(opt) => {
            let options = track!(make_rocksdb_options(opt))?;
            let kvs = track!(kvs::RocksDb::with_options(&opt.dir, options))?;
            track!(execute(kvs, workload))?;
        }
        RunCommand::Sled { dir } => {
            let kvs = track!(kvs::SledTree::new(dir))?;
            track!(execute(kvs, workload))?;
        }
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

fn handle_workload_subcommand(command: &WorkloadCommand) -> Result<()> {
    let tasks = match command {
        WorkloadCommand::Put { value_size, .. } => {
            track!(generate_tasks(command, |key| Task::Put {
                key,
                value: ValueSpec::Random { size: *value_size },
            }))?
        }
        WorkloadCommand::Get { .. } => track!(generate_tasks(command, |key| Task::Get { key }))?,
        WorkloadCommand::Delete { .. } => {
            track!(generate_tasks(command, |key| Task::Delete { key }))?
        }
    };
    track_any_err!(serde_json::to_writer(stdout(), &tasks))?;
    Ok(())
}

fn generate_tasks<F>(command: &WorkloadCommand, f: F) -> Result<Vec<Task>>
where
    F: Fn(Key) -> Task,
{
    let count = command.count();
    let key_size = command.key_size();
    let seed = command.seed();
    let population_size = command.population_size().unwrap_or(count);
    track_assert!(count <= population_size, Failed; count, population_size);

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

    if let Some(seed) = command.shuffle() {
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

fn handle_summary_subcommand() -> Result<()> {
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

fn handle_plot_subcommand(command: &PlotCommand) -> Result<()> {
    let mut options = ekvsb::plot::PlotOptions::new();

    match command {
        PlotCommand::Text { .. } => {
            options.terminal = "dumb".to_owned();
        }
        PlotCommand::Png {
            output_file,
            width,
            height,
            ..
        } => {
            options.terminal = format!("pngcairo size {}, {}", width, height);
            options.output_file = track_assert_some!(output_file.to_str(), Failed).to_string();
        }
    }
    options.sampling_rate = command.sampling_rate();
    options.logscale = command.logscale();
    if let Some(title) = command.title() {
        options.title = title.to_string();
    }
    if let Some(y_max) = command.y_max() {
        options.y_max = Some(y_max);
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
fn make_rocksdb_options(opt: &RocksDbOpt) -> Result<rocksdb::Options> {
    let mut options = rocksdb::Options::default();
    if opt.force_default {
        return Ok(options);
    }

    if opt.disable_advise_random_on_open {
        options.set_advise_random_on_open(false);
    }
    if opt.disable_concurrent_memtable_write
        || opt.memtable_factory_vector
        || opt.memtable_factory_hashlinklist_bucket_count.is_some()
        || opt.memtable_factory_hashskiplist_bucket_count.is_some()
    {
        options.set_allow_concurrent_memtable_write(false);
    }
    if opt.disable_auto_compactions {
        options.set_disable_auto_compactions(true);
    }
    if opt.use_direct_io_for_flush_and_compaction {
        options.set_use_direct_io_for_flush_and_compaction(true);
    }
    if opt.use_direct_reads {
        options.set_use_direct_reads(true);
    }
    if opt.use_fsync {
        options.set_use_fsync(true);
    }
    if let Some(v) = opt.bytes_per_sync {
        options.set_bytes_per_sync(v);
    }
    if let Some(v) = opt.compaction_readahead_size {
        options.set_compaction_readahead_size(v);
    }
    if let Some(ref v) = opt.compaction_style {
        let style = match v {
            CompactionStyle::Level => rocksdb::DBCompactionStyle::Level,
            CompactionStyle::Universal => rocksdb::DBCompactionStyle::Universal,
            CompactionStyle::Fifo => rocksdb::DBCompactionStyle::Fifo,
        };
        options.set_compaction_style(style);
    }
    if let Some(v) = opt.parallelism {
        options.increase_parallelism(v);
    }
    if let Some(v) = opt.level_zero_file_num_compaction_trigger {
        options.set_level_zero_file_num_compaction_trigger(v);
    }
    if let Some(v) = opt.level_zero_slowdown_writes_trigger {
        options.set_level_zero_slowdown_writes_trigger(v);
    }
    if let Some(v) = opt.level_zero_stop_writes_trigger {
        options.set_level_zero_stop_writes_trigger(v);
    }
    if let Some(v) = opt.max_bytes_for_level_base {
        options.set_max_bytes_for_level_base(v);
    }
    if let Some(v) = opt.max_bytes_for_level_multiplier {
        options.set_max_bytes_for_level_multiplier(v);
    }
    if let Some(v) = opt.max_manifest_file_size {
        options.set_max_manifest_file_size(v);
    }
    if let Some(v) = opt.max_write_buffer_number {
        options.set_max_write_buffer_number(v);
    }
    if let Some(v) = opt.memtable_prefix_bloom_ratio {
        options.set_memtable_prefix_bloom_ratio(v);
    }
    if let Some(v) = opt.min_write_buffer_number {
        options.set_min_write_buffer_number(v);
    }
    if let Some(v) = opt.min_write_buffer_number_to_merge {
        options.set_min_write_buffer_number_to_merge(v);
    }
    if let Some(v) = opt.num_levels {
        options.set_num_levels(v);
    }
    if let Some(v) = opt.optimize_for_point_lookup {
        options.optimize_for_point_lookup(v);
    }
    if let Some(v) = opt.optimize_level_style_compaction {
        options.optimize_level_style_compaction(v);
    }
    if let Some(v) = opt.table_cache_num_shard_bits {
        options.set_table_cache_num_shard_bits(v);
    }
    if let Some(v) = opt.target_file_size_base {
        options.set_target_file_size_base(v);
    }
    if let Some(v) = opt.write_buffer_size {
        options.set_write_buffer_size(v);
    }
    if opt.memtable_factory_vector {
        options.set_memtable_factory(rocksdb::MemtableFactory::Vector);
    } else if let Some(bucket_count) = opt.memtable_factory_hashskiplist_bucket_count {
        let height = track_assert_some!(opt.memtable_factory_hashskiplist_height, Failed);
        let branching_factor =
            track_assert_some!(opt.memtable_factory_hashskiplist_branching_factor, Failed);
        options.set_memtable_factory(rocksdb::MemtableFactory::HashSkipList {
            bucket_count,
            height,
            branching_factor,
        });
    } else if let Some(bucket_count) = opt.memtable_factory_hashlinklist_bucket_count {
        options.set_memtable_factory(rocksdb::MemtableFactory::HashLinkList { bucket_count });
    }
    let mut block_opts = rocksdb::BlockBasedOptions::default();
    if let Some(v) = opt.block_opt_block_size {
        block_opts.set_block_size(v);
    }
    if let Some(v) = opt.block_opt_lru_cache {
        block_opts.set_block_cache(&track_any_err!(Cache::new_lru_cache(v))?);
    }
    if opt.block_opt_disable_cache {
        block_opts.disable_cache();
    }
    if let Some(bits_per_key) = opt.block_opt_bloom_filter_bits_per_key {
        block_opts.set_bloom_filter(bits_per_key, opt.block_opt_bloom_filter_block_based);
    }
    if opt.block_opt_cache_index_and_filter_blocks {
        block_opts.set_cache_index_and_filter_blocks(true);
    }
    if let Some(ref index_type) = opt.block_opt_index_type {
        let t = match index_type {
            BlockBasedIndexType::BinarySearch => rocksdb::BlockBasedIndexType::BinarySearch,
            BlockBasedIndexType::HashSearch => rocksdb::BlockBasedIndexType::HashSearch,
            BlockBasedIndexType::TwoLevelIndexSearch => {
                rocksdb::BlockBasedIndexType::TwoLevelIndexSearch
            }
        };
        block_opts.set_index_type(t);
    }
    options.set_block_based_table_factory(&block_opts);
    Ok(options)
}

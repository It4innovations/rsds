use std::path::PathBuf;
use std::fs;

use rand::distributions::Alphanumeric;
use rand::Rng;
use structopt::StructOpt;

use rsds::setup_logging;
use rsds::worker::rpc::run_worker;
use rsds::worker::subworker::SubworkerPaths;

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[derive(Debug, StructOpt)]
#[structopt(name = "rsds-worker", about = "Rust Dask Worker")]
struct Opt {
    scheduler_address: String,

    #[structopt(long)]
    trace_file: Option<String>,

    #[structopt(long)]
    work_dir: Option<PathBuf>,

    #[structopt(long)]
    ncpus: Option<u32>,

    #[structopt(long)]
    local_directory: Option<PathBuf>,

    #[structopt(long)]
    preload: Option<PathBuf>,

    #[structopt(long)]
    no_dashboard: bool,
}

fn create_local_directory(prefix: PathBuf) -> Result<PathBuf, std::io::Error> {
    let mut work_dir = prefix;
    let rnd_string: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(5)
        .collect();
    work_dir.push(format!("rsds-{}", rnd_string));
    fs::create_dir_all(&work_dir)?;
    Ok(work_dir)
}

fn create_paths(workdir: PathBuf, local_directory: PathBuf) -> Result<SubworkerPaths, std::io::Error> {
    fs::create_dir_all(&workdir)?;
    let work_dir = fs::canonicalize(workdir)?;
    let local_dir = create_local_directory(local_directory)?;

    Ok(SubworkerPaths::new(work_dir, local_dir))
}

#[tokio::main(basic_scheduler)]
async fn main() -> rsds::Result<()> {
    let mut opt = Opt::from_args();
    setup_logging(opt.trace_file.take());

    log::info!("rsds worker v0.2 started: {:?}", opt);

    let paths = create_paths(
        opt.work_dir.unwrap_or(PathBuf::from("rsds-worker-space")),
        opt.local_directory.unwrap_or(std::env::temp_dir())
    )?;

    log::info!("subworker paths: {:?}", paths);

    let ncpus = opt.ncpus.unwrap_or(1);
    if ncpus < 1 {
        panic!("Invalid number of cpus");
    }
    let scheduler_address = opt.scheduler_address.trim_start_matches("tcp://");

    run_worker(scheduler_address, ncpus, paths).await?;
    log::info!("rsds worker ends");
    Ok(())
}

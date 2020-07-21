use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;
use std::{env, fs};

use rand::distributions::Alphanumeric;
use rand::Rng;
use structopt::StructOpt;
use tokio::net::lookup_host;
use tokio::net::TcpStream;

use rsds::setup_logging;
use rsds::trace::setup_file_trace;
use rsds::worker::rpc::run_worker;
use rsds::worker::subworker::start_subworkers;

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[derive(Debug, StructOpt)]
#[structopt(name = "rsds-worker", about = "Rust Dask Worker")]
struct Opt {
    scheduler_address: String,

    #[structopt(long)]
    trace_file: Option<String>,

    #[structopt(long)]
    work_dir: Option<String>,

    #[structopt(long)]
    ncpus: Option<u32>,

    #[structopt(long)]
    local_directory: Option<String>,

    #[structopt(long)]
    preload: Option<String>,

    #[structopt(long)]
    no_dashboard: bool,
}

fn create_work_directory() -> Result<PathBuf, std::io::Error> {
    let mut work_dir = env::temp_dir();
    let rnd_string: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(5)
        .collect();
    work_dir.push(format!("rsds-{}", rnd_string));
    fs::create_dir(&work_dir)?;
    Ok(work_dir)
}

#[tokio::main(basic_scheduler)]
async fn main() -> rsds::Result<()> {
    let mut opt = Opt::from_args();

    log::info!("rsds worker v0.2 started: {:?}", opt);

    let work_dir = if let Some(wd) = opt.work_dir {
        PathBuf::from_str(&wd).unwrap()
    } else {
        create_work_directory()?
    };

    log::info!("working directory: {}", work_dir.display());

    setup_logging(opt.trace_file.take());

    let ncpus = opt.ncpus.unwrap_or(1);
    if ncpus < 1 {
        panic!("Invalid number of cpus");
    }
    let (subworkers, sw_processes) = start_subworkers(&work_dir, "python3", ncpus).await?;
    let scheduler_address = opt.scheduler_address.trim_start_matches("tcp://");

    run_worker(scheduler_address, ncpus, subworkers, sw_processes).await?;
    log::info!("rsds worker ends");
    Ok(())
}

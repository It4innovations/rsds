use rsds::trace::setup_file_trace;
use rsds::worker::rpc::run_worker;
use rsds::worker::subworker::start_subworkers;
use std::time::Duration;
use structopt::StructOpt;
use tokio::net::lookup_host;
use tokio::net::TcpStream;
use tokio::time::delay_for;
use std::path::PathBuf;
use std::{env, fs};
use rand::Rng;
use rand::distributions::Alphanumeric;


#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[derive(Debug, StructOpt)]
#[structopt(name = "rsds-worker", about = "Rust Dask Worker")]
struct Opt {
    scheduler_address: String,
    #[structopt(long)]
    trace_file: Option<String>,

    #[structopt(long, default_value="1")]
    subworkers: u32,

    #[structopt(long)]
    local_directory: Option<String>,

    #[structopt(long)]
    preload: Option<String>,

    #[structopt(long)]
    no_dashboard: bool,
}

fn setup_logging(trace_file: Option<String>) {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }
    env_logger::builder().format_timestamp_millis().init();

    if let Some(trace_file) = trace_file {
        setup_file_trace(trace_file);
    }
}

fn create_work_directory() -> Result<PathBuf, std::io::Error>
{
    let mut work_dir = env::temp_dir();
    let rnd_string: String = rand::thread_rng().sample_iter(&Alphanumeric).take(5).collect();
    work_dir.push(format!("rsds-{}", rnd_string));
    fs::create_dir(&work_dir)?;
    Ok(work_dir)
}


#[tokio::main(basic_scheduler)]
async fn main() -> rsds::Result<()> {
    let mut opt = Opt::from_args();

    log::info!("rsds worker v0.1 started: {:?}", opt);

    let work_dir = create_work_directory()?;
    log::info!("working directory: {}", work_dir.display());

    setup_logging(opt.trace_file.take());

    if opt.subworkers < 1 {
        panic!("Invalid number of subworkers");
    }

    let address_str = opt.scheduler_address.trim_start_matches("tcp://");
    let address = lookup_host(&address_str)
        .await?
        .next()
        .expect("Invalid scheduler address");

    let subworkers = start_subworkers(&work_dir, "python3", opt.subworkers).await?;

    /*
    task_set.spawn_local(async move {
        delay_for(Duration::from_secs(5)).await;
        /*
        for _ in 0..10 {
            match TcpStream::connect(address).await {
                Ok(stream) => {
                    log::info!("Connected to {}", address_str);
                    stream.set_nodelay(true).unwrap();
                    run_worker(stream).await.unwrap();
                    break;
                }
                Err(e) => {
                    log::error!("Couldn't connect to {}, error: {}", address_str, e);
                    delay_for(Duration::from_secs(5)).await;
                }
            }
        }*/
    }).await;*/

    log::info!("rsds worker ends");

    Ok(())
}

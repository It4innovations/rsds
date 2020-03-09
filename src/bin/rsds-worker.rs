use rsds::trace::setup_file_trace;
use rsds::worker::rpc::run_worker;
use std::time::Duration;
use structopt::StructOpt;
use tokio::net::lookup_host;
use tokio::net::TcpStream;
use tokio::time::delay_for;

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[derive(Debug, StructOpt)]
#[structopt(name = "rsds-worker", about = "Rust Dask Worker")]
struct Opt {
    scheduler_address: String,
    #[structopt(long)]
    trace_file: Option<String>,

    #[structopt(long)]
    nthreads: Option<u32>,

    #[structopt(long)]
    nprocs: Option<u32>,

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

#[tokio::main(basic_scheduler)]
async fn main() -> rsds::Result<()> {
    let mut opt = Opt::from_args();

    log::info!("rsds worker v0.1 started: {:?}", opt);

    assert_eq!(opt.nthreads, Some(1));

    setup_logging(opt.trace_file.take());

    let address_str = opt.scheduler_address.trim_start_matches("tcp://");
    let address = lookup_host(&address_str)
        .await?
        .next()
        .expect("Invalid scheduler address");

    for _ in 0..10 {
        match TcpStream::connect(address).await {
            Ok(stream) => {
                log::info!("Connected to {}", address_str);
                run_worker(stream).await?;
                break;
            }
            Err(e) => {
                log::error!(
                    "Couldn't connect to {}, error: {}",
                    address_str,
                    e
                );
                delay_for(Duration::from_secs(5)).await;
            }
        }
    }

    log::info!("rsds worker ends");

    Ok(())
}

use std::net::{Ipv4Addr, SocketAddr};
use std::thread;

use futures::{FutureExt, StreamExt};
use structopt::StructOpt;
use tokio::net::TcpListener;

use rsds::server::comm::CommRef;
use rsds::scheduler::{
    drive_scheduler, prepare_scheduler_comm, BLevelMetric, SchedulerComm,
    TLevelMetric,
};
use rsds::server::comm::observe_scheduler;
use rsds::server::core::CoreRef;
use rsds::setup_interrupt;
use rsds::trace::setup_file_trace;
use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;
use std::time::Duration;

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

fn create_scheduler(
    r#type: SchedulerType,
    msd: Duration,
    comm: SchedulerComm,
) -> Pin<Box<dyn Future<Output = rsds::Result<()>>>> {
    match r#type {
        SchedulerType::Workstealing => Box::pin(drive_scheduler(
            rsds::scheduler::WorkstealingScheduler::default(),
            comm,
            msd,
        )),
        SchedulerType::Random => Box::pin(drive_scheduler(
            rsds::scheduler::RandomScheduler::default(),
            comm,
            msd,
        )),
        SchedulerType::Blevel => Box::pin(drive_scheduler(
            rsds::scheduler::LevelScheduler::<BLevelMetric>::default(),
            comm,
            msd,
        )),
        SchedulerType::Tlevel => Box::pin(drive_scheduler(
            rsds::scheduler::LevelScheduler::<TLevelMetric>::default(),
            comm,
            msd,
        )),
    }
}

#[derive(Debug)]
enum SchedulerType {
    Workstealing,
    Random,
    Blevel,
    Tlevel,
}

impl FromStr for SchedulerType {
    type Err = String;
    fn from_str(scheduler: &str) -> Result<Self, Self::Err> {
        match scheduler {
            "workstealing" => Ok(SchedulerType::Workstealing),
            "random" => Ok(SchedulerType::Random),
            "blevel" => Ok(SchedulerType::Blevel),
            "tlevel" => Ok(SchedulerType::Tlevel),
            _ => Err(format!("Scheduler '{}' does not exist", scheduler)),
        }
    }
}

#[derive(Debug, StructOpt)]
#[structopt(name = "rsds", about = "Rust Dask Scheduler")]
struct Opt {
    #[structopt(long, default_value = "8786")]
    port: u16,
    #[structopt(long, default_value = "workstealing")]
    scheduler: SchedulerType,
    #[structopt(long, default_value = "0")]
    msd: u64,
    #[structopt(long)]
    trace_file: Option<String>,
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

    log::info!("rsds v0.1 started: {:?}", opt);

    setup_logging(opt.trace_file.take());
    let mut end_rx = setup_interrupt();

    let address = SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), opt.port);
    log::info!("Listening on port {}", address);
    let listener = TcpListener::bind(address).await?;

    // Start "protocol2" on port + 1
    let address2 = SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), listener.local_addr()?.port() + 1);
    log::info!("Listening on port {}", address2);
    let listener2 = TcpListener::bind(address2).await?;

    let (comm, sender, receiver) = prepare_scheduler_comm();

    let msd = Duration::from_millis(opt.msd);
    let scheduler_thread = thread::spawn(move || {
        let mut runtime = tokio::runtime::Builder::new()
            .basic_scheduler()
            .enable_time()
            .build()
            .expect("Runtime creation failed");
        runtime
            .block_on(create_scheduler(opt.scheduler, msd, comm))
            .expect("Scheduler failed");
    });

    {
        let task_set = tokio::task::LocalSet::default();
        let comm_ref = CommRef::new(sender);
        let core_ref = CoreRef::default();
        let core_ref2 = core_ref.clone();
        let comm_ref2 = comm_ref.clone();
        task_set
            .run_until(async move {
                let scheduler = observe_scheduler(core_ref2, comm_ref2, receiver);
                let connection = rsds::server::rpc::dask::connection_initiator(listener, core_ref.clone(), comm_ref.clone());
                let connection2 = rsds::server::rpc::rsds::connection_initiator(listener2, core_ref, comm_ref);
                let end_flag = async move {
                    end_rx.next().await;
                    Ok(())
                };

                tokio::select! {
                    r = scheduler => r ,
                    r = connection => r ,
                    r = connection2 => r ,
                    r = end_flag => r,
                }
            })
            .await
            .expect("Rsds failed");
    }

    log::debug!("Waiting for scheduler to shut down...");
    scheduler_thread.join().expect("Scheduler thread failed");
    log::info!("rsds ends");

    Ok(())
}

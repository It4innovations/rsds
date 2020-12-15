use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr};
use std::pin::Pin;
use std::str::FromStr;
use std::thread;
use std::time::Duration;

use futures::StreamExt;
use structopt::StructOpt;
use tokio::net::TcpListener;

use rsds::scheduler::{
    drive_scheduler, prepare_scheduler_comm, BLevelMetric, SchedulerComm, TLevelMetric,
};
use rsds::server::comm::observe_scheduler;
use rsds::server::comm::CommRef;
use rsds::server::core::CoreRef;
use rsds::server::dask::DaskStateRef;
use rsds::{setup_interrupt, setup_logging};

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

#[tokio::main(basic_scheduler)]
async fn main() -> rsds::Result<()> {
    let mut opt = Opt::from_args();

    log::info!("rsds v0.1 started: {:?}", opt);

    setup_logging(opt.trace_file.take());
    let mut end_rx = setup_interrupt();

    let dask_address = SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), opt.port);
    log::info!("Listening for Dask messages on port {}", dask_address);
    let dask_listener = TcpListener::bind(dask_address).await?;

    // Start "protocol2" on port + 1
    let rsds_address = SocketAddr::new(
        Ipv4Addr::UNSPECIFIED.into(),
        dask_listener.local_addr()?.port() + 1,
    );
    log::info!("Listening for RSDS messages on port {}", rsds_address);
    let rsds_listener = TcpListener::bind(rsds_address).await?;

    let (comm, sender, receiver) = prepare_scheduler_comm();

    let msd = Duration::from_millis(opt.msd);
    //let worker_type = opt.worker;
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
        let dask_state_ref = DaskStateRef::new();
        let core_ref = CoreRef::new(dask_state_ref.get_gateway());
        task_set
            .run_until(async move {
                let scheduler = observe_scheduler(core_ref.clone(), comm_ref.clone(), receiver);
                let dask_connection = rsds::server::dask::rpc::connection_initiator(
                    dask_listener,
                    core_ref.clone(),
                    comm_ref.clone(),
                    dask_state_ref,
                );
                let rsds_connection =
                    rsds::server::rpc::connection_initiator(rsds_listener, core_ref, comm_ref);
                let end_flag = async move {
                    end_rx.next().await;
                    Ok(())
                };

                tokio::select! {
                    r = scheduler => r ,
                    r = dask_connection => r ,
                    r = rsds_connection => r ,
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

use std::net::{Ipv4Addr, SocketAddr};
use std::thread;

use futures::FutureExt;
use structopt::StructOpt;
use tokio::net::TcpListener;

use rsds::comm::{observe_scheduler, CommRef};
use rsds::core::CoreRef;
use rsds::scheduler::interface::prepare_scheduler_comm;

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[derive(Debug, StructOpt)]
#[structopt(name = "rsds", about = "Rust Dask Scheduler")]
struct Opt {
    #[structopt(long, default_value = "7070")]
    port: u16,
}

#[tokio::main(basic_scheduler)]
async fn main() -> rsds::Result<()> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }
    pretty_env_logger::init();

    let opt = Opt::from_args();

    log::info!("rsds v0.0 started");

    let address = SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), opt.port);
    log::info!("listening on port {}", address);
    let listener = TcpListener::bind(address).await?;

    let (comm, sender, receiver) = prepare_scheduler_comm();

    thread::spawn(move || {
        let scheduler = rsds::scheduler::implementation::Scheduler::new();
        let mut runtime = tokio::runtime::Builder::new()
            .enable_all()
            .basic_scheduler()
            .build()
            .expect("Runtime creation failed");
        runtime
            .block_on(scheduler.start(comm))
            .expect("Scheduler failed");
    });

    let task_set = tokio::task::LocalSet::new();
    let comm_ref = CommRef::new(sender);
    let core_ref = CoreRef::new();
    let core_ref2 = core_ref.clone();
    let comm_ref2 = comm_ref.clone();
    task_set
        .run_until(async move {
            let fut = tokio::task::spawn_local(observe_scheduler(core_ref2, comm_ref2, receiver))
                .boxed_local();
            let connection =
                rsds::comm::connection_initiator(listener, core_ref, comm_ref).boxed_local();

            futures::future::select(fut, connection).await
        })
        .await;
    Ok(())
}

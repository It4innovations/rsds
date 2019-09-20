use std::net::{Ipv4Addr, SocketAddr};
use std::thread;

use structopt::StructOpt;
use tokio::net::TcpListener;
use tokio::runtime::current_thread;

use rsds::prelude::*;
use rsds::scheduler::prepare_scheduler_comm;

#[derive(Debug, StructOpt)]
#[structopt(name = "rsds", about = "Rust Dask Scheduler")]
struct Opt {
    #[structopt(long, default_value = "7070")]
    port: u16
}

#[tokio::main(single_thread)]
async fn main() -> rsds::Result<()> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }
    pretty_env_logger::init();

    let opt = Opt::from_args();

    log::info!("rsds v0.0 started");

    let address = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), opt.port);
    log::info!("listening on port {}", address);
    let listener = TcpListener::bind(address).await?;

    let (comm, sender, receiver) = prepare_scheduler_comm();

    let scheduler = rsds::scheduler::BasicScheduler;

    thread::spawn(move || {
        let mut runtime = current_thread::Runtime::new().expect("Runtime creation failed");
        runtime
            .block_on(scheduler.start(comm))
            .expect("Scheduler failed");
    });


    let core_ref = CoreRef::new(sender);
    let core_ref2 = core_ref.clone();
    current_thread::spawn(async move {
        core_ref2
            .observe_scheduler(receiver)
            .await
    });

    rsds::connection::connection_initiator(listener, core_ref)
        .await
        .expect("Connection initiator failed");
    Ok(())
}

use std::thread;

use tokio::runtime::current_thread;

use rsds::prelude::*;
use rsds::scheduler::prepare_scheduler_comm;

use structopt::StructOpt;
use std::net::{SocketAddr, Ipv4Addr};

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

    let (comm, sender, receiver) = prepare_scheduler_comm();

    let scheduler = rsds::scheduler::BasicScheduler;

    thread::spawn(move || {
        let mut runtime = current_thread::Runtime::new().expect("Runtime creation failed");
        runtime
            .block_on(scheduler.start(comm))
            .expect("Scheduler failed");
    });


    let core_ref = CoreRef::new(sender);
    log::info!("rsds v0.0 started");

    let core_ref2 = core_ref.clone();
    current_thread::spawn(async move {
        core_ref2
            .observe_scheduler(receiver)
            .await
    });

    let address = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), opt.port);
    log::info!("listening on port {}", address);
    rsds::connection::connection_initiator(address, core_ref)
        .await
        .expect("Connection initiator failed");
    Ok(())
}

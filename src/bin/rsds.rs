use std::thread;

use tokio::runtime::current_thread;

use rsds::prelude::*;
use rsds::scheduler::prepare_scheduler_comm;

#[tokio::main(single_thread)]
async fn main() -> rsds::Result<()> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }
    pretty_env_logger::init();

    let (comm, sender, receiver) = prepare_scheduler_comm();

    let scheduler = rsds::scheduler::BasicScheduler;

    thread::spawn(move || {
        let mut runtime = current_thread::Runtime::new().expect("Runtime creation failed");
        runtime
            .block_on(scheduler.start(comm))
            .expect("Scheduler failed");
    });


    let core_ref = CoreRef::new(sender);
    log::info!("rsds v0.0 started at port 7070");

    let core_ref2 = core_ref.clone();
    current_thread::spawn(async move {
        core_ref2
            .observe_scheduler(receiver)
            .await
    });

    rsds::connection::connection_initiator("127.0.0.1:7070", core_ref)
        .await
        .expect("Connection initiator failed");
    Ok(())
}

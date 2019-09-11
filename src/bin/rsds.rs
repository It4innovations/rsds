use rsds::prelude::*;
use rsds::scheduler::Scheduler;
use tokio::runtime::current_thread;

#[tokio::main(single_thread)]
async fn main() -> rsds::Result<()> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }
    pretty_env_logger::init();

    let scheduler = rsds::scheduler::BasicScheduler;
    let (tx, rx) = scheduler.start();

    let core_ref = CoreRef::new(tx);
    log::info!("rsds v0.0 started at port 7070");

    let core2 = core_ref.clone();
    current_thread::spawn(async move {
        core2.observe_scheduler(rx).await.expect("Core failed");
    });

    current_thread::spawn(async move {
        rsds::connection::connection_initiator("127.0.0.1:7070", core_ref)
            .await
            .expect("Connection initiator failed");
    });

    Ok(())
}

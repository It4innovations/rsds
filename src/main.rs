mod client;
mod common;
mod connection;
mod core;
mod daskcodec;
mod messages;
mod prelude;
mod task;
mod worker;

use failure::Error;
use tokio::net::TcpListener;
use tokio::runtime::current_thread;

#[tokio::main(single_thread)]
async fn main() -> Result<(), Error> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }
    pretty_env_logger::init();
    let core_ref = core::CoreRef::new();
    let mut listener = TcpListener::bind("127.0.0.1:7070").await?;
    log::info!("rsds v0.0 started at port 7070");
    loop {
        let (socket, address) = listener.accept().await?;
        current_thread::spawn(connection::handle_connection(
            core_ref.clone(),
            socket,
            address,
        ));
    }
}

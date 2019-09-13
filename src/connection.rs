use crate::prelude::*;
use tokio::prelude::*;

use crate::client::start_client;
use crate::daskcodec::DaskCodec;
use crate::messages::generic::{GenericMessage, IdentityResponse, SimpleMessage};
use crate::worker::start_worker;
use rmp_serde as rmps;
use std::error::Error;
use tokio::codec::Framed;
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::current_thread;

pub async fn connection_initiator(address: &str, core_ref: CoreRef) -> crate::Result<()> {
    let mut listener = TcpListener::bind(address).await?;
    loop {
        let (socket, address) = listener.accept().await?;
        let core_ref = core_ref.clone();
        current_thread::spawn(async move {
            handle_connection(core_ref, socket, address).await.expect("Connection failed");
        });
    }
}

pub async fn handle_connection(
    core_ref: CoreRef,
    socket: TcpStream,
    address: std::net::SocketAddr,
) -> crate::Result<()> {
    socket.set_nodelay(true)?;
    let mut framed = Framed::new(socket, DaskCodec::new());
    log::debug!("New connection from {}", address);

    loop {
        let buffer = framed.next().await;
        match buffer {
            Some(data) => {
                let data = data?;
                let msg: Result<GenericMessage, _> = rmps::from_read(std::io::Cursor::new(&data.message));
                match msg {
                    Ok(GenericMessage::HeartbeatWorker(_)) => {
                        log::debug!("Heartbeat from worker");
                        continue;
                    }
                    Ok(GenericMessage::RegisterWorker(msg)) => {
                        log::debug!("Worker registration from {}", address);
                        break start_worker(&core_ref, address, framed, msg).await;
                    }
                    Ok(GenericMessage::RegisterClient(m)) => {
                        log::debug!("Client registration from {}", address);
                        let rsp = SimpleMessage { op: "stream-start" };
                        let data = rmp_serde::encode::to_vec_named(&[rsp])?;
                        framed.send(data.into()).await?;
                        break start_client(&core_ref, address, framed, m.client).await;
                    }
                    Ok(GenericMessage::Identity(_)) => {
                        log::debug!("Identity request from {}", address);
                        let rsp = IdentityResponse {
                            i_type: "Scheduler",
                            id: core_ref.uid(),
                        };
                        let data = rmp_serde::encode::to_vec_named(&rsp)?;
                        framed.send(data.into()).await?;
                    }
                    Err(e) => {
                        dbg!(data);
                        panic!(
                            "Invalid message from unregistered connection ({}): {}",
                            address,
                            e.description()
                        );
                    }
                }
            }
            None => {
                log::debug!("Unregistered connection {} closed", address);
                break Ok(());
            }
        }
    }
}

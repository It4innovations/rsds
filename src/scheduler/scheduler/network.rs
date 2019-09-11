use std::net::SocketAddr;
use crate::scheduler::{FromSchedulerMessage, ToSchedulerMessage, SchedulerComm};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use std::thread;
use crate::scheduler::schedproto::SchedulerRegistration;
use tokio::net::TcpStream;
use futures::StreamExt;

pub struct NetworkScheduler;

impl NetworkScheduler {
    pub async fn start(self, mut comm: SchedulerComm, address: &str) -> crate::Result<()> {
        let msg = FromSchedulerMessage::Register(SchedulerRegistration {
            protocol_version: 0,
            scheduler_name: "test_scheduler".into(),
            scheduler_version: "0.0".into(),
            reassigning: false,
        });
        comm.send.try_send(msg).unwrap();

        let conn = TcpStream::connect(address).await?;
        let (rx, tx) = conn.split();

        let receiver = async move {
            while let Some(msg) = comm.recv.next().await {
                // send msg to tx
            }
        };
        let sender = async move {
            /*while let Some(msg) = rx.next.await {
                // send msg to conn.send
            }*/
        };

        futures::future::join(receiver, sender).await;

        Ok(())
    }
}

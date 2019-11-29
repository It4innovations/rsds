use futures::{FutureExt, SinkExt, StreamExt};
use tokio::codec::{Framed, LengthDelimitedCodec};
use tokio::net::TcpStream;

use crate::scheduler::{FromSchedulerMessage};
use crate::scheduler::interface::SchedulerComm;

pub struct RemoteScheduler;

impl RemoteScheduler {
    pub async fn start(self, comm: SchedulerComm, address: &str) -> crate::Result<()> {
        let conn = TcpStream::connect(address).await?;
        let conn = Framed::new(conn, LengthDelimitedCodec::new());
        let (mut tx, mut rx) = conn.split();

        let SchedulerComm { mut recv, mut send } = comm;
        let receiver = async move {
            while let Some(msg) = recv.next().await {
                let data = serde_json::to_vec(&msg)?;
                log::debug!("Sending scheduler command: {:?}", msg);
                tx.send(bytes::Bytes::from(data)).await?;
            }
            Ok(())
        }
            .boxed_local();

        let sender = async move {
            while let Some(msg) = rx.next().await {
                let msg = msg?;
                let data: FromSchedulerMessage = serde_json::from_slice(&msg)?;
                log::debug!("Received scheduler command: {:?}", data);
                send.try_send(data).expect("Send failed");
            }
            Ok(())
        }
            .boxed_local();

        futures::future::select(receiver, sender)
            .await
            .factor_first()
            .0
    }
}

use futures::sink::SinkExt;

use crate::core::CoreRef;
use crate::protocol::clientmsg::{FromClientMessage, ToClientMessage};
use crate::protocol::protocol::{serialize_single_packet, Batch, DaskPacket};
use crate::reactor::{release_keys, update_graph, ReactorRef};
use futures::{FutureExt, Sink, Stream, StreamExt};

pub type ClientId = u64;

pub struct Client {
    id: ClientId,
    key: String,
    sender: tokio::sync::mpsc::UnboundedSender<DaskPacket>,
}

impl Client {
    pub fn send_message(&mut self, message: ToClientMessage) -> crate::Result<()> {
        log::debug!("Client send message {:?}", message);
        self.send_dask_message(serialize_single_packet(message)?)
    }

    pub fn send_dask_message(&mut self, packet: DaskPacket) -> crate::Result<()> {
        self.sender.send(packet).expect("Send to client failed");
        Ok(())
    }

    #[inline]
    pub fn id(&self) -> ClientId {
        self.id
    }

    #[inline]
    pub fn key(&self) -> &str {
        &self.key
    }
}

pub async fn execute_client<
    Reader: Stream<Item = crate::Result<Batch<FromClientMessage>>> + Unpin,
    Writer: Sink<DaskPacket, Error = crate::DsError> + Unpin,
>(
    core_ref: &CoreRef,
    reactor_ref: &ReactorRef,
    address: std::net::SocketAddr,
    mut receiver: Reader,
    mut sender: Writer,
    client_key: String,
) -> crate::Result<()> {
    let core_ref = core_ref.clone();
    let core_ref2 = core_ref.clone();

    let (snd_sender, mut snd_receiver) = tokio::sync::mpsc::unbounded_channel::<DaskPacket>();

    let client_id = {
        let mut core = core_ref.get_mut();
        let client_id = core.new_client_id();
        let client = Client {
            id: client_id,
            key: client_key,
            sender: snd_sender,
        };
        core.register_client(client);
        client_id
    };

    log::info!("Client {} registered from {}", client_id, address);

    let snd_loop = async move {
        while let Some(data) = snd_receiver.next().await {
            if let Err(e) = sender.send(data).await {
                return Err(e);
            }
        }
        Ok(())
    };

    let recv_loop = async move {
        'outer: while let Some(messages) = receiver.next().await {
            let messages = messages?;
            for message in messages {
                log::debug!("Client recv message {:?}", message);
                match message {
                    FromClientMessage::HeartbeatClient => { /* TODO, ignore heartbeat now */ }
                    FromClientMessage::ClientReleasesKeys(msg) => {
                        release_keys(&core_ref, &reactor_ref, msg.client, msg.keys);
                    }
                    FromClientMessage::UpdateGraph(update) => {
                        update_graph(&core_ref, &reactor_ref, client_id, update);
                    }
                    FromClientMessage::CloseClient => {
                        log::debug!("CloseClient message received");
                        break 'outer;
                    }
                    _ => {
                        panic!("Unhandled client message: {:?}", message);
                    }
                }
            }
        }
        Ok(())
    };

    let result = futures::future::select(recv_loop.boxed_local(), snd_loop.boxed_local()).await;
    if let Err(e) = result.factor_first().0 {
        log::error!(
            "Error in client connection (id={}, connection={}): {}",
            client_id,
            address,
            e
        );
    }
    log::info!(
        "Client {} connection closed (connection: {})",
        client_id,
        address
    );
    let mut core = core_ref2.get_mut();
    core.unregister_client(client_id);
    Ok(())
}

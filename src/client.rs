use futures::sink::SinkExt;

use crate::core::CoreRef;
use crate::protocol::clientmsg::{FromClientMessage, ToClientMessage};
use crate::protocol::protocol::{serialize_single_packet, Batch, DaskPacket};
use crate::reactor::{release_keys, update_graph};
use crate::comm::CommRef;
use futures::{FutureExt, Sink, Stream, StreamExt};
use tokio::sync::mpsc::UnboundedSender;

pub type ClientId = u64;

pub struct Client {
    id: ClientId,
    key: String,
    sender: UnboundedSender<DaskPacket>,
}

impl Client {
    #[inline]
    pub fn new(id: ClientId, key: String, sender: UnboundedSender<DaskPacket>) -> Self {
        Client {
            id,
            key,
            sender
        }
    }

    #[inline]
    pub fn id(&self) -> ClientId {
        self.id
    }

    #[inline]
    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn send_message(&mut self, message: ToClientMessage) -> crate::Result<()> {
        log::debug!("Client send message {:?}", message);
        self.send_dask_packet(serialize_single_packet(message)?)
    }

    pub fn send_dask_packet(&mut self, packet: DaskPacket) -> crate::Result<()> {
        self.sender.send(packet).expect("Send to client failed");
        Ok(())
    }
}

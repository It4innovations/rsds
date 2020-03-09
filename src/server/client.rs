use crate::protocol::protocol::DaskPacket;

use crate::protocol::key::{DaskKey, DaskKeyRef};
use tokio::sync::mpsc::UnboundedSender;

pub type ClientId = u64;

#[derive(Debug)]
pub struct Client {
    id: ClientId,
    key: DaskKey,
    sender: UnboundedSender<DaskPacket>,
}

impl Client {
    #[inline]
    pub fn new(id: ClientId, key: DaskKey, sender: UnboundedSender<DaskPacket>) -> Self {
        Client { id, key, sender }
    }

    #[inline]
    pub fn id(&self) -> ClientId {
        self.id
    }

    #[inline]
    pub fn key(&self) -> &DaskKeyRef {
        &self.key
    }

    pub fn send_dask_packet(&mut self, packet: DaskPacket) -> crate::Result<()> {
        self.sender.send(packet).expect("Send to client failed");
        Ok(())
    }
}

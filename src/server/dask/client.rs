use bytes::BytesMut;
use tokio::sync::mpsc::UnboundedSender;

use crate::server::dask::dasktransport::{make_dask_pickle_payload, DaskPacket, MessageBuilder};
use crate::server::dask::key::DaskKey;
use crate::server::dask::messages::client::{KeyInMemoryMsg, TaskErredMsg, ToClientMessage};
use crate::server::task::ErrorInfo;

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
    pub fn key(&self) -> &DaskKey {
        &self.key
    }

    fn send_dask_packet(&self, packet: DaskPacket) -> crate::Result<()> {
        self.sender.send(packet).expect("Send to client failed");
        Ok(())
    }

    pub fn send_finished_keys(&self, keys: Vec<DaskKey>) -> crate::Result<()> {
        let mut mbuilder = MessageBuilder::<ToClientMessage>::with_capacity(keys.len());
        for key in keys {
            mbuilder.add_message(ToClientMessage::KeyInMemory(KeyInMemoryMsg {
                key,
                r#type: Default::default(),
            }));
        }
        self.send_dask_packet(mbuilder.build_batch()?)
    }

    pub fn send_error(&self, key: DaskKey, error_info: &ErrorInfo) -> crate::Result<()> {
        let mut mbuilder = MessageBuilder::<ToClientMessage>::default();
        let exception = mbuilder.take_serialized(make_dask_pickle_payload(BytesMut::from(
            &error_info.exception[..],
        )));
        let traceback = mbuilder.take_serialized(make_dask_pickle_payload(BytesMut::from(
            &error_info.traceback[..],
        )));
        mbuilder.add_message(ToClientMessage::TaskErred(TaskErredMsg {
            key,
            exception,
            traceback,
        }));
        self.send_dask_packet(mbuilder.build_batch()?)
    }
}

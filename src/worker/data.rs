use crate::common::{WrappedRcRefCell};
use crate::server::protocol::key::DaskKey;
use std::rc::Rc;
use crate::common::data::SerializationType;
use bytes::Bytes;
use crate::worker::task::TaskRef;
use hashbrown::HashSet;

#[derive(Debug)]
pub struct LocalData {
    pub serializer: SerializationType,
    pub bytes: Bytes,
}

#[derive(Debug)]
pub struct RemoteData {
    pub workers: Vec<DaskKey>,
}

#[derive(Debug)]
pub enum DataObjectState {
    Remote(RemoteData),
    Local(LocalData),
    Removed,
}


pub struct DataObject {
    pub key: DaskKey,
    pub state: DataObjectState,
    pub consumers: HashSet<TaskRef>,
    pub size: u64,
}

impl DataObject {
    pub fn local_data(&self) -> Option<&LocalData> {
        match &self.state {
            DataObjectState::Local(x) => Some(x),
            _ => None
        }
    }
}

pub type DataObjectRef = WrappedRcRefCell<DataObject>;

impl DataObjectRef {
    pub fn new(key: DaskKey, size: u64, state: DataObjectState) -> Self {
        WrappedRcRefCell::wrap(DataObject {
            key, size, state, consumers: Default::default(),
        })
    }
}
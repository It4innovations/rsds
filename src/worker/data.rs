use bytes::Bytes;
use hashbrown::HashSet;

use crate::common::data::SerializationType;
use crate::common::WrappedRcRefCell;
use crate::scheduler::{TaskId, WorkerId};
use crate::server::dask::key::DaskKey;
use crate::worker::task::TaskRef;

#[derive(Debug)]
pub struct LocalData {
    pub serializer: SerializationType,
    pub bytes: Bytes,
}

#[derive(Debug)]
pub struct RemoteData {
    pub workers: Vec<WorkerId>,
}

#[derive(Debug)]
pub enum DataObjectState {
    Remote(RemoteData),
    Local(LocalData),
    Removed,
}

pub struct DataObject {
    pub id: TaskId,
    pub state: DataObjectState,
    pub consumers: HashSet<TaskRef>,
    pub size: u64,
}

impl DataObject {
    pub fn local_data(&self) -> Option<&LocalData> {
        match &self.state {
            DataObjectState::Local(x) => Some(x),
            _ => None,
        }
    }
}

pub type DataObjectRef = WrappedRcRefCell<DataObject>;

impl DataObjectRef {
    pub fn new(id: TaskId, size: u64, state: DataObjectState) -> Self {
        WrappedRcRefCell::wrap(DataObject {
            id,
            size,
            state,
            consumers: Default::default(),
        })
    }
}

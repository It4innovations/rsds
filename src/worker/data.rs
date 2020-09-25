use crate::common::{DataInfo, WrappedRcRefCell};
use crate::server::protocol::key::DaskKey;

#[derive(Debug)]
pub struct DataObject {
    pub key: DaskKey,
    pub info: DataInfo,
    pub bytes: Option<Vec<u8>>,
}

pub type DataObjectRef = WrappedRcRefCell<DataObject>;

impl DataObjectRef {
    pub fn new(key: DaskKey, info: DataInfo, bytes: Option<Vec<u8>>) -> Self {
        WrappedRcRefCell::wrap(DataObject {
            key, info, bytes
        })
    }
}
use serde::{Deserialize, Serialize, Serializer};
use super::subworker::SubworkerId;
use crate::protocol::key::DaskKey;

#[derive(Deserialize, Debug)]
pub(crate) struct RegisterSubworkerMessage {
    pub(crate) subworker_id: SubworkerId
}


#[derive(Serialize, Debug)]
pub struct ComputeTaskMsg<'a> {
    pub key: &'a DaskKey,

    pub function: &'a rmpv::Value,
    pub args: &'a rmpv::Value,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub kwargs: &'a Option<rmpv::Value>,
}


#[derive(Serialize, Debug)]
#[serde(tag = "op")]
pub enum ToSubworkerMessage<'a> {
    ComputeTask(ComputeTaskMsg<'a>),
}


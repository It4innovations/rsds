use serde::{Deserialize, Serialize, Serializer};
use super::subworker::SubworkerId;

#[derive(Deserialize, Debug)]
pub(crate) struct RegisterSubworkerMessage {
    pub(crate) subworker_id: SubworkerId
}
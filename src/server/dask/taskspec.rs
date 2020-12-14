use serde::Serialize;

use crate::scheduler::TaskId;
use crate::server::dask::dasktransport::{Frames, SerializedTransport};
use crate::server::dask::key::DaskKey;
use crate::server::dask::messages::client::{ClientTaskSpec, DirectTaskSpec};

#[derive(Serialize, Debug)]
pub struct DaskTaskSpec<'a> {
    function: rmpv::Value,

    #[serde(skip_serializing_if = "rmpv::Value::is_nil")]
    args: rmpv::Value,

    #[serde(skip_serializing_if = "rmpv::Value::is_nil")]
    kwargs: rmpv::Value,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    id_key_map: Vec<(TaskId, &'a DaskKey)>,
}

impl<'a> DaskTaskSpec<'a> {
    pub fn new(
        spec: ClientTaskSpec<SerializedTransport>,
        frames: &mut Frames,
        id_key_map: Vec<(TaskId, &'a DaskKey)>,
    ) -> Self {
        let (function, args, kwargs) = match spec {
            ClientTaskSpec::Direct(DirectTaskSpec {
                function,
                args,
                kwargs,
            }) => (
                function.unwrap().into_memory(frames).to_msgpack_value(),
                args.map(|x| x.into_memory(frames).to_msgpack_value())
                    .unwrap_or(rmpv::Value::Nil),
                kwargs
                    .map(|x| x.into_memory(frames).to_msgpack_value())
                    .unwrap_or(rmpv::Value::Nil),
            ),
            ClientTaskSpec::Serialized(s) => (
                s.into_memory(frames).to_msgpack_value(),
                rmpv::Value::Nil,
                rmpv::Value::Nil,
            ),
        };
        DaskTaskSpec {
            function,
            args,
            kwargs,
            id_key_map,
        }
    }
}

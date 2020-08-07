use std::time::{Duration, SystemTime};

use crate::common::Map;
use crate::protocol::key::to_dask_key;
use crate::protocol::protocol::{serialize_single_packet, SerializedTransport};
use crate::protocol::workermsg::{
    AddKeysMsg, ComputeTaskMsg, FromWorkerMessage, Status, TaskFinishedMsg,
};
use crate::worker::state::WorkerStateRef;

pub fn compute_task(state_ref: &WorkerStateRef, mut msg: ComputeTaskMsg) -> crate::Result<()> {
    let now = SystemTime::UNIX_EPOCH.elapsed().unwrap();
    let mut state = state_ref.get_mut();

    let fetched_keys: Vec<_> = std::mem::take(&mut msg.who_has)
        .into_iter()
        .map(|(k, _)| k)
        .filter(|k| !state.local_keys.contains(k))
        .collect();

    if !fetched_keys.is_empty() {
        for key in &fetched_keys {
            assert!(state.local_keys.insert(key.clone()));
        }
        state.send(serialize_single_packet(FromWorkerMessage::<
            SerializedTransport,
        >::AddKeys(AddKeysMsg {
            keys: fetched_keys,
        }))?);
    }
    state.local_keys.insert(msg.key.clone());

    let mut startstops = Map::new();
    startstops.insert(to_dask_key("action"), rmpv::Value::String(rmpv::Utf8String::from("compute")));
    startstops.insert(to_dask_key("start"), rmpv::Value::F64(now.as_secs_f64()));
    startstops.insert(to_dask_key("stop"), rmpv::Value::F64((now + Duration::from_micros(10)).as_secs_f64()));

    state.send(serialize_single_packet(FromWorkerMessage::<
        SerializedTransport,
    >::TaskFinished(
        TaskFinishedMsg {
            status: Status::Ok,
            key: msg.key,
            nbytes: 20,
            r#type: vec![],
            startstops: vec!(startstops),
        },
    ))?);
    Ok(())
}

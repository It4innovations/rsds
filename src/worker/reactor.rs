use crate::server::protocol::dasktransport::{serialize_single_packet, SerializedTransport};
use crate::server::protocol::daskmessages::worker::{
    AddKeysMsg, ComputeTaskMsg, FromWorkerMessage, Status, TaskFinishedMsg,
};
use crate::worker::state::{WorkerStateRef, WorkerState};
use std::time::{Duration, SystemTime};
use rand::seq::IteratorRandom;
use crate::worker::subworker::SubworkerRef;
use crate::worker::task::TaskState;


pub fn choose_subworker(state: &mut WorkerState) -> SubworkerRef {
    // TODO: Real implementation
    state.free_subworkers.pop().unwrap()
}

pub fn try_start_tasks(state: &mut WorkerState) {
    if state.free_subworkers.is_empty() {
        return;
    }
    while let Some((task_ref, _)) = state.task_queue.pop() {
        {
            let subworker_ref = choose_subworker(state);
            let mut task = task_ref.get_mut();
            task.set_running(subworker_ref.clone());
            let mut sw = subworker_ref.get_mut();
            assert!(sw.running_task.is_none());
            sw.running_task = Some(task_ref.clone());
            sw.start_task(&task);
        }
        if state.free_subworkers.is_empty() {
            return;
        }
    }
}

pub fn compute_task(state_ref: &WorkerStateRef, mut msg: ComputeTaskMsg) -> crate::Result<()> {
    todo!();
    /*
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
    state.send(serialize_single_packet(FromWorkerMessage::<
        SerializedTransport,
    >::TaskFinished(
        TaskFinishedMsg {
            status: Status::Ok,
            key: msg.key,
            nbytes: 20,
            r#type: vec![],
            startstops: vec![(
                "compute".into(),
                now.as_secs_f64(),
                (now + Duration::from_micros(10)).as_secs_f64(),
            )],
        },
    ))?);
    Ok(())*/
}

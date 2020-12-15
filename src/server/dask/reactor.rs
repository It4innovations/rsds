use bytes::BytesMut;
use futures::{Sink, SinkExt};

use crate::common::Map;
use crate::scheduler::TaskId;
use crate::server::comm::CommRef;
use crate::server::core::CoreRef;
use crate::server::dask::client::ClientId;
use crate::server::dask::dasktransport::{
    make_dask_payload, serialize_single_packet, DaskPacket, SerializedMemory,
};
use crate::server::dask::key::{to_dask_key, DaskKey};
use crate::server::dask::messages::client::{GetDataResponse, UpdateGraphMsg};
use crate::server::dask::messages::generic::{ScatterMsg, WhoHasMsgResponse};
use crate::server::dask::state::DaskState;
use crate::server::dask::taskspec::DaskTaskSpec;
use crate::server::dask::DaskStateRef;
use crate::server::notifications::Notifications;
use crate::server::reactor::scatter;
use crate::server::task::{DataInfo, TaskRef, TaskRuntimeState};

pub fn update_graph(
    core_ref: &CoreRef,
    comm_ref: &CommRef,
    state: &mut DaskState,
    client_id: ClientId,
    mut update: UpdateGraphMsg,
) -> crate::Result<()> {
    log::debug!("Updating graph from client {}", client_id);

    let mut core = core_ref.get_mut();
    let mut new_tasks = Vec::with_capacity(update.tasks.len());
    let lowest_id = core.new_task_id();
    //let mut new_task_ids: Map<DaskKey, TaskId> = Map::with_capacity(update.tasks.len());
    for (task_key, _) in &update.tasks {
        let new_task_id = core.new_task_id();
        log::debug!(
            "Dask task mapping: Task id={} key={}",
            new_task_id,
            task_key
        );
        state.assign_key(task_key.clone(), &mut core);
    }

    log::debug!(
        "New tasks (count={}) from client_id={}",
        update.tasks.len(),
        client_id
    );

    /* client send a user_priority in inverse meaning, so we use negative value
    to make same meaning in the rsds */
    let user_priority = -update.user_priority;
    for (task_key, task_spec) in update.tasks {
        let task_id = state.get_task_id(&task_key).unwrap();
        let inputs = if let Some(deps) = update.dependencies.get(&task_key) {
            let mut inputs: Vec<_> = deps
                .iter()
                .map(|key| state.get_task_id(&key).unwrap())
                .collect();
            inputs.sort_unstable();
            inputs.dedup();
            inputs
        } else {
            Vec::new()
        };

        let unfinished_deps = inputs
            .iter()
            .map(|task_id| {
                if *task_id >= lowest_id {
                    1
                } else if core.get_task_by_id_or_panic(*task_id).get().is_finished() {
                    0
                } else {
                    1
                }
            })
            .sum();

        //let task_spec = client_task_spec_to_memory(task_spec, &mut update.frames);

        let key_id_map: Vec<(&DaskKey, TaskId)> = inputs
            .iter()
            .map(|task_id| (state.get_task_key(*task_id).unwrap(), *task_id))
            .collect();
        let serialized_dask_spec = rmp_serde::to_vec_named(&DaskTaskSpec::new(
            task_spec,
            &mut update.frames,
            key_id_map,
        ))
        .unwrap();

        let client_priority = update.priority.get(&task_key).copied().unwrap_or_default();
        let task_ref = TaskRef::new(
            task_id,
            serialized_dask_spec,
            inputs,
            unfinished_deps,
            user_priority,
            client_priority,
        );
        new_tasks.push(task_ref);
    }

    let is_actor = update.actors.unwrap_or(false);
    assert!(!is_actor);

    let mut notifications = Notifications::with_scheduler_capacity(new_tasks.len());
    core.new_tasks(new_tasks, &mut notifications, lowest_id);
    comm_ref.get_mut().notify(&mut core, notifications)?;

    for task_key in update.keys {
        let task_id = state.get_task_id(&task_key).unwrap();
        state.subscribe_client_to_task(task_id, client_id);
    }
    Ok(())
}

pub fn release_keys(
    core_ref: &CoreRef,
    comm_ref: &CommRef,
    state_ref: &DaskStateRef,
    client_id: ClientId,
    task_keys: Vec<DaskKey>,
) -> crate::Result<()> {
    let mut core = core_ref.get_mut();
    let mut notifications = Notifications::default();
    for key in task_keys {
        log::debug!("Releasing dask key {}", key);
        let task_id_maybe = state_ref.get().get_task_id(&key);
        if let Some(task_id) = task_id_maybe {
            let unsubscribed = state_ref
                .get_mut()
                .unsubscribe_client_from_task(task_id, client_id);
            if unsubscribed {
                let task_ref = core.get_task_by_id_or_panic(task_id).clone();
                let mut task = task_ref.get_mut();
                // NOTE! remove_data_if_possible may borrow state_ref!
                task.remove_data_if_possible(&mut core, &mut notifications);
            } else {
                log::debug!("Unsubscribing invalid client from key");
            }
        } else {
            log::debug!("Unsubscribing invalid key");
        }
    }
    comm_ref.get_mut().notify(&mut core, notifications)
}

pub fn subscribe_keys(
    core_ref: &CoreRef,
    _comm_ref: &CommRef,
    state_ref: &DaskStateRef,
    client_key: &DaskKey,
    task_keys: Vec<DaskKey>,
) -> crate::Result<()> {
    let mut state = state_ref.get_mut();
    let core = core_ref.get();
    let mut finished = Vec::new();

    let client_id = match state.get_client_by_key(client_key) {
        Some(c) => c.id(),
        None => return Ok(()),
    };

    for key in task_keys {
        if let Some(task_id) = state.get_task_id(&key) {
            let task_ref = core.get_task_by_id_or_panic(task_id);
            state.subscribe_client_to_task(task_id, client_id);
            if task_ref.get().is_done() {
                finished.push(key);
            }
        }
    }

    if !finished.is_empty() {
        state
            .get_client_by_id_or_panic(client_id)
            .send_finished_keys(finished)?;
    }

    Ok(())
}

pub async fn gather<W: Sink<DaskPacket, Error = crate::Error> + Unpin>(
    core_ref: &CoreRef,
    _comm_ref: &CommRef,
    dask_state_ref: &DaskStateRef,
    address: std::net::SocketAddr,
    sink: &mut W,
    keys: Vec<DaskKey>,
) -> crate::Result<()> {
    let task_ids: Vec<_> = {
        let dask_state = dask_state_ref.get();
        keys.iter()
            .map(|key| dask_state.get_task_id(&key).unwrap())
            .collect()
    };

    let data_result = crate::server::reactor::gather(core_ref, &task_ids).await?;

    let result_map: Map<DaskKey, SerializedMemory> = data_result
        .into_iter()
        .map(|(task_id, data, serializer)| {
            let key = dask_state_ref.get().get_task_key(task_id).unwrap().clone();
            (key, make_dask_payload(serializer, data))
        })
        .collect();

    let msg = GetDataResponse {
        status: to_dask_key("OK"),
        data: result_map,
    };
    log::debug!("Sending gathered data {}", address);
    sink.send(serialize_single_packet(msg)?).await?;
    Ok(())
}

pub async fn get_ncores<W: Sink<DaskPacket, Error = crate::Error> + Unpin>(
    core_ref: &CoreRef,
    _comm_ref: &CommRef,
    writer: &mut W,
) -> crate::Result<()> {
    let cores: Map<String, _> = core_ref
        .get()
        .get_workers()
        .map(|w_ref| {
            let w = w_ref.get();
            (w.id.to_string(), w.ncpus)
        })
        .collect();
    writer.send(serialize_single_packet(cores)?).await?;
    Ok(())
}

pub async fn dask_scatter<W: Sink<DaskPacket, Error = crate::Error> + Unpin>(
    core_ref: &CoreRef,
    comm_ref: &CommRef,
    state_ref: &DaskStateRef,
    writer: &mut W,
    mut message: ScatterMsg,
) -> crate::Result<()> {
    assert!(!message.broadcast); // TODO: implement broadcast

    let client_id = state_ref
        .get()
        .get_client_by_key(&message.client)
        .unwrap()
        .id();

    if !core_ref.get().has_workers() {
        todo!(); // TODO: Implement timeout
    }

    let (workers, key_mapping, data): (Vec<_>, Vec<_>, Vec<_>) = {
        let mut core = core_ref.get_mut();
        let workers = match message.workers.take() {
            Some(workers) => {
                if workers.is_empty() {
                    return Ok(());
                }
                workers
                    .into_iter()
                    .map(|worker_key| {
                        core.get_worker_by_address(&worker_key.into_string())
                            .unwrap()
                            .clone()
                    })
                    .collect()
            }
            None => core.get_workers().cloned().collect(),
        };
        let mut key_mapping = Vec::with_capacity(message.data.len());
        let data: Vec<(TaskRef, BytesMut)> = message
            .data
            .into_iter()
            .map(|(key, value)| {
                let value = value.into_bytesmut().unwrap();
                let task_id = core.new_task_id();
                log::debug!("Scattering key={} as task={}", &key, task_id);
                key_mapping.push((task_id, key));
                let task_ref = TaskRef::new(
                    task_id,
                    Vec::new(),
                    Default::default(),
                    0,
                    Default::default(),
                    Default::default(),
                );
                {
                    let mut task = task_ref.get_mut();
                    task.state = TaskRuntimeState::Finished(
                        DataInfo {
                            size: value.len() as u64,
                        },
                        Default::default(),
                    );
                }
                (task_ref, value)
            })
            .collect();
        (workers, key_mapping, data)
    };

    let mut notifications = Notifications::default();
    scatter(&core_ref, &workers, data, &mut notifications).await;

    let keys: Vec<_> = key_mapping
        .iter()
        .map(|(_task_id, key)| key.clone())
        .collect();
    let mut state = state_ref.get_mut();
    // TODO: If client was disconnected during scatter, we should remove the uploaded tasks

    for (task_id, key) in key_mapping {
        state.insert_task_key(key, task_id);
        state.subscribe_client_to_task(task_id, client_id);
    }

    comm_ref
        .get_mut()
        .notify(&mut core_ref.get_mut(), notifications)
        .unwrap();

    {
        let client = state.get_client_by_id_or_panic(client_id);
        client.send_finished_keys(keys.clone())?;
    }

    writer.send(serialize_single_packet(keys)?).await?;
    Ok(())
}

pub async fn who_has<W: Sink<DaskPacket, Error = crate::Error> + Unpin>(
    core_ref: &CoreRef,
    _comm_ref: &CommRef,
    state_ref: &DaskStateRef,
    sink: &mut W,
    keys: Option<Vec<DaskKey>>,
) -> crate::Result<()> {
    let response: WhoHasMsgResponse = {
        let state = state_ref.get();
        let keys: Vec<DaskKey> = keys.unwrap_or_else(|| state.get_all_keys().cloned().collect());
        let core = core_ref.get();
        keys.into_iter()
            .map(|key| {
                let workers = match state
                    .get_task_id(&key)
                    .and_then(|task_id| core.get_task_by_id(task_id))
                {
                    Some(task) => match task.get().get_workers() {
                        Some(ws) => ws.iter().map(|w| w.get().address().into()).collect(),
                        None => Vec::new(),
                    },
                    None => Vec::new(),
                };
                (key, workers)
            })
            .collect()
    };
    sink.send(serialize_single_packet(response)?).await
}

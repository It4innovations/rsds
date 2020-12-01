use std::iter::FromIterator;

use bytes::BytesMut;
use futures::future::join_all;
use futures::stream::FuturesUnordered;
use futures::{Sink, SinkExt, Stream, StreamExt};
use rand::seq::SliceRandom;

use tokio::net::TcpStream;

use crate::common::data::SerializationType;
use crate::common::transport::make_protocol_builder;
use crate::common::{Map, Set};
use crate::error::DsError::GenericError;
use crate::scheduler::protocol::TaskId;
use crate::server::comm::CommRef;
use crate::server::core::CoreRef;
use crate::server::notifications::Notifications;
use futures::future::FutureExt;

use crate::server::dask::dasktransport::{
    asyncread_to_stream, asyncwrite_to_sink, make_dask_payload, serialize_single_packet,
    DaskPacket, MessageWrapper, SerializedMemory,
};
use crate::server::dask::key::{dask_key_ref_to_str, to_dask_key, DaskKey};
use crate::server::dask::messages::client::{
    client_task_spec_to_memory, GetDataResponse, UpdateGraphMsg,
};
use crate::server::dask::messages::generic::{
    ProxyMsg, ScatterMsg, ScatterResponse, WhoHasMsgResponse,
};
use crate::server::protocol::messages::worker::{
    DataRequest, DataResponse, FetchRequestMsg, UploadDataMsg,
};
use crate::server::task::{DataInfo, TaskRef, TaskRuntimeState};
use crate::server::worker::WorkerRef;
use crate::trace::{trace_task_new, trace_task_new_finished};

/*
pub fn update_graph(
    core_ref: &CoreRef,
    comm_ref: &CommRef,
    gateway: &DaskGateway,
    client_id: ClientId,
    mut update: UpdateGraphMsg,
) -> crate::Result<()> {
    log::debug!("Updating graph from client {}", client_id);

    let mut core = core_ref.get_mut();
    let mut new_tasks = Vec::with_capacity(update.tasks.len());

    let mut new_task_ids: Map<DaskKey, TaskId> = Map::with_capacity(update.tasks.len());

    let lowest_id = core.new_task_id();
    for (task_key, _) in &update.tasks {
        new_task_ids.insert(task_key.clone(), core.new_task_id());
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
        let task_id = *new_task_ids.get(&task_key).unwrap();
        let inputs = if let Some(deps) = update.dependencies.get(&task_key) {
            let mut inputs: Vec<_> = deps
                .iter()
                .map(|key| {
                    new_task_ids
                        .get(key)
                        .copied()
                        .unwrap_or_else(|| gateway.get_task_by_key(key).unwrap().get().id)
                })
                .collect();
            inputs.sort();
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
        log::debug!("New task id={}, key={}", task_id, task_key);
        trace_task_new(task_id, dask_key_ref_to_str(&task_key), &inputs);
        let task_spec = task_spec_to_memory(task_spec, &mut update.frames);
        let client_priority = update.priority.get(&task_key).copied().unwrap_or_default();
        let task_ref = TaskRef::new(
            task_id,
            Some(task_spec),
            inputs,
            unfinished_deps,
            user_priority,
            client_priority,
        );

        core.add_task(task_ref.clone());
        new_tasks.push(task_ref);
    }

    for task_ref in &new_tasks {
        let task = task_ref.get();
        for task_id in &task.dependencies {
            let tr = core.get_task_by_id_or_panic(*task_id);
            tr.get_mut().add_consumer(task_ref.clone());
        }
    }

    let is_actor = update.actors.unwrap_or(false);

    /* Send notification in topological ordering of tasks */
    let mut count = new_tasks.len();
    let mut notifications = Notifications::with_scheduler_capacity(count);
    let mut processed = Set::new();
    let mut stack: Vec<(TaskRef, usize)> = Default::default();

    for task_ref in new_tasks {
        if !task_ref.get().has_consumers() {
            stack.push((task_ref, 0));
            while let Some((tr, c)) = stack.pop() {
                let ii = {
                    let task = tr.get();
                    task.dependencies.get(c).copied()
                };
                if let Some(inp) = ii {
                    stack.push((tr, c + 1));
                    if inp > lowest_id && processed.insert(inp) {
                        stack.push((core.get_task_by_id_or_panic(inp).clone(), 0));
                    }
                } else {
                    count -= 1;
                    notifications.new_task(&tr.get());
                }
            }
        }
    }
    assert_eq!(count, 0);
    comm_ref.get_mut().notify(&mut core, notifications)?;

    for task_key in update.keys {
        let task_ref = core.get_task_by_key_or_panic(&task_key);
        let mut task = task_ref.get_mut();
        task.actor = is_actor;
        task.subscribe_client(client_id);
    }
    Ok(())
}*/

// TODO: Convert this to returning Stream instead of vector,
//   so data could be sent as they are received
pub async fn gather(
    core_ref: &CoreRef,
    task_ids: &[TaskId],
) -> crate::Result<Vec<(TaskId, BytesMut, SerializationType)>> {
    let mut worker_map: Map<String, Vec<TaskId>> = Default::default();
    {
        let core = core_ref.get();
        let mut rng = rand::thread_rng();
        for task_id in task_ids {
            let task_ref = core.get_task_by_id_or_panic(*task_id);
            let task = task_ref.get();
            task.get_workers().map(|ws| {
                let ws = Vec::from_iter(ws.into_iter());
                ws.choose(&mut rng).map(|w| {
                    worker_map
                        .entry(w.get().address().to_string())
                        .or_default()
                        .push(*task_id);
                })
            });
        }
    }

    //let mut result_map: Map<DaskKey, SerializedMemory> = Map::with_capacity(keys.len());
    let mut worker_futures: FuturesUnordered<_> = FuturesUnordered::from_iter(
        worker_map
            .into_iter()
            .map(|(worker, keys)| get_data_from_worker(worker, keys)),
    );

    let mut result = Vec::with_capacity(task_ids.len());

    while let Some(r) = worker_futures.next().await {
        result.append(&mut r?);
    }

    Ok(result)
}

pub async fn fetch_data(
    stream: &mut tokio_util::codec::Framed<TcpStream, tokio_util::codec::LengthDelimitedCodec>,
    task_id: TaskId,
) -> crate::Result<(BytesMut, SerializationType)> {
    let message = DataRequest::FetchRequest(FetchRequestMsg { task_id });
    let data = rmp_serde::to_vec_named(&message).unwrap();
    stream.send(data.into()).await?;

    let message: DataResponse = {
        let data = match stream.next().await {
            None => return Err(GenericError("Unexpected close of connection".into())),
            Some(data) => data?,
        };
        rmp_serde::from_slice(&data)?
    };
    let header = match message {
        DataResponse::NotAvailable => todo!(),
        DataResponse::Data(x) => x,
        DataResponse::DataUploaded(_) => {
            // Worker send complete garbage, it should be considered as invalid and termianted
            todo!()
        }
    };
    let data = match stream.next().await {
        None => return Err(GenericError("Unexpected close of connection".into())),
        Some(data) => data?,
    };
    Ok((data.into(), header.serializer))
}

pub async fn get_data_from_worker(
    worker_address: String,
    task_ids: Vec<TaskId>,
) -> crate::Result<Vec<(TaskId, BytesMut, SerializationType)>> {
    // TODO: Storing worker connection?
    // Directly resend to client?

    let connection = connect_to_worker(worker_address.clone()).await?;
    let mut stream = make_protocol_builder().new_framed(connection);

    let mut result = Vec::with_capacity(task_ids.len());
    for task_id in task_ids {
        log::debug!("Fetching {} from {}", &task_id, worker_address);
        let (data, serializer) = fetch_data(&mut stream, task_id).await?;
        log::debug!(
            "Fetched {} from {} ({} bytes)",
            &task_id,
            worker_address,
            data.len()
        );
        result.push((task_id, data, serializer));
    }
    Ok(result)
}

pub async fn update_data_on_worker(
    worker_ref: WorkerRef,
    data: Vec<(TaskId, BytesMut)>,
) -> crate::Result<()> {
    let connection = connect_to_worker(worker_ref.get().listen_address.clone()).await?;
    let mut stream = make_protocol_builder().new_framed(connection);

    for (task_id, data_for_id) in data {
        let message = DataRequest::UploadData(UploadDataMsg {
            task_id,
            serializer: SerializationType::Pickle,
        });
        let data = rmp_serde::to_vec_named(&message).unwrap();
        stream.send(data.into()).await?;
        stream.send(data_for_id.into()).await?;
        let data = stream.next().await.unwrap().unwrap();
        let message: DataResponse = rmp_serde::from_slice(&data).unwrap();
        match message {
            DataResponse::DataUploaded(msg) => {
                if msg.task_id != task_id {
                    panic!("Upload sanity check failed, different key returned");
                }
                if let Some(error) = msg.error {
                    panic!("Upload of {} failed: {}", &msg.task_id, error);
                }
                /* Ok */
            }
            _ => {
                panic!("Invalid response");
            }
        };
    }

    Ok(())
}

async fn connect_to_worker(address: String) -> crate::Result<tokio::net::TcpStream> {
    let address = address.trim_start_matches("tcp://");
    let stream = TcpStream::connect(address).await?;
    stream.set_nodelay(true)?;
    Ok(stream)
}

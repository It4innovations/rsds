use crate::client::ClientId;
use crate::comm::CommRef;
use crate::common::{Map, Set};
use crate::core::CoreRef;
use crate::comm::Notifications;
use crate::protocol::clientmsg::{ClientTaskSpec, UpdateGraphMsg};

use crate::protocol::generic::{ScatterMsg, ScatterResponse, WhoHasMsgResponse};
use crate::protocol::protocol::{
    asyncread_to_stream, asyncwrite_to_sink, dask_parse_stream, deserialize_packet,
    map_to_transport_clone, serialize_single_packet, Batch, DaskPacket, MessageBuilder,
    SerializedMemory,
};

use crate::protocol::workermsg::{
    GetDataMsg, GetDataResponse, ToWorkerMessage, UpdateDataMsg, UpdateDataResponse,
};
use crate::scheduler::schedproto::TaskId;

use crate::task::TaskRef;

use crate::util::OptionExt;
use futures::stream::FuturesUnordered;
use futures::{Sink, SinkExt, StreamExt};
use rand::seq::SliceRandom;
use std::iter::FromIterator;
use tokio::net::TcpStream;

pub fn update_graph(
    core_ref: &CoreRef,
    comm_ref: &CommRef,
    client_id: ClientId,
    update: UpdateGraphMsg,
) -> crate::Result<()> {
    log::debug!("Updating graph from client {}", client_id);

    let mut core = core_ref.get_mut();
    let mut new_tasks = Vec::with_capacity(update.tasks.len());

    let mut new_task_ids: Map<String, TaskId> = Default::default();

    let lowest_id = core.new_task_id();
    for task_key in update.tasks.keys() {
        new_task_ids.insert(task_key.clone(), core.new_task_id());
    }

    log::debug!(
        "New tasks (count={}) from client_id={}",
        update.tasks.len(),
        client_id
    );

    for (task_key, task_spec) in update.tasks {
        let task_id = *new_task_ids.get(&task_key).unwrap();
        let inputs = if let Some(deps) = update.dependencies.get(&task_key) {
            let mut inputs: Vec<_> = deps
                .iter()
                .map(|key| {
                    new_task_ids
                        .get(key)
                        .copied()
                        .unwrap_or_else(|| core.get_task_by_key_or_panic(key).get().id)
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
        let task_ref = TaskRef::new(task_id, task_key, task_spec, inputs, unfinished_deps);

        core.add_task(task_ref.clone());
        new_tasks.push(task_ref);
    }

    for task_ref in &new_tasks {
        let task = task_ref.get();
        for task_id in &task.dependencies {
            let tr = core.get_task_by_id_or_panic(*task_id);
            tr.get_mut().consumers.insert(task_ref.clone());
        }
    }

    let mut notifications = Notifications::default();

    /* Send notification in topological ordering of tasks */
    let mut processed = Set::new();
    let mut stack: Vec<(TaskRef, usize)> = Vec::new();
    let mut count = new_tasks.len();
    for task_ref in new_tasks {
        if task_ref.get().consumers.is_empty() {
            stack.push((task_ref, 0));
            while let Some((tr, c)) = stack.pop() {
                let ii = {
                    let task = tr.get();
                    task.dependencies.get(c).copied()
                };
                if let Some(inp) = ii {
                    stack.push((tr, c + 1));
                    if inp > lowest_id && !processed.contains(&inp) {
                        processed.insert(inp);
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
        task.subscribe_client(client_id);
    }
    Ok(())
}

pub fn release_keys(
    core_ref: &CoreRef,
    comm_ref: &CommRef,
    client_key: String,
    task_keys: Vec<String>,
) -> crate::Result<()> {
    let mut core = core_ref.get_mut();
    let client_id = core.get_client_id_by_key(&client_key);
    let mut notifications = Notifications::default();
    for key in task_keys {
        let task_ref = core.get_task_by_key_or_panic(&key).clone();
        let mut task = task_ref.get_mut();

        log::debug!("Unsubscribing task id={}, client={}", task.id, client_key);
        task.unsubscribe_client(client_id);

        if task.remove_data_if_possible(&mut notifications) {
            core.remove_task(&task); // TODO: recursively remove dependencies
        }
    }
    comm_ref.get_mut().notify(&mut core, notifications)
}

pub async fn gather<W: Sink<DaskPacket, Error = crate::DsError> + Unpin>(
    core_ref: &CoreRef,
    _comm_ref: &CommRef,
    address: std::net::SocketAddr,
    sink: &mut W,
    keys: Vec<String>,
) -> crate::Result<()> {
    let mut worker_map: Map<String, Vec<&str>> = Default::default();
    {
        let core = core_ref.get();
        let mut rng = rand::thread_rng();
        for key in &keys {
            let task_ref = core.get_task_by_key_or_panic(key);
            task_ref.get().get_workers().map(|ws| {
                ws.choose(&mut rng).map(|w| {
                    worker_map.entry(w.get().address().to_owned()).or_default().push(key);
                })
            });
        }
    }

    let mut result_map: Map<String, SerializedMemory> = Map::with_capacity(keys.len());
    let mut worker_futures: FuturesUnordered<_> = FuturesUnordered::from_iter(
        worker_map
            .iter()
            .map(|(worker, keys)| get_data_from_worker(worker.clone(), &keys)),
    );

    while let Some(data) = worker_futures.next().await {
        let data = data?;
        let mut responses: Batch<GetDataResponse> = deserialize_packet(data)?;
        assert_eq!(responses.len(), 1);

        let response = responses.pop().ensure();
        assert_eq!(response.status.as_bytes(), b"OK");
        response.data.into_iter().for_each(|(k, v)| {
            debug_assert!(!result_map.contains_key(&k));
            result_map.insert(k, v);
        });
    }

    log::debug!("Sending gathered data {}", address);

    let msg = GetDataResponse {
        status: "OK".to_owned(),
        data: result_map,
    };
    sink.send(serialize_single_packet(msg)?).await?;
    Ok(())
}

pub async fn get_ncores<W: Sink<DaskPacket, Error = crate::DsError> + Unpin>(
    core_ref: &CoreRef,
    _comm_ref: &CommRef,
    writer: &mut W,
) -> crate::Result<()> {
    let core = core_ref.get();
    let cores = core.get_worker_cores();
    writer.send(serialize_single_packet(cores)?).await?;
    Ok(())
}

pub async fn scatter<W: Sink<DaskPacket, Error = crate::DsError> + Unpin>(
    core_ref: &CoreRef,
    comm_ref: &CommRef,
    writer: &mut W,
    mut message: ScatterMsg,
) -> crate::Result<()> {
    assert!(!message.broadcast); // TODO: implement broadcast

    // TODO: implement scatter

    let workers = match message.workers.take() {
        Some(workers) => {
            let core = core_ref.get();
            workers
                .into_iter()
                .map(|worker_key| core.get_worker_by_key_or_panic(&worker_key).clone())
                .collect()
        }
        None => core_ref.get().get_workers(),
    };
    if workers.is_empty() {
        return Ok(());
    }

    {
        // TODO: round-robin
        let mut worker_futures: FuturesUnordered<_> = FuturesUnordered::from_iter(
            workers
                .into_iter()
                .map(|worker| update_data_on_worker(worker.get().address().to_owned(), &message.data)),
        );

        while let Some(data) = worker_futures.next().await {
            data.unwrap();
            // TODO: read/send response?
        }
    }

    let mut notifications: Notifications = Default::default();
    let response: ScatterResponse = message.data.keys().cloned().collect();

    {
        let mut core = core_ref.get_mut();
        let client_id = core.get_client_id_by_key(&message.client);
        for (key, spec) in message.data.into_iter() {
            let task_ref = TaskRef::new(
                core.new_task_id(),
                key,
                ClientTaskSpec::Serialized(spec),
                Default::default(),
                0,
            );
            {
                let mut task = task_ref.get_mut();
                task.subscribe_client(client_id);
                notifications.new_task(&task);
            }
            core.add_task(task_ref);
            // TODO: notify key-in-memory
        }
        comm_ref.get_mut().notify(&mut core, notifications).unwrap();
    }

    writer.send(serialize_single_packet(response)?).await?;
    Ok(())
}

pub async fn get_data_from_worker(worker_address: String, keys: &[&str]) -> crate::Result<DaskPacket> {
    let mut connection = connect_to_worker(worker_address).await?;
    let msg = ToWorkerMessage::GetData(GetDataMsg {
        keys,
        who: None,
        max_connections: false,
        reply: true,
    });

    let (reader, writer) = connection.split();
    let mut writer = asyncwrite_to_sink(writer);
    writer.send(serialize_single_packet(msg)?).await?;

    let mut reader = asyncread_to_stream(reader);
    // TODO: Error propagation
    // TODO: Storing worker connection?
    let response = reader.next().await.unwrap()?;
    writer.send(serialize_single_packet("OK")?).await?;

    Ok(response)
}

pub async fn update_data_on_worker(
    worker_address: String,
    data: &Map<String, SerializedMemory>,
) -> crate::Result<()> {
    let mut connection = connect_to_worker(worker_address).await?;

    let mut builder = MessageBuilder::<ToWorkerMessage>::new();
    let msg = ToWorkerMessage::UpdateData(UpdateDataMsg {
        data: map_to_transport_clone(data, &mut builder),
        reply: true,
        report: false,
    });
    builder.add_message(msg);

    let (reader, writer) = connection.split();
    let mut writer = asyncwrite_to_sink(writer);
    writer.send(builder.build_single()?).await?;

    let mut reader = dask_parse_stream::<UpdateDataResponse, _>(asyncread_to_stream(reader));
    let response: Batch<UpdateDataResponse> = reader.next().await.unwrap()?;
    assert_eq!(response[0].status.as_bytes(), b"OK");

    Ok(())
}

pub async fn who_has<W: Sink<DaskPacket, Error = crate::DsError> + Unpin>(
    core_ref: &CoreRef,
    _comm_ref: &CommRef,
    sink: &mut W,
    keys: Vec<String>,
) -> crate::Result<()> {
    let core = core_ref.get();
    let response: WhoHasMsgResponse = keys
        .into_iter()
        .map(|key| {
            let task = core.get_task_by_key_or_panic(&key);
            let workers = match task.get().get_workers() {
                Some(ws) => ws.iter().map(|w| w.get().key().to_owned()).collect(),
                None => vec![],
            };
            (key, workers)
        })
        .collect();

    sink.send(serialize_single_packet(response)?).await
}

async fn connect_to_worker(address: String) -> crate::Result<tokio::net::TcpStream> {
    let address = address.trim_start_matches("tcp://");
    Ok(TcpStream::connect(address).await?)
}

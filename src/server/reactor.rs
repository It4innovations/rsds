use std::iter::FromIterator;

use bytes::BytesMut;
use futures::future::join_all;
use futures::stream::FuturesUnordered;
use futures::{Sink, SinkExt, StreamExt};
use rand::seq::SliceRandom;

use tokio::net::TcpStream;

use crate::common::data::SerializationType;
use crate::common::transport::make_protocol_builder;
use crate::common::{Map, Set};
use crate::error::DsError::GenericError;
use crate::scheduler::protocol::TaskId;
use crate::server::client::ClientId;
use crate::server::comm::CommRef;
use crate::server::core::CoreRef;
use crate::server::notifications::Notifications;

use crate::server::protocol::daskmessages::client::{
    task_spec_to_memory, GetDataResponse, UpdateGraphMsg,
};
use crate::server::protocol::daskmessages::generic::{
    ProxyMsg, ScatterMsg, ScatterResponse, WhoHasMsgResponse,
};
use crate::server::protocol::dasktransport::{
    asyncread_to_stream, asyncwrite_to_sink, make_dask_payload, serialize_single_packet,
    DaskPacket, MessageWrapper, SerializedMemory,
};
use crate::server::protocol::key::{dask_key_ref_to_str, to_dask_key, DaskKey};
use crate::server::protocol::messages::worker::{FetchRequest, FetchResponse};
use crate::server::task::{DataInfo, TaskRef, TaskRuntimeState};
use crate::server::worker::WorkerRef;
use crate::trace::{trace_task_new, trace_task_new_finished};

pub fn update_graph(
    core_ref: &CoreRef,
    comm_ref: &CommRef,
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
        trace_task_new(task_id, dask_key_ref_to_str(&task_key), &inputs);
        let task_spec = task_spec_to_memory(task_spec, &mut update.frames);
        let client_priority = update.priority.get(&task_key).copied().unwrap_or_default();
        let task_ref = TaskRef::new(
            task_id,
            task_key,
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
}

pub fn release_keys(
    core_ref: &CoreRef,
    comm_ref: &CommRef,
    client_key: DaskKey,
    task_keys: Vec<DaskKey>,
) -> crate::Result<()> {
    let mut core = core_ref.get_mut();
    let client_id = core.get_client_id_by_key(&client_key);
    let mut notifications = Notifications::default();
    for key in task_keys {
        let task_ref = core.get_task_by_key_or_panic(&key).clone();
        let mut task = task_ref.get_mut();

        log::debug!("Unsubscribing task id={}, client={}", task.id, client_key);
        task.unsubscribe_client(client_id);

        task.remove_data_if_possible(&mut core, &mut notifications);
    }
    comm_ref.get_mut().notify(&mut core, notifications)
}

pub fn subscribe_keys(
    core_ref: &CoreRef,
    comm_ref: &CommRef,
    client_key: DaskKey,
    task_keys: Vec<DaskKey>,
) -> crate::Result<()> {
    let mut core = core_ref.get_mut();
    let client_id = core.get_client_id_by_key(&client_key);
    let mut notifications = Notifications::default();
    for key in task_keys {
        let task_ref = core.get_task_by_key_or_panic(&key).clone();
        {
            let mut task = task_ref.get_mut();
            task.subscribe_client(client_id);
        }

        let task = task_ref.get();
        match task.state {
            TaskRuntimeState::Finished(_, _) | TaskRuntimeState::Error(_) => {
                core.notify_key_in_memory(&task_ref, &mut notifications);
            }
            _ => {}
        }
    }
    comm_ref.get_mut().notify(&mut core, notifications)
}

pub async fn gather<W: Sink<DaskPacket, Error = crate::Error> + Unpin>(
    core_ref: &CoreRef,
    _comm_ref: &CommRef,
    address: std::net::SocketAddr,
    sink: &mut W,
    keys: Vec<DaskKey>,
) -> crate::Result<()> {
    let mut worker_map: Map<DaskKey, Vec<DaskKey>> = Default::default();
    {
        let core = core_ref.get();
        let mut rng = rand::thread_rng();
        for key in &keys {
            let task_ref = core.get_task_by_key_or_panic(key);
            let task = task_ref.get();
            //let data_info = task.data_info().unwrap();
            task.get_workers().map(|ws| {
                let ws = Vec::from_iter(ws.into_iter());
                ws.choose(&mut rng).map(|w| {
                    worker_map
                        .entry(w.get().address().into())
                        .or_default()
                        .push(key.clone());
                })
            });
        }
    }

    let mut result_map: Map<DaskKey, SerializedMemory> = Map::with_capacity(keys.len());
    let mut worker_futures: FuturesUnordered<_> = FuturesUnordered::from_iter(
        worker_map
            .into_iter()
            .map(|(worker, keys)| get_data_from_worker(worker, keys)),
    );

    while let Some(result) = worker_futures.next().await {
        result?.into_iter().for_each(|(k, v, s)| {
            result_map.insert(k, make_dask_payload(s, v));
        });
    }

    log::debug!("Sending gathered data {}", address);

    let msg = GetDataResponse {
        status: to_dask_key("OK"),
        data: result_map,
    };
    sink.send(serialize_single_packet(msg)?).await?;
    Ok(())
}

pub async fn get_ncores<W: Sink<DaskPacket, Error = crate::Error> + Unpin>(
    core_ref: &CoreRef,
    _comm_ref: &CommRef,
    writer: &mut W,
) -> crate::Result<()> {
    let cores = core_ref.get().get_worker_cores();
    writer.send(serialize_single_packet(cores)?).await?;
    Ok(())
}

fn scatter_tasks(
    data: Map<DaskKey, SerializedMemory>,
    workers: &[WorkerRef],
    counter: usize,
) -> Map<WorkerRef, Map<DaskKey, SerializedMemory>> {
    let total_cpus: usize = workers.iter().map(|wr| wr.get().ncpus as usize).sum();
    let mut counter = counter % total_cpus;

    let mut cpu = 0;
    let mut index = 0;
    for (i, wr) in workers.iter().enumerate() {
        let ncpus = wr.get().ncpus as usize;
        if counter >= ncpus {
            counter -= ncpus;
        } else {
            cpu = counter;
            index = i;
            break;
        }
    }

    let mut worker_ref = &workers[index];
    let mut ncpus = worker_ref.get().ncpus as usize;

    let mut result: Map<WorkerRef, Map<DaskKey, SerializedMemory>> = Map::new();

    for (key, keydata) in data {
        result
            .entry(worker_ref.clone())
            .or_default()
            .insert(key, keydata);
        cpu += 1;
        if cpu >= ncpus {
            cpu = 0;
            index += 1;
            index %= workers.len();
            worker_ref = &workers[index];
            ncpus = worker_ref.get().ncpus as usize;
        }
    }
    result
}

pub async fn scatter<W: Sink<DaskPacket, Error = crate::Error> + Unpin>(
    core_ref: &CoreRef,
    comm_ref: &CommRef,
    writer: &mut W,
    mut message: ScatterMsg,
) -> crate::Result<()> {
    assert!(!message.broadcast); // TODO: implement broadcast

    let (workers, counter) = {
        let mut core = core_ref.get_mut();
        if !core.has_workers() {
            todo!(); // TODO: Implement timeout
        }
        let workers = match message.workers.take() {
            Some(workers) => {
                if workers.is_empty() {
                    return Ok(());
                }
                workers
                    .into_iter()
                    .map(|worker_key| core.get_worker_by_key_or_panic(&worker_key).clone())
                    .collect()
            }
            None => core.get_workers(),
        };
        let counter = core.get_and_move_scatter_counter(message.data.len());
        (workers, counter)
    };

    let response: ScatterResponse = message.data.keys().cloned().collect();
    let who_what = scatter_tasks(message.data, &workers, counter);
    let placement: Vec<(WorkerRef, Vec<DaskKey>)> = who_what
        .iter()
        .map(|(wr, ts)| (wr.clone(), ts.keys().cloned().collect()))
        .collect();
    let sizes: Vec<Map<DaskKey, u64>> = {
        let worker_futures = join_all(
            who_what
                .into_iter()
                .map(|(worker, data)| update_data_on_worker(worker.get().address().into(), data)),
        );
        worker_futures
            .await
            .into_iter()
            .collect::<crate::Result<Vec<_>>>()?
    };

    let mut notifications: Notifications = Default::default();
    {
        let mut core = core_ref.get_mut();
        let client_id = core.get_client_id_by_key(&message.client);
        for ((wr, keys), sizes) in placement.into_iter().zip(sizes.into_iter()) {
            for key in keys.into_iter() {
                let size: u64 = *sizes.get(&key).unwrap();
                let mut set = Set::new();
                set.insert(wr.clone());
                let task_ref = TaskRef::new(
                    core.new_task_id(),
                    key,
                    None,
                    Default::default(),
                    0,
                    Default::default(),
                    Default::default(),
                );
                {
                    let mut task = task_ref.get_mut();
                    task.state = TaskRuntimeState::Finished(DataInfo { size }, set);
                    task.subscribe_client(client_id);
                    notifications.new_finished_task(&task);
                    trace_task_new_finished(
                        task.id,
                        dask_key_ref_to_str(&task.key()),
                        size,
                        wr.get().id,
                    );
                }

                core.add_task(task_ref.clone());
                notifications.notify_client_key_in_memory(client_id, task_ref)
            }
        }
        comm_ref.get_mut().notify(&mut core, notifications).unwrap();
    }

    writer.send(serialize_single_packet(response)?).await?;
    Ok(())
}

/*pub async fn fetch_from_worker(worker_address: &DaskKey, key: DaskKey) {

}*/

pub async fn fetch_data(
    stream: &mut tokio_util::codec::Framed<TcpStream, tokio_util::codec::LengthDelimitedCodec>,
    key: DaskKey,
) -> crate::Result<(BytesMut, SerializationType)> {
    let message = FetchRequest { key };
    let data = rmp_serde::to_vec_named(&message).unwrap();
    stream.send(data.into()).await?;

    let message: FetchResponse = {
        let data = match stream.next().await {
            None => return Err(GenericError("Unexpected close of connection".into())),
            Some(data) => data?,
        };
        rmp_serde::from_slice(&data)?
    };
    let header = match message {
        FetchResponse::NotAvailable => todo!(),
        FetchResponse::Data(x) => x,
    };
    let data = match stream.next().await {
        None => return Err(GenericError("Unexpected close of connection".into())),
        Some(data) => data?,
    };
    Ok((data.into(), header.serializer))
}

pub async fn get_data_from_worker(
    worker_address: DaskKey,
    key: Vec<DaskKey>,
) -> crate::Result<Vec<(DaskKey, BytesMut, SerializationType)>> {
    // TODO: Storing worker connection?
    // Directly resend to client?

    let connection = connect_to_worker(worker_address.clone()).await?;
    let mut stream = make_protocol_builder().new_framed(connection);

    let mut result = Vec::with_capacity(key.len());
    for key in key {
        log::debug!("Fetching {} from {}", &key, worker_address);
        let (data, serializer) = fetch_data(&mut stream, key.clone()).await?;
        log::debug!(
            "Fetched {} from {} ({} bytes)",
            &key,
            worker_address,
            data.len()
        );
        result.push((key, data, serializer));
    }
    Ok(result)
}

pub async fn update_data_on_worker(
    worker_address: DaskKey,
    _data: Map<DaskKey, SerializedMemory>,
) -> crate::Result<Map<DaskKey, u64>> {
    let _connection = connect_to_worker(worker_address).await?;
    todo!();

    /* OLD DASK PROTOCOL
    let mut builder = MessageBuilder::<ToWorkerMessage>::default();
    let msg = ToWorkerMessage::UpdateData(UpdateDataMsg {
        data: map_to_transport(data, &mut builder),
        reply: true,
        report: false,
    });
    builder.add_message(msg);

    let (reader, writer) = connection.split();
    let mut writer = asyncwrite_to_sink(writer);
    writer.send(builder.build_single()?).await?;

    let mut reader = dask_parse_stream::<UpdateDataResponse, _>(asyncread_to_stream(reader));
    let mut response: Batch<UpdateDataResponse> = reader.next().await.unwrap()?;
    assert_eq!(response[0].status.as_bytes(), b"OK");
    Ok(response.pop().unwrap().nbytes)
    */
}

pub async fn who_has<W: Sink<DaskPacket, Error = crate::Error> + Unpin>(
    core_ref: &CoreRef,
    _comm_ref: &CommRef,
    sink: &mut W,
    keys: Option<Vec<DaskKey>>,
) -> crate::Result<()> {
    let response: WhoHasMsgResponse = {
        let core = core_ref.get();
        let keys = keys.unwrap_or_else(|| {
            core.get_tasks()
                .map(|tr| tr.get().key_ref().into())
                .collect()
        });
        keys.into_iter()
            .map(|key| {
                let workers = match core.get_task_by_key(&key) {
                    Some(task) => match task.get().get_workers() {
                        Some(ws) => ws.iter().map(|w| w.get().key().into()).collect(),
                        None => vec![],
                    },
                    None => vec![],
                };
                (key, workers)
            })
            .collect()
    };

    sink.send(serialize_single_packet(response)?).await
}

pub async fn proxy_to_worker<W: Sink<DaskPacket, Error = crate::Error> + Unpin>(
    core_ref: &CoreRef,
    _comm_ref: &CommRef,
    sink: &mut W,
    msg: ProxyMsg,
) -> crate::Result<()> {
    let worker_address: DaskKey = {
        core_ref
            .get()
            .get_worker_by_key_or_panic(&msg.worker)
            .get()
            .address()
            .into()
    };
    let mut connection = connect_to_worker(worker_address).await?;
    let (reader, writer) = connection.split();
    let mut writer = asyncwrite_to_sink(writer);
    let packet = DaskPacket::from_wrapper(MessageWrapper::Message(msg.msg), msg.frames)?;
    writer.send(packet).await?;

    let mut reader = asyncread_to_stream(reader);
    if let Some(packet) = reader.next().await {
        sink.send(packet?).await?;
    }
    Ok(())
}

async fn connect_to_worker(address: DaskKey) -> crate::Result<tokio::net::TcpStream> {
    let address = address.to_string();
    let address = address.trim_start_matches("tcp://");
    let stream = TcpStream::connect(address).await?;
    stream.set_nodelay(true)?;
    Ok(stream)
}

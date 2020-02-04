use crate::client::ClientId;
use crate::common::{Map, Set, WrappedRcRefCell};
use crate::core::{CoreRef, Core};
use crate::notifications::{Notifications, WorkerNotification, ClientNotification};
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
use crate::task::TaskRuntimeState;
use crate::protocol::clientmsg::{ToClientMessage, TaskErredMsg, KeyInMemoryMsg};
use crate::protocol::workermsg::{DeleteDataMsg, StealRequestMsg};
use crate::scheduler::schedproto::TaskId;
use crate::task::TaskRef;
use crate::util::OptionExt;
use crate::worker::WorkerRef;
use futures::stream::FuturesUnordered;
use futures::{Sink, SinkExt, StreamExt};
use rand::seq::SliceRandom;
use std::iter::FromIterator;
use tokio::sync::mpsc::{UnboundedSender, UnboundedReceiver};
use crate::scheduler::{ToSchedulerMessage, FromSchedulerMessage};

pub type ReactorRef = WrappedRcRefCell<Reactor>;

pub struct Reactor {
    sender: UnboundedSender<Vec<ToSchedulerMessage>>
}

impl Reactor {
    pub fn send(&mut self, core: &mut Core, notifications: Notifications) -> crate::Result<()> {
            if !notifications.scheduler_messages.is_empty() {
                self.sender.send(notifications.scheduler_messages).unwrap();
            }

            self.send_to_workers(&core, notifications.workers)?;
            self.send_to_clients(core, notifications.clients)?;
        Ok(())
    }

    fn send_to_clients(&mut self, core: &mut Core, notifications: Map<ClientId, ClientNotification>) -> crate::Result<()> {
        for (client_id, c_update) in notifications {
            let mut mbuilder =
                MessageBuilder::<ToClientMessage>::with_capacity(c_update.error_tasks.len() + c_update.in_memory_tasks.len());
            for task_ref in c_update.error_tasks {
                let task = task_ref.get();
                if let TaskRuntimeState::Error(error_info) = &task.state {
                    let exception = mbuilder.copy_serialized(&error_info.exception);
                    let traceback = mbuilder.copy_serialized(&error_info.traceback);
                    mbuilder.add_message(ToClientMessage::TaskErred(TaskErredMsg {
                        key: task.key.clone(),
                        exception,
                        traceback,
                    }));
                } else {
                    panic!("Task is not in error state");
                };
            }

            for task_ref in c_update.in_memory_tasks {
                let task = task_ref.get();
                mbuilder.add_message(ToClientMessage::KeyInMemory(KeyInMemoryMsg {
                    key: task.key.clone(),
                    r#type: task.data_info().unwrap().r#type.clone(),
                }));
            }

            if !mbuilder.is_empty() {
                let client = core.get_client_by_id_or_panic(client_id);
                client.send_dask_message(mbuilder.build_batch()?)?;
            }
        }
        Ok(())
    }
    fn send_to_workers(&mut self, core: &Core, notifications: Map<WorkerRef, WorkerNotification>) -> crate::Result<()> {
        for (worker_ref, w_update) in notifications {
            let mut mbuilder = MessageBuilder::new();

            for task in w_update.compute_tasks {
                task.get().make_compute_task_msg(core, &mut mbuilder);
            }

            if !w_update.delete_keys.is_empty() {
                mbuilder.add_message(ToWorkerMessage::DeleteData(DeleteDataMsg {
                    keys: w_update.delete_keys,
                    report: false,
                }));
            }

            for tref in w_update.steal_tasks {
                let task = tref.get();
                mbuilder.add_message(ToWorkerMessage::StealRequest(StealRequestMsg {
                    key: task.key.clone(),
                }));
            }

            if !mbuilder.is_empty() {
                let mut worker = worker_ref.get_mut();
                worker.send_dask_message(mbuilder.build_batch()?)
                    .unwrap_or_else(|_| {
                        // !!! Do not propagate error right now, we need to finish sending protocol to others
                        // Worker cleanup is done elsewhere (when worker future terminates),
                        // so we can safely ignore this. Since we are nice guys we log (debug) message.
                        log::debug!("Sending tasks to worker {} failed", worker.id);
                    });
            }
        }

        Ok(())
    }
}

impl ReactorRef {
    pub fn new(sender: UnboundedSender<Vec<ToSchedulerMessage>>) -> Self {
        WrappedRcRefCell::wrap(Reactor {
            sender
        })
    }
}

pub fn update_graph(core_ref: &CoreRef, reactor_ref: &ReactorRef, client_id: ClientId, update: UpdateGraphMsg) {
    log::debug!("Updating graph from client {}", client_id);

    /*for vals in update.dependencies.values() {
        for key in vals {
            if !update.tasks.contains_key(key) {
                // TODO: Error handling
                panic!("Invalid key in dependecies: {}", key);
            }
        }
    }*/
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
    reactor_ref.get_mut().send(&mut core, notifications).unwrap();

    for task_key in update.keys {
        let task_ref = core.get_task_by_key_or_panic(&task_key);
        let mut task = task_ref.get_mut();
        task.subscribe_client(client_id);
    }
}

pub fn release_keys(core_ref: &CoreRef, reactor_ref: &ReactorRef, client_key: String, task_keys: Vec<String>) {
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
    reactor_ref.get_mut().send(&mut core, notifications).unwrap();
}

pub async fn gather<W: Sink<DaskPacket, Error = crate::DsError> + Unpin>(
    core_ref: &CoreRef,
    reactor_ref: &ReactorRef,
    address: std::net::SocketAddr,
    sink: &mut W,
    keys: Vec<String>,
) -> crate::Result<()> {
    let mut worker_map: Map<WorkerRef, Vec<&str>> = Default::default();
    {
        let core = core_ref.get();
        let mut rng = rand::thread_rng();
        for key in &keys {
            let task_ref = core.get_task_by_key_or_panic(key);
            task_ref.get().get_workers().map(|ws| {
                ws.choose(&mut rng).map(|w| {
                    worker_map.entry(w.clone()).or_default().push(key);
                })
            });
        }
    }

    let mut result_map: Map<String, SerializedMemory> = Map::with_capacity(keys.len());
    let mut worker_futures: FuturesUnordered<_> = FuturesUnordered::from_iter(
        worker_map
            .iter()
            .map(|(worker, keys)| get_data_from_worker(&worker, &keys)),
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
    reactor_ref: &ReactorRef,
    writer: &mut W,
) -> crate::Result<()> {
    let core = core_ref.get();
    let cores = core.get_worker_cores();
    writer.send(serialize_single_packet(cores)?).await?;
    Ok(())
}

pub async fn scatter<W: Sink<DaskPacket, Error = crate::DsError> + Unpin>(
    core_ref: &CoreRef,
    reactor_ref: &ReactorRef,
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
                .map(|worker| update_data_on_worker(worker, &message.data)),
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
        reactor_ref.get_mut().send(&mut core, notifications).unwrap();
    }

    writer.send(serialize_single_packet(response)?).await?;
    Ok(())
}

pub async fn get_data_from_worker(worker: &WorkerRef, keys: &[&str]) -> crate::Result<DaskPacket> {
    let mut connection = worker.connect().await?;
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
    worker: WorkerRef,
    data: &Map<String, SerializedMemory>,
) -> crate::Result<()> {
    let mut connection = worker.connect().await?;

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
    reactor_ref: &ReactorRef,
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

pub async fn observe_scheduler(core_ref: CoreRef,
                               reactor_ref: ReactorRef,
                               mut receiver: UnboundedReceiver<FromSchedulerMessage>) {
    log::debug!("Starting scheduler");

    match receiver.next().await {
        Some(crate::scheduler::FromSchedulerMessage::Register(r)) => log::debug!("Scheduler registered: {:?}", r),
        None => panic!("Scheduler closed connection without registration"),
        _ => panic!("First message of scheduler has to be registration")
    }

    while let Some(msg) = receiver.next().await {
        match msg {
            FromSchedulerMessage::TaskAssignments(assignments) => {
                let mut core = core_ref.get_mut();
                let mut notifications = Default::default();
                core.process_assignments(assignments, &mut notifications);
                reactor_ref.get_mut().send(&mut core, notifications).unwrap();
            }
            FromSchedulerMessage::Register(_) => {
                panic!("Double registration of scheduler");
            }
        }
    }
}

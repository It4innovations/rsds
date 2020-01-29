use futures::future::FutureExt;
use futures::sink::SinkExt;
use futures::stream::{FuturesUnordered, StreamExt};

use futures::{future, Sink, Stream};

use crate::core::CoreRef;
use crate::notifications::Notifications;
use crate::protocol::clientmsg::{FromClientMessage, ToClientMessage, UpdateGraphMsg};
use crate::protocol::protocol::{
    deserialize_packet, serialize_single_packet, Batch, DaskPacket, SerializedMemory,
};
use crate::protocol::workermsg::GetDataResponse;
use crate::scheduler::schedproto::TaskId;
use crate::task::TaskRef;
use crate::worker::WorkerRef;

use crate::common::Map;
use crate::util::OptionExt;
use hashbrown::HashSet;
use rand::seq::SliceRandom;
use std::iter::FromIterator;

pub type ClientId = u64;

pub struct Client {
    id: ClientId,
    key: String,
    sender: tokio::sync::mpsc::UnboundedSender<DaskPacket>,
}

impl Client {
    pub fn send_message(&mut self, message: ToClientMessage) -> crate::Result<()> {
        log::debug!("Client send message {:?}", message);
        self.send_dask_message(serialize_single_packet(message)?)
    }

    pub fn send_dask_message(&mut self, packet: DaskPacket) -> crate::Result<()> {
        self.sender.send(packet).expect("Send to client failed");
        Ok(())
    }

    #[inline]
    pub fn id(&self) -> ClientId {
        self.id
    }

    #[inline]
    pub fn key(&self) -> &str {
        &self.key
    }
}

pub async fn start_client<
    Reader: Stream<Item = crate::Result<Batch<FromClientMessage>>> + Unpin,
    Writer: Sink<DaskPacket, Error = crate::DsError> + Unpin,
>(
    core_ref: &CoreRef,
    address: std::net::SocketAddr,
    mut receiver: Reader,
    mut sender: Writer,
    client_key: String,
) -> crate::Result<()> {
    let core_ref = core_ref.clone();
    let core_ref2 = core_ref.clone();

    let (snd_sender, mut snd_receiver) = tokio::sync::mpsc::unbounded_channel::<DaskPacket>();

    let client_id = {
        let mut core = core_ref.get_mut();
        let client_id = core.new_client_id();
        let client = Client {
            id: client_id,
            key: client_key,
            sender: snd_sender,
        };
        core.register_client(client);
        client_id
    };

    log::info!("Client {} registered from {}", client_id, address);

    let snd_loop = async move {
        while let Some(data) = snd_receiver.next().await {
            if let Err(e) = sender.send(data).await {
                return Err(e);
            }
        }
        Ok(())
    };

    let recv_loop = async move {
        'outer: while let Some(messages) = receiver.next().await {
            let messages = messages?;
            for message in messages {
                log::debug!("Client recv message {:?}", message);
                match message {
                    FromClientMessage::HeartbeatClient => { /* TODO, ignore heartbeat now */ }
                    FromClientMessage::ClientReleasesKeys(msg) => {
                        release_keys(&core_ref, msg.client, msg.keys);
                    }
                    FromClientMessage::UpdateGraph(update) => {
                        update_graph(&core_ref, client_id, update);
                    }
                    FromClientMessage::CloseClient => {
                        log::debug!("CloseClient message received");
                        break 'outer;
                    }
                    _ => {
                        log::warn!("Unhandled client message: {:?}", message);
                    }
                }
            }
        }
        Ok(())
    };

    let result = future::select(recv_loop.boxed_local(), snd_loop.boxed_local()).await;
    if let Err(e) = result.factor_first().0 {
        log::error!(
            "Error in client connection (id={}, connection={}): {}",
            client_id,
            address,
            e
        );
    }
    log::info!(
        "Client {} connection closed (connection: {})",
        client_id,
        address
    );
    let mut core = core_ref2.get_mut();
    core.unregister_client(client_id);
    Ok(())
}

pub fn update_graph(core_ref: &CoreRef, client_id: ClientId, update: UpdateGraphMsg) {
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
        let unfinished_deps = inputs.len() as u32;

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
    let mut processed = HashSet::new();
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
    notifications
        .send(&mut core)
        .expect("Couldn't send notifications");

    for task_key in update.keys {
        let task_ref = core.get_task_by_key_or_panic(&task_key);
        let mut task = task_ref.get_mut();
        task.subscribe_client(client_id);
    }
}

fn release_keys(core_ref: &CoreRef, client_key: String, task_keys: Vec<String>) {
    let mut core = core_ref.get_mut();
    let client_id = core.get_client_id_by_key(&client_key);
    let mut notifications = Notifications::default();
    for key in task_keys {
        let task_ref = core.get_task_by_key_or_panic(&key).clone();
        let mut task = task_ref.get_mut();

        log::debug!("Unsubscribing task id={}, client={}", task.id, client_key);
        task.unsubscribe_client(client_id);

        if task.check_if_data_cannot_be_removed(&mut notifications) {
            core.remove_task(&task); // TODO: recursively remove dependencies
        }
    }
    notifications.send(&mut core).unwrap();
}

pub async fn gather<W: Sink<DaskPacket, Error = crate::DsError> + Unpin>(
    core_ref: &CoreRef,
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
            .map(|(worker, keys)| super::worker::get_data_from_worker(&worker, &keys)),
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

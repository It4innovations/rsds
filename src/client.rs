use std::collections::HashMap;
use std::convert::TryInto;

use futures::future;
use futures::future::FutureExt;
use futures::sink::SinkExt;
use futures::stream::StreamExt;
use futures::stream::TryStreamExt;
use rmp_serde as rmps;
use tokio::codec::Framed;
use tokio::net::TcpStream;

use crate::daskcodec::{DaskCodec, DaskMessage};
use crate::messages::aframe::AfDescriptor;
use crate::messages::clientmsg::{FromClientMessage, ToClientMessage, UpdateGraphMsg};
use crate::messages::workermsg::{GetDataMsg, GetDataResponse, Status, ToWorkerMessage};
use crate::prelude::*;
use crate::worker::{send_worker_updates, WorkerUpdateMap};

pub type ClientId = u64;

pub struct Client {
    id: ClientId,
    key: String,
    sender: tokio::sync::mpsc::UnboundedSender<ToClientMessage>,
}

impl Client {
    pub fn send_message(&mut self, message: ToClientMessage) {
        self.sender.try_send(message).unwrap();
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

pub async fn start_client(
    core_ref: &CoreRef,
    address: std::net::SocketAddr,
    framed: Framed<TcpStream, DaskCodec>,
    client_key: String,
) -> crate::Result<()> {
    let core_ref = core_ref.clone();
    let core_ref2 = core_ref.clone();

    let (snd_sender, mut snd_receiver) = tokio::sync::mpsc::unbounded_channel::<ToClientMessage>();

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

    log::info!("New client registered as {} from {}", client_id, address);

    let (mut sender, receiver) = framed.split();
    let snd_loop = async move {
        while let Some(data) = snd_receiver.next().await {
            let serialized = rmps::to_vec_named(&data)?;
            if let Err(e) = sender.send(serialized.into()).await {
                log::error!("Send to worker failed");
                return Err(e);
            }
        }
        Ok(())
    }
        .boxed_local();

    let recv_loop = receiver.try_for_each(move |data| {
        let msgs: Result<Vec<FromClientMessage>, _> = rmps::from_read(std::io::Cursor::new(&data.message));
        if let Err(e) = msgs {
            dbg!(data);
            panic!("Invalid message from client ({}): {}", client_id, e);
        }
        for msg in msgs.unwrap() {
            match msg {
                FromClientMessage::HeartbeatClient => { /* TODO, ignore heartbeat now */ }
                FromClientMessage::ClientReleasesKeys(msg) => {
                    release_keys(&core_ref, msg.client, msg.keys);
                }
                FromClientMessage::UpdateGraph(update) => {
                    update_graph(&core_ref, client_id, update);
                }
                _ => {}
            }
        }
        future::ready(Ok(()))
    });

    let result = future::select(recv_loop, snd_loop).await;
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

    /* Validate dependencies */
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

    let mut new_task_ids: HashMap<String, TaskId> = Default::default();

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
            let mut inputs: Vec<_> = deps.iter()
                .map(|key| new_task_ids.get(key).map(|v| *v).unwrap_or_else(|| {
                    core.get_task_by_key_or_panic(key).get().id
                }))
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

        new_tasks.push(task_ref.clone());
        core.add_task(task_ref);
    }

    for task_ref in new_tasks {
        for task_id in &task_ref.get().dependencies {
            let tr = core.get_task_by_id_or_panic(*task_id);
            tr.get_mut().consumers.insert(task_ref.clone());
        }
    }

    for task_key in update.keys {
        let task_ref = core.get_task_by_key_or_panic(&task_key);
        let mut task = task_ref.get_mut();
        task.subscribe_client(client_id);
    }

    core.send_scheduler_update(false);
}

fn release_keys(core_ref: &CoreRef, client_key: String, task_keys: Vec<String>) {
    let mut core = core_ref.get_mut();
    let client_id = core.get_client_id_by_key(&client_key);
    let tasks = task_keys.into_iter().map(|key| core.get_task_by_key_or_panic(&key));
    let mut worker_updates = WorkerUpdateMap::new();
    for task_ref in tasks {
        let mut task = task_ref.get_mut();
        log::debug!("Unsubscribing task id={}, client={}", task.id, client_key);
        task.unsubscribe_client(client_id);
        task.check_if_data_cannot_be_removed(&mut worker_updates);
    }
    send_worker_updates(&core, worker_updates);
}


pub async fn gather(core_ref: &CoreRef,
                    address: std::net::SocketAddr,
                    framed: &mut Framed<TcpStream, DaskCodec>,
                    keys: Vec<String>) -> crate::Result<()> {
    let mut worker_map: HashMap<WorkerRef, Vec<&str>> = Default::default();
    {
        let core = core_ref.get();
        for key in &keys {
            let task_ref = core.get_task_by_key_or_panic(key);
            let worker_ref = task_ref.get().worker.clone().unwrap(); // TODO: Error check if no worker has data
            worker_map.entry(worker_ref).or_default().push(key);
        }
    }
    let mut descriptors = Vec::new();
    let mut frames = vec![Default::default()];

    // TODO: use join to run futures in parallel
    for (worker, keys) in &worker_map {
        let data = super::worker::get_data_from_worker(&worker, &keys).await?;
        let _response: GetDataResponse = rmp_serde::from_slice(&data.message).unwrap_or_else(|e| {
            dbg!(&data.message);
            panic!("Get data response error {:?}", e);
        });
        //assert_eq!(response.status, Status::Ok);
        // TODO: Error when additional frames are empty
        let descriptor: AfDescriptor = rmps::from_slice(&data.additional_frames[0]).unwrap_or_else(|e| {
            dbg!(&data.additional_frames[0]);
            panic!("Get data response af descriptor error {:?}", e);
        });
        descriptors.push(descriptor);
        frames.extend_from_slice(&data.additional_frames[1..]);
    }

    let mut descriptor: AfDescriptor = Default::default();
    for d in descriptors {
        descriptor.headers.extend(d.headers);
        descriptor.keys.extend(d.keys);
    }
    frames[0] = rmps::to_vec_named(&descriptor)?.into();

    let message = rmps::to_vec_named(&GetDataResponse {
        status: "OK".into(), // Status::Ok,
        data: Default::default(),
    })?.into();

    log::debug!("Sending gathered data {}", address);

    framed.send(DaskMessage {
        message,
        additional_frames: frames,
    }).await
}

use std::collections::HashMap;

use futures::future;
use futures::future::FutureExt;
use futures::sink::SinkExt;
use futures::stream::StreamExt;
use futures::stream::TryStreamExt;
use rmp_serde as rmps;
use tokio::codec::Framed;
use tokio::net::TcpStream;

use crate::daskcodec::{DaskCodec, DaskMessage};
use crate::messages::aframe::{AdditionalFrame, AfDescriptor, group_aframes, parse_aframes};
use crate::messages::clientmsg::{FromClientMessage, ToClientMessage, UpdateGraphMsg};
use crate::messages::workermsg::{GetDataMsg, GetDataResponse, Status, ToWorkerMessage};
use crate::notifications::Notifications;
use crate::prelude::*;

pub type ClientId = u64;

pub struct Client {
    id: ClientId,
    key: String,
    sender: tokio::sync::mpsc::UnboundedSender<DaskMessage>,
}

impl Client {
    pub fn send_message(&mut self, message: ToClientMessage) -> crate::Result<()> {
        let data = rmp_serde::encode::to_vec_named(&message).unwrap();
        self.send_dask_message(data.into())
    }

    pub fn send_dask_message(&mut self, dask_message: DaskMessage) -> crate::Result<()> {
        self.sender.try_send(dask_message).unwrap(); // TODO: bail!("Send to client XXX failed")
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

pub async fn start_client(
    core_ref: &CoreRef,
    address: std::net::SocketAddr,
    framed: Framed<TcpStream, DaskCodec>,
    client_key: String,
) -> crate::Result<()> {
    let core_ref = core_ref.clone();
    let core_ref2 = core_ref.clone();

    let (snd_sender, mut snd_receiver) = tokio::sync::mpsc::unbounded_channel::<DaskMessage>();

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
            if let Err(e) = sender.send(data).await {
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

        let mut aframes = parse_aframes(data.additional_frames);

        for (i, msg) in msgs.unwrap().into_iter().enumerate() {
            match msg {
                FromClientMessage::HeartbeatClient => { /* TODO, ignore heartbeat now */ }
                FromClientMessage::ClientReleasesKeys(msg) => {
                    release_keys(&core_ref, msg.client, msg.keys);
                }
                FromClientMessage::UpdateGraph(update) => {
                    update_graph(&core_ref, client_id, update, aframes.remove(&(i as u64)).unwrap_or_else(Vec::new));
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

pub fn update_graph(core_ref: &CoreRef, client_id: ClientId, update: UpdateGraphMsg, aframes: Vec<AdditionalFrame>) {
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

    let mut notifications = Notifications::new();

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
        core.add_task(task_ref, &mut notifications);
    }

    notifications.send(&mut core);

    for task_ref in new_tasks {
        for task_id in &task_ref.get().dependencies {
            let tr = core.get_task_by_id_or_panic(*task_id);
            tr.get_mut().consumers.insert(task_ref.clone());
        }
    }

    let groupped_task_frames = group_aframes(aframes, |key| {
        if key.len() > 3 && Some("tasks") == key[1].as_str() {
            key[2].as_str().map(|s| s.to_string())
        } else {
            None
        }
    });

    for (task_key, aframes) in groupped_task_frames {
        let task_ref = core.get_task_by_key_or_panic(&task_key);
        let mut task = task_ref.get_mut();
        for AdditionalFrame { data, header, key } in aframes {
            match key.get(3).and_then(|x| x.as_str()) {
                Some("args") => {
                    task.args_data = data;
                    task.args_header = Some(header);
                }
                x => { panic!("Invalid key part {:?}", x) }
            };
        }
    }

    for task_key in update.keys {
        let task_ref = core.get_task_by_key_or_panic(&task_key);
        let mut task = task_ref.get_mut();
        task.subscribe_client(client_id);
    }

    //core.send_scheduler_update(false);
}

fn release_keys(core_ref: &CoreRef, client_key: String, task_keys: Vec<String>) {
    let mut core = core_ref.get_mut();
    let client_id = core.get_client_id_by_key(&client_key);
    let tasks = task_keys.into_iter().map(|key| core.get_task_by_key_or_panic(&key));
    let mut notifications = Notifications::new();
    for task_ref in tasks {
        let mut task = task_ref.get_mut();
        log::debug!("Unsubscribing task id={}, client={}", task.id, client_key);
        task.unsubscribe_client(client_id);
        task.check_if_data_cannot_be_removed(&mut notifications);
    }
    notifications.send(&mut core);
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
            // TODO: Randomize workers
            task_ref.get().get_workers().map(|ws| ws.get(0).map(|w| {
                worker_map.entry(w.clone()).or_default().push(key);
            }));
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

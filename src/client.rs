use crate::daskcodec::DaskCodec;
use crate::messages::clientmsg::{FromClientMessage, UpdateGraphMsg, ToClientMessage};
use crate::prelude::*;
use futures::future;
use futures::future::FutureExt;
use futures::sink::SinkExt;
use futures::stream::StreamExt;
use futures::stream::TryStreamExt;

use rmp_serde as rmps;
use std::collections::HashMap;
use std::rc::Rc;
use tokio::codec::Framed;
use tokio::net::TcpStream;
use tokio::runtime::current_thread;
use crate::messages::workermsg::{ToWorkerMessage, GetDataMsg, GetDataResponse, Status};
use crate::messages::generic::SimpleMessage;

pub type ClientId = u64;

pub struct Client {
    id: ClientId,
    id_string: String,
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
}

pub async fn start_client(
    core_ref: &CoreRef,
    address: std::net::SocketAddr,
    framed: Framed<TcpStream, DaskCodec>,
    id_string: String,
) -> crate::Result<()> {
    let core_ref = core_ref.clone();
    let core_ref2 = core_ref.clone();

    let (snd_sender, mut snd_receiver) = tokio::sync::mpsc::unbounded_channel::<ToClientMessage>();

    let client_id = {
        let mut core = core_ref.get_mut();
        let client_id = core.new_client_id();
        let client = Client {
            id: client_id,
            id_string,
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
                FromClientMessage::ClientReleasesKeys(_) => {
                    /* TODO */
                    println!("Releasing keys");
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
    for vals in update.dependencies.values() {
        for key in vals {
            if !update.tasks.contains_key(key) {
                // TODO: Error handling
                panic!("Invalid key in dependecies: {}", key);
            }
        }
    }

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
        let inputs: Vec<_> = if let Some(deps) = update.dependencies.get(&task_key) {
            deps.iter()
                .map(|key| *new_task_ids.get(key).unwrap())
                .collect()
        } else {
            Vec::new()
        };
        let unfinished_deps = inputs.len() as u32;

        log::debug!("New task id={}, key={}", task_id, task_key);
        let task_ref = TaskRef::new(task_id, task_key, task_spec, inputs, unfinished_deps);

        new_tasks.push(task_ref.clone());
        if unfinished_deps == 0 {
            core.set_task_state_changed(task_ref.clone());
        }
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
        task.subscribed_clients.insert(client_id);
    }

    core.send_scheduler_update();
}

async fn get_data_from_worker<'a, 'b>(worker: &'a WorkerRef, keys: &'b Vec<&str>) -> crate::Result<HashMap<&'b str, Bytes>>
{
    unimplemented!();
    /*
    let connection = TcpStream::connect(&worker.get().listen_address.trim_start_matches("tcp://")).await?;
    let mut connection = Framed::new(connection, DaskCodec::new());

    // send get data request
    let msg = ToWorkerMessage::GetData(GetDataMsg {
        keys,
        who: None,
        max_connections: false,
        reply: true
    });
    connection.send(rmp_serde::to_vec_named(&msg)?.into()).await?;

    let response = connection.next().await;
    match response {
        Some(data) => {
            let data = data?;
            println!("{:?}", data.message);
            let response: GetDataResponse = rmp_serde::from_slice(&data.message).unwrap();
            assert_eq!(response.status, "OK");
            println!("{:?}", data.additional_frames);
            panic!();
            connection.send("OK".into()).await?;
        }
        None => unimplemented!()
    }

    Ok(Default::default())*/
}

pub async fn start_gather(core_ref: &CoreRef,
                          address: std::net::SocketAddr,
                          mut framed: Framed<TcpStream, DaskCodec>,
                          keys: Vec<String>) -> crate::Result<()> {
    unimplemented!();
    /*let mut worker_map: HashMap<WorkerRef, Vec<&str>> = Default::default();
    let core = core_ref.get();
    for key in &keys {
        let task_ref = core.get_task_by_key_or_panic(key);
        let worker_ref = task_ref.get().worker.unwrap(); // TODO: Error check
        worker_map.entry(worker).or_default().push(key);
    }

    let mut result: HashMap<&str, Bytes> = Default::default();

    // TODO: use join to run futures in parallel
    for (worker, keys) in &worker_map {
        let worker_result = get_data_from_worker(&worker, &keys).await?;
        for (key, data) in worker_result {
            result.insert(key, data);
        }
    }*/

    Ok(())
}

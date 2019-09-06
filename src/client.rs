use crate::daskcodec::DaskCodec;
use crate::messages::clientmsg::{ClientMessage, UpdateGraphMsg};
use crate::prelude::*;
use futures::future;
use futures::future::FutureExt;
use futures::sink::SinkExt;
use futures::stream::StreamExt;
use futures::stream::{TryStreamExt};

use rmp_serde as rmps;
use std::rc::Rc;
use tokio::codec::Framed;
use tokio::net::TcpStream;
use tokio::runtime::current_thread;

pub type ClientId = u64;

pub struct Client {
    id: ClientId,
    id_string: String,
    sender: tokio::sync::mpsc::UnboundedSender<Bytes>,
}

impl Client {
    pub fn send_message(&mut self, message: Bytes) {
        self.sender.try_send(message).unwrap();
    }

    #[inline]
    pub fn id(&self) -> ClientId {
        self.id
    }
}

pub fn start_client(
    core_ref: &CoreRef,
    address: std::net::SocketAddr,
    framed: Framed<TcpStream, DaskCodec>,
    id_string: String,
) {
    let core_ref = core_ref.clone();
    let core_ref2 = core_ref.clone();

    let (snd_sender, mut snd_receiver) = tokio::sync::mpsc::unbounded_channel::<Bytes>();

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
        loop {
            if let Some(data) = snd_receiver.next().await {
                if let Err(e) = sender.send(data).await {
                    log::error!("Send to worker failed");
                    return Err(e);
                }
            } else {
                return Ok(());
            }
        }
    }
        .boxed();

    //let snd_loop = forward(snd_receiver, sender).boxed();

    let recv_loop = receiver.try_for_each(move |data| {
        let msgs: Result<Vec<ClientMessage>, _> = rmps::from_read(std::io::Cursor::new(&data));
        if let Err(e) = msgs {
            dbg!(data);
            panic!("Invalid message from client ({}): {}", client_id, e);
        }
        for msg in msgs.unwrap() {
            match msg {
                ClientMessage::HeartbeatClient => { /* TODO, ignore heartbeat now */ }
                ClientMessage::ClientReleasesKeys(_) => {
                    /* TODO */
                    println!("Releasing keys");
                }
                ClientMessage::UpdateGraph(update) => {
                    update_graph(&core_ref, client_id, update);
                }
            }
        }
        future::ready(Ok(()))
    });

    current_thread::spawn(future::select(recv_loop, snd_loop).map(move |r| {
        if let Err(e) = r.factor_first().0 {
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
    }));
}

pub fn update_graph(core_ref: &CoreRef, client_id: ClientId, mut update: UpdateGraphMsg) {
    log::debug!("Updating graph from client {}", client_id);

    /* Validate dependencies */
    for vals in update.dependencies.values() {
        for key in vals {
            if !update.tasks.contains_key(key) {
                // TOOD: Error handling
                panic!("Invalid key in dependecies: {}", key);
            }
        }
    }

    let mut core = core_ref.get_mut();
    let mut new_tasks = Vec::with_capacity(update.tasks.len());

    for (task_key, task_spec) in update.tasks {
        let deps = update
            .dependencies
            .remove(&task_key)
            .unwrap_or_else(Vec::new);
        let unfinished_deps = deps.len() as u32;
        let task_rc = Rc::new(Task::new(task_key, task_spec, deps, unfinished_deps));

        new_tasks.push(TaskRef::new(task_rc.clone()));
        core.add_task(task_rc);
    }

    for task_ref in new_tasks {
        for task_key in &task_ref.get().dependencies {
            let tr = core.get_task_or_panic(task_key);
            tr.info.borrow_mut().consumers.insert(task_ref.clone());
        }
    }
}

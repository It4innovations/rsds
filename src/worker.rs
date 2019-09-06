use crate::common::WrappedRcRefCell;
use crate::daskcodec::DaskCodec;
use crate::messages::workermsg::{HeartbeatResponse, WorkerMessage};
use crate::prelude::*;
use futures::future;
use futures::future::FutureExt;
use futures::sink::SinkExt;
use futures::stream::StreamExt;
use futures::stream::{TryStreamExt};

use rmp_serde as rmps;
use tokio::codec::Framed;
use tokio::net::TcpStream;
use tokio::runtime::current_thread;

pub type WorkerId = u64;

pub struct Worker {
    id: WorkerId,
    sender: tokio::sync::mpsc::UnboundedSender<Bytes>,
}

impl Worker {
    #[inline]
    pub fn id(&self) -> WorkerId {
        self.id
    }
}

pub type WorkerRef = WrappedRcRefCell<Worker>;

pub fn start_worker(
    core_ref: &CoreRef,
    address: std::net::SocketAddr,
    framed: Framed<TcpStream, DaskCodec>,
) {
    let core_ref = core_ref.clone();
    let core_ref2 = core_ref.clone();

    let (mut snd_sender, mut snd_receiver) = tokio::sync::mpsc::unbounded_channel::<Bytes>();

    let hb = HeartbeatResponse {
        status: "OK",
        time: 0.0,
        heartbeat_interval: 1.0,
        worker_plugins: Vec::new(),
    };
    let data = rmp_serde::encode::to_vec_named(&hb).unwrap();
    snd_sender.try_send(data.into()).unwrap();

    let worker_id = {
        let mut core = core_ref.get_mut();
        let worker_id = core.new_worker_id();
        let worker = WorkerRef::wrap(Worker {
            id: worker_id,
            sender: snd_sender,
        });
        core.register_worker(worker);
        worker_id
    };

    log::info!("New worker registered as {} from {}", worker_id, address);

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

    let recv_loop = receiver.try_for_each(move |data| {
        let msgs: Result<Vec<WorkerMessage>, _> = rmps::from_read(std::io::Cursor::new(&data));
        if let Err(e) = msgs {
            dbg!(data);
            panic!("Invalid message from worker ({}): {}", worker_id, e);
        }
        for msg in msgs.unwrap() {
            match msg {
                WorkerMessage::KeepAlive => { /* Do nothing by design */ }
            }
        }
        future::ready(Ok(()))
    });

    current_thread::spawn(future::select(recv_loop, snd_loop).map(move |r| {
        if let Err(e) = r.factor_first().0 {
            log::error!(
                "Error in worker connection (id={}, connection={}): {}",
                worker_id,
                address,
                e
            );
        }
        log::info!(
            "Worker {} connection closed (connection: {})",
            worker_id,
            address
        );
        let mut core = core_ref2.get_mut();
        core.unregister_worker(worker_id);
    }));
}

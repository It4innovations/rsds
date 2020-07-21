use crate::common::WrappedRcRefCell;

use crate::scheduler::protocol::WorkerInfo;
use crate::server::core::Core;
use crate::server::protocol::dasktransport::DaskPacket;
use crate::server::protocol::key::{DaskKey, DaskKeyRef};
use crate::server::protocol::messages::worker::ToWorkerMessage;
use bytes::Bytes;
use std::str;
use tokio::sync::mpsc::UnboundedSender;

pub type WorkerId = u64;

#[derive(Debug)]
pub enum WorkerMessage {
    Dask(DaskPacket),
    Rsds(Bytes),
}

#[derive(Debug)]
pub struct Worker {
    pub id: WorkerId,
    pub sender: UnboundedSender<WorkerMessage>,
    pub ncpus: u32,
    pub listen_address: DaskKey,
}

impl Worker {
    #[inline]
    pub fn id(&self) -> WorkerId {
        self.id
    }

    #[inline]
    pub fn key(&self) -> &DaskKeyRef {
        &self.listen_address
    }

    #[inline]
    pub fn address(&self) -> &DaskKeyRef {
        &self.listen_address
    }

    pub fn hostname(&self) -> String {
        let s = self.listen_address.as_str();
        let s: &str = s.find("://").map(|p| &s[p + 3..]).unwrap_or(s);
        s.chars().take_while(|x| *x != ':').collect()
    }

    pub fn make_sched_info(&self) -> WorkerInfo {
        WorkerInfo {
            id: self.id,
            n_cpus: self.ncpus,
            hostname: self.hostname(),
        }
    }

    pub fn send_dask_message(&self, packet: DaskPacket) {
        self.send_message(WorkerMessage::Dask(packet))
    }

    pub fn send_rsds_message(&self, message: ToWorkerMessage) {
        let data = rmp_serde::to_vec_named(&message).unwrap();
        self.send_message(WorkerMessage::Rsds(data.into()))
    }

    fn send_message(&self, data: WorkerMessage) {
        self.sender.send(data).expect("Send to worker failed");
    }
}

pub type WorkerRef = WrappedRcRefCell<Worker>;

impl WorkerRef {
    pub fn new(
        id: WorkerId,
        ncpus: u32,
        sender: UnboundedSender<WorkerMessage>,
        listen_address: DaskKey,
    ) -> Self {
        Self::wrap(Worker {
            id,
            ncpus,
            sender,
            listen_address,
        })
    }
}

pub(crate) fn create_worker(
    core: &mut Core,
    sender: UnboundedSender<WorkerMessage>,
    address: DaskKey,
    ncpus: u32,
) -> WorkerRef {
    let worker_ref = WorkerRef::new(core.new_worker_id(), ncpus, sender, address);
    core.register_worker(worker_ref.clone());
    worker_ref
}

use crate::common::WrappedRcRefCell;

use crate::server::protocol::key::{DaskKey, DaskKeyRef};
use std::str;
use bytes::Bytes;
use crate::server::protocol::messages::worker::ToWorkerMessage;

pub type WorkerId = u64;

#[derive(Debug)]
pub struct Worker {
    pub id: WorkerId,
    pub sender: tokio::sync::mpsc::UnboundedSender<Bytes>,
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

    pub fn make_sched_info(&self) -> crate::scheduler::protocol::WorkerInfo {
        crate::scheduler::protocol::WorkerInfo {
            id: self.id,
            n_cpus: self.ncpus,
            hostname: self.hostname(),
        }
    }

    pub fn send_message(&self, message: ToWorkerMessage) {
        let data = rmp_serde::to_vec_named(&message).unwrap();
        self.sender.send(data.into()).expect("Send to worker failed");
    }
}

pub type WorkerRef = WrappedRcRefCell<Worker>;

impl WorkerRef {
    pub fn new(
        id: WorkerId,
        ncpus: u32,
        sender: tokio::sync::mpsc::UnboundedSender<Bytes>,
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
/*
pub(crate) fn create_worker(
    core: &mut Core,
    sender: tokio::sync::mpsc::UnboundedSender<DaskPacket>,
    address: DaskKey,
    ncpus: u32,
) -> WorkerRef {
    let worker_ref = WorkerRef::new(core.new_worker_id(), ncpus, sender, address);
    core.register_worker(worker_ref.clone());
    worker_ref
}*/

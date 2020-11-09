use std::str;

use bytes::Bytes;
use tokio::sync::mpsc::UnboundedSender;

use crate::common::WrappedRcRefCell;
use crate::scheduler::protocol::WorkerInfo;

use crate::server::protocol::key::{DaskKey, DaskKeyRef};
use crate::server::protocol::messages::worker::ToWorkerMessage;

pub type WorkerId = u64;

#[derive(Debug)]
pub struct Worker {
    pub id: WorkerId,
    pub sender: UnboundedSender<Bytes>,
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

    /*pub fn send_dask_message(&self, packet: DaskPacket) {
        self.send_message(WorkerMessage::Dask(packet))
    }*/

    pub fn send_message(&self, message: ToWorkerMessage) {
        let data = rmp_serde::to_vec_named(&message).unwrap();
        self.sender
            .send(data.into())
            .expect("Send to worker failed");
    }

    /*fn send_message(&self, data: WorkerMessage) {
        self.sender.send(data).expect("Send to worker failed");
    }*/
}

pub type WorkerRef = WrappedRcRefCell<Worker>;

impl WorkerRef {
    pub fn new(
        id: WorkerId,
        ncpus: u32,
        sender: UnboundedSender<Bytes>,
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

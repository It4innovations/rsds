use crate::common::WrappedRcRefCell;
use crate::core::Core;

use crate::protocol::key::{DaskKey, DaskKeyRef};
use crate::protocol::protocol::DaskPacket;
use std::str;

pub type WorkerId = u64;

#[derive(Debug)]
pub struct Worker {
    pub id: WorkerId,
    pub sender: tokio::sync::mpsc::UnboundedSender<DaskPacket>,
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

    pub fn make_sched_info(&self) -> crate::scheduler::schedproto::WorkerInfo {
        crate::scheduler::schedproto::WorkerInfo {
            id: self.id,
            n_cpus: self.ncpus,
            hostname: self.hostname(),
        }
    }

    pub fn send_dask_packet(&mut self, message: DaskPacket) -> crate::Result<()> {
        self.sender.send(message).expect("Send to worker failed");
        Ok(())
    }
}

pub type WorkerRef = WrappedRcRefCell<Worker>;

impl WorkerRef {
    pub fn new(
        id: WorkerId,
        ncpus: u32,
        sender: tokio::sync::mpsc::UnboundedSender<DaskPacket>,
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
    sender: tokio::sync::mpsc::UnboundedSender<DaskPacket>,
    address: DaskKey,
    ncpus: u32,
) -> WorkerRef {
    let worker_ref = WorkerRef::new(core.new_worker_id(), ncpus, sender, address);
    core.register_worker(worker_ref.clone());
    worker_ref
}

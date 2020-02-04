use crate::common::WrappedRcRefCell;
use crate::core::CoreRef;

use crate::protocol::protocol::DaskPacket;

pub type WorkerId = u64;

pub struct Worker {
    pub id: WorkerId,
    pub sender: tokio::sync::mpsc::UnboundedSender<DaskPacket>,
    pub ncpus: u32,
    pub listen_address: String,
}

impl Worker {
    #[inline]
    pub fn id(&self) -> WorkerId {
        self.id
    }

    #[inline]
    pub fn key(&self) -> &str {
        &self.listen_address
    }

    #[inline]
    pub fn address(&self) -> &str {
        &self.listen_address
    }

    pub fn make_sched_info(&self) -> crate::scheduler::schedproto::WorkerInfo {
        crate::scheduler::schedproto::WorkerInfo {
            id: self.id,
            n_cpus: self.ncpus,
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
        listen_address: String,
    ) -> Self {
        WorkerRef::wrap(Worker {
            id,
            ncpus,
            sender,
            listen_address,
        })
    }
}

pub(crate) fn create_worker(
    core_ref: &CoreRef,
    sender: tokio::sync::mpsc::UnboundedSender<DaskPacket>,
    address: String,
) -> (WorkerId, WorkerRef) {
    let mut core = core_ref.get_mut();
    let worker_id = core.new_worker_id();

    // TODO: real cpus
    let worker_ref = WorkerRef::new(worker_id, 1, sender, address);
    core.register_worker(worker_ref.clone());

    (worker_id, worker_ref)
}

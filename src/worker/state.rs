use crate::common::{Set, WrappedRcRefCell};
use crate::protocol::key::DaskKey;
use crate::protocol::protocol::DaskPacket;
use tokio::sync::mpsc::UnboundedSender;

pub type WorkerStateRef = WrappedRcRefCell<WorkerState>;

pub struct WorkerState {
    pub sender: UnboundedSender<DaskPacket>,
    pub local_keys: Set<DaskKey>,
    pub ncpus: u32,
    pub listen_address: String,
}

impl WorkerState {
    pub fn send(&self, packet: DaskPacket) {
        self.sender.send(packet).unwrap();
    }
}

impl WorkerStateRef {
    pub fn new(sender: UnboundedSender<DaskPacket>, ncpus: u32, listen_address: String) -> Self {
        Self::wrap(WorkerState {
            sender,
            ncpus,
            listen_address,
            local_keys: Default::default(),
        })
    }
}

use futures::sink::SinkExt;

use tokio::net::TcpStream;

use crate::common::WrappedRcRefCell;
use crate::core::CoreRef;
use crate::notifications::Notifications;
use crate::protocol::generic::RegisterWorkerMsg;
use crate::protocol::protocol::{serialize_batch_packet, Batch, DaskPacket};
use crate::protocol::workermsg::{Status, WorkerState};
use crate::protocol::workermsg::{FromWorkerMessage, ToWorkerMessage};
use crate::task::ErrorInfo;
use futures::{FutureExt, Sink, Stream, StreamExt};

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

    pub fn make_sched_info(&self) -> crate::scheduler::schedproto::WorkerInfo {
        crate::scheduler::schedproto::WorkerInfo {
            id: self.id,
            n_cpus: self.ncpus,
        }
    }

    pub fn send_message(&mut self, messages: Batch<ToWorkerMessage>) -> crate::Result<()> {
        log::debug!("Worker send message {:?}", messages);
        self.send_dask_message(serialize_batch_packet(messages)?)
    }

    pub fn send_dask_message(&mut self, message: DaskPacket) -> crate::Result<()> {
        self.sender.send(message).unwrap(); // TODO: bail!("Send of worker XYZ failed")
        Ok(())
    }
}

pub type WorkerRef = WrappedRcRefCell<Worker>;

impl WorkerRef {
    pub async fn connect(&self) -> crate::Result<tokio::net::TcpStream> {
        // a copy is needed to avoid runtime Borrow errors
        let address: String = {
            self.get()
                .listen_address
                .trim_start_matches("tcp://")
                .to_owned()
        };
        Ok(TcpStream::connect(address).await?)
    }
}

pub(crate) fn create_worker(
    msg: RegisterWorkerMsg,
    core_ref: &CoreRef,
    sender: tokio::sync::mpsc::UnboundedSender<DaskPacket>,
) -> (WorkerId, WorkerRef) {
    let mut core = core_ref.get_mut();
    let worker_id = core.new_worker_id();
    let worker_ref = WorkerRef::wrap(Worker {
        id: worker_id,
        ncpus: 1, // TODO: real cpus
        sender,
        listen_address: msg.address,
    });
    let mut notifications = Notifications::default();
    core.register_worker(worker_ref.clone(), &mut notifications);
    notifications.send(&mut core).unwrap();
    (worker_id, worker_ref)
}

pub async fn execute_worker<
    Reader: Stream<Item = crate::Result<Batch<FromWorkerMessage>>> + Unpin,
    Writer: Sink<DaskPacket, Error = crate::DsError> + Unpin,
>(
    core_ref: &CoreRef,
    address: std::net::SocketAddr,
    mut receiver: Reader,
    mut sender: Writer,
    msg: RegisterWorkerMsg,
) -> crate::Result<()> {
    let core_ref2 = core_ref.clone();
    let (queue_sender, mut queue_receiver) = tokio::sync::mpsc::unbounded_channel::<DaskPacket>();

    let (worker_id, worker_ref) = create_worker(msg, core_ref, queue_sender);

    log::info!("Worker {} registered from {}", worker_id, address);

    let snd_loop = async move {
        while let Some(data) = queue_receiver.next().await {
            if let Err(e) = sender.send(data).await {
                log::error!("Send to worker failed");
                return Err(e);
            }
        }
        Ok(())
    };

    let recv_loop = async move {
        'outer: while let Some(messages) = receiver.next().await {
            let mut notifications = Notifications::default();

            let messages = messages?;
            for message in messages {
                log::debug!("Worker recv message {:?}", message);
                match message {
                    FromWorkerMessage::TaskFinished(msg) => {
                        assert_eq!(msg.status, Status::Ok); // TODO: handle other cases ??
                        let mut core = core_ref.get_mut();
                        core.on_task_finished(&worker_ref, msg, &mut notifications);
                    }
                    FromWorkerMessage::AddKeys(msg) => {
                        let mut core = core_ref.get_mut();
                        core.on_tasks_transferred(&worker_ref, msg.keys, &mut notifications);
                    }
                    FromWorkerMessage::TaskErred(msg) => {
                        assert_eq!(msg.status, Status::Error); // TODO: handle other cases ??
                        let error_info = ErrorInfo {
                            exception: msg.exception,
                            traceback: msg.traceback,
                        };
                        let mut core = core_ref.get_mut();
                        core.on_task_error(&worker_ref, msg.key, error_info, &mut notifications);
                        // TODO: Inform scheduler
                    },
                    FromWorkerMessage::StealResponse(msg) => {
                        let mut core = core_ref.get_mut();
                        core.on_steal_response(&worker_ref, msg, &mut notifications);
                    }
                    FromWorkerMessage::KeepAlive => { /* Do nothing by design */ }
                    FromWorkerMessage::Unregister => break 'outer,
                }
            }
            let mut core = core_ref.get_mut();
            notifications.send(&mut core).unwrap();
        }
        Ok(())
    };

    /*if !new_ready_scheduled.is_empty() {
        let mut tasks_per_worker: Map<WorkerRef, Vec<TaskRef>> = Default::default();
        for task_ref in new_ready_scheduled {
            let worker = {
                let mut task = task_ref.get_mut();
                let worker_ref = task.worker.clone().unwrap();
                task.state = TaskRuntimeState::Assigned;
                log::debug!("Task id={} assigned to worker={}", task.id, worker_ref.get().id);
                worker_ref
            };
            let v = tasks_per_worker.entry(worker).or_insert_with(Vec::new);
            v.push(task_ref);
        }
        let core = core_ref.get();
        send_tasks_to_workers(&core, tasks_per_worker);
    }*/

    let result = futures::future::select(recv_loop.boxed_local(), snd_loop.boxed_local()).await;
    if let Err(e) = result.factor_first().0 {
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
    Ok(())
}

use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::Stdio;

use bytes::Bytes;
use futures::stream::{SplitSink, SplitStream};
use futures::{Future, FutureExt, StreamExt, SinkExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::process::Command;
use tokio::sync::oneshot;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use crate::common::transport::make_protocol_builder;
use crate::common::{WrappedRcRefCell};
use crate::worker::messages::{ComputeTaskMsg, ToSubworkerMessage, FromSubworkerMessage, Upload, RegisterSubworkerResponse};
use crate::worker::task::{Task, TaskRef};
use crate::worker::messages;

use super::messages::RegisterSubworkerMessage;
use crate::worker::state::WorkerStateRef;
use crate::worker::data::{DataObjectRef, DataObjectState, LocalData};
use crate::server::protocol::messages::worker::FromWorkerMessage::TaskFinished;
use crate::server::protocol::messages::worker::{FromWorkerMessage, TaskFinishedMsg, TaskFailedMsg};
use std::rc::Rc;
use crate::worker::reactor::try_start_tasks;

pub(crate) type SubworkerId = u32;

pub struct Subworker {
    pub id: SubworkerId,
    pub sender: tokio::sync::mpsc::UnboundedSender<Bytes>,
    pub running_task: Option<TaskRef>,
}

pub type SubworkerRef = WrappedRcRefCell<Subworker>;

impl Subworker {
    pub fn start_task(&self, task: &Task) {
        let uploads : Vec<Upload> = task.deps.iter().map(|data_ref| {
            let data_obj = data_ref.get();
            Upload { key: data_obj.key.clone(),
                     serializer: data_obj.local_data().unwrap().serializer.clone() }
        }).collect();

        log::debug!("Starting task {} in subworker {} ({} uploads)", task.key, self.id, uploads.len());
        // Send message to subworker
        let message = ToSubworkerMessage::ComputeTask(ComputeTaskMsg {
            key: &task.key,
            function: &task.function,
            args: &task.args,
            kwargs: &task.kwargs,
            uploads,
        });
        let data = rmp_serde::to_vec_named(&message).unwrap();
        self.sender.send(data.into()).unwrap();

        for data_ref in &task.deps {
            let data_obj = data_ref.get();
            self.sender.send(data_obj.local_data().unwrap().bytes.clone()).unwrap();
        }
    }
}

impl SubworkerRef {
    pub fn new(id: SubworkerId, sender: tokio::sync::mpsc::UnboundedSender<Bytes>) -> Self {
        Self::wrap(Subworker {
            id,
            sender,
            running_task: None,
        })
    }
}

async fn subworker_handshake(
    state_ref: WorkerStateRef,
    mut listener: UnixListener,
    subworker_id: SubworkerId,
) -> Result<
    (
        SplitSink<Framed<UnixStream, LengthDelimitedCodec>, Bytes>,
        SplitStream<Framed<UnixStream, LengthDelimitedCodec>>,
    ),
    crate::Error,
> {
    if let Some(Ok(stream)) = listener.next().await {
        let mut framed = make_protocol_builder().new_framed(stream);
        let message = framed.next().await;

        if message.is_none() {
            panic!("Subworker did not sent register message");
        }
        let message = message.unwrap().unwrap();
        let register_message: RegisterSubworkerMessage = rmp_serde::from_slice(&message).unwrap();

        if register_message.subworker_id != subworker_id {
            panic!("Subworker registered with an invalid id");
        }
        
        let message = RegisterSubworkerResponse {
            worker: state_ref.get().listen_address.clone().into(),
        };
        framed.send(rmp_serde::to_vec_named(&message).unwrap().into()).await.unwrap();
        
        Ok(framed.split())
    } else {
        panic!("Listening on subworker socket failed");
    }
}


fn subworker_task_finished(state_ref: &WorkerStateRef, subworker_ref: &SubworkerRef, msg: messages::TaskFinishedMsg) {
    let mut state = state_ref.get_mut();
    {
        let mut sw = subworker_ref.get_mut();
        log::debug!("Task {} finished in subworker {}", msg.key, sw.id);
        let task_ref = sw.running_task.take().unwrap();
        state.free_subworkers.push(subworker_ref.clone());
        assert_eq!(task_ref.get().key, msg.key);
        task_ref.get_mut().set_done(&task_ref);
        state.remove_task(task_ref);

        let message = FromWorkerMessage::TaskFinished(TaskFinishedMsg {
            key: msg.key.clone(),
            nbytes: msg.result.len() as u64,
        });
        state.send_message_to_server(rmp_serde::to_vec_named(&message).unwrap());


        let data_ref = DataObjectRef::new(
            msg.key,
            msg.result.len() as u64,
            DataObjectState::Local(LocalData {
                serializer: msg.serializer,
                bytes: msg.result.into(),
            }));
        state.add_data_object(data_ref);
    }
    try_start_tasks(&mut state);
}


fn subworker_task_fail(state_ref: &WorkerStateRef, subworker_ref: &SubworkerRef, msg: messages::TaskFailedMsg) {
    let mut state = state_ref.get_mut();
    {
        let mut sw = subworker_ref.get_mut();
        log::debug!("Task {} failed in subworker {}", msg.key, sw.id);
        let task_ref = sw.running_task.take().unwrap();
        state.free_subworkers.push(subworker_ref.clone());
        assert_eq!(task_ref.get().key, msg.key);
        task_ref.get_mut().set_done(&task_ref);
        state.remove_task(task_ref);

        let message = FromWorkerMessage::TaskFailed(TaskFailedMsg {
            key: msg.key.clone(),
            exception: msg.exception,
            traceback: msg.traceback,
        });
        state.send_message_to_server(rmp_serde::to_vec_named(&message).unwrap());
    }
    try_start_tasks(&mut state);
}


async fn run_subworker_message_loop(
    state_ref: WorkerStateRef,
    subworker_ref: SubworkerRef,
    mut stream: SplitStream<Framed<UnixStream, LengthDelimitedCodec>>,
) -> crate::Result<()> {
    while let Some(message) = stream.next().await {
        let message: FromSubworkerMessage = rmp_serde::from_slice(&message?)?;
        match message {
            FromSubworkerMessage::TaskFinished(msg) => {
                subworker_task_finished(&state_ref, &subworker_ref, msg);
            }
            FromSubworkerMessage::TaskFailed(msg) => {
                subworker_task_fail(&state_ref, &subworker_ref, msg);
            }
        };
    }
    Ok(())
}

async fn run_subworker(
    state_ref: WorkerStateRef,
    work_dir: PathBuf,
    python_program: String,
    subworker_id: SubworkerId,
    ready_shot: oneshot::Sender<SubworkerRef>,
) -> Result<(), crate::Error> {
    let mut socket_path = work_dir.clone();
    socket_path.push(format!("subworker-{}.sock", subworker_id));

    let listener = UnixListener::bind(&socket_path)?;

    let mut log_path = work_dir;
    log_path.push(format!("subworker-{}.log", subworker_id));
    let mut process_future = {
        let log_stdout = File::create(&log_path)?;
        let log_stderr = log_stdout.try_clone()?;

        Command::new(python_program)
            .stdout(Stdio::from(log_stdout))
            .stderr(Stdio::from(log_stderr))
            .env("RSDS_SUBWORKER_SOCKET", &socket_path)
            .env("RSDS_SUBWORKER_ID", format!("{}", subworker_id))
            .args(&["-m", "rsds.subworker"])
            .spawn()?
    };

    std::mem::drop(socket_path);

    let (writer, reader) = tokio::select! {
        result = &mut process_future => {
            panic!("Subworker {} failed without registration: {}, see {}", subworker_id, result?, log_path.display());
        },
        result = subworker_handshake(state_ref.clone(), listener, subworker_id) => {
            result?
        }
    };

    let (queue_sender, queue_receiver) = tokio::sync::mpsc::unbounded_channel::<Bytes>();

    // TODO: pass writing end
    let subworker = SubworkerRef::new(subworker_id, queue_sender);
    if let Err(_) = ready_shot.send(subworker.clone()) {
        panic!("Announcing subworker failed");
    }

    tokio::select! {
        result = process_future => {
            panic!("Subworker {} failed: {}, see {}", subworker_id, result?, log_path.display());
        },
        result = crate::common::rpc::forward_queue_to_sink(queue_receiver, writer) => {
            panic!("Sending a message to subworker failed");
        }
        r = run_subworker_message_loop(state_ref, subworker, reader) => {
            match r {
                Err(e) => panic!("Subworker {} loop failed: {}, log: {}", subworker_id, e, log_path.display()),
                Ok(()) => panic!("Subworker {} closed stream, see {}", subworker_id, log_path.display()),
            }

        }
    };

    Ok(())
}

pub async fn start_subworkers(
    state: &WorkerStateRef,
    work_dir: &Path,
    python_program: &str,
    count: u32,
) -> Result<(Vec<SubworkerRef>, impl Future<Output = usize>), crate::Error> {
    let mut ready = Vec::with_capacity(count as usize);
    let processes: Vec<_> = (0..count)
        .map(|i| {
            let (sx, rx) = oneshot::channel();
            ready.push(rx);
            run_subworker(
                state.clone(),
                work_dir.to_path_buf(),
                python_program.to_string(),
                i as SubworkerId,
                sx,
            )
            .boxed_local()
        })
        .collect();
    let mut all_processes = futures::future::select_all(processes).map(|(_, idx, _)| idx);

    tokio::select! {
        idx = &mut all_processes => {
            panic!("Subworker {} terminated", idx);
        }
        subworkers = futures::future::join_all(ready) => {
            Ok((subworkers.into_iter().map(|sw| sw.unwrap()).collect(), all_processes))
        }
    }
}

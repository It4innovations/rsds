use std::env;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::Stdio;

use bytes::{Bytes, BytesMut};
use futures::{Future, FutureExt};
use futures::stream::{SplitSink, SplitStream};
use futures::task::LocalSpawnExt;
use hashbrown::HashSet;
use rand::distributions::Alphanumeric;
use rand::Rng;
use tokio::io::AsyncWrite;
use tokio::net::{UnixListener, UnixStream};
use tokio::process::Command;
use tokio::stream::{Stream, StreamExt};
use tokio::sync::oneshot;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use crate::common::transport::make_protocol_builder;
use crate::common::WrappedRcRefCell;
use crate::worker::messages::{ComputeTaskMsg, ToSubworkerMessage};
use crate::worker::task::{Task, TaskRef};

use super::messages::RegisterSubworkerMessage;

pub(crate) type SubworkerId = u32;

pub struct Subworker {
    pub id: SubworkerId,
    pub sender: tokio::sync::mpsc::UnboundedSender<Bytes>,
    pub running_task: Option<TaskRef>,
}

pub type SubworkerRef = WrappedRcRefCell<Subworker>;

impl Subworker {
    pub fn start_task(&self, task: &Task) {
        log::debug!("Starting task {} in subworker {}", task.key, self.id);
        // Send message to subworker
        let message = ToSubworkerMessage::ComputeTask(ComputeTaskMsg {
            key: &task.key,
            function: &task.function,
            args: &task.args,
            kwargs: &task.kwargs,
        });
        let data = rmp_serde::to_vec_named(&message).unwrap();
        self.sender.send(data.into()).unwrap();
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
    mut listener: UnixListener,
    subworker_id: SubworkerId,
) -> Result<(SplitSink<Framed<UnixStream, LengthDelimitedCodec>, Bytes>,
             SplitStream<Framed<UnixStream, LengthDelimitedCodec>>), crate::Error> {
    if let Some(Ok(mut stream)) = listener.next().await {
        let mut framed = make_protocol_builder().new_framed(stream);
        let message = tokio::stream::StreamExt::next(&mut framed).await;

        if message.is_none() {
            panic!("Subworker did not sent register message");
        }
        let message = message.unwrap().unwrap();
        let register_message: RegisterSubworkerMessage = rmp_serde::from_slice(&message).unwrap();

        if register_message.subworker_id != subworker_id {
            panic!("Subworker registered with an invalid id");
        }
        use futures::stream::StreamExt;
        Ok(framed.split())
    } else {
        panic!("Listening on subworker socket failed");
    }
}

async fn run_subworker_message_loop(mut stream: SplitStream<Framed<UnixStream, LengthDelimitedCodec>>) -> crate::Result<()> {
    while let Some(messages) = stream.next().await {
        todo!()
    }
    Ok(())
}

async fn run_subworker(
    work_dir: PathBuf,
    python_program: String,
    subworker_id: SubworkerId,
    ready_shot: oneshot::Sender<SubworkerRef>,
) -> Result<(), crate::Error>
{
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
            .args(&["-m", "rsds.subworker"]).spawn()?
    };

    std::mem::drop(socket_path);

    let (writer, reader) = tokio::select! {
        result = &mut process_future => {
            panic!("Subworker {} failed without registration: {}, see {}", subworker_id, result?, log_path.display());
        },
        result = subworker_handshake(listener, subworker_id) => {
            result?
        }
    };

    let (queue_sender, queue_receiver) = tokio::sync::mpsc::unbounded_channel::<Bytes>();

    // TODO: pass writing end
    let subworker = SubworkerRef::new(subworker_id, queue_sender);
    if let Err(_) = ready_shot.send(subworker) {
        panic!("Announcing subworker failed");
    }

    tokio::select! {
        result = process_future => {
            panic!("Subworker {} failed: {}, see {}", subworker_id, result?, log_path.display());
        },
        result = crate::util::forward_queue_to_sink(queue_receiver, writer) => {
            panic!("Sending a message to subworker failed");
        }
        _ = run_subworker_message_loop(reader) => {
            panic!("Subworker {} closed stream, see {}", subworker_id, log_path.display());
        }
    }
    ;

    Ok(())
}

pub async fn start_subworkers(
    work_dir: &Path,
    python_program: &str,
    count: u32,
) -> Result<(Vec<SubworkerRef>, impl Future<Output=usize>), crate::Error> {
    let mut ready = Vec::with_capacity(count as usize);
    let processes: Vec<_> = (0..count).map(|i| {
        let (sx, rx) = oneshot::channel();
        ready.push(rx);
        run_subworker(work_dir.to_path_buf(), python_program.to_string(), i as SubworkerId, sx).boxed_local()
    }).collect();
    let mut all_processes = futures::future::select_all(processes).map(|(_, idx, _)| idx);

    tokio::select! {
        idx = &mut all_processes => {
            panic!("Subworker {} terminated", idx);
        }
        subworkers = futures::future::join_all(ready) => {
            // TODO: Return also "all_processes"
            Ok((subworkers.into_iter().map(|sw| sw.unwrap()).collect(), all_processes))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_start() {
        assert_ne!(generate_socket_name(), generate_socket_name());
    }
}

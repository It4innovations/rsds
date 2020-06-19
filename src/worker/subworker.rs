use crate::common::WrappedRcRefCell;
use std::env;
use std::path::{Path, PathBuf};
use rand::distributions::Alphanumeric;
use rand::Rng;
use tokio::net::UnixListener;
use tokio::process::Command;
use futures::{Future, FutureExt, Stream, Sink};
use std::process::Stdio;
use tokio::stream::StreamExt;
use futures::task::LocalSpawnExt;
use hashbrown::HashSet;
use super::messages::RegisterSubworkerMessage;
use bytes::BytesMut;
use std::fs::File;
use tokio::sync::oneshot;

pub(crate) type SubworkerId = u32;

pub struct Subworker {
    id: SubworkerId,
}

pub type SubworkerRef = WrappedRcRefCell<Subworker>;

impl SubworkerRef {
    pub fn new(id: SubworkerId) -> Self {
        Self::wrap(Subworker {
            id
        })
    }
}


async fn subworker_handshake(mut listener: UnixListener, subworker_id: SubworkerId) -> Result<impl Stream<Item=Result<BytesMut, std::io::Error>>, crate::Error> {
    if let Some(Ok(mut stream)) = listener.next().await {
        let mut builder = tokio_util::codec::LengthDelimitedCodec::builder();
        builder.little_endian();
        builder.max_frame_length(128 * 1024 * 1024);


        let mut framed = builder.new_framed(stream);
        let message = framed.next().await;

        if message.is_none() {
            panic!("Subworker did not sent register message");
        }
        let message = message.unwrap().unwrap();
        let register_message : RegisterSubworkerMessage = rmp_serde::from_slice(&message).unwrap();

        if register_message.subworker_id != subworker_id {
            panic!("Subworker registered with an invalid id");
        }
        Ok(framed)
    } else {
        panic!("Listening on subworker socket failed");
    }
}

async fn run_subworker(work_dir: PathBuf, python_program: &str, subworker_id: SubworkerId, ready_shot: oneshot::Sender<SubworkerRef>) -> Result<(), crate::Error>
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

    let framed = tokio::select! {
        result = &mut process_future => {
            panic!("Subworker {} failed without registration: {}, see {}", subworker_id, result?, log_path.display());
        },
        result = subworker_handshake(listener, subworker_id) => {
            result?
        }
    };

    // TODO: pass writing end
    let subworker = SubworkerRef::new(subworker_id);
    if let Err(_) = ready_shot.send(subworker) {
        panic!("Announcing subworker failed");
    }

    let framed = tokio::select! {
        result = process_future => {
            panic!("Subworker {} failed: {}, see {}", subworker_id, result?, log_path.display());
        },
        // TODO: RPC loop
    };

    Ok(())
}


pub async fn start_subworkers(work_dir: &Path, python_program: &str, count: u32) -> Result<Vec<SubworkerRef>, crate::Error>  {
    let mut ready = Vec::with_capacity(count as usize);
    let processes : Vec<_> = (0..count).map(|i| {
        let (sx, rx) = oneshot::channel();
        ready.push(rx);
        run_subworker(work_dir.to_path_buf(), python_program, i as SubworkerId, sx).boxed_local()
    }).collect();
    let mut all_processes = futures::future::select_all(processes);

    tokio::select! {
        (result, _, _) = &mut all_processes => {
            let _ = result?;
            panic!("Subworker terminated");
        }
        subworkers = futures::future::join_all(ready) => {
            // TODO: Return also "all_processes"
            Ok(subworkers.into_iter().map(|sw| sw.unwrap()).collect())
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_start() {
        dbg!(generate_socket_name());
        panic!("X");
    }
}

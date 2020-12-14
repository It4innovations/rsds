use std::iter::FromIterator;

use bytes::BytesMut;
use futures::future::join_all;
use futures::stream::FuturesUnordered;
use futures::{SinkExt, StreamExt};
use rand::seq::SliceRandom;

use tokio::net::TcpStream;

use crate::common::data::SerializationType;
use crate::common::transport::make_protocol_builder;
use crate::common::Map;
use crate::error::DsError::GenericError;
use crate::scheduler::protocol::TaskId;
use crate::server::core::CoreRef;
use crate::server::notifications::Notifications;

use crate::server::protocol::messages::worker::{
    DataRequest, DataResponse, FetchRequestMsg, UploadDataMsg,
};
use crate::server::task::{TaskRef, TaskRuntimeState};
use crate::server::worker::WorkerRef;
use crate::trace::trace_task_new_finished;

// TODO: Convert this to returning Stream instead of vector,
//   so data could be sent as they are received
pub async fn gather(
    core_ref: &CoreRef,
    task_ids: &[TaskId],
) -> crate::Result<Vec<(TaskId, BytesMut, SerializationType)>> {
    let mut worker_map: Map<String, Vec<TaskId>> = Default::default();
    {
        let core = core_ref.get();
        let mut rng = rand::thread_rng();
        for task_id in task_ids {
            let task_ref = core.get_task_by_id_or_panic(*task_id);
            let task = task_ref.get();
            task.get_workers().map(|ws| {
                let ws = Vec::from_iter(ws.into_iter());
                ws.choose(&mut rng).map(|w| {
                    worker_map
                        .entry(w.get().address().to_string())
                        .or_default()
                        .push(*task_id);
                })
            });
        }
    }

    let mut worker_futures: FuturesUnordered<_> = FuturesUnordered::from_iter(
        worker_map
            .into_iter()
            .map(|(worker, keys)| get_data_from_worker(worker, keys)),
    );

    let mut result = Vec::with_capacity(task_ids.len());
    while let Some(r) = worker_futures.next().await {
        result.append(&mut r?);
    }
    Ok(result)
}

pub async fn fetch_data(
    stream: &mut tokio_util::codec::Framed<TcpStream, tokio_util::codec::LengthDelimitedCodec>,
    task_id: TaskId,
) -> crate::Result<(BytesMut, SerializationType)> {
    let message = DataRequest::FetchRequest(FetchRequestMsg { task_id });
    let data = rmp_serde::to_vec_named(&message).unwrap();
    stream.send(data.into()).await?;

    let message: DataResponse = {
        let data = match stream.next().await {
            None => return Err(GenericError("Unexpected close of connection".into())),
            Some(data) => data?,
        };
        rmp_serde::from_slice(&data)?
    };
    let header = match message {
        DataResponse::NotAvailable => todo!(),
        DataResponse::Data(x) => x,
        DataResponse::DataUploaded(_) => {
            // Worker send complete garbage, it should be considered as invalid and termianted
            todo!()
        }
    };
    let data = match stream.next().await {
        None => return Err(GenericError("Unexpected close of connection".into())),
        Some(data) => data?,
    };
    Ok((data, header.serializer))
}

pub async fn get_data_from_worker(
    worker_address: String,
    task_ids: Vec<TaskId>,
) -> crate::Result<Vec<(TaskId, BytesMut, SerializationType)>> {
    // TODO: Storing worker connection?
    // Directly resend to client?

    let connection = connect_to_worker(worker_address.clone()).await?;
    let mut stream = make_protocol_builder().new_framed(connection);

    let mut result = Vec::with_capacity(task_ids.len());
    for task_id in task_ids {
        log::debug!("Fetching {} from {}", &task_id, worker_address);
        let (data, serializer) = fetch_data(&mut stream, task_id).await?;
        log::debug!(
            "Fetched {} from {} ({} bytes)",
            &task_id,
            worker_address,
            data.len()
        );
        result.push((task_id, data, serializer));
    }
    Ok(result)
}

pub async fn update_data_on_worker(
    worker_ref: WorkerRef,
    data: Vec<(TaskRef, BytesMut)>,
) -> crate::Result<()> {
    let connection = connect_to_worker(worker_ref.get().listen_address.clone()).await?;
    let mut stream = make_protocol_builder().new_framed(connection);

    for (task_ref, data_for_id) in data {
        let task_id = task_ref.get().id;
        let message = DataRequest::UploadData(UploadDataMsg {
            task_id,
            serializer: SerializationType::Pickle,
        });
        let size = data_for_id.len() as u64;
        let data = rmp_serde::to_vec_named(&message).unwrap();
        stream.send(data.into()).await?;
        stream.send(data_for_id.into()).await?;
        let data = stream.next().await.unwrap().unwrap();
        let message: DataResponse = rmp_serde::from_slice(&data).unwrap();
        match message {
            DataResponse::DataUploaded(msg) => {
                if msg.task_id != task_id {
                    panic!("Upload sanity check failed, different key returned");
                }
                if let Some(error) = msg.error {
                    panic!("Upload of {} failed: {}", &msg.task_id, error);
                }
                /* Ok */
            }
            _ => {
                panic!("Invalid response");
            }
        };
        match &mut task_ref.get_mut().state {
            TaskRuntimeState::Finished(_, ref mut set) => {
                set.insert(worker_ref.clone());
            }
            _ => unreachable!(),
        };
        trace_task_new_finished(task_ref.get().id, size, worker_ref.get().id);
    }

    Ok(())
}

async fn connect_to_worker(address: String) -> crate::Result<tokio::net::TcpStream> {
    let address = address.trim_start_matches("tcp://");
    let stream = TcpStream::connect(address).await?;
    stream.set_nodelay(true)?;
    Ok(stream)
}

fn scatter_tasks<D>(
    data: Vec<D>,
    workers: &[WorkerRef],
    counter: usize,
) -> Vec<(WorkerRef, Vec<D>)> {
    let total_cpus: usize = workers.iter().map(|wr| wr.get().ncpus as usize).sum();
    let mut counter = counter % total_cpus;

    let mut cpu = 0;
    let mut index = 0;
    for (i, wr) in workers.iter().enumerate() {
        let ncpus = wr.get().ncpus as usize;
        if counter >= ncpus {
            counter -= ncpus;
        } else {
            cpu = counter;
            index = i;
            break;
        }
    }

    let mut worker_ref = &workers[index];
    let mut ncpus = worker_ref.get().ncpus as usize;

    let mut result: Map<WorkerRef, Vec<D>> = Map::new();

    for d in data {
        result.entry(worker_ref.clone()).or_default().push(d);
        cpu += 1;
        if cpu >= ncpus {
            cpu = 0;
            index += 1;
            index %= workers.len();
            worker_ref = &workers[index];
            ncpus = worker_ref.get().ncpus as usize;
        }
    }
    result.into_iter().collect()
}

pub async fn scatter(
    core_ref: &CoreRef,
    workers: &[WorkerRef],
    data: Vec<(TaskRef, BytesMut)>,
    notifications: &mut Notifications,
) {
    let counter = core_ref.get_mut().get_and_move_scatter_counter(data.len());
    let tasks: Vec<_> = data.iter().map(|(t_ref, _)| t_ref.clone()).collect();
    let placement = scatter_tasks(data, workers, counter);
    let worker_futures = join_all(
        placement
            .into_iter()
            .map(|(worker, data)| update_data_on_worker(worker, data)),
    );

    // TODO: Proper error handling
    // Note that successful uploads are written in tasks
    worker_futures.await.iter().for_each(|x| assert!(x.is_ok()));

    let mut core = core_ref.get_mut();
    for t_ref in tasks {
        {
            let task = t_ref.get();
            notifications.new_finished_task(&task);
        }
        core.add_task(t_ref);
    }
}

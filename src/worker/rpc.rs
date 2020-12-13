use std::cmp::Reverse;
use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

use bytes::buf::BufMutExt;
use bytes::{Bytes, BytesMut};
use futures::{SinkExt, Stream, StreamExt};
use rand::rngs::SmallRng;
use rand::seq::SliceRandom;
use rand::SeedableRng;

use tokio::net::lookup_host;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::LocalSet;
use tokio::time::delay_for;

use crate::common::rpc::forward_queue_to_sink;
use crate::common::transport::make_protocol_builder;

//use crate::server::protocol::messages::worker::{GetDataResponse, ToWorkerGenericMessage};

use crate::server::protocol::messages::generic::{GenericMessage, RegisterWorkerMsg};
use crate::server::protocol::messages::worker::{
    DataRequest, DataResponse, FetchResponseData, FromWorkerMessage, StealResponseMsg,
    ToWorkerMessage, UploadResponseMsg, WorkerRegistrationResponse,
};
use crate::server::protocol::Priority;
use crate::server::protocol::PriorityValue;
use crate::server::reactor::fetch_data;
use crate::worker::data::{DataObjectRef, DataObjectState, LocalData};

use crate::worker::state::WorkerStateRef;
use crate::worker::subworker::{start_subworkers, SubworkerPaths};
use crate::worker::task::TaskRef;

async fn start_listener() -> crate::Result<(TcpListener, String)> {
    let address = SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), 0);
    let listener = TcpListener::bind(address).await?;
    let address = {
        let socketaddr = listener.local_addr()?;
        format!(
            "{}:{}",
            gethostname::gethostname().into_string().unwrap(),
            socketaddr.port()
        )
    };
    log::info!("Listening on {}", address);
    Ok((listener, address))
}

async fn connect_to_server(scheduler_address: &str) -> crate::Result<TcpStream> {
    log::info!("Connecting to server {}", scheduler_address);
    let address = lookup_host(&scheduler_address)
        .await?
        .next()
        .expect("Invalid scheduler address");

    let max_attempts = 20;
    for _ in 0..max_attempts {
        match TcpStream::connect(address).await {
            Ok(stream) => {
                log::debug!("Connected to server");
                return Ok(stream);
            }
            Err(e) => {
                log::error!("Could not connect to {}, error: {}", scheduler_address, e);
                delay_for(Duration::from_secs(2)).await;
            }
        }
    }
    Result::Err(crate::Error::GenericError(
        "Server could not be connected".into(),
    ))
}

pub async fn run_worker(scheduler_address: &str, ncpus: u32, subworker_paths: SubworkerPaths) -> crate::Result<()> {
    let (listener, address) = start_listener().await?;
    let stream = connect_to_server(&scheduler_address).await?;
    let (queue_sender, queue_receiver) = tokio::sync::mpsc::unbounded_channel::<Bytes>();
    let (download_sender, download_reader) =
        tokio::sync::mpsc::unbounded_channel::<(DataObjectRef, (PriorityValue, PriorityValue))>();
    let (mut writer, mut reader) = make_protocol_builder().new_framed(stream).split();

    let taskset = LocalSet::default();
    {
        let message = GenericMessage::RegisterWorker(RegisterWorkerMsg {
            address: address.clone(),
            ncpus,
        });
        let mut frame = BytesMut::default().writer();
        rmp_serde::encode::write_named(&mut frame, &message)?;
        writer.send(frame.into_inner().into()).await.unwrap();
    }

    let state = {
        match reader.next().await {
            Some(data) => {
                let message: WorkerRegistrationResponse =
                    rmp_serde::from_slice(&data.unwrap()).unwrap();
                WorkerStateRef::new(
                    message.worker_id,
                    queue_sender,
                    ncpus,
                    address,
                    download_sender,
                    message.worker_addresses,
                )
            }
            None => panic!("Connection closed without receiving registration response"),
        }
    };

    log::info!("Starting {} subworkers", ncpus);
    let (subworkers, sw_processes) = start_subworkers(&state, subworker_paths, "python3", ncpus).await?;
    log::debug!("Subworkers started");

    state.get_mut().set_subworkers(subworkers);

    tokio::select! {
        _ = worker_message_loop(state.clone(), reader) => {
            panic!("Connection to server lost");
        }
        _ = forward_queue_to_sink(queue_receiver, writer) => {
            panic!("Cannot send a message to server");
        }
        _result = taskset.run_until(connection_initiator(listener, state.clone())) => {
            panic!("Taskset failed");
        }
        idx = sw_processes => {
            panic!("Subworker process {} failed", idx);
        }
        _ = worker_data_downloader(state, download_reader) => {
            unreachable!()
        }
    }
    //Ok(())
}

async fn worker_data_downloader(
    state_ref: WorkerStateRef,
    mut stream: tokio::sync::mpsc::UnboundedReceiver<(DataObjectRef, Priority)>,
) {
    // TODO: Limit downloads, more parallel downloads, respect priorities
    // TODO: Reuse connections
    let _queue: priority_queue::PriorityQueue<DataObjectRef, Reverse<Priority>> =
        Default::default();
    let mut random = SmallRng::from_entropy();
    loop {
        let (data_ref, _priority) = stream.next().await.unwrap();

        let (worker_id, task_id) = {
            let data_obj = data_ref.get();
            let workers = match &data_obj.state {
                DataObjectState::Remote(rs) => &rs.workers,
                DataObjectState::Local(_) | DataObjectState::Removed => {
                    /* It is already finished */
                    continue;
                }
            };
            let worker_id = workers.choose(&mut random).cloned().unwrap();
            (worker_id, data_obj.id)
        };
        let address = state_ref
            .get()
            .worker_addresses
            .get(&worker_id)
            .unwrap()
            .clone();
        let connection = connect_to_worker(address).await.unwrap();
        let mut stream = make_protocol_builder().new_framed(connection);
        let (data, serializer) = fetch_data(&mut stream, task_id).await.unwrap();
        state_ref
            .get_mut()
            .on_data_downloaded(data_ref, data, serializer);
    }
}

/* TODO: Refactor this! the same function is in the server */
async fn connect_to_worker(address: String) -> crate::Result<tokio::net::TcpStream> {
    let address = address.trim_start_matches("tcp://");
    let stream = TcpStream::connect(address).await?;
    stream.set_nodelay(true)?;
    Ok(stream)
}

async fn worker_message_loop(
    state_ref: WorkerStateRef,
    mut stream: impl Stream<Item = Result<BytesMut, std::io::Error>> + Unpin,
) -> crate::Result<()> {
    while let Some(data) = stream.next().await {
        let data = data?;
        let message: ToWorkerMessage = rmp_serde::from_slice(&data)?;
        let mut state = state_ref.get_mut();
        match message {
            ToWorkerMessage::ComputeTask(mut msg) => {
                log::debug!("Task assigned: {}", msg.id);
                let dep_info = std::mem::take(&mut msg.dep_info);
                let task_ref = TaskRef::new(msg);
                for (task_id, size, workers) in dep_info {
                    state.add_dependancy(&task_ref, task_id, size, workers);
                }
                state.add_task(task_ref);
            }
            ToWorkerMessage::DeleteData(msg) => {
                for id in msg.ids {
                    state.remove_data(id);
                }
            }
            ToWorkerMessage::StealTasks(msg) => {
                log::debug!("Steal {} attempts", msg.ids.len());
                let responses: Vec<_> = msg
                    .ids
                    .iter()
                    .map(|task_id| {
                        let response = state.steal_task(*task_id);
                        log::debug!("Steal attempt: {}, response {:?}", task_id, response);
                        (*task_id, response)
                    })
                    .collect();
                let message = FromWorkerMessage::StealResponse(StealResponseMsg { responses });
                state.send_message_to_server(rmp_serde::to_vec_named(&message).unwrap());
            }
            ToWorkerMessage::NewWorker(msg) => {
                log::debug!("New worker={} announced at {}", msg.worker_id, &msg.address);
                assert!(state
                    .worker_addresses
                    .insert(msg.worker_id, msg.address)
                    .is_none())
            }
        }
    }
    Ok(())
}

pub async fn connection_initiator(
    mut listener: TcpListener,
    state_ref: WorkerStateRef,
) -> crate::Result<()> {
    loop {
        let (socket, address) = listener.accept().await?;
        socket.set_nodelay(true)?;

        let state = state_ref.clone();
        tokio::task::spawn_local(async move {
            log::debug!("New connection: {}", address);
            connection_rpc_loop(socket, state, address)
                .await
                .expect("Connection failed");
            log::debug!("Connection ended: {}", address);
        });
    }
}

async fn connection_rpc_loop(
    stream: TcpStream,
    state_ref: WorkerStateRef,
    address: SocketAddr,
) -> crate::Result<()> {
    let mut stream = make_protocol_builder().new_framed(stream);
    loop {
        let data = match stream.next().await {
            None => return Ok(()),
            Some(data) => data?,
        };
        let request: DataRequest = rmp_serde::from_slice(&data)?;
        match request {
            DataRequest::FetchRequest(msg) => {
                log::debug!("Object {} request from {} started", msg.task_id, address);
                let state = state_ref.get();
                let (bytes, serializer) = {
                    let data_obj = match state.data_objects.get(&msg.task_id) {
                        None => {
                            let response = DataResponse::NotAvailable;
                            let data = rmp_serde::to_vec_named(&response).unwrap();
                            stream.send(data.into()).await?;
                            continue;
                        }
                        Some(x) => x.get(),
                    };
                    let data = data_obj.local_data().unwrap();
                    (data.bytes.clone(), data.serializer.clone())
                };
                let response = DataResponse::Data(FetchResponseData { serializer });
                let data = rmp_serde::to_vec_named(&response).unwrap();
                stream.send(data.into()).await?;
                stream.send(bytes).await?;
                log::debug!("Object {} request from {} finished", msg.task_id, address);
            }
            DataRequest::UploadData(msg) => {
                log::debug!("Object {} upload from {} started", msg.task_id, address);
                let data = match stream.next().await {
                    None => {
                        log::error!(
                            "Object {} started to upload but data did not arrived",
                            msg.task_id
                        );
                        return Ok(());
                    }
                    Some(data) => data?,
                };
                let mut state = state_ref.get_mut();
                let mut error = None;
                match state.data_objects.get(&msg.task_id) {
                    None => {
                        let data_ref = DataObjectRef::new(
                            msg.task_id,
                            data.len() as u64,
                            DataObjectState::Local(LocalData {
                                serializer: msg.serializer,
                                bytes: data.into(),
                            }),
                        );
                        state.add_data_object(data_ref);
                    }
                    Some(data_ref) => {
                        let data_obj = data_ref.get();
                        match &data_obj.state {
                            DataObjectState::Remote(_) => {
                                /* set the data and check waiting tasks */
                                todo!()
                            }
                            DataObjectState::Local(local) => {
                                log::debug!("Uploaded data {} is already in worker", &msg.task_id);
                                if local.serializer != msg.serializer
                                    || local.bytes.len() != data.len()
                                {
                                    log::error!(
                                        "Incompatible data {} was data uploaded",
                                        &msg.task_id
                                    );
                                    error = Some("Incompatible data was uploaded".into());
                                }
                            }
                            DataObjectState::Removed => unreachable!(),
                        }
                    }
                };

                log::debug!("Object {} upload from {} finished", &msg.task_id, address);
                let response = DataResponse::DataUploaded(UploadResponseMsg {
                    task_id: msg.task_id,
                    error,
                });
                let data = rmp_serde::to_vec_named(&response).unwrap();
                stream.send(data.into()).await?;
            }
        }
    }
}

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use rsds::client::{Client, ClientId};
use rsds::comm::reactor::update_graph;
use rsds::comm::CommRef;
use rsds::core::CoreRef;
use rsds::protocol::clientmsg::{ClientTaskSpec, UpdateGraphMsg};
use rsds::protocol::key::DaskKey;
use rsds::protocol::protocol::{DaskPacket, SerializedTransport};
use rsds::scheduler::ToSchedulerMessage;
use tokio::sync::mpsc::UnboundedReceiver;

struct Context {
    core: CoreRef,
    comm: CommRef,
    client_id: ClientId,
    _client_receiver: UnboundedReceiver<DaskPacket>,
    _comm_receiver: UnboundedReceiver<Vec<ToSchedulerMessage>>,
}

pub fn update_graph_bench(c: &mut Criterion) {
    let task_count = 2000;
    c.bench_with_input(
        BenchmarkId::new("Tasks without deps", task_count),
        &task_count,
        |b, &task_count| {
            b.iter_with_setup(
                || {
                    let core_ref = CoreRef::default();
                    let (ctx, _crx) = tokio::sync::mpsc::unbounded_channel();
                    let client_id = {
                        let mut core = core_ref.get_mut();
                        let client = Client::new(core.new_client_id(), "client".into(), ctx);
                        let id = client.id();
                        core.register_client(client);
                        id
                    };
                    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
                    let comm = CommRef::new(tx);

                    let mut tasks: Vec<(DaskKey, ClientTaskSpec<SerializedTransport>)> = vec![];
                    for i in 0..task_count {
                        tasks.push((
                            format!("key-{}", i).into(),
                            ClientTaskSpec::Serialized(SerializedTransport::Inline(
                                rmpv::Value::Binary(vec![1, 2, 3, 4, 5]),
                            )),
                        ));
                    }
                    let keys = tasks.iter().map(|v| v.0.clone()).collect();
                    let msg = UpdateGraphMsg {
                        tasks: tasks.into_iter().collect(),
                        dependencies: Default::default(),
                        keys,
                        actors: None,
                        frames: Default::default(),
                        priority: Default::default(),
                        user_priority: Default::default(),
                    };
                    (
                        Context {
                            core: core_ref,
                            comm,
                            client_id,
                            _client_receiver: _crx,
                            _comm_receiver: _rx,
                        },
                        msg,
                    )
                },
                |(ctx, msg)| update_graph(&ctx.core, &ctx.comm, ctx.client_id, msg),
            );
        },
    );
}

criterion_group!(reactor, update_graph_bench);
criterion_main!(reactor);

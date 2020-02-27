use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rsds::client::{Client, ClientId};
use rsds::comm::CommRef;
use rsds::core::CoreRef;
use rsds::protocol::clientmsg::{ClientTaskSpec, UpdateGraphMsg};
use rsds::protocol::key::DaskKey;
use rsds::protocol::protocol::{SerializedTransport, DaskPacket};
use rsds::reactor::update_graph;
use tokio::sync::mpsc::UnboundedReceiver;
use rsds::scheduler::ToSchedulerMessage;

struct Context {
    core: CoreRef,
    comm: CommRef,
    client_id: ClientId,
    client_receiver: UnboundedReceiver<DaskPacket>,
    comm_receiver: UnboundedReceiver<Vec<ToSchedulerMessage>>,
}

pub fn reactor(c: &mut Criterion) {
    c.bench_function("update_graph", |b| {
        b.iter_with_setup(|| {
            let core_ref = CoreRef::new();
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
            for i in 0..2000 {
                tasks.push((
                    format!("key-{}", i).into(),
                    ClientTaskSpec::Serialized(SerializedTransport::Inline(rmpv::Value::Binary(vec![
                        1, 2, 3, 4, 5,
                    ]))),
                ));
            }
            let keys = tasks.iter().map(|v| v.0.clone()).collect();
            let msg = UpdateGraphMsg {
                tasks: tasks.into_iter().collect(),
                dependencies: Default::default(),
                keys,
                actors: None,
                frames: Default::default(),
            };
            (Context {
                core: core_ref,
                comm,
                client_id,
                client_receiver: _crx,
                comm_receiver: _rx
            }, msg)
        }, |(ctx, msg)| update_graph(&ctx.core, &ctx.comm, ctx.client_id, msg));
    });
}

criterion_group!(benches, reactor);
criterion_main!(benches);

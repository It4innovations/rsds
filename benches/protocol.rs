use bytes::BytesMut;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use futures::SinkExt;
use rsds::protocol::clientmsg::{ClientTaskSpec, UpdateGraphMsg};
use rsds::protocol::key::DaskKey;
use rsds::protocol::protocol::{asyncwrite_to_sink, DaskPacket, Frame, SerializedTransport};
use std::fs::OpenOptions;
use tokio::fs::File;
use tokio::runtime;
use std::time::Duration;

fn encode_packet(c: &mut Criterion) {
    let mut group = c.benchmark_group("Encode");
    group.warm_up_time(Duration::from_secs(1));
    group.sample_size(10);

    let sizes = vec![
        256,
        1024,
        8 * 1024,
        64 * 1024,
        128 * 1024,
        1024 * 1024,
        32 * 1024 * 1024,
        256 * 1024 * 1024,
    ];
    for size in sizes {
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::new("Packet", size), &size, |b, &size| {
            let mut rt = runtime::Builder::new()
                .basic_scheduler()
                .enable_io()
                .build()
                .unwrap();

            b.iter_with_setup(
                || {
                    let file =
                        File::from_std(OpenOptions::new().write(true).open("/dev/null").unwrap());
                    let sink = asyncwrite_to_sink(file);

                    let bytes = BytesMut::from(vec![0u8; size].as_slice());
                    let packet = DaskPacket::new(bytes.freeze(), vec![]);
                    (sink, packet)
                },
                |(mut sink, packet)| {
                    rt.block_on(async move {
                        sink.send(packet).await.unwrap();
                    });
                },
            );
        });
    }
    group.finish();
}

criterion_group!(protocol, encode_packet);
criterion_main!(protocol);

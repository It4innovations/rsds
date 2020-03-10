use bytes::{Bytes, BytesMut};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use futures::SinkExt;

use rsds::protocol::protocol::{asyncwrite_to_sink, DaskPacket};
use std::fs::OpenOptions;
use std::time::Duration;
use tokio::fs::File;
use tokio::runtime;

fn create_bytes(size: usize) -> Bytes {
    BytesMut::from(vec![0u8; size].as_slice()).freeze()
}

fn encode(c: &mut Criterion) {
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
        // group.bench_with_input(BenchmarkId::new("Inmemory", size), &size, |b, &size| {
        //     b.iter_with_setup(
        //         || {
        //             let partsize = size / 4;
        //             let packet = DaskPacket::new(
        //                 create_bytes(partsize),
        //                 (0..3).map(|_| create_bytes(partsize)).collect(),
        //             );
        //             packet
        //         },
        //         |packet| {
        //             let mut target = BytesMut::default();
        //             let mut codec = DaskCodec::default();
        //             let parts = split_packet_into_parts(packet, 64 * 1024);
        //             for part in parts {
        //                 codec.encode(part, &mut target).unwrap();
        //             }
        //         },
        //     );
        // });
        group.bench_with_input(BenchmarkId::new("Sink", size), &size, |b, &size| {
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

                    let packet = DaskPacket::new(create_bytes(size), vec![]);
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

criterion_group!(protocol, encode);
criterion_main!(protocol);

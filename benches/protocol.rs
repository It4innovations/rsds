use bytes::{BytesMut, Bytes};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use futures::{SinkExt, StreamExt};

use rsds::protocol::protocol::{asyncwrite_to_sink, DaskCodec, DaskPacket, Frame, asyncread_to_stream};
use rsds::protocol::clientmsg::{ClientTaskSpec, UpdateGraphMsg};
use rsds::protocol::key::DaskKey;
use std::fs::OpenOptions;
use std::io::Write;
use tokio::fs::File;
use tokio::runtime;
use tokio_util::codec::Encoder;
use tempfile::NamedTempFile;
use std::time::Duration;

fn create_bytes(size: usize) -> Bytes {
    BytesMut::from(vec![0u8; size].as_slice()).freeze()
}

fn serialize_packet(packet: DaskPacket) -> BytesMut {
    let mut bytes = BytesMut::default();
    /*let mut codec = DaskCodec::default();
    for part in split_packet_into_parts(packet, 64 * 1024) {
        codec.encode(part, &mut bytes).unwrap();
    } TODO*/
    bytes
}

fn decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("Decode");
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
        group.bench_with_input(BenchmarkId::new("Stream", size), &size, |b, &size| {
            let mut rt = runtime::Builder::new()
                .basic_scheduler()
                .enable_io()
                .build()
                .unwrap();

            let mut packet_file = NamedTempFile::new().unwrap();
            let bytes = serialize_packet(DaskPacket::new(
                BytesMut::from(vec![0u8; size].as_slice()).freeze(),
                vec![],
            ));
            packet_file.write(&bytes).unwrap();

            b.iter_with_setup(
                || {
                    let file = File::from_std(packet_file.reopen().unwrap());
                    asyncread_to_stream(file)
                },
                |mut stream| {
                    rt.block_on(async move {
                        stream.next().await.unwrap().unwrap();
                    });
                },
            );
        });
    }
    group.finish();
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
        group.bench_with_input(BenchmarkId::new("Inmemory", size), &size, |b, &size| {
            b.iter_with_setup(
                || {
                    let partsize = size / 4;
                    let packet = DaskPacket::new(
                        create_bytes(partsize),
                        (0..3).map(|_| create_bytes(partsize)).collect(),
                    );
                    packet
                },
                |packet| {
                    let mut target = BytesMut::default();
                    let mut codec = DaskCodec::default();
                    /*let parts = split_packet_into_parts(packet, 64 * 1024);
                    for part in parts {
                        codec.encode(part, &mut target).unwrap();
                    } TODO*/
                },
            );
        });
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

criterion_group!(protocol, encode, decode);
criterion_main!(protocol);

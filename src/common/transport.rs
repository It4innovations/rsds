use tokio_util::codec::length_delimited::{LengthDelimitedCodec, Builder};

pub fn make_protocol_builder() -> Builder {
    let mut builder = LengthDelimitedCodec::builder();
    builder.little_endian();
    builder.max_frame_length(128 * 1024 * 1024);
    builder
}
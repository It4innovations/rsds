use crate::prelude::*;
use byteorder::{LittleEndian, ReadBytesExt};
use std::io::Cursor;
use tokio::codec::{Decoder, Encoder};
use std::collections::VecDeque;

pub struct DaskCodec {
    sizes: VecDeque<u64>,
}

impl DaskCodec {
    pub fn new() -> Self {
        DaskCodec {
            sizes: Default::default(),
        }
    }
}

impl Decoder for DaskCodec {
    type Item = BytesMut;
    type Error = crate::DsError;

    fn decode(&mut self, src: &mut BytesMut) -> crate::Result<Option<BytesMut>> {
        let src = if self.sizes.is_empty() {
            let size = src.len() as u64;
            if size < 8 {
                return Ok(None);
            }
            let mut cursor = Cursor::new(src);
            let count: u64 = cursor.read_u64::<LittleEndian>()?;
            let header_size = (count + 1) * 8;
            if size < header_size {
                return Ok(None);
            }
            for _ in 0..count {
                self.sizes.push_back(cursor.read_u64::<LittleEndian>()?);
            }
            let src = cursor.into_inner();
            src.advance(header_size as usize);
            src
        } else {
            src
        };

        while let Some(frame_size) = self.sizes.pop_front() {
            if frame_size > 0 {
                return if (src.len() as u64) < frame_size {
                    self.sizes.push_front(frame_size);
                    Ok(None)
                }
                else {
                    Ok(Some(src.split_to(frame_size as usize)))
                }
            }
        }

        Ok(None)
    }
}

impl Encoder for DaskCodec {
    type Item = Bytes;
    type Error = crate::DsError;

    fn encode(&mut self, data: Bytes, dst: &mut BytesMut) -> crate::Result<()> {
        let n = data.len() + 8 * 3;
        dst.reserve(n);
        dst.put_u64_le(2);
        dst.put_u64_le(0);
        dst.put_u64_le(data.len() as u64);
        dst.extend_from_slice(&data[..]);
        Ok(())
    }
}

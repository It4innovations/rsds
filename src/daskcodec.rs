use crate::prelude::*;
use byteorder::{LittleEndian, ReadBytesExt};
use failure::Error;
use smallvec::SmallVec;
use std::io::Cursor;
use tokio::codec::{Decoder, Encoder};

pub struct DaskCodec {
    sizes: SmallVec<[u64; 2]>,
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
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<BytesMut>, Error> {
        let src = if self.sizes.is_empty() {
            let size = src.len() as u64;
            if size < 8 {
                return Ok(None);
            }
            let mut cursor = Cursor::new(src);
            let count: u64 = cursor.read_u64::<LittleEndian>().unwrap();
            let header_size = (count + 1) * 8;
            if size < header_size {
                return Ok(None);
            }
            // let mut sizes : SmallVec<[u64; 2]> = smallvec!();
            for _ in 0..count {
                self.sizes.push(cursor.read_u64::<LittleEndian>().unwrap());
            }
            // !! This just a HACK! for specific observed frames
            assert!(self.sizes.len() == 2);
            assert!(*self.sizes.get(0).unwrap() == 0);
            let src = cursor.into_inner();
            src.advance(header_size as usize);
            src
        } else {
            src
        };

        // !! HACK only
        let data_size = *self.sizes.get(1).unwrap();
        if (src.len() as u64) < data_size {
            return Ok(None);
        }
        self.sizes.clear();
        Ok(Some(src.split_to(data_size as usize)))
    }
}

impl Encoder for DaskCodec {
    type Item = Bytes;
    type Error = Error;

    fn encode(&mut self, data: Bytes, dst: &mut BytesMut) -> Result<(), Error> {
        let n = data.len() + 8 * 3;
        dst.reserve(n);
        dst.put_u64_le(2);
        dst.put_u64_le(0);
        dst.put_u64_le(data.len() as u64);
        dst.extend_from_slice(&data[..]);
        Ok(())
    }
}

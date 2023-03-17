use crate::bindings::*;
use crate::pool::{Buffer, MemoryBuffer};
use crate::varint::VarInt;
use anyhow::{format_err, Result};
use cid::Cid;
use core2::io::Cursor;
use prost::Message;
use serde::{Deserialize, Serialize};
use std::ops::RangeBounds;

mod unixfs_pb {
    include!(concat!(env!("OUT_DIR"), "/unixfs_pb.rs"));
}

mod dag_pb {
    include!(concat!(env!("OUT_DIR"), "/merkledag_pb.rs"));
}

// CAR V1 header, should contain a single root and be CBOR encoded
#[derive(Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct CarHeader {
    pub roots: Vec<Cid>,
    pub version: u64,
}

// Unixfs data type enum
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, num_enum::IntoPrimitive, num_enum::TryFromPrimitive,
)]
#[repr(i32)]
pub enum DataType {
    Raw = 0,
    Directory = 1,
    File = 2,
    Metadata = 3,
    Symlink = 4,
    HamtShard = 5,
}

/// CarFrameReader return each length prefixed frame included in the given byte range.
/// It keeps each frame intact so we don't need to allocate extra buffers.
pub struct CarFrameReader<'a, R: RangeBounds<u64>> {
    range: R,
    buffers: *mut ngx_chain_t,
    pos: u64,
    current: &'a [u8],
    header: &'a [u8],
}

impl<'a, R: RangeBounds<u64>> CarFrameReader<'a, R> {
    pub fn new(range: R, input: *mut ngx_chain_t) -> Result<Self> {
        if input.is_null() {
            return Err(format_err!("null buffer chain ptr"));
        }

        let buf = unsafe { MemoryBuffer::from_ngx_buf((*input).buf) };
        let bytes = buf.as_bytes();

        let (size, read) =
            usize::decode_var(bytes).ok_or_else(|| format_err!("could not decode header frame"))?;

        let (header, current) = bytes.split_at(size + read);

        Ok(Self {
            range,
            buffers: input,
            pos: 0,
            current,
            header,
        })
    }

    pub fn header_frame(&self) -> &'a [u8] {
        self.header
    }

    // If codec is unixfs, advance the cursor else just return an error
    fn consume(&mut self, cid: Cid, data: &[u8]) -> Result<()> {
        match cid.codec() {
            0x70 => {
                let outer = dag_pb::PbNode::decode(data).map_err(|e| format_err!("{}", e))?;
                let inner_data = outer
                    .data
                    .as_ref()
                    .cloned()
                    .ok_or_else(|| format_err!("missing unxifs data field"))?;
                let inner =
                    unixfs_pb::Data::decode(inner_data).map_err(|e| format_err!("{}", e))?;

                // let dt: DataType = inner.r#type.try_into().ok()?;

                if outer.links.len() == 0 && inner.data.is_some() {
                    self.pos += inner.data.as_ref().map(|d| d.len() as u64).unwrap();
                    return Ok(());
                }
            }
            0x55 => {
                self.pos += data.len() as u64;
                return Ok(());
            }
            _ => (),
        }
        Err(format_err!("not unixfs chunk"))
    }
}

impl<'a, R: RangeBounds<u64>> Iterator for CarFrameReader<'a, R> {
    type Item = &'a [u8];

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.current.is_empty() {
                let input = unsafe { (*self.buffers).next };
                if input.is_null() {
                    return None;
                }

                let buf = unsafe { MemoryBuffer::from_ngx_buf((*input).buf) };
                self.current = buf.as_bytes();
                self.buffers = input;
            }

            let (size, read) = usize::decode_var(self.current)?;
            let (frame, next) = &self.current.split_at(size + read);
            self.current = next;

            let mut cursor = Cursor::new(&frame[read..]);
            let cid = Cid::read_bytes(&mut cursor).ok()?;
            // block data
            let data = &frame[cursor.position() as usize..];

            // If the blocks were consumed as unixfs chunks we check
            // whether the cursor is within the range.
            if let Ok(()) = self.consume(cid, data) {
                if !self.range.contains(&self.pos) {
                    continue;
                }
            }

            return Some(frame);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn to_ngx_buf(buf: &[u8]) -> ngx_buf_s {
        let slice_ptr = buf.as_ptr_range();

        ngx_buf_s {
            pos: slice_ptr.start as *mut u_char,
            last: slice_ptr.end as *mut u_char,
            file_pos: 0,
            file_last: 0,
            start: slice_ptr.start as *mut u_char,
            end: slice_ptr.end as *const _ as *mut u_char,
            tag: std::ptr::null_mut(),
            file: std::ptr::null_mut(),
            shadow: std::ptr::null_mut(),
            _bitfield_align_1: [0u8; 0],
            _bitfield_1: ngx_buf_s::new_bitfield_1(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            num: 0,
        }
    }

    #[test]
    fn test_car_single_block() {
        use crate::bindings::*;
        let car_data = hex::decode("38a265726f6f747381d82a582300122046d44814b9c5af141c3aaab7c05dc5e844ead5f91f12858b021eba45768b4c0e6776657273696f6e0136122046d44814b9c5af141c3aaab7c05dc5e844ead5f91f12858b021eba45768b4c0e0a120802120c68656c6c6f20776f726c640a180c").unwrap();

        let buf = to_ngx_buf(&car_data[..]);

        let chain = ngx_chain_s {
            buf: &buf as *const _ as *mut _,
            next: std::ptr::null_mut(),
        };

        let cr = CarFrameReader::new(.., &chain as *const _ as *mut _).unwrap();

        let mut buf = cr.header_frame().to_vec();

        for frame in cr {
            buf.extend_from_slice(frame);
        }

        assert_eq!(buf, car_data);
    }

    #[test]
    fn test_car_iter() {
        use crate::bindings::*;
        use std::fs::File;
        use std::io::{BufRead, BufReader};

        let f = File::open("iconfixture.car").unwrap();
        let mut reader = BufReader::new(f);

        let car_data = reader.fill_buf().unwrap();

        let buf = to_ngx_buf(car_data);

        let chain = ngx_chain_s {
            buf: &buf as *const _ as *mut _,
            next: std::ptr::null_mut(),
        };

        let cr = CarFrameReader::new(.., &chain as *const _ as *mut _).unwrap();

        let mut buf = cr.header_frame().to_vec();

        for frame in cr {
            buf.extend_from_slice(frame);
        }

        assert_eq!(buf, car_data.to_vec());
    }

    #[test]
    fn test_car_iter_range() {
        use crate::bindings::*;
        use std::fs::File;
        use std::io::{BufRead, BufReader};

        let f = File::open("iconfixture.car").unwrap();
        let mut reader = BufReader::new(f);

        let car_data = reader.fill_buf().unwrap();

        let car_slice = &car_data[..];

        let buf = to_ngx_buf(car_slice);

        let chain = ngx_chain_s {
            buf: &buf as *const _ as *mut _,
            next: std::ptr::null_mut(),
        };

        let cr = CarFrameReader::new(..4000, &chain as *const _ as *mut _).unwrap();

        let count = cr.count();

        assert_eq!(count, 4);
    }
}

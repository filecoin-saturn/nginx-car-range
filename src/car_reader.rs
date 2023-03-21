use crate::bindings::*;
use crate::pool::{Buffer, MemoryBuffer};
use crate::varint::VarInt;
use anyhow::{format_err, Result};
use cid::Cid;
use core2::io::Cursor;
use prost::Message;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
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

/// CarBufferReader is an iteraror returning nginx buffers ready to be inserted in an output chain
pub struct CarBufferReader<'a, R: RangeBounds<u64>> {
    // byte range to be selected in a unixfs file
    range: R,
    // pointer to the nginx output buffer chain
    buffers: *mut ngx_chain_t,
    // cursor position within the unixfs file
    unixfs_pos: u64,
    // cursor position within the CAR file
    car_pos: usize,
    // TODO: rename?. Bytes to include from a previous frame.
    offset: usize,
    // lifetime of the buffers is bound by the lifetime of the chain
    _marker: PhantomData<&'a ()>,
}

impl<'a, R: RangeBounds<u64>> CarBufferReader<'a, R> {
    pub fn new(range: R, input: *mut ngx_chain_t) -> Result<Self> {
        if input.is_null() {
            return Err(format_err!("null buffer chain ptr"));
        }

        let buf = unsafe { MemoryBuffer::from_ngx_buf((*input).buf) };
        let bytes = buf.as_bytes();

        let (size, read) =
            usize::decode_var(bytes).ok_or_else(|| format_err!("could not decode header frame"))?;

        Ok(Self {
            range,
            buffers: input,
            unixfs_pos: 0,
            car_pos: 0,
            offset: size + read,
            _marker: PhantomData,
        })
    }

    // If codec is unixfs, advance the cursor else just return an error
    fn consume(&mut self, cid: Cid, data: &[u8]) -> Result<u64> {
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
                    let size = inner.data.as_ref().map(|d| d.len() as u64).unwrap();
                    return Ok(size);
                }
            }
            0x55 => {
                return Ok(data.len() as u64);
            }
            _ => (),
        }
        Err(format_err!("not unixfs chunk"))
    }
}

impl<'a, R: RangeBounds<u64>> Iterator for CarBufferReader<'a, R> {
    type Item = MemoryBuffer<'a>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.buffers.is_null() {
            return None;
        }

        // the cursor has moved past the desired range.
        // iterator is done.
        if !self.range.contains(&self.unixfs_pos) {
            // skip the next buffers
            while !self.buffers.is_null() {
                unsafe { (*(*self.buffers).buf).pos = (*(*self.buffers).buf).last };
                self.buffers = unsafe { (*self.buffers).next };
            }
            return None;
        }

        let mut buf = unsafe { MemoryBuffer::from_ngx_buf((*self.buffers).buf) };

        self.buffers = unsafe { (*self.buffers).next };

        let mut current = &buf.as_bytes()[self.offset..];
        let lastn = self.car_pos + buf.len();

        self.car_pos += self.offset;
        // reset the offset
        self.offset = 0;

        while !current.is_empty() {
            let (size, read) = usize::decode_var(current)?;

            // TODO: handle frames spawning multiple buffers
            if size + read > current.len() {
                break;
            }

            let (frame, next) = &current.split_at(size + read);
            current = next;

            let mut cursor = Cursor::new(&frame[read..]);
            let cid = Cid::read_bytes(&mut cursor).ok()?;
            // block data
            let data = &frame[read + cursor.position() as usize..];

            // If the blocks were consumed as unixfs chunks we check
            // whether the cursor is within the range.
            match self.consume(cid, data) {
                Ok(read) => {
                    if !self.range.contains(&self.unixfs_pos) {
                        continue;
                    }
                    self.unixfs_pos += read;
                }
                Err(_) => {}
            }
            self.car_pos += size + read;
        }

        let sub = lastn - self.car_pos;
        if sub > 0 {
            let inner = buf.as_ngx_buf_mut();

            unsafe {
                let last = (*inner).last;
                (*inner).last = last.wrapping_sub(sub);
            }
            buf.set_last_buf(true);
            buf.set_last_in_chain(true);
        }

        Some(buf)
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

        let cr = CarBufferReader::new(.., &chain as *const _ as *mut _).unwrap();

        let mut buf = vec![];

        for b in cr {
            buf.extend_from_slice(b.as_bytes());
        }

        assert_eq!(buf, car_data);
    }

    #[test]
    fn test_car_iter() {
        use crate::bindings::*;
        use std::fs::File;
        use std::io::{BufRead, BufReader};

        let f = File::open("fixture.car").unwrap();
        let mut reader = BufReader::new(f);

        let car_data = reader.fill_buf().unwrap();

        let buf = to_ngx_buf(car_data);

        let chain = ngx_chain_s {
            buf: &buf as *const _ as *mut _,
            next: std::ptr::null_mut(),
        };

        let cr = CarBufferReader::new(.., &chain as *const _ as *mut _).unwrap();

        let mut buf = vec![];

        for b in cr {
            buf.extend_from_slice(b.as_bytes());
        }

        assert_eq!(buf, car_data.to_vec());
    }

    #[test]
    fn test_car_iter_range() {
        use crate::bindings::*;
        use std::fs::File;
        use std::io::{BufRead, BufReader};

        let f = File::open("fixture.car").unwrap();
        let mut reader = BufReader::new(f);

        let car_data = reader.fill_buf().unwrap();

        let buf = to_ngx_buf(car_data);

        let chain = ngx_chain_s {
            buf: &buf as *const _ as *mut _,
            next: std::ptr::null_mut(),
        };

        let cr = CarBufferReader::new(..1024, &chain as *const _ as *mut _).unwrap();

        let mut buf = vec![];

        for b in cr {
            buf.extend_from_slice(b.as_bytes());
        }

        // header + unxifs_root + raw block(1000) + raw_block(1000)
        assert_eq!(buf.len(), 59 + 379 + 1038 + 1038);
    }

    #[test]
    fn test_car_iter_range_multi_buffers() {
        use crate::bindings::*;
        use std::fs::File;
        use std::io::{BufRead, BufReader};

        let f = File::open("fixture.car").unwrap();
        let mut reader = BufReader::new(f);

        let car_data = reader.fill_buf().unwrap();

        let buf1 = to_ngx_buf(&car_data[..3000]);
        let buf2 = to_ngx_buf(&car_data[3001..]);

        let next = ngx_chain_s {
            buf: &buf2 as *const _ as *mut _,
            next: std::ptr::null_mut(),
        };

        let chain = ngx_chain_s {
            buf: &buf1 as *const _ as *mut _,
            next: &next as *const _ as *mut _,
        };

        let cr = CarBufferReader::new(..1024, &chain as *const _ as *mut _).unwrap();

        let mut buf = vec![];

        for b in cr {
            buf.extend_from_slice(b.as_bytes());
        }

        // header + unxifs_root + raw block(1000) + raw_block(1000)
        assert_eq!(buf.len(), 59 + 379 + 1038 + 1038);
    }

    // #[test]
    // fn test_buf_filter_chain() {
    //     let data: Vec<&str> = "Mary had a little lamb".split(' ').collect();

    //     let bufs: Vec<ngx_buf_s> = data.iter().map(|s| to_ngx_buf(s.as_bytes())).collect();

    //     let chain4 = ngx_chain_s {
    //         buf: &bufs[4] as *const _ as *mut _,
    //         next: std::ptr::null_mut(),
    //     };

    //     let chain3 = ngx_chain_s {
    //         buf: &bufs[3] as *const _ as *mut _,
    //         next: &chain4 as *const _ as *mut _,
    //     };

    //     let chain2 = ngx_chain_s {
    //         buf: &bufs[2] as *const _ as *mut _,
    //         next: &chain3 as *const _ as *mut _,
    //     };

    //     let chain1 = ngx_chain_s {
    //         buf: &bufs[1] as *const _ as *mut _,
    //         next: &chain2 as *const _ as *mut _,
    //     };

    //     let chain0 = ngx_chain_s {
    //         buf: &bufs[0] as *const _ as *mut _,
    //         next: &chain1 as *const _ as *mut _,
    //     };

    //     let br = CarBufferReader::new(..12, &chain0 as *const _ as *mut _).unwrap();

    //     assert_eq!(br.count(), 5);
    // }
}

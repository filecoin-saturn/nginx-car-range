use crate::bindings::*;
use crate::pool::{Buffer, MemoryBuffer};
use crate::varint::VarInt;
use anyhow::{format_err, Result};
use cid::Cid;
use core2::io::Cursor;
use prost::Message;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use std::ops::{Bound, RangeBounds};

mod unixfs_pb {
    include!(concat!(env!("OUT_DIR"), "/unixfs_pb.rs"));
}

mod dag_pb {
    include!(concat!(env!("OUT_DIR"), "/merkledag_pb.rs"));
}

fn lt_bound(bound: Bound<&u64>, val: u64) -> bool {
    match bound {
        Bound::Included(&b) => b >= val,
        Bound::Excluded(&b) => b > val,
        Bound::Unbounded => false,
    }
}

fn gt_bound(bound: Bound<&u64>, val: u64) -> bool {
    match bound {
        Bound::Included(&b) => b <= val,
        Bound::Excluded(&b) => b < val,
        Bound::Unbounded => false,
    }
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

pub struct CarBufferContext<'a, R: RangeBounds<u64>> {
    range: R,
    // request: &'a mut Request,
    pub size: usize,
    pub count: usize,
    offset: usize,
    unixfs_pos: usize,
    frame_pos: usize,
    last_codec: u64,
    done: usize,
    _marker: PhantomData<&'a ()>,
}

impl<'a, R: RangeBounds<u64>> CarBufferContext<'a, R> {
    // might be best to pass the request object in the buffer fn each time?
    pub fn new(range: R) -> Self {
        Self {
            range,
            offset: 0,
            size: 0,
            count: 0,
            unixfs_pos: 0,
            frame_pos: 0,
            last_codec: 0,
            done: 0,
            _marker: PhantomData,
        }
    }
    pub fn buffer<F: Fn() -> *mut ngx_chain_t>(
        &mut self,
        input: *mut ngx_chain_t,
        alloc_cl: F,
    ) -> *mut ngx_chain_t {
        // start with the first chain link
        let mut cl = input;
        // output buffer chain is null by default
        let mut out: *mut ngx_chain_t = std::ptr::null_mut();
        if self.done == 1 {
            return out;
        }
        // keep track of the last link so we can append to it
        let mut ll = &mut out;
        // iterate over the chain until the next link is null
        while !cl.is_null() {
            let mut buf = unsafe { MemoryBuffer::from_ngx_buf((*cl).buf) };
            cl = unsafe { (*cl).next };

            macro_rules! append_buf {
                () => {
                    let sub = self.size - self.frame_pos;

                    let is_last = match self.range.end_bound() {
                        Bound::Included(&b) => b == self.unixfs_pos as u64,
                        Bound::Excluded(&b) => b - 1 == self.unixfs_pos as u64,
                        // if the range is unbounded the last buffer should already be
                        // set as last.
                        Bound::Unbounded => false,
                    };

                    if sub > 0 || is_last {
                        self.done = 1;
                        buf.set_last_buf(true);
                    }

                    let mut cl = alloc_cl();
                    if cl.is_null() {
                        continue;
                    }
                    unsafe {
                        (*cl).buf = buf.as_ngx_buf_mut();
                        (*cl).next = std::ptr::null_mut();

                        if sub > 0 {
                            let last = (*(*cl).buf).last;
                            (*(*cl).buf).last = last.wrapping_sub(sub);
                        }
                    }
                    *ll = cl;
                    ll = unsafe { &mut (*cl).next };
                };
            }

            let mut current = buf.as_bytes();
            self.size += current.len();

            // if the current frame extends beyond the buffer size
            if self.offset > current.len() {
                // currently reading partial chunks from a unixfs raw leaf
                if self.last_codec == 0x55 {
                    self.unixfs_pos += current.len();
                }
                self.offset -= current.len();
                self.frame_pos += current.len();

                append_buf!();
                continue;
            }

            // if the current frame ends within this buffer
            if self.offset > 0 {
                if self.last_codec == 0x55 {
                    self.unixfs_pos += self.offset;
                }
                current = &current[self.offset..];
                self.frame_pos += self.offset;
                self.offset = 0;
            }

            while !current.is_empty() {
                let (size, read) = match usize::decode_var(current) {
                    Some(var) => var,
                    None => {
                        continue;
                    }
                };

                let frame_size = size + read;

                let split = if frame_size <= current.len() {
                    frame_size
                } else {
                    self.offset = frame_size - current.len();
                    current.len()
                };

                let (frame, next) = current.split_at(split);
                current = next;

                if self.frame_pos == 0 {
                    self.frame_pos += frame.len();
                    // CAR header can be skipped
                    continue;
                }

                let mut reader = Cursor::new(&frame[read..]);
                let cid = match Cid::read_bytes(&mut reader) {
                    Ok(cid) => cid,
                    // TODO: if CID is across 2 buffers we need some buffering
                    Err(_) => continue,
                };
                self.last_codec = cid.codec();
                match cid.codec() {
                    0x70 => {
                        // TODO: what to do for unixfs Data nodes?
                        // prob need some buffering...
                        self.frame_pos += frame.len();
                    }
                    0x55 => {
                        if self.range.contains(&(self.unixfs_pos as u64)) {
                            self.frame_pos += frame.len();
                        }
                        self.unixfs_pos += frame.len() - (read + reader.position() as usize);
                    }
                    _ => {
                        // include anything else
                        self.frame_pos += frame.len();
                    }
                };
            }

            self.count += 1;

            append_buf!();
        }
        out
    }
}

/// CarBufferReader is an iteraror returning nginx buffers ready to be inserted in an output chain
pub struct CarBufferReader<'a, R: RangeBounds<u64>> {
    // byte range to be selected in a unixfs file
    range: R,
    // pointer to the nginx output buffer chain
    buffers: *mut ngx_chain_t,
    // cursor position within the unixfs file
    unixfs_pos: u64,
    // size of the output CAR file
    car_size: usize,
    // cursor posiiton within the buffer chain
    buf_pos: usize,
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

        let header_size = size + read;

        Ok(Self {
            range,
            buffers: input,
            unixfs_pos: 0,
            car_size: header_size,
            buf_pos: 0,
            offset: header_size,
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
        'buff_chain: while !self.buffers.is_null() {
            let mut buf = unsafe { MemoryBuffer::from_ngx_buf((*self.buffers).buf) };

            self.buffers = unsafe { (*self.buffers).next };

            let start = self.buf_pos;
            let end = buf.len();
            self.buf_pos += end;

            let mut current = &buf.as_bytes()[self.offset..];
            // reset the offset
            self.offset = 0;

            while !current.is_empty() {
                if gt_bound(self.range.end_bound(), self.unixfs_pos) {
                    continue 'buff_chain;
                }

                let (size, read) = usize::decode_var(current)?;

                let frame_size = size + read;

                // TODO: handle frames spawning multiple buffers
                if frame_size > current.len() {
                    unimplemented!(
                        "TODO: frame size: {}, current size: {}",
                        frame_size,
                        current.len()
                    );
                }

                let (frame, next) = &current.split_at(frame_size);
                current = next;

                let mut cursor = Cursor::new(&frame[read..]);
                let cid = Cid::read_bytes(&mut cursor).ok()?;
                // block data
                let data = &frame[read + cursor.position() as usize..];

                // If the blocks were consumed as unixfs chunks we check
                // whether the cursor is within the range.
                match self.consume(cid, data) {
                    Ok(unixfs_read) => {
                        self.unixfs_pos += unixfs_read;
                    }
                    Err(_) => {}
                }
                self.car_size += frame_size;

                if gt_bound(self.range.end_bound(), self.unixfs_pos) {
                    break;
                }
            }

            // shorten buffer at the start
            // let inner = buf.as_ngx_buf_mut();

            // unsafe {
            //     let pos = (*inner).pos;
            //     (*inner).pos = pos.add(skip_start);
            // }

            // shorten buffer at the end
            if self.car_size < end {
                let sub = end - self.car_size;
                let inner = buf.as_ngx_buf_mut();

                unsafe {
                    let last = (*inner).last;
                    (*inner).last = last.wrapping_sub(sub);
                }
                buf.set_last_buf(true);
                buf.set_last_in_chain(true);
            }

            return Some(buf);
        }
        None
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
    fn test_range_single_buffer() {
        use crate::bindings::*;
        use std::fs::File;
        use std::io::{BufRead, BufReader};

        let f = File::open("fixture.car").unwrap();
        let mut reader = BufReader::new(f);

        let car_data = reader.fill_buf().unwrap();

        let buf1 = to_ngx_buf(&car_data[..3552]);

        let l1 = ngx_chain_s {
            buf: &buf1 as *const _ as *mut _,
            next: std::ptr::null_mut(),
        };

        let mut ctx = CarBufferContext::new(..1024);

        let mut buf = vec![];

        let cl = ngx_chain_s {
            buf: std::ptr::null_mut(),
            next: std::ptr::null_mut(),
        };

        let o1 = ctx.buffer(&l1 as *const _ as *mut _, || &cl as *const _ as *mut _);
        let b1 = unsafe { MemoryBuffer::from_ngx_buf((*o1).buf) };

        assert!(b1.is_last());

        buf.extend_from_slice(b1.as_bytes());

        // header + unxifs_root + raw block(1000) + raw_block(1000)
        assert_eq!(buf.len(), 59 + 379 + 1038 + 1038);
    }

    #[test]
    fn test_range_eq_bound() {
        use crate::bindings::*;
        use std::fs::File;
        use std::io::{BufRead, BufReader};

        let f = File::open("fixture.car").unwrap();
        let mut reader = BufReader::new(f);

        let car_data = reader.fill_buf().unwrap();

        let buf1 = to_ngx_buf(&car_data[..3552]);

        let l1 = ngx_chain_s {
            buf: &buf1 as *const _ as *mut _,
            next: std::ptr::null_mut(),
        };

        let mut ctx = CarBufferContext::new(..3001);

        let mut buf = vec![];

        let cl = ngx_chain_s {
            buf: std::ptr::null_mut(),
            next: std::ptr::null_mut(),
        };

        let o1 = ctx.buffer(&l1 as *const _ as *mut _, || &cl as *const _ as *mut _);
        let b1 = unsafe { MemoryBuffer::from_ngx_buf((*o1).buf) };

        assert!(b1.is_last());

        buf.extend_from_slice(b1.as_bytes());

        // header + unxifs_root + raw block(1000) + raw_block(1000) + raw_block(1000)
        assert_eq!(buf.len(), 59 + 379 + 1038 + 1038 + 1038);
    }

    #[test]
    fn test_range_multi_buffers() {
        use crate::bindings::*;
        use std::fs::File;
        use std::io::{BufRead, BufReader};

        let f = File::open("fixture.car").unwrap();
        let mut reader = BufReader::new(f);

        let car_data = reader.fill_buf().unwrap();

        let buf1 = to_ngx_buf(&car_data[..3552]);
        let buf2 = to_ngx_buf(&car_data[3552..]);

        let l2 = ngx_chain_s {
            buf: &buf2 as *const _ as *mut _,
            next: std::ptr::null_mut(),
        };

        let l1 = ngx_chain_s {
            buf: &buf1 as *const _ as *mut _,
            next: std::ptr::null_mut(),
        };

        let mut ctx = CarBufferContext::new(..3500);

        let mut buf = vec![];

        let cl1 = ngx_chain_s {
            buf: std::ptr::null_mut(),
            next: std::ptr::null_mut(),
        };
        let cl2 = ngx_chain_s {
            buf: std::ptr::null_mut(),
            next: std::ptr::null_mut(),
        };

        let o = ctx.buffer(&l1 as *const _ as *mut _, || &cl1 as *const _ as *mut _);
        let b = unsafe { MemoryBuffer::from_ngx_buf((*o).buf) };

        buf.extend_from_slice(b.as_bytes());

        let o = ctx.buffer(&l2 as *const _ as *mut _, || &cl2 as *const _ as *mut _);
        let b = unsafe { MemoryBuffer::from_ngx_buf((*o).buf) };

        assert!(b.is_last());

        buf.extend_from_slice(b.as_bytes());

        // header + unxifs_root + raw block(1000) + raw_block(1000) + raw_block(1000) +
        // raw_block(1000)
        assert_eq!(buf.len(), 59 + 379 + 1038 + 1038 + 1038 + 1038);
    }

    #[test]
    fn test_range_misaligned_buffers() {
        use crate::bindings::*;
        use std::fs::File;
        use std::io::{BufRead, BufReader};

        let f = File::open("fixture.car").unwrap();
        let mut reader = BufReader::new(f);

        let car_data = reader.fill_buf().unwrap();

        let buf1 = to_ngx_buf(&car_data[..4096]);
        let buf2 = to_ngx_buf(&car_data[4096..]);

        let l2 = ngx_chain_s {
            buf: &buf2 as *const _ as *mut _,
            next: std::ptr::null_mut(),
        };

        let l1 = ngx_chain_s {
            buf: &buf1 as *const _ as *mut _,
            next: std::ptr::null_mut(),
        };

        let mut ctx = CarBufferContext::new(..3500);

        let mut buf = vec![];

        let cl1 = ngx_chain_s {
            buf: std::ptr::null_mut(),
            next: std::ptr::null_mut(),
        };
        let cl2 = ngx_chain_s {
            buf: std::ptr::null_mut(),
            next: std::ptr::null_mut(),
        };

        let o = ctx.buffer(&l1 as *const _ as *mut _, || &cl1 as *const _ as *mut _);
        let b = unsafe { MemoryBuffer::from_ngx_buf((*o).buf) };

        buf.extend_from_slice(b.as_bytes());

        let o = ctx.buffer(&l2 as *const _ as *mut _, || &cl2 as *const _ as *mut _);
        let b = unsafe { MemoryBuffer::from_ngx_buf((*o).buf) };

        assert!(b.is_last());

        buf.extend_from_slice(b.as_bytes());

        // header + unxifs_root + raw block(1000) + raw_block(1000) + raw_block(1000) +
        // raw_block(1000)
        assert_eq!(buf.len(), 59 + 379 + 1038 + 1038 + 1038 + 1038);
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

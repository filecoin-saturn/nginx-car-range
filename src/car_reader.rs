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
    header: usize,
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
            header: 0,
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

            let start = self.size;

            let mut pos = start;
            let mut skip = 0;

            macro_rules! append_buf {
                () => {
                    let sub = if pos > start {
                        self.size - (pos + skip)
                    } else {
                        0
                    };

                    println!(
                        "append buf: frame_pos {}, start {}, end {}, unixfs_pos {}",
                        pos, start, self.size, self.unixfs_pos,
                    );

                    if skip == buf.len() {
                        buf.set_empty();
                        continue;
                    }

                    let is_last = match self.range.end_bound() {
                        Bound::Included(&b) => b == self.unixfs_pos as u64,
                        Bound::Excluded(&b) => b - 1 == self.unixfs_pos as u64,
                        // if the range is unbounded the last buffer should already be
                        // set as last.
                        Bound::Unbounded => false,
                    };
                    let is_seek = lt_bound(self.range.start_bound(), self.unixfs_pos as u64);

                    if sub > 0 && !is_seek || is_last {
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

                        if skip > 0 {
                            let pos = (*(*cl).buf).pos;
                            (*(*cl).buf).pos = pos.add(skip);
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

                if lt_bound(self.range.start_bound(), self.unixfs_pos as u64) {
                    skip += current.len();
                } else {
                    pos += current.len();
                }

                self.offset -= current.len();

                append_buf!();
                continue;
            }

            // if the current frame ends within this buffer
            if self.offset > 0 {
                if self.last_codec == 0x55 {
                    self.unixfs_pos += self.offset;
                }
                if lt_bound(self.range.start_bound(), self.unixfs_pos as u64) {
                    skip += self.offset;
                } else {
                    pos += self.offset;
                }
                current = &current[self.offset..];
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

                println!(
                    "frame_size {}, frame_pos {}, unixfs_pos {}, size {}",
                    frame_size, pos, self.unixfs_pos, self.size
                );

                let split = if frame_size <= current.len() {
                    frame_size
                } else {
                    self.offset = frame_size - current.len();
                    current.len()
                };

                let (frame, next) = current.split_at(split);
                current = next;

                if self.header == 0 {
                    self.header += frame.len();
                    pos += frame.len();
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
                        pos += frame.len();
                    }
                    0x55 => {
                        let unixfs_size = frame.len() - (read + reader.position() as usize);
                        let is_seek = lt_bound(self.range.start_bound(), self.unixfs_pos as u64);
                        if is_seek
                            && self
                                .range
                                .contains(&((self.unixfs_pos + unixfs_size) as u64))
                            || self.range.contains(&(self.unixfs_pos as u64))
                        {
                            pos += frame.len();
                        } else if is_seek && pos == start {
                            skip += frame.len();
                        }
                        self.unixfs_pos += unixfs_size;
                    }
                    _ => {
                        // include anything else
                        pos += frame.len();
                    }
                };
            }

            self.count += 1;

            append_buf!();
        }
        out
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

    #[test]
    fn test_range_start_multi_buffers() {
        use crate::bindings::*;
        use std::fs::File;
        use std::io::{BufRead, BufReader};

        let f = File::open("fixture.car").unwrap();
        let mut reader = BufReader::new(f);

        let car_data = reader.fill_buf().unwrap();

        let buf1 = to_ngx_buf(&car_data[..3552]);
        let mut buf2 = to_ngx_buf(&car_data[3552..]);
        buf2.set_last_buf(1);

        let l2 = ngx_chain_s {
            buf: &buf2 as *const _ as *mut _,
            next: std::ptr::null_mut(),
        };

        let l1 = ngx_chain_s {
            buf: &buf1 as *const _ as *mut _,
            next: std::ptr::null_mut(),
        };

        let mut ctx = CarBufferContext::new(4500..);

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

        // header + unxifs_root + raw_block(1000) + raw_block(1000) + raw_block(157)
        assert_eq!(buf.len(), 59 + 379 + 1038 + 1038 + 157);
    }

    #[test]
    fn test_range_filter_start_multi_buffers() {
        use crate::bindings::*;
        use std::fs::File;
        use std::io::{BufRead, BufReader};

        let f = File::open("fixture.car").unwrap();
        let mut reader = BufReader::new(f);

        let car_data = reader.fill_buf().unwrap();

        let buf1 = to_ngx_buf(&car_data[..3552]);
        let mut buf2 = to_ngx_buf(&car_data[3552..]);
        buf2.set_last_buf(1);

        let mut expected = vec![];
        expected.extend_from_slice(&car_data[..438]);
        expected.extend_from_slice(&car_data[5628..]);

        let l2 = ngx_chain_s {
            buf: &buf2 as *const _ as *mut _,
            next: std::ptr::null_mut(),
        };

        let l1 = ngx_chain_s {
            buf: &buf1 as *const _ as *mut _,
            next: std::ptr::null_mut(),
        };

        let mut ctx = CarBufferContext::new(5500..);

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

        // header + unxifs_root + raw_block(1000) + raw_block(157)
        assert_eq!(buf.len(), 59 + 379 + 1038 + 157);
        assert_eq!(buf, expected);
    }

    #[test]
    fn test_range_skip_start_multi_buffers() {
        use crate::bindings::*;
        use std::fs::File;
        use std::io::{BufRead, BufReader};

        let f = File::open("fixture.car").unwrap();
        let mut reader = BufReader::new(f);

        let car_data = reader.fill_buf().unwrap();

        let buf1 = to_ngx_buf(&car_data[..2514]);
        let buf2 = to_ngx_buf(&car_data[2514..4590]);
        let mut buf3 = to_ngx_buf(&car_data[4590..]);
        buf3.set_last_buf(1);

        let mut expected = vec![];
        expected.extend_from_slice(&car_data[..438]);
        expected.extend_from_slice(&car_data[5628..]);

        let l3 = ngx_chain_s {
            buf: &buf3 as *const _ as *mut _,
            next: std::ptr::null_mut(),
        };

        let l2 = ngx_chain_s {
            buf: &buf2 as *const _ as *mut _,
            next: std::ptr::null_mut(),
        };

        let l1 = ngx_chain_s {
            buf: &buf1 as *const _ as *mut _,
            next: std::ptr::null_mut(),
        };

        let mut ctx = CarBufferContext::new(5500..);

        let mut buf = vec![];

        let cl1 = ngx_chain_s {
            buf: std::ptr::null_mut(),
            next: std::ptr::null_mut(),
        };
        let cl2 = ngx_chain_s {
            buf: std::ptr::null_mut(),
            next: std::ptr::null_mut(),
        };
        let cl3 = ngx_chain_s {
            buf: std::ptr::null_mut(),
            next: std::ptr::null_mut(),
        };

        let o = ctx.buffer(&l1 as *const _ as *mut _, || &cl1 as *const _ as *mut _);
        let b = unsafe { MemoryBuffer::from_ngx_buf((*o).buf) };

        buf.extend_from_slice(b.as_bytes());

        let o = ctx.buffer(&l2 as *const _ as *mut _, || &cl2 as *const _ as *mut _);
        assert!(o.is_null());
        let b = MemoryBuffer::from_ngx_buf(l2.buf);
        assert!(b.is_empty());

        let o = ctx.buffer(&l3 as *const _ as *mut _, || &cl3 as *const _ as *mut _);
        let b = unsafe { MemoryBuffer::from_ngx_buf((*o).buf) };

        assert!(b.is_last());

        buf.extend_from_slice(b.as_bytes());

        // header + unxifs_root + raw_block(1000) + raw_block(157)
        assert_eq!(buf.len(), 59 + 379 + 1038 + 157);
        assert_eq!(buf, expected);
    }

    #[test]
    fn test_range_filter_start_missaligned_buffers() {
        use crate::bindings::*;
        use std::fs::File;
        use std::io::{BufRead, BufReader};

        let f = File::open("fixture.car").unwrap();
        let mut reader = BufReader::new(f);

        let car_data = reader.fill_buf().unwrap();

        let buf1 = to_ngx_buf(&car_data[..4096]);
        let mut buf2 = to_ngx_buf(&car_data[4096..]);
        buf2.set_last_buf(1);

        let mut expected = vec![];
        expected.extend_from_slice(&car_data[..438]);
        expected.extend_from_slice(&car_data[5628..]);

        let l2 = ngx_chain_s {
            buf: &buf2 as *const _ as *mut _,
            next: std::ptr::null_mut(),
        };

        let l1 = ngx_chain_s {
            buf: &buf1 as *const _ as *mut _,
            next: std::ptr::null_mut(),
        };

        let mut ctx = CarBufferContext::new(5500..);

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

        // header + unxifs_root + raw_block(1000) + raw_block(157)
        assert_eq!(buf.len(), 59 + 379 + 1038 + 157);
        assert_eq!(buf, expected);
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

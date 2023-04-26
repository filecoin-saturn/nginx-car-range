use crate::bindings::*;
use crate::pool::{Buffer, MemoryBuffer};
use crate::varint::VarInt;
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
    pub size: usize,
    pub count: usize,
    pub unixfs_pos: usize,
    offset: usize,
    header: usize,
    last_codec: u64,
    done: usize,
    buf: Vec<u8>,
    unixfs_buf: Vec<u8>,
    buffer_chain: *mut ngx_chain_t,
    _marker: PhantomData<&'a ()>,
}

impl<'a, R: RangeBounds<u64>> CarBufferContext<'a, R> {
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
            buf: Vec::with_capacity(64),
            // set the capacity to the default unixfs block size
            unixfs_buf: Vec::with_capacity(262154),
            buffer_chain: std::ptr::null_mut(),
            _marker: PhantomData,
        }
    }

    pub fn buffer<F: FnMut() -> *mut ngx_chain_t>(
        &mut self,
        input: *mut ngx_chain_t,
        mut alloc_cl: F,
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

            // we call this macro when we've parsed and filtered all the frames in the chunk
            // depending on the buffer size we might iterate over multiple frames or just append it
            // to our buffer until we get to the next boundary.
            macro_rules! append_buf {
                () => {
                    let sub = if pos > start {
                        self.size - (pos + skip)
                    } else {
                        0
                    };

                    if skip == buf.len() {
                        // if the buffer should be skipped and the last codec was unixfs
                        // we save it in case the boundary is in this chunk.
                        if self.last_codec == 0x70 {
                            let mut cl = alloc_cl();
                            if cl.is_null() {
                                continue;
                            }
                            unsafe {
                                (*cl).buf = buf.as_ngx_buf_mut();
                                (*cl).next = std::ptr::null_mut();
                            }
                            self.append_to_chain(cl);
                        } else {
                            buf.set_empty();
                        }
                        continue;
                    }

                    let is_last = match self.range.end_bound() {
                        Bound::Included(&b) => b == self.unixfs_pos as u64,
                        Bound::Excluded(&b) => b - 1 == self.unixfs_pos as u64,
                        // if the range is unbounded the last buffer should already be
                        // set as last.
                        Bound::Unbounded => false,
                    };
                    if sub > 0 && !self.is_seek() || is_last {
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
                            ngx_buf_remove_end((*cl).buf, sub);
                        }

                        if skip > 0 {
                            ngx_buf_remove_start((*cl).buf, skip);
                        }
                    }
                    *ll = cl;
                    ll = unsafe { &mut (*cl).next };
                };
            }

            // current is a slice of the current buffer that we shrink as we read data
            let mut current = buf.as_bytes();
            self.size += current.len();

            // if last_code == 0, there was not enough data in the last buffer to read the CID
            if self.last_codec == 0 && self.offset > 0 {
                // fill the buffer if enough data is available
                let avail = self.buf.capacity() - self.buf.len();
                let eb = if avail > current.len() {
                    current.len()
                } else {
                    avail
                };
                // efficiently append the current buffer to self.buf until the capacity is reached
                unsafe {
                    let ptr = self.buf.as_mut_ptr().add(self.buf.len());
                    std::ptr::copy_nonoverlapping(current.as_ptr(), ptr, eb);
                    self.buf.set_len(self.buf.len() + eb);
                }

                // attempt at reading a CID from buffered data
                let mut reader = Cursor::new(&self.buf[..]);
                match Cid::read_bytes(&mut reader) {
                    Ok(cid) => {
                        self.last_codec = cid.codec();
                        let sb = self.buf.len()
                            - (reader.position() as usize)
                            - (self.buf.capacity() - avail);
                        self.buf.clear();
                        current = &buf.as_bytes()[sb..];
                        self.offset -= sb;
                    }
                    Err(_) => {
                        // extend the buffer cap ?;
                    }
                };
            }
            // a macro to append the buffered chain to the end of the current chain
            macro_rules! append_buffered_chain {
                () => {
                    if !self.buffer_chain.is_null() {
                        let mut cl = self.buffer_chain;
                        while !unsafe { (*cl).next.is_null() } {
                            cl = unsafe { (*cl).next };
                        }
                        // we need to include the previous chunks too
                        *ll = self.buffer_chain;
                        ll = unsafe { &mut (*cl).next };
                    }
                };
            }
            // a macro to advance the skip or pos values based on a given size and last_codec
            // used only when we continue reading a previous frame in this buffer.
            macro_rules! advance {
                ($size:expr) => {
                    match self.last_codec {
                        0x70 => {
                            let mut i = 0;
                            while i < $size {
                                self.unixfs_buf.push(current[i]);
                                i += 1;
                            }
                            // if the offset is within the current buffer
                            if self.offset <= current.len() {
                                let frame = &self.unixfs_buf[..];
                                if let Some(unixfs_size) = self.decode_unixfs_size(frame) {
                                    self.unixfs_pos += unixfs_size;
                                    if self.is_seek() {
                                        // we're still seeking so we can skip the part of the
                                        // buffer before it gets buffered by the unixfs decoder
                                        ngx_buf_remove_start(buf.as_ngx_buf_mut(), $size);
                                    } else {
                                        append_buffered_chain!();
                                        pos += $size;
                                    }
                                } else {
                                    // if there is no unixfs size, it must be an intermediary node
                                    // so we forward it.
                                    append_buffered_chain!();
                                    pos += $size;
                                }
                                self.unixfs_buf.clear();
                                self.buffer_chain = std::ptr::null_mut();
                            } else {
                                if self.is_seek() {
                                    skip += $size;
                                } else {
                                    pos += $size;
                                }
                            }
                        }
                        0x55 => {
                            self.unixfs_pos += $size;
                            if self.is_seek() {
                                skip += $size;
                            } else {
                                pos += $size;
                            }
                        }
                        0 => {}
                        _ => {
                            pos += $size;
                        }
                    };
                };
            }

            // if the current frame extends beyond the buffer size
            if self.offset >= current.len() {
                // use the advance macro based on current.len()
                advance!(current.len());

                self.offset -= current.len();

                append_buf!();
                continue;
            }

            // if the current frame ends within this buffer
            if self.offset > 0 {
                // use the advance macro based on self.offset
                advance!(self.offset);

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
                // reset previous frame codec.
                self.last_codec = 0;

                let frame_size = size + read;

                println!("frame size: {}", frame_size);

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

                    let header: CarHeader = serde_ipld_dagcbor::from_slice(&frame[read..]).unwrap();
                    if header.version != 1 {
                        // TODO: we should error here
                        panic!("unsupported car version {}", header.version);
                    }
                    // CAR header can be skipped
                    continue;
                }

                let mut reader = Cursor::new(&frame[read..]);
                let cid = match Cid::read_bytes(&mut reader) {
                    Ok(cid) => cid,
                    // If CID is across 2 buffers we need some buffering
                    Err(_) => {
                        let mut i = read;
                        while i < frame.len() {
                            self.buf.push(frame[i]);
                            i += 1;
                        }

                        continue;
                    }
                };
                self.last_codec = cid.codec();
                let position = read + reader.position() as usize;
                if let Some(unixfs_size) = match cid.codec() {
                    // dagpb
                    0x70 => {
                        if self.offset > 0 {
                            // need to buffer the whole frame before we can decode :/
                            if self.is_seek() || self.range.contains(&(self.unixfs_pos as u64)) {
                                let mut i = position;
                                while i < frame.len() {
                                    self.unixfs_buf.push(frame[i]);
                                    i += 1;
                                }
                                if self.is_seek() {
                                    skip += frame.len();
                                } else {
                                    pos += frame.len();
                                }
                            } else {
                                continue;
                            }
                            None
                        } else {
                            self.decode_unixfs_size(&frame[position..]).or_else(|| {
                                pos += frame.len();
                                None
                            })
                        }
                    }
                    // raw leaf
                    0x55 => Some(frame.len() - position),
                    _ => {
                        // include everything else
                        pos += frame.len();
                        None
                    }
                } {
                    if self.is_seek()
                        && self
                            .range
                            .contains(&((self.unixfs_pos + unixfs_size) as u64))
                        || self.range.contains(&(self.unixfs_pos as u64))
                    {
                        pos += frame.len();
                    } else if self.is_seek() && pos == start {
                        skip += frame.len();
                    }
                    self.unixfs_pos += unixfs_size;
                }
            }

            self.count += 1;

            append_buf!();
        }

        out
    }

    fn is_seek(&self) -> bool {
        lt_bound(self.range.start_bound(), self.unixfs_pos as u64)
    }

    // a util method for decoding a unixfs node from a byte slice
    fn decode_unixfs_size(&self, data: &[u8]) -> Option<usize> {
        let outer = dag_pb::PbNode::decode(data).ok()?;
        if !outer.links.is_empty() {
            return None;
        }
        let inner_data = outer.data?;
        let size = inner_data.len();
        let inner = match unixfs_pb::Data::decode(inner_data) {
            Ok(inner) => inner,
            Err(_) => {
                return Some(size);
            }
        };
        inner.data.map(|data| data.len()).or(Some(size))
    }

    // append a buffer to the end of the buffer_chain
    fn append_to_chain(&mut self, link: *mut ngx_chain_t) {
        unsafe {
            // if the buffer chain is empty, set the first buffer to the new buffer
            if self.buffer_chain.is_null() {
                self.buffer_chain = link;
            } else {
                // otherwise, append the buffer to the end of the chain
                let mut current = self.buffer_chain;
                while !(*current).next.is_null() {
                    current = (*current).next;
                }
                (*current).next = link;
            }
        }
    }
}

// a function to remove bytes at the end of a ngx_buf_s mutable pointer
fn ngx_buf_remove_end(buf: *mut ngx_buf_s, len: usize) {
    // assert that the buffer is not null
    assert!(!buf.is_null());
    unsafe {
        (*buf).last = (*buf).last.sub(len);
        // if the buffer is in a file, adjust the file_last value
        if (*buf).in_file() == 1 {
            (*buf).file_last -= len as i64;
        }
    }
}

fn ngx_buf_remove_start(buf: *mut ngx_buf_s, len: usize) {
    // assert that the buffer is not null
    assert!(!buf.is_null());
    unsafe {
        (*buf).pos = (*buf).pos.add(len);
        if (*buf).in_file() == 1 {
            (*buf).file_pos += len as i64;
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

    #[test]
    fn test_range_skip_start_shorter_buffers() {
        use crate::bindings::*;
        use std::fs::File;
        use std::io::{BufRead, BufReader};

        let f = File::open("fixture.car").unwrap();
        let mut reader = BufReader::new(f);

        let car_data = reader.fill_buf().unwrap();

        let buf1 = to_ngx_buf(&car_data[..2614]);
        let buf2 = to_ngx_buf(&car_data[2614..3100]);
        let mut buf3 = to_ngx_buf(&car_data[3100..]);
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

    // test against some small buffers that are not aligned with the block size.
    // the buffers were generated by a real nginx instance.
    #[test]
    fn test_range_tiny_buffers() {
        use crate::bindings::*;

        let chunk1 = hex::decode("3aa265726f6f747381d82a58250001701220afcf9cd411b135aa1db2dd89bef443a93fb7894d2dfcbb657a732671a363b7b56776657273696f6e01").unwrap();
        let chunk2 = hex::decode("61").unwrap();
        let chunk3 =
            hex::decode("01701220afcf9cd411b135aa1db2dd89bef443a93fb7894d2dfcbb657a732671a363b7b5")
                .unwrap();
        let chunk4 = hex::decode("12370a2401701220cbd6719e57578084e7fecd530689626bfbe56aa4b0b6cfb334fc6f0667dcad2d120a4d657869636f2e4a504718a4c6dc010a020801").unwrap();
        let chunk5 = hex::decode("f501").unwrap();
        let chunk6 =
            hex::decode("01701220cbd6719e57578084e7fecd530689626bfbe56aa4b0b6cfb334fc6f0667dcad2d")
                .unwrap();
        let chunk7 = hex::decode("122c0a2401551220e82a3d5e3a3ec338f410510043f2b923d8d62bfb158d9ae5133cf160fda8defd120018808040122c0a2401551220607a8cb575d30e374f784922d1cdd2103953331ee0e42592f5e76a55ba4ee5be120018808040122c0a2401551220fb34ed5811abf2bf81e6e0ad788e1c38344446826b5ff399226d3262259bd98b120018808040122c0a2401551220fa38ae984cf83a6076147dbe8aac2cedb24509c35649ca7380a7f233de10d70a120018d3c41c0a17080218d3c4dc0120808040208080402080804020d3c41c").unwrap();

        let exp = [
            &chunk1[..],
            &chunk2[..],
            &chunk3[..],
            &chunk4[..],
            &chunk5[..],
            &chunk6[..],
            &chunk7[..],
        ]
        .concat();

        let buf1 = to_ngx_buf(&chunk1[..]);
        let buf2 = to_ngx_buf(&chunk2[..]);
        let buf3 = to_ngx_buf(&chunk3[..]);
        let buf4 = to_ngx_buf(&chunk4[..]);
        let buf5 = to_ngx_buf(&chunk5[..]);
        let buf6 = to_ngx_buf(&chunk6[..]);
        let buf7 = to_ngx_buf(&chunk7[..]);

        let l7 = ngx_chain_s {
            buf: &buf7 as *const _ as *mut _,
            next: std::ptr::null_mut(),
        };
        let l6 = ngx_chain_s {
            buf: &buf6 as *const _ as *mut _,
            next: &l7 as *const _ as *mut _,
        };
        let l5 = ngx_chain_s {
            buf: &buf5 as *const _ as *mut _,
            next: &l6 as *const _ as *mut _,
        };
        let l4 = ngx_chain_s {
            buf: &buf4 as *const _ as *mut _,
            next: &l5 as *const _ as *mut _,
        };
        let l3 = ngx_chain_s {
            buf: &buf3 as *const _ as *mut _,
            next: &l4 as *const _ as *mut _,
        };
        let l2 = ngx_chain_s {
            buf: &buf2 as *const _ as *mut _,
            next: &l3 as *const _ as *mut _,
        };
        let l1 = ngx_chain_s {
            buf: &buf1 as *const _ as *mut _,
            next: &l2 as *const _ as *mut _,
        };

        // new links mocked alloc from the ngx pool
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
        let cl4 = ngx_chain_s {
            buf: std::ptr::null_mut(),
            next: std::ptr::null_mut(),
        };
        let cl5 = ngx_chain_s {
            buf: std::ptr::null_mut(),
            next: std::ptr::null_mut(),
        };
        let cl6 = ngx_chain_s {
            buf: std::ptr::null_mut(),
            next: std::ptr::null_mut(),
        };
        let cl7 = ngx_chain_s {
            buf: std::ptr::null_mut(),
            next: std::ptr::null_mut(),
        };

        let mut cli = 0;

        let mut ctx = CarBufferContext::new(5500..);

        let o = ctx.buffer(&l1 as *const _ as *mut _, || {
            cli += 1;
            let cl = match cli {
                1 => &cl1,
                2 => &cl2,
                3 => &cl3,
                4 => &cl4,
                5 => &cl5,
                6 => &cl6,
                _ => &cl7,
            };
            cl as *const _ as *mut _
        });

        assert!(&cl1 as *const _ as *mut _ == o);

        let mut result = vec![];

        let mut cl = o;
        while !cl.is_null() {
            let buf = unsafe { MemoryBuffer::from_ngx_buf((*cl).buf) };
            cl = unsafe { (*cl).next };
            result.extend_from_slice(buf.as_bytes());
        }

        assert_eq!(result, exp);
    }

    // verify that ngx_buf_remove_end can remove 24 bytes at the end of a 1kb buffer
    #[test]
    fn test_buf_remove_end() {
        let mut buf = to_ngx_buf(&vec![0u8; 1024][..]);
        let mut buf = MemoryBuffer::from_ngx_buf(&mut buf);
        ngx_buf_remove_end(buf.as_ngx_buf_mut(), 24);
        assert_eq!(buf.len(), 1000);
    }

    // test CarBufferContext::buffer against a chain of 1 empty buffer
    // and an unbounded range
    #[test]
    fn test_buf_filter_chain_empty() {
        let buf = to_ngx_buf(&vec![0u8; 0][..]);
        // check that the buffer is empty
        assert_eq!(buf.last, buf.pos);

        let chain = ngx_chain_s {
            buf: &buf as *const _ as *mut _,
            next: std::ptr::null_mut(),
        };

        let mut ctx = CarBufferContext::new(..);

        let o = ctx.buffer(&chain as *const _ as *mut _, || {
            panic!("should not be called");
        });

        assert!(o.is_null());
    }

    #[test]
    fn test_buf_file_dag_pb_leaves() {
        use crate::bindings::*;
        use std::fs::File;
        use std::io::{BufReader, Read};

        let f = File::open("./midfixture.car").unwrap();
        let mut reader = BufReader::new(f);

        let mut car_data = vec![];
        reader.read_to_end(&mut car_data).unwrap();

        // break down the car_data into a list of 32768 byte buffers
        let mut bufs = vec![];
        let mut offset = 0;
        while offset < car_data.len() {
            let buf = to_ngx_buf(&car_data[offset..std::cmp::min(car_data.len(), offset + 32768)]);
            bufs.push(buf);
            offset += 32768;
        }

        // create a vec of ngx_chain_s containing each buffer
        let mut chains: Vec<ngx_chain_s> = Vec::with_capacity(bufs.len());
        for _ in 0..bufs.len() {
            let chain = ngx_chain_s {
                buf: std::ptr::null_mut(),
                next: std::ptr::null_mut(),
            };
            chains.push(chain);
        }

        // create a list of empty chain links to return in the callback
        let mut empty_chains = Vec::with_capacity(chains.len());
        for _ in 0..chains.len() {
            let chain = ngx_chain_s {
                buf: std::ptr::null_mut(),
                next: std::ptr::null_mut(),
            };
            empty_chains.push(chain);
        }

        let mut ctx = CarBufferContext::new(..200000);

        let mut buf = vec![];

        // iterate over the chains and buffer them
        // the callback will return an empty chain
        let mut i = 0;
        while ctx.done == 0 {
            assert!(buf.len() <= 265577);
            let cl: *mut ngx_chain_t = &chains[i] as *const _ as *mut _;
            unsafe { (*cl).buf = &bufs[i] as *const _ as *mut _ };
            let o = ctx.buffer(cl, || {
                let chain = &empty_chains[i] as *const _ as *mut _;
                chain
            });

            i += 1;
            // add the buffered data to the output buffer
            let mut cl = o;
            while !cl.is_null() {
                let b = unsafe { MemoryBuffer::from_ngx_buf((*cl).buf) };
                cl = unsafe { (*cl).next };
                b.as_bytes().iter().for_each(|b| buf.push(*b));
            }
        }

        // header(57) + unxifs_dir(591) + unixfs_file(2734) + unixfs_block(262195)
        assert_eq!(buf.len(), 57 + 591 + 2734 + 262195);
    }

    #[test]
    fn test_buf_file_dag_pb_leaves_offset() {
        use crate::bindings::*;
        use std::fs::File;
        use std::io::{BufReader, Read};

        let f = File::open("./midfixture.car").unwrap();
        let mut reader = BufReader::new(f);

        let mut car_data = vec![];
        reader.read_to_end(&mut car_data).unwrap();

        // break down the car_data into a list of 32768 byte buffers
        let mut bufs = vec![];
        let mut offset = 0;
        while offset < car_data.len() {
            let buf = to_ngx_buf(&car_data[offset..std::cmp::min(car_data.len(), offset + 32768)]);
            bufs.push(buf);
            offset += 32768;
        }

        // create a vec of ngx_chain_s containing each buffer
        let mut chains: Vec<ngx_chain_s> = Vec::with_capacity(bufs.len());
        for _ in 0..bufs.len() {
            let chain = ngx_chain_s {
                buf: std::ptr::null_mut(),
                next: std::ptr::null_mut(),
            };
            chains.push(chain);
        }

        // create a list of empty chain links to return in the callback
        let mut empty_chains = Vec::with_capacity(chains.len());
        for _ in 0..chains.len() {
            let chain = ngx_chain_s {
                buf: std::ptr::null_mut(),
                next: std::ptr::null_mut(),
            };
            empty_chains.push(chain);
        }

        // create a list of empty buffers to return in the callback
        let mut empty_bufs = Vec::with_capacity(chains.len());
        for _ in 0..chains.len() {
            let buf = ngx_buf_s {
                pos: std::ptr::null_mut(),
                last: std::ptr::null_mut(),
                file_pos: 0,
                file_last: 0,
                start: std::ptr::null_mut(),
                end: std::ptr::null_mut(),
                tag: std::ptr::null_mut(),
                file: std::ptr::null_mut(),
                shadow: std::ptr::null_mut(),
                _bitfield_align_1: [0u8; 0],
                _bitfield_1: ngx_buf_s::new_bitfield_1(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
                num: 0,
            };
            empty_bufs.push(buf);
        }

        // select a range in the second chunk
        let mut ctx = CarBufferContext::new(263000..333333);

        let mut buf = vec![];

        // iterate over the chains and buffer them
        // the callback will return an empty chain
        let mut i = 0;
        let mut j = 0;
        while ctx.done == 0 {
            assert!(buf.len() <= 265577);
            let cl: *mut ngx_chain_t = &chains[i] as *const _ as *mut _;
            unsafe { (*cl).buf = &bufs[i] as *const _ as *mut _ };
            let o = ctx.buffer(cl, || {
                let chain = &empty_chains[i] as *const _ as *mut _;
                i += 1;
                chain
            });

            j += 1;
            // add the buffered data to the output buffer
            let mut cl = o;
            while !cl.is_null() {
                let b = unsafe { MemoryBuffer::from_ngx_buf((*cl).buf) };
                println!("return buff {}", b.len());
                cl = unsafe { (*cl).next };
                b.as_bytes().iter().for_each(|b| buf.push(*b));
            }
        }

        // header(57) + unxifs_dir(591) + unixfs_file(2734) + unixfs_block(262195)
        assert_eq!(buf.len(), 57 + 591 + 2734 + 262195);
    }

    #[test]
    fn test_buf_dag_pb_leaves_offset_2blks() {
        use crate::bindings::*;
        use std::fs::File;
        use std::io::{BufReader, Read};

        let f = File::open("./midfixture.car").unwrap();
        let mut reader = BufReader::new(f);

        let mut car_data = vec![];
        reader.read_to_end(&mut car_data).unwrap();

        // break down the car_data into a list of 32768 byte buffers
        let mut bufs = vec![];
        let mut offset = 0;
        while offset < car_data.len() {
            let buf = to_ngx_buf(&car_data[offset..std::cmp::min(car_data.len(), offset + 32768)]);
            bufs.push(buf);
            offset += 32768;
        }

        // create a vec of ngx_chain_s containing each buffer
        let mut chains: Vec<ngx_chain_s> = Vec::with_capacity(bufs.len());
        for _ in 0..bufs.len() {
            let chain = ngx_chain_s {
                buf: std::ptr::null_mut(),
                next: std::ptr::null_mut(),
            };
            chains.push(chain);
        }

        // create a list of empty chain links to return in the callback
        let mut empty_chains = Vec::with_capacity(chains.len());
        for _ in 0..chains.len() {
            let chain = ngx_chain_s {
                buf: std::ptr::null_mut(),
                next: std::ptr::null_mut(),
            };
            empty_chains.push(chain);
        }

        // create a list of empty buffers to return in the callback
        let mut empty_bufs = Vec::with_capacity(chains.len());
        for _ in 0..chains.len() {
            let buf = ngx_buf_s {
                pos: std::ptr::null_mut(),
                last: std::ptr::null_mut(),
                file_pos: 0,
                file_last: 0,
                start: std::ptr::null_mut(),
                end: std::ptr::null_mut(),
                tag: std::ptr::null_mut(),
                file: std::ptr::null_mut(),
                shadow: std::ptr::null_mut(),
                _bitfield_align_1: [0u8; 0],
                _bitfield_1: ngx_buf_s::new_bitfield_1(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
                num: 0,
            };
            empty_bufs.push(buf);
        }

        // select a range in the second chunk
        let mut ctx = CarBufferContext::new(555555..999999);

        let mut buf = vec![];

        // iterate over the chains and buffer them
        // the callback will return an empty chain
        let mut i = 0;
        let mut j = 0;
        while ctx.done == 0 {
            let cl: *mut ngx_chain_t = &chains[i] as *const _ as *mut _;
            unsafe { (*cl).buf = &bufs[i] as *const _ as *mut _ };
            let o = ctx.buffer(cl, || {
                let chain = &empty_chains[i] as *const _ as *mut _;
                i += 1;
                chain
            });

            j += 1;
            // add the buffered data to the output buffer
            let mut cl = o;
            while !cl.is_null() {
                let b = unsafe { MemoryBuffer::from_ngx_buf((*cl).buf) };
                cl = unsafe { (*cl).next };
                b.as_bytes().iter().for_each(|b| buf.push(*b));
            }
        }

        // header(57) + unxifs_dir(591) + unixfs_file(2734) + unixfs_block(262195) + unixfs_block(262195)
        assert_eq!(buf.len(), 57 + 591 + 2734 + 262195 + 262195);
    }
}

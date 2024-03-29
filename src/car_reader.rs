use crate::bindings::*;
use crate::pool::{Allocator, Buffer, MemoryBuffer};
use crate::varint::VarInt;
use cid::Cid;
use core2::io::{self, Cursor};
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use std::ops::{Bound, Range, RangeBounds};

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

fn ranges_overlap<T: RangeBounds<u64>>(range1: T, range2: Range<usize>) -> bool {
    let (start1, end1) = (
        match range1.start_bound() {
            Bound::Included(x) => *x,
            Bound::Excluded(x) => *x + 1,
            Bound::Unbounded => u64::MIN,
        },
        match range1.end_bound() {
            Bound::Included(x) => *x + 1,
            Bound::Excluded(x) => *x,
            Bound::Unbounded => u64::MAX,
        },
    );
    let (start2, end2) = (range2.start as u64, range2.end as u64);

    start1 < end2 && start2 < end1
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

pub struct CarBufferContext<'a, R: RangeBounds<u64> + Clone, A: Allocator> {
    pool: A,
    framed: Framed<R>,
    done: usize,
    pos: usize,
    _marker: PhantomData<&'a ()>,
}

impl<'a, R: RangeBounds<u64> + Clone, A: Allocator> CarBufferContext<'a, R, A> {
    pub fn new(range: R, pool: A) -> Self {
        Self {
            pool,
            framed: Framed::new(range),
            done: 0,
            pos: 0,
            _marker: PhantomData,
        }
    }

    pub fn buffer(&mut self, input: *mut ngx_chain_t) -> *mut ngx_chain_t {
        // start with the first chain link
        let mut cl = input;
        // output buffer chain is null by default
        let mut out: *mut ngx_chain_t = std::ptr::null_mut();
        // once we sent the last buffer this method will always return null
        if self.done == 1 {
            return out;
        }
        // keep track of the last link so we can append to it
        let mut ll = &mut out;
        // iterate over the chain until the next link is null
        while !cl.is_null() {
            let mut buf = unsafe { MemoryBuffer::from_ngx_buf((*cl).buf) };
            cl = unsafe { (*cl).next };

            println!("==> buf.len(): {}", buf.len());

            // TODO: handle internal errors
            let parts = self.framed.next(buf.as_bytes()).unwrap();

            for (start, end) in parts {
                println!("==> start: {}, end: {}", start, end);
                self.pos = end;
                let sub = buf.len() - end;

                let is_last = match self.framed.range.end_bound() {
                    Bound::Included(&b) => b == self.framed.unixfs_read as u64,
                    Bound::Excluded(&b) => b - 1 == self.framed.unixfs_read as u64,
                    // if the range is unbounded the last buffer should already be
                    // set as last.
                    Bound::Unbounded => false,
                };

                if sub > 0 && !self.framed.is_seek() || is_last {
                    println!("==> sub: {}, is_last: {}", sub, is_last);
                    self.done = 1;
                    buf.set_last_buf(true);
                    buf.set_last_in_chain(true);
                }

                if sub == buf.len() || start == end {
                    buf.set_empty();
                    continue;
                }

                let mut cl = self.pool.alloc_chain();
                if cl.is_null() {
                    continue;
                }
                unsafe {
                    (*cl).buf = buf.as_ngx_buf_mut();
                    (*cl).next = std::ptr::null_mut();

                    if sub > 0 {
                        ngx_buf_remove_end((*cl).buf, sub);
                    }

                    if start > 0 {
                        ngx_buf_remove_start((*cl).buf, start);
                    }
                }
                *ll = cl;
                ll = unsafe { &mut (*cl).next };

                // TODO: for now we don't handle splitting buffers
                break;
            }
        }

        out
    }

    pub fn done(&self) -> bool {
        self.done == 1
    }

    pub fn unixfs_read(&self) -> usize {
        self.framed.unixfs_read
    }

    pub fn pos(&self) -> usize {
        self.pos
    }
}

// a function to remove bytes at the end of a ngx_buf_s mutable pointer
fn ngx_buf_remove_end(buf: *mut ngx_buf_s, len: usize) {
    // assert that the buffer is not null
    assert!(!buf.is_null());
    unsafe {
        (*buf).last = (*buf).last.wrapping_sub(len);
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

#[derive(Debug, PartialEq)]
enum WireType {
    Varint = 0,
    SixtyFourBit = 1,
    LengthDelimited = 2,
    StartGroup = 3,
    EndGroup = 4,
    ThirtyTwoBit = 5,
}

impl TryFrom<u64> for WireType {
    type Error = anyhow::Error;

    #[inline]
    fn try_from(value: u64) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(WireType::Varint),
            1 => Ok(WireType::SixtyFourBit),
            2 => Ok(WireType::LengthDelimited),
            3 => Ok(WireType::StartGroup),
            4 => Ok(WireType::EndGroup),
            5 => Ok(WireType::ThirtyTwoBit),
            _ => Err(anyhow::format_err!("invalid wire type value: {}", value)),
        }
    }
}

#[derive(Debug, PartialEq)]
enum FrameType {
    CarHeader,
    Block,
    Cid,
    RawLeaf,
    MerkleDag,
    PBLinks,
    PBData,
    UnixFs,
    DataType,
    FileSize,
    BlockSizes,
    UnixFsData,
}

struct Framed<R: RangeBounds<u64> + Clone> {
    // the size of the current frame
    len: usize,
    // the size of the CAR block containing the current frame
    blk_len: usize,
    // the position of the current frame in the CAR block
    blk_pos: usize,
    // the buffer containing enough bytes to decode the varing or CID
    buf: Vec<u8>,
    // the range of the CAR file we are reading from.
    range: R,
    // the current position in the unixfs file data
    unixfs_read: usize,
    // the size of the unixfs frame
    unixfs_len: usize,
    // if the current dag node has links in which case it will be included
    has_links: bool,
    // the current frame type
    state: FrameType,
}

impl<R: RangeBounds<u64> + Clone> Framed<R> {
    fn new(range: R) -> Self {
        Self {
            len: 0,
            blk_len: 0,
            blk_pos: 0,
            unixfs_read: 0,
            unixfs_len: 0,
            has_links: false,
            range,
            buf: Vec::with_capacity(72),
            state: FrameType::CarHeader,
        }
    }

    // reads all the frames in the buffer returning the number of bytes to remove from the start
    // and end.
    fn next(&mut self, buf: &[u8]) -> io::Result<Vec<(usize, usize)>> {
        let mut ranges = Vec::with_capacity(2);
        let mut start = 0;
        let mut pos = 0;
        let mut maybe = 0;
        let mut current = buf;
        while current.len() > 0 {
            if gt_bound(self.range.end_bound(), self.unixfs_read as u64) {
                ranges.push((start, pos));
                return Ok(ranges);
            }
            if self.state == FrameType::Cid {
                match self.decode_cid(current) {
                    Some((cid, read)) => {
                        println!("cid: {:?}, read {}", cid, read);
                        self.state = FrameType::Block;
                        current = &current[read..];

                        if self.include_block() {
                            pos += read;
                        } else {
                            println!("skipping block pos: {}", pos);
                            maybe += read;
                        }

                        match cid.codec() {
                            0x55 => {
                                self.state = FrameType::RawLeaf;
                                self.len = self.blk_len - self.blk_pos;
                                self.unixfs_len = self.len;
                            }
                            0x70 => {
                                self.state = FrameType::MerkleDag;
                            }
                            _ => {
                                unimplemented!();
                            }
                        };

                        if self.include_block() || self.blk_len < 1000 {
                            pos += maybe;
                            maybe = 0;
                        }
                        continue;
                    }
                    None => {
                        current = &[];

                        // bit of a hack but we assume that a unixfs chunk won't be smaller
                        // than 1kb so we consider it some kind of intermediate node and include it.
                        if self.include_block() || self.blk_len < 1000 {
                            pos = buf.len();
                        }

                        continue;
                    }
                };
            }
            // beginning of the frame
            if self.len == 0 {
                match self.decode_len(current) {
                    Some((size, read)) => {
                        println!("decoded size: {}, read: {}", size, read);
                        current = &current[read..];
                        self.len = size;

                        if self.include_block() {
                            pos += read;
                        } else {
                            println!("skipping block pos: {}", pos);
                            maybe += read;
                        }

                        match self.state {
                            FrameType::Block => {
                                self.state = FrameType::Cid;
                                self.blk_len = size;
                                self.len = 0;
                                self.has_links = false;

                                // best effort
                                if self.blk_len < 1000 {
                                    pos += maybe;
                                    maybe = 0;
                                }
                            }
                            FrameType::MerkleDag => {
                                self.blk_pos += read;

                                let key = size as u64;
                                let wire_type = WireType::try_from(key & 0x7).unwrap();
                                let tag = key as u32 >> 3;

                                match tag {
                                    2 => {
                                        self.state = FrameType::PBLinks;
                                        self.len = 0;
                                    }
                                    1 => {
                                        self.state = FrameType::PBData;
                                        self.len = 0;
                                    }
                                    _ => unreachable!(),
                                };
                            }
                            FrameType::UnixFs => {
                                self.blk_pos += read;

                                let key = size as u64;
                                let wire_type = WireType::try_from(key & 0x7).unwrap();
                                let tag = key as u32 >> 3;

                                match tag {
                                    1 => {
                                        self.state = FrameType::DataType;
                                        self.len = 0;
                                    }
                                    2 => {
                                        self.state = FrameType::UnixFsData;
                                        self.len = 0;
                                    }
                                    3 => {
                                        self.state = FrameType::FileSize;
                                        self.len = 0;
                                    }
                                    4 => {
                                        self.state = FrameType::BlockSizes;
                                        self.len = 0;
                                    }
                                    5 => {
                                        println!("Data::HashType");
                                    }
                                    6 => {
                                        println!("Data::Fanout");
                                    }
                                    _ => unreachable!(),
                                };
                            }
                            FrameType::PBLinks => {
                                self.blk_pos += read;
                                self.has_links = true;
                                println!("blk len: {}, blk pos: {}", self.blk_len, self.blk_pos);
                            }
                            FrameType::UnixFsData => {
                                self.blk_pos += read;
                                self.unixfs_len = size;
                                self.len = self.blk_len - self.blk_pos;
                            }
                            FrameType::PBData
                            | FrameType::DataType
                            | FrameType::FileSize
                            | FrameType::BlockSizes => {
                                self.blk_pos += read;
                                self.len = 0;

                                println!("left {}", current.len());

                                if matches!(self.state, FrameType::DataType) {
                                    let tp = size as i32;
                                    let dt: DataType = tp.try_into().unwrap();
                                    println!("data type: {:?}", dt);
                                }

                                if self.blk_len - self.blk_pos == 0 {
                                    self.state = FrameType::Block;
                                    self.blk_pos = 0;
                                    println!(
                                        "end of block, maybe: {}, pos {}, unixfs_len {}",
                                        maybe, pos, self.unixfs_len
                                    );
                                    // include any intermediary blocks so they are ones
                                    // with no unixfs data
                                    if self.unixfs_len == 0 {
                                        pos += maybe;
                                        maybe = 0;
                                    }
                                } else {
                                    self.state = FrameType::UnixFs;
                                }
                            }
                            _ => {}
                        };
                    }
                    None => {
                        current = &[];
                        if self.include_block() {
                            pos = buf.len();
                        } else {
                            println!("skipping block pos: {}", pos);
                            maybe = buf.len();
                        }
                        if matches!(
                            self.state,
                            FrameType::MerkleDag
                                | FrameType::UnixFs
                                | FrameType::PBData
                                | FrameType::DataType
                                | FrameType::FileSize
                                | FrameType::BlockSizes
                                | FrameType::PBLinks
                                | FrameType::UnixFsData
                        ) {
                            self.blk_pos += self.buf.len();
                        }
                    }
                };

                if self.has_links {
                    pos += maybe;
                    maybe = 0;
                }

            // end of the frame
            } else if current.len() >= self.len {
                println!("end of frame, len: {}", self.len);
                if self.include_block() {
                    pos += self.len;
                    pos += maybe;
                    maybe = 0;
                } else {
                    println!("skipping block pos: {}", pos);
                    maybe += self.len;
                }
                match self.state {
                    FrameType::CarHeader => {
                        self.state = FrameType::Block;
                    }
                    FrameType::PBLinks => {
                        self.state = FrameType::MerkleDag;
                        self.blk_pos += self.len;
                    }
                    FrameType::UnixFsData | FrameType::RawLeaf => {
                        if maybe > 0 {
                            println!(
                                "pushing range start {}, pos {}, maybe {}",
                                start, pos, maybe
                            );
                            if pos > start {
                                ranges.push((start, pos));
                                start = start + pos + maybe;
                            } else {
                                start += maybe;
                            }
                            pos = start;
                            maybe = 0;
                        }

                        self.blk_pos = 0;
                        self.state = FrameType::Block;
                        self.unixfs_read += self.unixfs_len;
                        self.unixfs_len = 0;

                        println!(
                            "end of unixfs chunk: pos: {}, maybe: {}, start: {}",
                            pos, maybe, start
                        );
                    }
                    _ => {}
                };
                current = &current[self.len..];
                self.len = 0;
            // partial frame
            } else {
                println!("partial frame, len: {}, maybe: {}", current.len(), maybe);
                if self.include_block() {
                    pos += current.len();
                    pos += maybe;
                    maybe = 0;
                } else {
                    println!("skipping block pos: {}", pos);
                    maybe += current.len();
                }

                match self.state {
                    FrameType::PBLinks => {
                        self.blk_pos += current.len();

                        // Assume if we have pblink frame we should include this intermediary node
                        pos += maybe;
                    }
                    FrameType::UnixFsData => {
                        self.blk_pos += current.len();
                    }
                    _ => {}
                };
                self.len -= current.len();
                current = &[];
            }
        }
        ranges.push((start, pos));
        Ok(ranges)
    }

    // since the end bound is inclusive, we add 1 to the unixfs cursor
    fn include_block(&self) -> bool {
        println!(
            "?include block? {:?}, unixfs_read {}, unixfs_len {}",
            self.state, self.unixfs_read, self.unixfs_len
        );
        match self.state {
            FrameType::CarHeader => true,
            FrameType::UnixFsData | FrameType::RawLeaf => {
                if self.unixfs_read == 0 || self.unixfs_len == 0 {
                    self.range.contains(&(self.unixfs_read as u64 + 1))
                } else {
                    ranges_overlap(
                        self.range.clone(),
                        self.unixfs_read + 1..self.unixfs_read + self.unixfs_len,
                    )
                }
            }
            _ => self.range.contains(&(self.unixfs_read as u64 + 1)),
        }
    }

    fn decode_len(&mut self, buf: &[u8]) -> Option<(usize, usize)> {
        let mut i = 0;
        loop {
            self.buf.push(buf[i]);
            match usize::decode_var(&self.buf[..]) {
                Some((size, _)) => {
                    self.buf.clear();
                    return Some((size, i + 1));
                }
                None => {
                    if buf.len() - (i + 1) > 0 {
                        i += 1;
                        continue;
                    } else {
                        return None;
                    }
                }
            };
        }
    }

    fn decode_cid(&mut self, buf: &[u8]) -> Option<(Cid, usize)> {
        let mut i = 0;

        let filled = self.buf.len();

        loop {
            for j in i..std::cmp::min(i + 36, buf.len()) {
                self.buf.push(buf[j]);
                i = j;
            }
            // start from the next index
            i += 1;
            let mut reader = Cursor::new(&self.buf[..]);
            match Cid::read_bytes(&mut reader) {
                Ok(cid) => {
                    let read = reader.position() as usize;
                    self.buf.clear();
                    self.blk_pos += read;
                    return Some((cid, read - filled));
                }
                Err(_) => {
                    if buf.len() > (i + 1) {
                        continue;
                    } else {
                        return None;
                    }
                }
            };
        }
    }

    fn is_seek(&self) -> bool {
        lt_bound(self.range.start_bound(), self.unixfs_read as u64)
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

    struct MockPool;

    impl Allocator for MockPool {
        fn as_ngx_pool_mut(&mut self) -> *mut ngx_pool_s {
            std::ptr::null_mut()
        }
        fn alloc_chain(&mut self) -> *mut ngx_chain_s {
            let link = Box::new(ngx_chain_s {
                buf: std::ptr::null_mut(),
                next: std::ptr::null_mut(),
            });
            Box::into_raw(link)
        }
        fn calloc_buf(&mut self) -> *mut ngx_buf_s {
            let buf = Box::new(ngx_buf_s {
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
            });
            Box::into_raw(buf)
        }
    }

    // check the CAR file is a valid car file and contains the given blocks only
    fn check_car(buf: &[u8], blks: Vec<Cid>) {
        let mut current = buf;
        let (size, read) = usize::decode_var(current).unwrap();
        let header: CarHeader =
            serde_ipld_dagcbor::from_slice(&current[read..size + read]).unwrap();
        assert_eq!(header.roots[0], blks[0]);
        current = &buf[size + read..];

        for cid in blks {
            let (size, read) = usize::decode_var(current).unwrap();
            let mut reader = Cursor::new(&current[read..]);
            assert_eq!(cid, Cid::read_bytes(&mut reader).unwrap());

            current = &current[size + read..];
        }
        assert_eq!(current.len(), 0);
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

        let mut ctx = CarBufferContext::new(..1024, MockPool);

        let mut buf = vec![];

        let o1 = ctx.buffer(&l1 as *const _ as *mut _);
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

        let mut ctx = CarBufferContext::new(..3001, MockPool);

        let mut buf = vec![];

        let o1 = ctx.buffer(&l1 as *const _ as *mut _);
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

        let mut ctx = CarBufferContext::new(..3500, MockPool);

        let mut buf = vec![];

        let o = ctx.buffer(&l1 as *const _ as *mut _);
        let b = unsafe { MemoryBuffer::from_ngx_buf((*o).buf) };

        buf.extend_from_slice(b.as_bytes());

        let o = ctx.buffer(&l2 as *const _ as *mut _);
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

        let mut ctx = CarBufferContext::new(..3500, MockPool);

        let mut buf = vec![];

        let o = ctx.buffer(&l1 as *const _ as *mut _);
        let b = unsafe { MemoryBuffer::from_ngx_buf((*o).buf) };

        buf.extend_from_slice(b.as_bytes());

        let o = ctx.buffer(&l2 as *const _ as *mut _);
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

        let mut ctx = CarBufferContext::new(4500.., MockPool);

        let mut buf = vec![];

        let o = ctx.buffer(&l1 as *const _ as *mut _);
        let b = unsafe { MemoryBuffer::from_ngx_buf((*o).buf) };

        buf.extend_from_slice(b.as_bytes());

        let o = ctx.buffer(&l2 as *const _ as *mut _);
        let b = unsafe { MemoryBuffer::from_ngx_buf((*o).buf) };

        assert!(b.is_last());

        buf.extend_from_slice(b.as_bytes());

        // header + unxifs_root + raw_block(1000) + raw_block(1000) + raw_block(157)
        assert_eq!(buf.len(), 59 + 379 + 1038 + 1038 + 157);

        check_car(
            &buf,
            vec![
                "bafybeihnavzumupz6aqh3hi2swo6wyjmgij2y62qbcsadrqa4trwo5zrre",
                "bafkreidkf4neosfwhflqnuzuwidksaqjhq3q7mro2eha6t42fzoea4hgtq",
                "bafkreigy7zqrooauu5pift4gmzkjgov3mob57e2nawkxxp5hpg2in4yomq",
                "bafkreick5l3gihtmimedxayy5m4dlex3uxaiztaa2kd6rko4nlj3huuugm",
            ]
            .iter()
            .map(|s| s.clone().try_into().unwrap())
            .collect(),
        );
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

        let mut ctx = CarBufferContext::new(5500.., MockPool);

        let mut buf = vec![];

        let o = ctx.buffer(&l1 as *const _ as *mut _);
        let b = unsafe { MemoryBuffer::from_ngx_buf((*o).buf) };

        buf.extend_from_slice(b.as_bytes());

        let o = ctx.buffer(&l2 as *const _ as *mut _);
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

        let mut ctx = CarBufferContext::new(5500.., MockPool);

        let mut buf = vec![];

        let o = ctx.buffer(&l1 as *const _ as *mut _);
        let b = unsafe { MemoryBuffer::from_ngx_buf((*o).buf) };

        buf.extend_from_slice(b.as_bytes());

        let o = ctx.buffer(&l2 as *const _ as *mut _);
        assert!(o.is_null());
        let b = MemoryBuffer::from_ngx_buf(l2.buf);
        assert!(b.is_empty());

        let o = ctx.buffer(&l3 as *const _ as *mut _);
        let b = unsafe { MemoryBuffer::from_ngx_buf((*o).buf) };

        assert!(b.is_last());

        buf.extend_from_slice(b.as_bytes());

        // header + unxifs_root + raw_block(1000) + raw_block(157)
        check_car(
            &buf,
            vec![
                "bafybeihnavzumupz6aqh3hi2swo6wyjmgij2y62qbcsadrqa4trwo5zrre",
                "bafkreigy7zqrooauu5pift4gmzkjgov3mob57e2nawkxxp5hpg2in4yomq",
                "bafkreick5l3gihtmimedxayy5m4dlex3uxaiztaa2kd6rko4nlj3huuugm",
            ]
            .iter()
            .map(|s| s.clone().try_into().unwrap())
            .collect(),
        );
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

        let l2 = ngx_chain_s {
            buf: &buf2 as *const _ as *mut _,
            next: std::ptr::null_mut(),
        };

        let l1 = ngx_chain_s {
            buf: &buf1 as *const _ as *mut _,
            next: std::ptr::null_mut(),
        };

        let mut ctx = CarBufferContext::new(5500.., MockPool);

        let mut buf = vec![];

        let o = ctx.buffer(&l1 as *const _ as *mut _);
        let b = unsafe { MemoryBuffer::from_ngx_buf((*o).buf) };

        buf.extend_from_slice(b.as_bytes());

        let o = ctx.buffer(&l2 as *const _ as *mut _);
        let b = unsafe { MemoryBuffer::from_ngx_buf((*o).buf) };

        assert!(b.is_last());

        buf.extend_from_slice(b.as_bytes());

        // header + unxifs_root + raw_block(1000) + raw_block(157)
        check_car(
            &buf,
            vec![
                "bafybeihnavzumupz6aqh3hi2swo6wyjmgij2y62qbcsadrqa4trwo5zrre",
                "bafkreigy7zqrooauu5pift4gmzkjgov3mob57e2nawkxxp5hpg2in4yomq",
                "bafkreick5l3gihtmimedxayy5m4dlex3uxaiztaa2kd6rko4nlj3huuugm",
            ]
            .iter()
            .map(|s| s.clone().try_into().unwrap())
            .collect(),
        );
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

        let mut ctx = CarBufferContext::new(5500.., MockPool);

        let mut buf = vec![];

        let o = ctx.buffer(&l1 as *const _ as *mut _);
        let b = unsafe { MemoryBuffer::from_ngx_buf((*o).buf) };

        buf.extend_from_slice(b.as_bytes());

        let o = ctx.buffer(&l2 as *const _ as *mut _);
        assert!(o.is_null());
        let b = MemoryBuffer::from_ngx_buf(l2.buf);
        assert!(b.is_empty());

        let o = ctx.buffer(&l3 as *const _ as *mut _);
        let b = unsafe { MemoryBuffer::from_ngx_buf((*o).buf) };

        assert!(b.is_last());

        buf.extend_from_slice(b.as_bytes());

        // header + unxifs_root + raw_block(1000) + raw_block(157)
        check_car(
            &buf,
            vec![
                "bafybeihnavzumupz6aqh3hi2swo6wyjmgij2y62qbcsadrqa4trwo5zrre",
                "bafkreigy7zqrooauu5pift4gmzkjgov3mob57e2nawkxxp5hpg2in4yomq",
                "bafkreick5l3gihtmimedxayy5m4dlex3uxaiztaa2kd6rko4nlj3huuugm",
            ]
            .iter()
            .map(|s| s.clone().try_into().unwrap())
            .collect(),
        );
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

        let mut ctx = CarBufferContext::new(5500.., MockPool);

        let o = ctx.buffer(&l1 as *const _ as *mut _);

        let mut result: Vec<u8> = vec![];

        let mut cl = o;
        while !cl.is_null() {
            let buf = unsafe { MemoryBuffer::from_ngx_buf((*cl).buf) };
            cl = unsafe { (*cl).next };
            println!("** buf out: {:?} \n", buf.as_bytes());
            result.extend_from_slice(buf.as_bytes());
        }

        assert_eq!(result.len(), exp.len());

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

        let mut ctx = CarBufferContext::new(.., MockPool);

        let o = ctx.buffer(&chain as *const _ as *mut _);

        assert!(o.is_null());
    }

    #[test]
    fn test_buf_file_dag_pb_leaves_end_bound() {
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

        let mut ctx = CarBufferContext::new(..200000, MockPool);

        let mut buf = vec![];

        // iterate over the chains and buffer them
        // the callback will return an empty chain
        let mut i = 0;
        while ctx.done == 0 {
            assert!(buf.len() <= 265577);
            let cl: *mut ngx_chain_t = &chains[i] as *const _ as *mut _;
            unsafe { (*cl).buf = &bufs[i] as *const _ as *mut _ };
            let o = ctx.buffer(cl);

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
        let mut ctx = CarBufferContext::new(263000..333333, MockPool);

        let mut buf = vec![];

        // iterate over the chains and buffer them
        // the callback will return an empty chain
        let mut i = 0;
        while ctx.done == 0 {
            assert!(buf.len() <= 265577);
            let cl: *mut ngx_chain_t = &chains[i] as *const _ as *mut _;
            unsafe { (*cl).buf = &bufs[i] as *const _ as *mut _ };
            let o = ctx.buffer(cl);

            i += 1;
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
        let mut ctx = CarBufferContext::new(555555..999999, MockPool);

        let mut buf = vec![];

        // iterate over the chains and buffer them
        // the callback will return an empty chain
        let mut i = 0;
        while ctx.done == 0 {
            let cl: *mut ngx_chain_t = &chains[i] as *const _ as *mut _;
            unsafe { (*cl).buf = &bufs[i] as *const _ as *mut _ };

            let o = ctx.buffer(cl);
            i += 1;

            println!("-> buffered, null {}", o.is_null());

            // add the buffered data to the output buffer
            let mut cl = o;
            while !cl.is_null() {
                let b = unsafe { MemoryBuffer::from_ngx_buf((*cl).buf) };
                println!("> size {}", b.len());
                cl = unsafe { (*cl).next };
                b.as_bytes().iter().for_each(|b| buf.push(*b));
            }
        }

        // header(57) + unxifs_dir(591) + unixfs_file(2734) + unixfs_block(262195) + unixfs_block(262195)
        assert_eq!(buf.len(), 57 + 591 + 2734 + 262195 + 262195);

        check_car(
            &buf,
            vec![
                "QmafUYju2Ab4ETi5HJG1cqjmnjs2xw9PUuBKzU7Hi3zvXU",
                "QmRsGoycNMQAJbeLAhuPBFtYb3prVfamDExWj8FYQNkFxr",
                "QmYr7bPfP1Jxxnie8n42NCXHrWEj3Z8rXygY7YuXBzdZhj",
                "Qma5rt7vkYcoMwrFMdoRzzgHUE3A3SptoHfHKRHkmVQvvB",
            ]
            .iter()
            .map(|b| b.clone().try_into().unwrap())
            .collect(),
        );
    }

    struct TC {
        range: Range<u64>,
        size: usize,
        blks: Vec<Cid>,
    }

    impl TC {
        fn new(range: Range<u64>, size: usize, blks: Vec<&str>) -> Self {
            Self {
                range,
                size,
                blks: blks.iter().map(|b| b.clone().try_into().unwrap()).collect(),
            }
        }
    }

    #[test]
    fn test_frame_loop() {
        use std::fs::File;
        use std::io::{BufReader, Read};

        let f = File::open("./sm-dagpb.car").unwrap();
        let mut reader = BufReader::new(f);

        let mut car_data = vec![];
        reader.read_to_end(&mut car_data).unwrap();

        let ranges = [
            TC::new(
                0..7000,
                6805,
                vec![
                    "bafybeidutwlr3tfcjuytejeusetv65lltrw6epessuyjzxhjg3kk4wt6ea",
                    "bafybeihrwler3wpt3vws4eutcr6cyqhielsin2hne6ogzkfarfoitt3jwa",
                    "bafybeigy57ubfegrckt4dq33s2g2vcwiahct5fgki6s2likb2yvztft63y",
                    "bafybeia42q3rzcpjozdz7o6n6l6w6nuaugm3x7y2are4b6nfid2yhnw3oa",
                    "bafybeiew7intdgz5ghmxoygcx77jdd3exsaw22g4nlr42oi27dytbbbfcm",
                    "bafybeicp6ecmucwm2remreb5ah7j26yuihr3l42lcmuff7fguuqwj4jgoq",
                    "bafybeicbb6atqrrq3wyrde56qteh6gk35gmjdq2n62iafs6qi3xltjpxoq",
                ],
            ),
            TC::new(
                0..1500,
                2538,
                vec![
                    "bafybeidutwlr3tfcjuytejeusetv65lltrw6epessuyjzxhjg3kk4wt6ea",
                    "bafybeihrwler3wpt3vws4eutcr6cyqhielsin2hne6ogzkfarfoitt3jwa",
                    "bafybeigy57ubfegrckt4dq33s2g2vcwiahct5fgki6s2likb2yvztft63y",
                ],
            ),
            TC::new(
                0..1025,
                1465,
                vec![
                    "bafybeidutwlr3tfcjuytejeusetv65lltrw6epessuyjzxhjg3kk4wt6ea",
                    "bafybeihrwler3wpt3vws4eutcr6cyqhielsin2hne6ogzkfarfoitt3jwa",
                ],
            ),
            TC::new(
                1025..1048,
                1465,
                vec![
                    "bafybeidutwlr3tfcjuytejeusetv65lltrw6epessuyjzxhjg3kk4wt6ea",
                    "bafybeigy57ubfegrckt4dq33s2g2vcwiahct5fgki6s2likb2yvztft63y",
                ],
            ),
        ];

        for range in ranges.iter() {
            let factors = [1, 5, 12, 31, 40, 55, 120, 300];

            for factor in factors.iter() {
                let section_size = car_data.len() / factor;

                let sections = car_data.chunks(section_size);

                let mut reader = Framed::new(range.range.clone());

                let mut buf = vec![];

                for section in sections {
                    println!("new section of size {}", section.len());
                    match reader.next(section) {
                        Ok(parts) => {
                            for (start, end) in parts {
                                println!("=> start {} end {}", start, end);
                                buf.extend_from_slice(&section[start..end]);
                            }
                        }
                        Err(e) => panic!("failed to read all bytes for factor {}: {}", factor, e),
                    }
                }
                assert_eq!(buf.len(), range.size);

                check_car(&buf[..], range.blks.clone());

                println!("\n");
            }
        }
    }
}

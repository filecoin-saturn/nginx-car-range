use crate::bindings::*;
use crate::log::ngx_log_debug_http;
use crate::pool::{Buffer, MemoryBuffer};
use crate::request::Request;
use crate::varint::VarInt;
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

pub fn read_car(req: &mut Request, input: *mut ngx_chain_t) -> Option<CarHeader> {
    let range = req.range()?;
    let mut header: Option<CarHeader> = None;
    let mut cl = input;

    let mut current = 0;

    while !cl.is_null() {
        let buf = unsafe { MemoryBuffer::from_ngx_buf((*cl).buf) };

        let bytes = buf.as_bytes();
        let mut pos = 0;

        while pos < bytes.len() {
            if let Some((size, read)) = usize::decode_var(&bytes[pos..]) {
                let bound = size + read;
                pos += bound;

                // The first frame is the CAR header
                if header.is_none() {
                    header = serde_ipld_dagcbor::from_slice(&bytes[read..bound]).ok();
                    continue;
                }

                // else it's just a block
                let mut cursor = Cursor::new(&bytes[read..bound]);
                if let Ok(cid) = Cid::read_bytes(&mut cursor) {
                    let data = &bytes[cursor.position() as usize..bound];

                    ngx_log_debug_http!(req, "car_range decoded block {:?}", cid);

                    match cid.codec() {
                        0x70 => {
                            let outer = dag_pb::PbNode::decode(data).ok()?;
                            let inner_data = outer.data.as_ref().cloned()?;
                            let inner = unixfs_pb::Data::decode(inner_data).ok()?;

                            // let dt: DataType = inner.r#type.try_into().ok()?;

                            if outer.links.len() == 0 && inner.data.is_some() {
                                let skip = !range.contains(&current);
                                current += inner.data.as_ref().map(|d| d.len() as u64).unwrap();
                                if skip {
                                    continue;
                                }
                            }
                        }
                        0x55 => {
                            let skip = !range.contains(&current);
                            current += data.len() as u64;
                            if skip {
                                continue;
                            }
                        }
                        _ => (),
                    }
                }
            } else {
                // TODO: the frame is split between multiple buffers?
                // would need to allocate in a new buffer.
                return None;
            }
        }

        cl = unsafe { (*cl).next };
    }

    header
}

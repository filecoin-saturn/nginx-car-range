use cid::Cid;
use integer_encoding::{VarIntReader, VarIntWriter};
use prost::Message;
use serde::{Deserialize, Serialize};
use std::io::{self, Read, Write};
use std::ops::{Bound, RangeBounds};

mod unixfs_pb {
    include!(concat!(env!("OUT_DIR"), "/unixfs_pb.rs"));
}

mod dag_pb {
    include!(concat!(env!("OUT_DIR"), "/merkledag_pb.rs"));
}

// CAR V1 header, should contain a single root and be CBOR encoded
#[derive(Debug, Default, Serialize, Deserialize, PartialEq)]
struct CarHeader {
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Link {
    pub cid: Cid,
    pub name: Option<String>,
    pub tsize: Option<u64>,
}

// Reads a length prefixed chunk
fn lp_read<R: Read>(reader: &mut R) -> io::Result<Vec<u8>> {
    let l: usize = reader.read_varint()?;
    let mut buf = vec![0u8; l];
    reader.read_exact(&mut buf)?;
    Ok(buf)
}

// Writes a length prefixed byte slice and flushes the writer
fn lp_write<W: Write>(writer: &mut W, bytes: &[u8]) -> io::Result<()> {
    writer.write_varint(bytes.len())?;
    writer.write_all(bytes)?;
    writer.flush()?;
    Ok(())
}

// Decodes the length prefixed block frame, returns the CID and data payload
fn read_block<R: Read>(r: &mut R) -> Option<anyhow::Result<(Cid, Vec<u8>)>> {
    let buf = match lp_read(r) {
        Ok(buf) => buf,
        Err(e) => match e.kind() {
            io::ErrorKind::UnexpectedEof => return None,
            _ => return Some(Err(e.into())),
        },
    };
    if buf.is_empty() {
        return None;
    }

    let mut cursor = io::Cursor::new(&buf);
    match Cid::read_bytes(&mut cursor) {
        Ok(cid) => Some(Ok((cid, buf[cursor.position() as usize..].to_vec()))),
        Err(e) => Some(Err(e.into())),
    }
}

// Read the header and write it back to the stream before returning.
fn read_header<R: Read + Write>(stdin: &mut R) -> anyhow::Result<CarHeader> {
    let header_buf = lp_read(stdin)?;

    let header: CarHeader = serde_ipld_dagcbor::from_slice(&header_buf[..])?;

    if header.roots.is_empty() {
        return Err(anyhow::format_err!("invalid CAR file"));
    }

    if header.version != 1 {
        return Err(anyhow::format_err!("only CAR v1 is supported"));
    }

    // forward it back
    lp_write(stdin, &header_buf)?;

    Ok(header)
}

// Abstraction over an nginx HTTP request providing read and write interface with a range iterator.
pub trait Request: Read + Write {
    fn range<'a>(&'a self) -> Box<dyn Iterator<Item = (Bound<u64>, Bound<u64>)> + 'a>;
    fn forward(&mut self) -> io::Result<u64>;
}

// The nginx module handler takes a request trait and returns Ok if all goes well.
// The Result will be translated to an nginx status integer code.
pub fn nginx_handler<R: Request>(mut req: R) -> anyhow::Result<()> {
    let maybe_range = req.range().next();
    // TODO: support multiple range if needed.
    let range = match maybe_range {
        Some(rg) => rg,
        None => {
            // send the bytes to the writer immediately
            req.forward()?;
            return Ok(());
        }
    };

    let _car_header = read_header(&mut req)?;

    let mut current: u64 = 0;

    while let Some(blk) = read_block(&mut req) {
        let (cid, data) = blk?;

        // by default we write back the blocks, skipping a block is an explicit rule added below.
        match cid.codec() {
            0x70 => {
                let outer = dag_pb::PbNode::decode(&data[..])?;
                let inner_data = outer
                    .data
                    .as_ref()
                    .cloned()
                    .ok_or_else(|| anyhow::format_err!("missing data"))?;
                let inner = unixfs_pb::Data::decode(inner_data)?;

                let dt: DataType = inner.r#type.try_into()?;

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
        lp_write(&mut req, &[cid.to_bytes(), data].concat())?;
    }
    Ok(())
}

// This is a request object for testing the handler.
pub struct MockRequest<R, W> {
    reader: R,
    writer: W,
    range: Option<String>,
}

impl<R, W> MockRequest<R, W> {
    pub fn new(reader: R, writer: W, range: impl RangeBounds<u64>) -> Self {
        Self {
            reader,
            writer,
            range: match (range.start_bound(), range.end_bound()) {
                (Bound::Unbounded, Bound::Included(end)) => Some(format!("bytes=-{}", end)),
                (Bound::Unbounded, Bound::Excluded(&end)) => Some(format!("bytes=-{}", end - 1)),
                (Bound::Included(start), Bound::Included(end)) => {
                    Some(format!("bytes={}-{}", start, end))
                }
                (Bound::Included(start), Bound::Excluded(&end)) => {
                    Some(format!("bytes={}-{}", start, end - 1))
                }
                (Bound::Included(start), Bound::Unbounded) => Some(format!("bytes={}-", start)),
                _ => None,
            },
        }
    }
}

fn parse_bound(s: &str) -> Option<Bound<u64>> {
    if s.is_empty() {
        return Some(Bound::Unbounded);
    }

    s.parse().ok().map(Bound::Included)
}

impl<R: Read, W: Write> Request for MockRequest<R, W> {
    fn range<'a>(&'a self) -> Box<dyn Iterator<Item = (Bound<u64>, Bound<u64>)> + 'a> {
        if let Some(s) = self.range.as_ref() {
            Box::new(s["bytes=".len()..].split(',').filter_map(|spec| {
                let mut iter = spec.trim().splitn(2, '-');
                Some((parse_bound(iter.next()?)?, parse_bound(iter.next()?)?))
            }))
        } else {
            Box::new(std::iter::empty())
        }
    }
    fn forward(&mut self) -> io::Result<u64> {
        io::copy(&mut self.reader, &mut self.writer)
    }
}

impl<R: Read, W: Write> Read for MockRequest<R, W> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.reader.read(buf)
    }
}

impl<R: Read, W: Write> Write for MockRequest<R, W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.writer.write(buf)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn non_unixfs_request() {
        let mut buff = Cursor::new(vec![0; 104]);

        let car_data = hex::decode("3aa265726f6f747381d82a58250001711220151fe9e73c6267a7060c6f6c4cca943c236f4b196723489608edb42a8b8fa80b6776657273696f6e012c01711220151fe9e73c6267a7060c6f6c4cca943c236f4b196723489608edb42a8b8fa80ba165646f646779f5").unwrap();

        let req = MockRequest::new(&car_data[..], &mut buff, ..);

        nginx_handler(req).unwrap();

        // the handler should forward the car file as is
        assert_eq!(buff.get_ref(), &car_data);
    }

    #[test]
    fn unixfs_single_node() {
        let mut buff = Cursor::new(vec![0; 112]);

        let car_data = hex::decode("38a265726f6f747381d82a582300122046d44814b9c5af141c3aaab7c05dc5e844ead5f91f12858b021eba45768b4c0e6776657273696f6e0136122046d44814b9c5af141c3aaab7c05dc5e844ead5f91f12858b021eba45768b4c0e0a120802120c68656c6c6f20776f726c640a180c").unwrap();

        let req = MockRequest::new(&car_data[..], &mut buff, 0..50);

        nginx_handler(req).unwrap();

        // the handler should forward the car file as is because the range is withing the only
        // block.
        assert_eq!(buff.get_ref(), &car_data);
    }
}

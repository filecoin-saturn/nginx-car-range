use cid::Cid;
use integer_encoding::VarIntReader;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{self, BufRead, BufReader, Read};

#[derive(Debug, Default, Serialize, Deserialize, PartialEq)]
struct CarHeader {
    pub roots: Vec<Cid>,
    pub version: u64,
}

#[derive(Clone, Debug)]
struct Block {
    cid: Cid,
    data: Vec<u8>,
}

struct Blocks<B> {
    buf: B,
}

// Reads a length prefixed chunk
fn lp_read<R: Read>(reader: &mut R) -> io::Result<Vec<u8>> {
    let l: usize = reader.read_varint()?;
    let mut buf = vec![0u8; l];
    reader.read_exact(&mut buf)?;
    Ok(buf)
}

impl<B: BufRead> Iterator for Blocks<B> {
    type Item = anyhow::Result<Block>;

    fn next(&mut self) -> Option<anyhow::Result<Block>> {
        let buf = match lp_read(&mut self.buf) {
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
            Ok(cid) => Some(Ok(Block {
                cid,
                data: buf[cursor.position() as usize..].to_vec(),
            })),
            Err(e) => Some(Err(e.into())),
        }
    }
}

fn read_header<R: BufRead>(stdin: &mut R) -> anyhow::Result<CarHeader> {
    let header_buf = lp_read(stdin)?;

    let header: CarHeader = serde_ipld_dagcbor::from_slice(&header_buf[..])?;

    if header.roots.is_empty() {
        return Err(anyhow::format_err!("invalid CAR file"));
    }

    if header.version != 1 {
        return Err(anyhow::format_err!("only CAR v1 is supported"));
    }

    Ok(header)
}

fn main() -> anyhow::Result<()> {
    let car_file = File::open("fixture.car")?;
    let mut car_reader = BufReader::new(car_file);

    let header = read_header(&mut car_reader)?;

    println!("root: {:?}", header.roots[0]);

    let blocks = Blocks { buf: car_reader };

    for blk in blocks {
        println!("{:?}", blk?.cid);
    }

    Ok(())
}

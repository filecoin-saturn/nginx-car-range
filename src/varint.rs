// This is a no-std varint implementation extracted from
// https://github.com/dermesser/integer-encoding-rs/blob/master/src/varint.rs
// so we don't import the whole library.
//
/// Most-significant byte, == 0x80
pub const MSB: u8 = 0b1000_0000;
/// All bits except for the most significant. Can be used as bitmask to drop the most-signficant
/// bit using `&` (binary-and).
const DROP_MSB: u8 = 0b0111_1111;

/// Varint (variable length integer) encoding, as described in
/// https://developers.google.com/protocol-buffers/docs/encoding.
///
/// Uses zigzag encoding (also described there) for signed integer representation.
pub trait VarInt: Sized + Copy {
    /// Returns the number of bytes this number needs in its encoded form. Note: This varies
    /// depending on the actual number you want to encode.
    fn required_space(self) -> usize;
    /// Decode a value from the slice. Returns the value and the number of bytes read from the
    /// slice (can be used to read several consecutive values from a big slice)
    /// return None if all bytes has MSB set.
    fn decode_var(src: &[u8]) -> Option<(Self, usize)>;
    /// Encode a value into the slice. The slice must be at least `required_space()` bytes long.
    /// The number of bytes taken by the encoded integer is returned.
    fn encode_var(self, src: &mut [u8]) -> usize;

    /// Helper: Encode a value and return the encoded form as Vec. The Vec must be at least
    /// `required_space()` bytes long.
    fn encode_var_vec(self) -> Vec<u8> {
        let mut v = Vec::new();
        v.resize(self.required_space(), 0);
        self.encode_var(&mut v);
        v
    }
}

/// How many bytes an integer uses when being encoded as a VarInt.
#[inline]
fn required_encoded_space_unsigned(mut v: u64) -> usize {
    if v == 0 {
        return 1;
    }

    let mut logcounter = 0;
    while v > 0 {
        logcounter += 1;
        v >>= 7;
    }
    logcounter
}

impl VarInt for usize {
    fn required_space(self) -> usize {
        required_encoded_space_unsigned(self as u64)
    }

    fn decode_var(src: &[u8]) -> Option<(Self, usize)> {
        let (n, s) = u64::decode_var(src)?;
        Some((n as Self, s))
    }

    fn encode_var(self, dst: &mut [u8]) -> usize {
        (self as u64).encode_var(dst)
    }
}

impl VarInt for u64 {
    fn required_space(self) -> usize {
        required_encoded_space_unsigned(self)
    }

    #[inline]
    fn decode_var(src: &[u8]) -> Option<(Self, usize)> {
        let mut result: u64 = 0;
        let mut shift = 0;

        let mut success = false;
        for b in src.iter() {
            let msb_dropped = b & DROP_MSB;
            result |= (msb_dropped as u64) << shift;
            shift += 7;

            if b & MSB == 0 || shift > (9 * 7) {
                success = b & MSB == 0;
                break;
            }
        }

        if success {
            Some((result, shift / 7 as usize))
        } else {
            None
        }
    }

    #[inline]
    fn encode_var(self, dst: &mut [u8]) -> usize {
        assert!(dst.len() >= self.required_space());
        let mut n = self;
        let mut i = 0;

        while n >= 0x80 {
            dst[i] = MSB | (n as u8);
            i += 1;
            n >>= 7;
        }

        dst[i] = n as u8;
        i + 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_car() {
        let car_data = hex::decode("3aa265726f6f747381d82a58250001711220151fe9e73c6267a7060c6f6c4cca943c236f4b196723489608edb42a8b8fa80b6776657273696f6e012c01711220151fe9e73c6267a7060c6f6c4cca943c236f4b196723489608edb42a8b8fa80ba165646f646779f5").unwrap();

        let (size, read) = usize::decode_var(&car_data[..]).unwrap();

        // Size of the CAR header is 58 bytes
        assert_eq!(size, 58);
        // We read 1 byte
        assert_eq!(read, 1);
    }

    #[test]
    fn test_buffer_fun() {
        use crate::bindings::*;

        let car_data = hex::decode("3aa265726f6f747381d82a58250001711220151fe9e73c6267a7060c6f6c4cca943c236f4b196723489608edb42a8b8fa80b6776657273696f6e012c01711220151fe9e73c6267a7060c6f6c4cca943c236f4b196723489608edb42a8b8fa80ba165646f646779f5").unwrap();

        let (size, read) = usize::decode_var(&car_data[..]).unwrap();

        let car_head = &car_data[read..size + read];

        let buf = ngx_buf_s {
            pos: &car_head.as_ptr() as *const _ as *mut u_char,
            last: &car_head.as_ptr() as *const _ as *mut u_char,
            file_pos: 0,
            file_last: 0,
            start: &car_head.as_ptr() as *const _ as *mut u_char,
            end: &car_head.as_ptr() as *const _ as *mut u_char,
            tag: std::ptr::null_mut(),
            file: std::ptr::null_mut(),
            shadow: std::ptr::null_mut(),
            _bitfield_align_1: [0u8; 0],
            _bitfield_1: ngx_buf_s::new_bitfield_1(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            num: 0,
        };
    }
}

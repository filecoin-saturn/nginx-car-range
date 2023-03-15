use crate::bindings::*;
use std::borrow::Cow;
use std::ops::Bound;

impl ngx_str_t {
    /// Yields a `&str` slice if the [`NgxStr`] contains valid UTF-8.
    pub fn to_str(&self) -> Result<&str, std::str::Utf8Error> {
        // SAFETY: The caller has provided a valid `ngx_str_t` with a `data` pointer that points
        // to range of bytes of at least `len` bytes, whose content remains valid and doesn't
        // change for the lifetime of the returned `NgxStr`.
        let bytes = unsafe { std::slice::from_raw_parts_mut(self.data, self.len) };
        std::str::from_utf8(bytes)
    }
}

fn parse_bound(s: &str) -> Option<Bound<u64>> {
    if s == "*" {
        return Some(Bound::Unbounded);
    }

    s.parse().ok().map(Bound::Included)
}

fn parse_range(args: &str) -> Option<(Bound<u64>, Bound<u64>)> {
    let mut it = form_urlencoded::parse(args.as_bytes());
    while let Some((Cow::Borrowed(key), Cow::Borrowed(val))) = it.next() {
        if key == "bytes" {
            let mut iter = val.trim().splitn(2, ":");
            return Some((parse_bound(iter.next()?)?, parse_bound(iter.next()?)?));
        }
    }
    None
}

#[repr(transparent)]
pub struct Request(pub ngx_http_request_t);

impl Request {
    /// Create a [`Request`] from an [`ngx_http_request_t`].
    ///
    /// [`ngx_http_request_t`]: https://nginx.org/en/docs/dev/development_guide.html#http_request
    pub unsafe fn from_ngx_http_request<'a>(r: *mut ngx_http_request_t) -> &'a mut Request {
        // SAFETY: The caller has provided a valid non-null pointer to a valid `ngx_http_request_t`
        // which shares the same representation as `Request`.
        &mut *r.cast::<Request>()
    }

    pub fn connection(&self) -> *mut ngx_connection_t {
        self.0.connection
    }

    pub fn range(&self) -> Option<(Bound<u64>, Bound<u64>)> {
        let args = self.0.args.to_str().ok()?;
        parse_range(args)
    }

    pub fn accept_car(&self) -> bool {
        // Headers is a ngx list that's actually a sequence of arrays:
        // struct ngx_list_t {
        //     last: *mut ngx_list_part_t,
        //     part: ngx_list_part_t,
        //     size: usize,
        //     nalloc: ngx_uint_t,
        //     pool: *mut ngx_pool_t,
        // }
        // The array looks like:
        // struct ngx_list_part_t {
        //     elts: *mut ::std::os::raw::c_void,
        //     nelts: ngx_uint_t,
        //     next: *mut ngx_list_part_t,
        // }

        let headers = self.0.headers_in.headers;

        // let part = headers.part;
        // let mut v = part.elts;
        let mut i = 0;

        if headers.part.elts.is_null() {
            return false;
        }

        let arr = unsafe {
            // let arr = *(v as *mut ngx_array_t);
            std::slice::from_raw_parts_mut(headers.part.elts, headers.part.nelts)
        };

        loop {
            if i >= headers.part.nelts {
                break;
                // if part.next.is_null() {
                //     break;
                // }
                // part = unsafe { *part.next };
                // v = part.elts;
                // i = 0;
            }

            let ptr = &mut arr[i] as *mut std::os::raw::c_void;

            i += 1;

            if ptr.is_null() {
                continue;
            }

            // struct ngx_table_elt_s {
            //     hash: ngx_uint_t,
            //     key: ngx_str_t,
            //     value: ngx_str_t,
            //     lowcase_key: *mut u_char,
            //     next: *mut ngx_table_elt_t,
            // }
            let header = ptr as *mut ngx_table_elt_t;
            let key = unsafe { (*header).key };
            if key.len == 0 || key.data.is_null() {
                continue;
            }

            let bytes = unsafe { std::slice::from_raw_parts_mut(key.data, key.len) };
            if bytes.is_empty() {
                continue;
            }

            let vec = bytes.to_vec();

            let k = unsafe { std::str::from_utf8_unchecked(&vec[..]) };

            if k.is_empty() {
                continue;
            }

            // if let Some((k, v)) = h.key.to_str().ok().zip(h.value.to_str().ok()) {
            //     if k == "Accept" && v == "application/vnd.ipld.car" {
            //         return true;
            //     }
            // }
        }

        false
    }

    pub fn set_status(&mut self, status: ngx_uint_t) {
        self.0.headers_out.status = status;
    }

    pub fn set_content_length(&mut self, n: usize) {
        self.0.headers_out.content_length_n = n as off_t;
    }

    pub fn set_content_type(&mut self, ct: ngx_str_t) {
        self.0.headers_out.content_type = ct;
    }

    pub fn send_header(&mut self) -> ngx_int_t {
        unsafe { ngx_http_send_header(&mut self.0) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_range() {
        assert_eq!(
            parse_range("bytes=0:100").unwrap(),
            (Bound::Included(0), Bound::Included(100))
        );

        assert_eq!(
            parse_range("bytes=1024:*").unwrap(),
            (Bound::Included(1024), Bound::Unbounded)
        );
    }
}

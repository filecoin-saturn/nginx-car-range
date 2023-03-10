use crate::bindings::*;
use std::borrow::Cow;
use std::ops::Bound;

impl ngx_str_t {
    /// Yields a `&str` slice if the [`NgxStr`] contains valid UTF-8.
    pub fn to_str(&self) -> Result<&str, std::str::Utf8Error> {
        // SAFETY: The caller has provided a valid `ngx_str_t` with a `data` pointer that points
        // to range of bytes of at least `len` bytes, whose content remains valid and doesn't
        // change for the lifetime of the returned `NgxStr`.
        let bytes = unsafe { std::slice::from_raw_parts(self.data, self.len) };
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
        let headers = self.0.headers_in.headers;

        let mut part = headers.part;
        let mut v = part.elts;
        let mut i = 0;

        loop {
            if i >= part.nelts {
                if part.next.is_null() {
                    break;
                }

                part = unsafe { *part.next };
                v = part.elts;
                i = 0;
            }

            let arr = unsafe {
                let arr = *(v as *mut ngx_array_t);
                std::slice::from_raw_parts_mut(arr.elts, arr.nelts)
            };

            let header = (&mut arr[i] as *mut std::os::raw::c_void) as *mut ngx_table_elt_t;

            i += 1;

            let h = unsafe {
                if header.is_null() {
                    continue;
                } else {
                    *header
                }
            };

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

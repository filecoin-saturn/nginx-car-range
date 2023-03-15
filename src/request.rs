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

pub struct NgxStr([u_char]);

impl NgxStr {
    /// Create an [`NgxStr`] from an [`ngx_str_t`].
    ///
    /// [`ngx_str_t`]: https://nginx.org/en/docs/dev/development_guide.html#string_overview
    pub unsafe fn from_ngx_str<'a>(str: ngx_str_t) -> &'a NgxStr {
        // SAFETY: The caller has provided a valid `ngx_str_t` with a `data` pointer that points
        // to range of bytes of at least `len` bytes, whose content remains valid and doesn't
        // change for the lifetime of the returned `NgxStr`.
        std::slice::from_raw_parts(str.data, str.len).into()
    }

    /// Access the [`NgxStr`] as a byte slice.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Yields a `&str` slice if the [`NgxStr`] contains valid UTF-8.
    pub fn to_str(&self) -> Result<&str, std::str::Utf8Error> {
        std::str::from_utf8(self.as_bytes())
    }

    /// Converts an [`NgxStr`] into a [`Cow<str>`], replacing invalid UTF-8 sequences.
    ///
    /// See [`String::from_utf8_lossy`].
    pub fn to_string_lossy(&self) -> Cow<str> {
        String::from_utf8_lossy(self.as_bytes())
    }

    /// Returns `true` if the [`NgxStr`] is empty, otherwise `false`.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl From<&[u8]> for &NgxStr {
    fn from(bytes: &[u8]) -> Self {
        // SAFETY: An `NgxStr` is identical to a `[u8]` slice, given `u_char` is an alias for `u8`.
        unsafe { &*(bytes as *const [u8] as *const NgxStr) }
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

    pub fn accept_car(&self) -> String {
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
                // let arr = *(v as *mut ngx_array_t);
                std::slice::from_raw_parts_mut(v, part.nelts)
            };

            let ptr = &mut arr[i] as *mut std::os::raw::c_void;

            i += 1;

            if ptr.is_null() {
                continue;
            }

            let header = ptr as *mut ngx_table_elt_t;

            let key = unsafe { (*header).key };

            if key.len == 0 || key.data.is_null() {
                continue;
            }

            let keystr = unsafe { NgxStr::from_ngx_str(key) };

            if let Ok(k) = keystr.to_str() {
                //     if i == 1 {
                //         return k.to_string();
                //     }
            }

            // if let Some(k) = unsafe { (*header).key.to_str().ok() } {
            //     if k == "Accept" {
            //         return true;
            //     }
            // }

            // if let Some((k, v)) = h.key.to_str().ok().zip(h.value.to_str().ok()) {
            //     if k == "Accept" && v == "application/vnd.ipld.car" {
            //         return true;
            //     }
            // }
        }

        "".to_string()
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

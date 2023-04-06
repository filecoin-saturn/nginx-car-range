use crate::bindings::*;
use crate::pool::Pool;
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

// Wrapper for the nginx http request to provide safer access and operations.
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

    /// Request pool.
    pub fn pool(&self) -> Pool {
        // SAFETY: This request is allocated from `pool`, thus must be a valid pool.
        unsafe { Pool::from_ngx_pool(self.0.pool) }
    }

    pub fn range(&self) -> Option<(Bound<u64>, Bound<u64>)> {
        let args = self.0.args.to_str().ok()?;
        parse_range(args)
    }

    pub fn get_context(&self, module: &ngx_module_t) -> *mut std::os::raw::c_void {
        unsafe { *self.0.ctx.add(module.ctx_index) }
    }

    pub fn set_context(&self, module: &ngx_module_t, ctx: *mut std::os::raw::c_void) {
        unsafe { *self.0.ctx.add(module.ctx_index) = ctx }
    }

    pub fn accept_car(&self) -> bool {
        // Headers is a ngx list which is a sequence of arrays:
        // struct ngx_list_t {
        //     last: *mut ngx_list_part_t,
        //     part: ngx_list_part_t,
        //     size: usize,
        //     nalloc: ngx_uint_t,
        //     pool: *mut ngx_pool_t,
        // }
        // The array parts looks like:
        // struct ngx_list_part_t {
        //     elts: *mut ::std::os::raw::c_void,
        //     nelts: ngx_uint_t,
        //     next: *mut ngx_list_part_t,
        // }
        let headers = self.0.headers_in.headers;

        // we iterate over the array and then go to the next one in the list
        let mut part = headers.part;
        let mut i = 0;

        // There should at least be a few headers but just to be safe...
        if part.elts.is_null() {
            return false;
        }

        // Each HTTP header in the array is shaped as:
        // struct ngx_table_elt_s {
        //     hash: ngx_uint_t,
        //     key: ngx_str_t,
        //     value: ngx_str_t,
        //     lowcase_key: *mut u_char,
        //     next: *mut ngx_table_elt_t,
        // }
        // Create a slice over the first array in the list
        let mut arr: &[ngx_table_elt_t] =
            unsafe { std::slice::from_raw_parts(part.elts as *const ngx_table_elt_t, part.nelts) };

        loop {
            // only iterate first array for now
            if i >= arr.len() {
                if part.next.is_null() {
                    break;
                }
                part = unsafe { *part.next };
                arr = unsafe {
                    std::slice::from_raw_parts(part.elts as *const ngx_table_elt_t, part.nelts)
                };
                i = 0;
            }

            let table = arr[i];

            // increment the index for the next iteration
            i += 1;

            // the key and values are nginx string objects
            // struct ngx_str_t {
            //      len: usize,
            //      data: *mut u_char,
            // }
            // create a byte slice from the nginx string object
            let bytes = unsafe { std::slice::from_raw_parts(table.key.data, table.key.len) };
            if bytes.is_empty() {
                continue;
            }

            // As per RFC5987, the character set and language encoding in HTTP headers
            // must be UTF-8 characters so we can skip the expensive validation check.
            let k = unsafe { std::str::from_utf8_unchecked(bytes) };

            if !k.contains("Accept") {
                continue;
            }

            let bytes = unsafe { std::slice::from_raw_parts(table.value.data, table.value.len) };
            if bytes.is_empty() {
                continue;
            }

            let v = unsafe { std::str::from_utf8_unchecked(bytes) };

            // Check that the Accept header is in CAR format
            if v == "application/vnd.ipld.car" {
                return true;
            }
        }

        false
    }

    pub fn set_content_length_missing(&mut self) {
        self.0.headers_out.content_length_n = -1 as off_t;
        if !self.0.headers_out.content_length.is_null() {
            unsafe {
                (*self.0.headers_out.content_length).hash = 0;
                (*self.0.headers_out.content_length).next = std::ptr::null_mut();
            }
            self.0.headers_out.content_length = std::ptr::null_mut();
        }
    }

    pub fn set_content_type(&mut self, ct: ngx_str_t) {
        self.0.headers_out.content_type = ct;
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

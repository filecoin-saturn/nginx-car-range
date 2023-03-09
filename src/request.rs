use crate::bindings::*;

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

    pub fn range(&self) -> Option<&str> {
        self.0.args.to_str().ok()
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

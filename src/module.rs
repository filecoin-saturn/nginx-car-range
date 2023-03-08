use crate::bindings::*;
use std::os::raw::{c_char, c_void};
use std::ptr;

/// Static string initializer for [`ngx_str_t`].
///
/// The resulting byte string is always nul-terminated (just like a C string).
///
/// [`ngx_str_t`]: https://nginx.org/en/docs/dev/development_guide.html#string_overview
macro_rules! ngx_string {
    ($s:expr) => {{
        ngx_str_t {
            len: $s.len(),
            data: concat!($s, "\0").as_ptr() as *mut u8,
        }
    }};
}

/// [`NGX_LOG_DEBUG_HTTP`]: https://nginx.org/en/docs/dev/development_guide.html#logging
macro_rules! ngx_log_debug {
    ( $request:expr, $($arg:tt)* ) => {
        let log = unsafe { (*$request.connection()).log };
        let level = NGX_LOG_DEBUG as ngx_uint_t;
        let fmt = std::ffi::CString::new("%s").unwrap();
        let c_message = std::ffi::CString::new(format!($($arg)*)).unwrap();
        unsafe {
            ngx_log_error_core(level, log, 0, fmt.as_ptr(), c_message.as_ptr());
        }
    }
}

#[no_mangle]
static mut ngx_car_range_commands: [ngx_command_t; 2] = [
    ngx_command_t {
        name: ngx_string!("car_range"), /* directive */
        type_: (NGX_HTTP_LOC_CONF | NGX_CONF_NOARGS) as ngx_uint_t, /* location context and takes no arguments*/
        set: Some(ngx_car_range), /* configuration setup function */
        conf: 0,                  /* No offset. Only one context is supported. */
        offset: 0,                /* No offset when storing the module configuration on struct. */
        post: ptr::null_mut(),
    },
    /* command termination */
    ngx_command_t {
        name: ngx_str_t {
            len: 0,
            data: ::std::ptr::null_mut(),
        },
        type_: 0,
        set: None,
        conf: 0,
        offset: 0,
        post: ::std::ptr::null_mut(),
    },
];

#[no_mangle]
static ngx_car_range_module_ctx: ngx_http_module_t = ngx_http_module_t {
    preconfiguration: None,
    postconfiguration: None,

    create_main_conf: None,
    init_main_conf: None,

    create_srv_conf: None,
    merge_srv_conf: None,

    create_loc_conf: None,
    merge_loc_conf: None,
};

#[no_mangle]
pub static mut ngx_car_range_module: ngx_module_t = ngx_module_t {
    ctx_index: ngx_uint_t::max_value(),
    index: ngx_uint_t::max_value(),
    name: ptr::null_mut(),
    spare0: 0,
    spare1: 0,
    version: nginx_version as ngx_uint_t,
    signature: NGX_RS_MODULE_SIGNATURE.as_ptr() as *const c_char,

    ctx: &ngx_car_range_module_ctx as *const _ as *mut _,
    commands: unsafe { &ngx_car_range_commands[0] as *const _ as *mut _ },
    type_: NGX_HTTP_MODULE as ngx_uint_t,

    init_master: None,
    init_module: None,
    init_process: None,
    init_thread: None,
    exit_thread: None,
    exit_process: None,
    exit_master: None,

    spare_hook0: 0,
    spare_hook1: 0,
    spare_hook2: 0,
    spare_hook3: 0,
    spare_hook4: 0,
    spare_hook5: 0,
    spare_hook6: 0,
    spare_hook7: 0,
};

unsafe fn ngx_http_conf_get_module_loc_conf(
    cf: *mut ngx_conf_t,
    module: &ngx_module_t,
) -> *mut c_void {
    let http_conf_ctx = (*cf).ctx as *mut ngx_http_conf_ctx_t;
    *(*http_conf_ctx).loc_conf.add(module.ctx_index)
}

#[no_mangle]
unsafe extern "C" fn ngx_car_range(
    cf: *mut ngx_conf_t,
    _cmd: *mut ngx_command_t,
    _conf: *mut c_void,
) -> *mut c_char {
    let clcf = ngx_http_conf_get_module_loc_conf(cf, &ngx_http_core_module)
        as *mut ngx_http_core_loc_conf_t;
    (*clcf).handler = Some(ngx_car_range_handler);

    ptr::null_mut()
}

impl ngx_str_t {
    /// Yields a `&str` slice if the [`NgxStr`] contains valid UTF-8.
    fn to_str(&self) -> Result<&str, std::str::Utf8Error> {
        // SAFETY: The caller has provided a valid `ngx_str_t` with a `data` pointer that points
        // to range of bytes of at least `len` bytes, whose content remains valid and doesn't
        // change for the lifetime of the returned `NgxStr`.
        let bytes = unsafe { std::slice::from_raw_parts(self.data, self.len) };
        std::str::from_utf8(bytes)
    }
}

#[repr(transparent)]
pub struct Request(ngx_http_request_t);

impl Request {
    /// Create a [`Request`] from an [`ngx_http_request_t`].
    ///
    /// [`ngx_http_request_t`]: https://nginx.org/en/docs/dev/development_guide.html#http_request
    unsafe fn from_ngx_http_request<'a>(r: *mut ngx_http_request_t) -> &'a mut Request {
        // SAFETY: The caller has provided a valid non-null pointer to a valid `ngx_http_request_t`
        // which shares the same representation as `Request`.
        &mut *r.cast::<Request>()
    }

    fn connection(&self) -> *mut ngx_connection_t {
        self.0.connection
    }

    fn range(&self) -> Option<&str> {
        unsafe { (*self.0.headers_in.range).value.to_str().ok() }
    }

    fn set_status(&mut self, status: ngx_uint_t) {
        self.0.headers_out.status = status;
    }

    fn set_content_length(&mut self, n: usize) {
        self.0.headers_out.content_length_n = n as off_t;
    }

    fn set_content_type(&mut self, ct: ngx_str_t) {
        self.0.headers_out.content_type = ct;
    }

    fn send_header(&mut self) -> ngx_int_t {
        unsafe { ngx_http_send_header(&mut self.0) }
    }
}

#[no_mangle]
extern "C" fn ngx_car_range_handler(r: *mut ngx_http_request_t) -> ngx_int_t {
    let req = unsafe { &mut Request::from_ngx_http_request(r) };

    ngx_log_debug!(req, "http car_range handler");

    // Check if range request
    let range = req.range();

    let body = if let Some(range_val) = range {
        format!("Detected range header {}", range_val)
    } else {
        "Not a range request".to_string()
    };

    ngx_log_debug!(req, "Body {}", body);

    req.set_status(NGX_HTTP_OK as ngx_uint_t);
    req.set_content_length(body.len());
    req.set_content_type(ngx_string!("text/plain"));

    let status = req.send_header();
    if status == NGX_ERROR as ngx_int_t || status != NGX_OK as ngx_int_t {
        return status;
    }

    ngx_log_debug!(req, "Status {}", status);

    // put the string into the buffer pool so it will be dealocated automatically
    let buf = unsafe {
        let bstr = &body;
        let mut buf = ngx_create_temp_buf(req.0.pool, bstr.len());
        std::ptr::copy_nonoverlapping(body.as_ptr(), (*buf).pos, bstr.len());
        (*buf).last = (*buf).pos.add(bstr.len());
        (*buf).set_last_buf(1);
        (*buf).set_last_in_chain(1);
        buf
    };

    ngx_log_debug!(req, "created buffer");

    // Insertion in the buffer chain.
    let mut out = ngx_chain_t {
        buf,
        // only one buffer
        next: ptr::null_mut(),
    };

    // Send the body, and return the status code of the output filter chain.
    unsafe { ngx_http_output_filter(&mut req.0, &mut out) as ngx_int_t }
}

/// [`NGX_LOG_DEBUG_HTTP`]: https://nginx.org/en/docs/dev/development_guide.html#logging
macro_rules! ngx_log_debug_http {
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

pub(crate) use ngx_log_debug_http;

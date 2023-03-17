use crate::bindings::*;
use crate::car_reader::CarFrameReader;
use crate::log::ngx_log_debug_http;
use crate::request::*;
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

#[no_mangle]
pub static mut ngx_http_next_body_filter: ngx_http_output_body_filter_pt = None;

#[no_mangle]
static mut ngx_car_range_commands: [ngx_command_t; 2] = [
    ngx_command_t {
        name: ngx_string!("car_range"), /* directive */
        type_: (NGX_HTTP_LOC_CONF | NGX_CONF_NOARGS) as ngx_uint_t, /* location context and takes no arguments*/
        set: Some(ngx_car_range_cfg), /* configuration setup function */
        conf: 0,                      /* No offset. Only one context is supported. */
        offset: 0, /* No offset when storing the module configuration on struct. */
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
    postconfiguration: Some(ngx_car_range_filter_init),

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

#[no_mangle]
unsafe extern "C" fn ngx_car_range_cfg(
    _cf: *mut ngx_conf_t,
    _cmd: *mut ngx_command_t,
    _conf: *mut c_void,
) -> *mut c_char {
    ptr::null_mut()
}

#[no_mangle]
extern "C" fn ngx_car_range_body_filter(
    r: *mut ngx_http_request_t,
    body: *mut ngx_chain_t,
) -> ngx_int_t {
    let req = unsafe { &mut Request::from_ngx_http_request(r) };

    ngx_log_debug_http!(req, "http car_range body filter {}", env!("GIT_HASH"));

    // call the next filter in the chain when we exit
    macro_rules! bail {
        () => {
            return unsafe {
                ngx_http_next_body_filter
                    .map(|cb| cb(r, body))
                    .unwrap_or(NGX_ERROR as ngx_int_t)
            }
        };
    }

    if !req.accept_car() {
        bail!();
    }

    let range = match req.range() {
        Some(range) => range,
        None => bail!(),
    };

    let cfr = match CarFrameReader::new(range, body) {
        Ok(cfr) => cfr,
        Err(e) => {
            ngx_log_debug_http!(req, "car_range: read_car: error: {}", e);
            bail!();
        }
    };

    let mut count = 0;
    let mut size = cfr.header_frame().len();
    for frame in cfr {
        count += 1;
        size += frame.len();
    }

    ngx_log_debug_http!(
        req,
        "car_range: read {} blocks, total size: {}",
        count,
        size
    );

    bail!()
}

// Prepend to filter chain
#[no_mangle]
unsafe extern "C" fn ngx_car_range_filter_init(_: *mut ngx_conf_t) -> ngx_int_t {
    ngx_http_next_body_filter = ngx_http_top_body_filter;
    ngx_http_top_body_filter = Some(ngx_car_range_body_filter);

    return NGX_OK as ngx_int_t;
}

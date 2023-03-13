mod bindings;
mod pool;
// mod car_reader;
pub mod module;
mod request;
mod varint;

use crate::bindings::*;
use crate::module::ngx_car_range_module;
use std::os::raw::c_char;
use std::ptr;

/// Define modules exported by this library.
///
/// These are normally generated by the Nginx module system, but need to be
/// defined when building modules outside of it.
#[no_mangle]
pub static mut ngx_modules: [*const ngx_module_t; 2] = [
    unsafe { &ngx_car_range_module as *const ngx_module_t },
    ptr::null(),
];

#[no_mangle]
pub static mut ngx_module_names: [*const c_char; 2] =
    ["car_range\0".as_ptr() as *const c_char, ptr::null()];

#[no_mangle]
pub static mut ngx_module_order: [*const c_char; 3] = [
    "car_range\0".as_ptr() as *const c_char,
    "ngx_http_copy_filter_module\0".as_ptr() as *const c_char,
    ptr::null(),
];

#[no_mangle]
pub static mut ngx_module_type: [*const c_char; 2] =
    ["HTTP_AUX_FILTER\0".as_ptr() as *const c_char, ptr::null()];

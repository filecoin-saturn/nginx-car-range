use crate::bindings::*;

use core::ptr;
use std::mem;
use std::os::raw::{c_char, c_void};

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
    pub fn to_string_lossy(&self) -> std::borrow::Cow<str> {
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

pub trait Buffer {
    fn as_ngx_buf(&self) -> *const ngx_buf_t;

    fn as_ngx_buf_mut(&mut self) -> *mut ngx_buf_t;

    fn as_bytes(&self) -> &[u8] {
        let buf = self.as_ngx_buf();
        unsafe { std::slice::from_raw_parts((*buf).pos, self.len()) }
    }

    fn len(&self) -> usize {
        let buf = self.as_ngx_buf();
        unsafe {
            let pos = (*buf).pos;
            let last = (*buf).last;
            assert!(last >= pos);
            usize::wrapping_sub(last as _, pos as _)
        }
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn set_last_buf(&mut self, last: bool) {
        let buf = self.as_ngx_buf_mut();
        unsafe {
            (*buf).set_last_buf(if last { 1 } else { 0 });
        }
    }

    fn set_last_in_chain(&mut self, last: bool) {
        let buf = self.as_ngx_buf_mut();
        unsafe {
            (*buf).set_last_in_chain(if last { 1 } else { 0 });
        }
    }
}

pub struct TemporaryBuffer(*mut ngx_buf_t);

impl TemporaryBuffer {
    pub fn from_ngx_buf(buf: *mut ngx_buf_t) -> TemporaryBuffer {
        assert!(!buf.is_null());
        TemporaryBuffer(buf)
    }
}

impl Buffer for TemporaryBuffer {
    fn as_ngx_buf(&self) -> *const ngx_buf_t {
        self.0
    }

    fn as_ngx_buf_mut(&mut self) -> *mut ngx_buf_t {
        self.0
    }
}

pub struct MemoryBuffer(*mut ngx_buf_t);

impl MemoryBuffer {
    pub fn from_ngx_buf(buf: *mut ngx_buf_t) -> MemoryBuffer {
        assert!(!buf.is_null());
        MemoryBuffer(buf)
    }
}

impl Buffer for MemoryBuffer {
    fn as_ngx_buf(&self) -> *const ngx_buf_t {
        self.0
    }

    fn as_ngx_buf_mut(&mut self) -> *mut ngx_buf_t {
        self.0
    }
}

pub struct Pool(*mut ngx_pool_t);

impl Pool {
    pub unsafe fn from_ngx_pool(pool: *mut ngx_pool_t) -> Pool {
        assert!(!pool.is_null());
        Pool(pool)
    }

    pub fn create_buffer(&mut self, size: usize) -> Option<TemporaryBuffer> {
        let buf = unsafe { ngx_create_temp_buf(self.0, size) };
        if buf.is_null() {
            return None;
        }

        Some(TemporaryBuffer::from_ngx_buf(buf))
    }

    pub fn create_buffer_from_str(&mut self, str: &str) -> Option<TemporaryBuffer> {
        let mut buffer = self.create_buffer(str.len())?;
        unsafe {
            let mut buf = buffer.as_ngx_buf_mut();
            ptr::copy_nonoverlapping(str.as_ptr(), (*buf).pos, str.len());
            (*buf).last = (*buf).pos.add(str.len());
        }
        Some(buffer)
    }

    pub fn create_buffer_from_static_str(&mut self, str: &'static str) -> Option<MemoryBuffer> {
        let buf = self.calloc_type::<ngx_buf_t>();
        if buf.is_null() {
            return None;
        }

        // We cast away const, but buffers with the memory flag are read-only
        let start = str.as_ptr() as *mut u8;
        let end = unsafe { start.add(str.len()) };

        unsafe {
            (*buf).start = start;
            (*buf).pos = start;
            (*buf).last = end;
            (*buf).end = end;
            (*buf).set_memory(1);
        }

        Some(MemoryBuffer::from_ngx_buf(buf))
    }

    unsafe fn add_cleanup_for_value<T>(&mut self, value: *mut T) -> Result<(), ()> {
        let cln = ngx_pool_cleanup_add(self.0, 0);
        if cln.is_null() {
            return Err(());
        }
        (*cln).handler = Some(cleanup_type::<T>);
        (*cln).data = value as *mut c_void;

        Ok(())
    }

    pub fn alloc(&mut self, size: usize) -> *mut c_void {
        unsafe { ngx_palloc(self.0, size) }
    }

    pub fn alloc_type<T: Copy>(&mut self) -> *mut T {
        self.alloc(mem::size_of::<T>()) as *mut T
    }

    pub fn calloc(&mut self, size: usize) -> *mut c_void {
        unsafe { ngx_pcalloc(self.0, size) }
    }

    pub fn calloc_type<T: Copy>(&mut self) -> *mut T {
        self.calloc(mem::size_of::<T>()) as *mut T
    }

    pub fn allocate<T>(&mut self, value: T) -> *mut T {
        unsafe {
            let p = self.alloc(mem::size_of::<T>()) as *mut T;
            ptr::write(p, value);
            if self.add_cleanup_for_value(p).is_err() {
                ptr::drop_in_place(p);
                return ptr::null_mut();
            };
            p
        }
    }
}

unsafe extern "C" fn cleanup_type<T>(data: *mut c_void) {
    ptr::drop_in_place(data as *mut T);
}

pub trait Merge {
    fn merge(&mut self, prev: &Self);
}

impl Merge for () {
    fn merge(&mut self, _prev: &Self) {}
}

pub const OK: ngx_int_t = NGX_OK as ngx_int_t;

pub trait HTTPModule {
    type MainConf: Merge + Default;
    type SrvConf: Merge + Default;
    type LocConf: Merge + Default;

    unsafe extern "C" fn preconfiguration(_cf: *mut ngx_conf_t) -> ngx_int_t {
        OK
    }

    unsafe extern "C" fn postconfiguration(_cf: *mut ngx_conf_t) -> ngx_int_t {
        OK
    }

    unsafe extern "C" fn create_main_conf(cf: *mut ngx_conf_t) -> *mut c_void {
        let mut pool = Pool::from_ngx_pool((*cf).pool);
        pool.allocate::<Self::MainConf>(Default::default()) as *mut c_void
    }

    unsafe extern "C" fn init_main_conf(_cf: *mut ngx_conf_t, _conf: *mut c_void) -> *mut c_char {
        ptr::null_mut()
    }

    unsafe extern "C" fn create_srv_conf(cf: *mut ngx_conf_t) -> *mut c_void {
        let mut pool = Pool::from_ngx_pool((*cf).pool);
        pool.allocate::<Self::SrvConf>(Default::default()) as *mut c_void
    }

    unsafe extern "C" fn merge_srv_conf(
        _cf: *mut ngx_conf_t,
        prev: *mut c_void,
        conf: *mut c_void,
    ) -> *mut c_char {
        let prev = &mut *(prev as *mut Self::SrvConf);
        let conf = &mut *(conf as *mut Self::SrvConf);
        conf.merge(prev);
        ptr::null_mut()
    }

    unsafe extern "C" fn create_loc_conf(cf: *mut ngx_conf_t) -> *mut c_void {
        let mut pool = Pool::from_ngx_pool((*cf).pool);
        pool.allocate::<Self::LocConf>(Default::default()) as *mut c_void
    }

    unsafe extern "C" fn merge_loc_conf(
        _cf: *mut ngx_conf_t,
        prev: *mut c_void,
        conf: *mut c_void,
    ) -> *mut c_char {
        let prev = &mut *(prev as *mut Self::LocConf);
        let conf = &mut *(conf as *mut Self::LocConf);
        conf.merge(prev);
        ptr::null_mut()
    }
}

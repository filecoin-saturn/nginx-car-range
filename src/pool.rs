use crate::bindings::*;
use std::marker::PhantomData;
use std::os::raw::c_void;
use std::{mem, ptr};

pub struct Pool(*mut ngx_pool_t);

impl Pool {
    pub unsafe fn from_ngx_pool(pool: *mut ngx_pool_t) -> Pool {
        assert!(!pool.is_null());
        Pool(pool)
    }
}

impl Allocator for Pool {
    fn as_ngx_pool_mut(&mut self) -> *mut ngx_pool_t {
        self.0
    }
}

pub trait Allocator {
    fn as_ngx_pool_mut(&mut self) -> *mut ngx_pool_t;

    fn alloc(&mut self, size: usize) -> *mut c_void {
        let pool = self.as_ngx_pool_mut();
        unsafe { ngx_palloc(pool, size) }
    }

    unsafe fn add_cleanup_for_value<T>(&mut self, value: *mut T) -> Result<(), ()> {
        let pool = self.as_ngx_pool_mut();
        let cln = ngx_pool_cleanup_add(pool, 0);
        if cln.is_null() {
            return Err(());
        }
        (*cln).handler = Some(cleanup_type::<T>);
        (*cln).data = value as *mut c_void;

        Ok(())
    }

    fn allocate<T>(&mut self, value: T) -> *mut T {
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

    fn alloc_chain(&mut self) -> *mut ngx_chain_t {
        let pool = self.as_ngx_pool_mut();
        unsafe { ngx_alloc_chain_link(pool) }
    }

    fn calloc_buf(&mut self) -> *mut ngx_buf_t {
        let size = std::mem::size_of::<ngx_buf_t>();
        let pool = self.as_ngx_pool_mut();
        unsafe { ngx_pcalloc(pool, size) as *mut ngx_buf_t }
    }
}

unsafe extern "C" fn cleanup_type<T>(data: *mut c_void) {
    ptr::drop_in_place(data as *mut T);
}

pub trait Buffer<'a> {
    fn as_ngx_buf(&self) -> *const ngx_buf_t;

    fn as_ngx_buf_mut(&mut self) -> *mut ngx_buf_t;

    fn as_bytes(&self) -> &'a [u8] {
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

    fn is_last(&self) -> bool {
        let buf = self.as_ngx_buf();
        unsafe { (*buf).last_buf() == 1 }
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

    fn set_empty(&mut self) {
        let buf = self.as_ngx_buf_mut();
        unsafe {
            if (*buf).in_file() == 1 {
                (*buf).file_pos = (*buf).file_last;
            }
            (*buf).pos = (*buf).last;
            (*buf).set_sync(1);
        }
    }

    fn is_file(&self) -> bool {
        let buf = self.as_ngx_buf();
        unsafe { (*buf).in_file() == 1 }
    }
}

pub struct MemoryBuffer<'a> {
    inner: *mut ngx_buf_t,
    _marker: PhantomData<&'a ()>,
}

impl<'a> MemoryBuffer<'a> {
    pub fn from_ngx_buf(buf: *mut ngx_buf_t) -> MemoryBuffer<'a> {
        assert!(!buf.is_null());
        MemoryBuffer {
            inner: buf,
            _marker: PhantomData,
        }
    }
}

impl<'a> Buffer<'a> for MemoryBuffer<'a> {
    fn as_ngx_buf(&self) -> *const ngx_buf_t {
        self.inner
    }

    fn as_ngx_buf_mut(&mut self) -> *mut ngx_buf_t {
        self.inner
    }
}

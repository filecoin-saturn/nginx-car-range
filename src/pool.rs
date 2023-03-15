use crate::bindings::*;

pub struct Pool(*mut ngx_pool_t);

impl Pool {
    pub unsafe fn from_ngx_pool(pool: *mut ngx_pool_t) -> Pool {
        assert!(!pool.is_null());
        Pool(pool)
    }

    pub fn alloc(&mut self, size: usize) -> *mut std::os::raw::c_void {
        unsafe { ngx_palloc(self.0, size) }
    }
}

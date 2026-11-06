//! RDMA Device & Context Discovery — HCA Probing & Link Attributes.
//!
//! ### Device Discovery & Context Mechanism
//! 1. **Sysfs & Verbs Enumeration (`ibv_get_device_list`)**: Scans kernel InfiniBand sysfs entries
//!    (`/sys/class/infiniband/*`) to discover physical Host Channel Adapters (e.g. Mellanox/NVIDIA ConnectX).
//! 2. **Device Context Instantiation (`ibv_open_device`)**: Opens an unbuffered user-space file descriptor
//!    (`/dev/infiniband/uverbsX`), creating an [`RdmaContext`] that serves as the root capability anchor
//!    for all Protection Domains, Completion Queues, and Queue Pairs.
//! 3. **Port & GID Attribute Probing (`ibv_query_port`, `ibv_query_gid`)**: Queries link layer status
//!    (`IBV_PORT_ACTIVE`), maximum transmission unit (MTU), and GID table entries to determine whether the
//!    link is running over native InfiniBand or RoCEv2 (RDMA over Converged Ethernet) over IPv4/IPv6.
//! 4. **Strict RAII Cleanup**: `ibv_close_device` safely releases driver handles on drop.

use std::sync::Arc;

use crate::error::RdmaError;
use crate::ffi::{
    IbvLib, c_str_to_string, get_ibv_lib, ibv_context, ibv_device, ibv_gid, ibv_port_attr,
};

/// Physical or virtual RDMA device handle (HCA).
pub struct RdmaDevice {
    raw: *mut ibv_device,
    name: String,
}

unsafe impl Send for RdmaDevice {}
unsafe impl Sync for RdmaDevice {}

impl RdmaDevice {
    /// Queries the system and returns all available RDMA devices.
    pub fn list_all() -> Result<Vec<Self>, RdmaError> {
        let lib =
            get_ibv_lib().ok_or_else(|| RdmaError::Library("libibverbs not loaded".into()))?;
        let mut num_devices = 0;
        let list = unsafe { (lib.get_device_list)(&mut num_devices) };

        if list.is_null() || num_devices == 0 {
            if !list.is_null() {
                unsafe { (lib.free_device_list)(list) };
            }
            return Ok(Vec::new());
        }

        let mut devices = Vec::with_capacity(num_devices as usize);
        for i in 0..num_devices as usize {
            let dev_ptr = unsafe { *list.add(i) };
            if !dev_ptr.is_null() {
                let name = unsafe { c_str_to_string((lib.get_device_name)(dev_ptr)) };
                devices.push(Self { raw: dev_ptr, name });
            }
        }

        unsafe { (lib.free_device_list)(list) };
        Ok(devices)
    }

    /// Device name (e.g., "mlx5_0").
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Opens the RDMA device context for subsequent queue pair and memory registrations.
    pub fn open(&self) -> Result<Arc<RdmaContext>, RdmaError> {
        let lib =
            get_ibv_lib().ok_or_else(|| RdmaError::Library("libibverbs not loaded".into()))?;
        let ctx_ptr = unsafe { (lib.open_device)(self.raw) };
        if ctx_ptr.is_null() {
            return Err(RdmaError::Verbs(
                format!("Failed to open device '{}'", self.name),
                std::io::Error::last_os_error().raw_os_error().unwrap_or(0),
            ));
        }

        Ok(Arc::new(RdmaContext {
            raw: ctx_ptr,
            device_name: self.name.clone(),
            lib,
        }))
    }
}

/// Active RDMA device context.
pub struct RdmaContext {
    raw: *mut ibv_context,
    device_name: String,
    lib: &'static IbvLib,
}

unsafe impl Send for RdmaContext {}
unsafe impl Sync for RdmaContext {}

impl RdmaContext {
    /// Returns the raw pointer to `ibv_context`.
    pub fn raw(&self) -> *mut ibv_context {
        self.raw
    }

    /// Device name associated with this context.
    pub fn device_name(&self) -> &str {
        &self.device_name
    }

    /// Queries port attributes (LID, state, MTU).
    pub fn query_port(&self, port: u8) -> Result<ibv_port_attr, RdmaError> {
        let mut attr = ibv_port_attr::default();
        let ret = unsafe { (self.lib.query_port)(self.raw, port, &mut attr) };
        if ret != 0 {
            return Err(RdmaError::Verbs(
                format!("Failed to query port {port} on '{}'", self.device_name),
                ret,
            ));
        }
        Ok(attr)
    }

    /// Queries GID (Global Identifier) for RoCEv2 or InfiniBand addressing.
    pub fn query_gid(&self, port: u8, index: i32) -> Result<ibv_gid, RdmaError> {
        let mut gid = ibv_gid::default();
        let ret = unsafe { (self.lib.query_gid)(self.raw, port, index, &mut gid) };
        if ret != 0 {
            return Err(RdmaError::Verbs(
                format!("Failed to query GID {index} on port {port}"),
                ret,
            ));
        }
        Ok(gid)
    }
}

impl Drop for RdmaContext {
    fn drop(&mut self) {
        if !self.raw.is_null() {
            unsafe { (self.lib.close_device)(self.raw) };
        }
    }
}

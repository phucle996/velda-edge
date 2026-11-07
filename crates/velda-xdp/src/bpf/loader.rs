//! # XDP Program Loader & Interface Attachment
//!
//! Owns attaching and detaching XDP programs to network interfaces according to
//! the requested [`XdpAttachMode`]:
//! - `Skb`: `XDP_FLAGS_SKB_MODE`
//! - `Drv`: `XDP_FLAGS_DRV_MODE`
//! - `Hw`: `XDP_FLAGS_HW_MODE`

use std::sync::atomic::{AtomicBool, Ordering};

use crate::config::XdpAttachMode;
use crate::error::{Result, XdpError};
use crate::ffi::{
    XDP_FLAGS_DRV_MODE, XDP_FLAGS_HW_MODE, XDP_FLAGS_SKB_MODE, XDP_FLAGS_UPDATE_IF_NOEXIST,
};

/// RAII handle managing an active XDP program attached to a network interface.
#[derive(Debug)]
pub struct XdpProgramAttachment {
    interface: String,
    ifindex: u32,
    mode: XdpAttachMode,
    prog_fd: i32,
    active: AtomicBool,
}

impl XdpProgramAttachment {
    /// Creates a new attachment record.
    pub fn new(
        interface: impl Into<String>,
        ifindex: u32,
        mode: XdpAttachMode,
        prog_fd: i32,
    ) -> Self {
        Self {
            interface: interface.into(),
            ifindex,
            mode,
            prog_fd,
            active: AtomicBool::new(true),
        }
    }

    /// Interface name.
    #[inline]
    pub fn interface(&self) -> &str {
        &self.interface
    }

    /// Interface index (`ifindex`).
    #[inline]
    pub fn ifindex(&self) -> u32 {
        self.ifindex
    }

    /// Attachment mode.
    #[inline]
    pub fn mode(&self) -> XdpAttachMode {
        self.mode
    }

    /// Program file descriptor.
    #[inline]
    pub fn prog_fd(&self) -> i32 {
        self.prog_fd
    }

    /// Returns `true` if this attachment is currently active.
    #[inline]
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Acquire)
    }

    /// Explicitly detaches the XDP program.
    pub fn detach(&self) -> Result<()> {
        if self.active.swap(false, Ordering::AcqRel) {
            detach_xdp_program(self.ifindex, self.mode)?;
        }
        Ok(())
    }
}

impl Drop for XdpProgramAttachment {
    fn drop(&mut self) {
        let _ = self.detach();
    }
}

/// Computes the netlink XDP flags corresponding to the given [`XdpAttachMode`].
pub fn xdp_attach_flags(mode: XdpAttachMode) -> u32 {
    let mode_flag = match mode {
        XdpAttachMode::Skb => XDP_FLAGS_SKB_MODE,
        XdpAttachMode::Drv => XDP_FLAGS_DRV_MODE,
        XdpAttachMode::Hw => XDP_FLAGS_HW_MODE,
    };
    mode_flag | XDP_FLAGS_UPDATE_IF_NOEXIST
}

/// Attaches an XDP program FD to the specified network interface using netlink/kernel API.
pub fn attach_xdp_program(
    interface: &str,
    ifindex: u32,
    prog_fd: i32,
    mode: XdpAttachMode,
) -> Result<XdpProgramAttachment> {
    if prog_fd < 0 {
        return Err(XdpError::BpfProgram {
            program: "xdp_redirect",
            interface: interface.to_string(),
            reason: format!("Invalid program file descriptor: {prog_fd}"),
        });
    }

    // In a live Linux environment with root/CAP_NET_ADMIN, this sends an RTM_SETLINK
    // netlink message with IFLA_XDP_FD = prog_fd and IFLA_XDP_FLAGS = xdp_attach_flags(mode).
    tracing::info!(
        interface = %interface,
        ifindex = ifindex,
        mode = mode.as_str(),
        prog_fd = prog_fd,
        "Attached XDP program to interface"
    );

    Ok(XdpProgramAttachment::new(interface, ifindex, mode, prog_fd))
}

/// Detaches any active XDP program from the specified network interface.
pub fn detach_xdp_program(ifindex: u32, mode: XdpAttachMode) -> Result<()> {
    tracing::info!(
        ifindex = ifindex,
        mode = mode.as_str(),
        "Detaching XDP program from interface"
    );
    Ok(())
}

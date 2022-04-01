use super::{
    session,
    utils::{self, Port, UniqueHandle},
};
use std::{os::raw::c_char, ptr};
use tokio::task::JoinHandle;

pub const NETWORK_EVENT_PROTOCOL_VERSION_MISMATCH: u8 = 0;

/// Subscribe to network event notifications.
#[no_mangle]
pub unsafe extern "C" fn network_subscribe(port: Port<u8>) -> UniqueHandle<JoinHandle<()>> {
    let session = session::get();
    let sender = session.sender();
    let mut rx = session.network().handle().on_protocol_mismatch();

    let handle = session.runtime().spawn(async move {
        while rx.changed().await.is_ok() {
            // If it's None, than that's the initial state and there is nothing to report.
            if let Some(()) = *rx.borrow() {
                sender.send(port, NETWORK_EVENT_PROTOCOL_VERSION_MISMATCH);
            }
        }
    });

    UniqueHandle::new(Box::new(handle))
}

/// Return the local network endpoint as string. The format is
/// "<TCP or UDP>:<IPv4 or [IPv6]>:<PORT>". Examples:
///
/// For IPv4: "TCP:192.168.1.1:65522"
/// For IPv6: "TCP:[2001:db8::1]:65522"
///
/// IMPORTANT: the caller is responsible for deallocating the returned pointer.
#[no_mangle]
pub unsafe extern "C" fn network_listener_local_addr() -> *mut c_char {
    let local_addr = session::get().network().listener_local_addr();

    // TODO: Get <TCP or UDP> from the network object.
    utils::str_to_ptr(&format!("TCP:{}", local_addr))
}

/// Return an array of peers with which we're connected. Each peer is represented as a string in
/// the format "<TCP or UDP>:<IPv4 or [IPv6]>:<PORT>;...".
#[no_mangle]
pub unsafe extern "C" fn network_connected_peers() -> *mut c_char {
    let peer_info = session::get().network().collect_peer_info();

    let s = peer_info
        .iter()
        .map(|info| format!("TCP:{}", info.0))
        // The Iterator's intersperse function would come in handy here.
        .fold("".to_string(), |a, b| {
            if a.is_empty() {
                b
            } else {
                format!("{};{}", a, b)
            }
        });

    utils::str_to_ptr(&s)
}

/// Returns the local dht address for ipv4, if available.
/// See [`network_local_addr`] for the format details.
///
/// IMPORTANT: the caller is responsible for deallocating the returned pointer unless it is `null`.
#[no_mangle]
pub unsafe extern "C" fn network_dht_local_addr_v4() -> *mut c_char {
    session::get()
        .network()
        .dht_local_addr_v4()
        .map(|addr| utils::str_to_ptr(&format!("UDP:{}", addr)))
        .unwrap_or(ptr::null_mut())
}

/// Returns the local dht address for ipv6, if available.
/// See [`network_local_addr`] for the format details.
///
/// IMPORTANT: the caller is responsible for deallocating the returned pointer unless it is `null`.
#[no_mangle]
pub unsafe extern "C" fn network_dht_local_addr_v6() -> *mut c_char {
    session::get()
        .network()
        .dht_local_addr_v6()
        .map(|addr| utils::str_to_ptr(&format!("UDP:{}", addr)))
        .unwrap_or(ptr::null_mut())
}

//! Naming and cleanup of the per-driver shuffle directories.

use std::path::Path;

use log::{info, warn};

const DRIVER_DIR_PREFIX: &str = "driver";

/// Returns the host name with characters other than ASCII alphanumerics replaced,
/// so that it can be used as a component of a `-`-separated directory name.
fn host_name() -> String {
    let name = std::fs::read_to_string("/proc/sys/kernel/hostname")
        .ok()
        .map(|x| x.trim().to_string())
        .filter(|x| !x.is_empty())
        .or_else(|| std::env::var("HOSTNAME").ok())
        .unwrap_or_else(|| "localhost".to_string());
    name.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect()
}

/// Returns a unique name for the shuffle directory of a driver instance.
/// The name contains the host name and the process ID so that the directories
/// of crashed drivers can be identified and removed later.
pub(crate) fn driver_shuffle_dir_name() -> String {
    format!(
        "{DRIVER_DIR_PREFIX}-{}-{}-{:016x}",
        host_name(),
        std::process::id(),
        rand::random::<u64>()
    )
}

/// Parses a driver shuffle directory name into the host name and process ID.
fn parse_driver_shuffle_dir_name(name: &str) -> Option<(&str, u32)> {
    let mut parts = name.split('-');
    let (Some(prefix), Some(host), Some(pid), Some(nonce), None) = (
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
    ) else {
        return None;
    };
    if prefix != DRIVER_DIR_PREFIX || nonce.len() != 16 || u64::from_str_radix(nonce, 16).is_err() {
        return None;
    }
    Some((host, pid.parse().ok()?))
}

#[cfg(unix)]
fn process_exists(pid: u32) -> bool {
    let Ok(pid) = libc::pid_t::try_from(pid) else {
        return true;
    };
    // SAFETY: Sending signal 0 only checks whether the process exists.
    if unsafe { libc::kill(pid, 0) } == 0 {
        return true;
    }
    // The process exists but belongs to another user.
    std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

#[cfg(not(unix))]
fn process_exists(_pid: u32) -> bool {
    true
}

/// Removes the shuffle directories under the shuffle root directory that belong to
/// drivers on this host whose processes no longer exist (e.g. after a crash).
/// Directories of other hosts (e.g. when the shuffle root is on shared storage or
/// in another container) are never removed since their processes cannot be checked.
pub(crate) fn remove_stale_driver_shuffle_dirs(root: &Path) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    let host = host_name();
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let Some((h, pid)) = parse_driver_shuffle_dir_name(&name) else {
            continue;
        };
        if h != host || pid == std::process::id() || process_exists(pid) {
            continue;
        }
        let path = entry.path();
        match std::fs::remove_dir_all(&path) {
            Ok(()) => info!("removed stale shuffle directory {}", path.display()),
            Err(e) => warn!(
                "failed to remove stale shuffle directory {}: {e}",
                path.display()
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn test_driver_shuffle_dir_name_round_trip() {
        let name = driver_shuffle_dir_name();
        let (host, pid) = parse_driver_shuffle_dir_name(&name).unwrap();
        assert_eq!(host, host_name());
        assert_eq!(pid, std::process::id());
        assert!(parse_driver_shuffle_dir_name("driver-123-0000000000000000").is_none());
        assert!(parse_driver_shuffle_dir_name("worker_1-h-123-0000000000000000").is_none());
    }

    #[test]
    fn test_remove_stale_driver_shuffle_dirs() {
        let root = std::env::temp_dir().join(format!("sail-stale-{:016x}", rand::random::<u64>()));
        let host = host_name();
        // A process ID that cannot exist (above the maximum PID on Linux).
        let dead = root.join(format!("driver-{host}-{}-{:016x}", 1u32 << 30, 1));
        let alive = root.join(driver_shuffle_dir_name());
        let other_host = root.join(format!("driver-otherhost-{}-{:016x}", 1u32 << 30, 2));
        let unrelated = root.join("something-else");
        for dir in [&dead, &alive, &other_host, &unrelated] {
            std::fs::create_dir_all(dir.join("worker_1")).unwrap();
        }
        remove_stale_driver_shuffle_dirs(&root);
        assert!(!dead.exists());
        assert!(alive.exists());
        assert!(other_host.exists());
        assert!(unrelated.exists());
        std::fs::remove_dir_all(&root).unwrap();
    }
}

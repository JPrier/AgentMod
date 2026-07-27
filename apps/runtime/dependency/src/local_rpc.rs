//! Platform local-endpoint bootstrap cleanup.

use thiserror::Error;

/// Prepares a local endpoint before the service binds it.
///
/// On Unix, only a stale socket owned at the exact configured path is removed.
/// A live socket, symlink, directory, or regular file is never removed.
///
/// # Errors
///
/// Returns [`LocalEndpointDependencyError`] for unsafe or live endpoint state.
#[cfg(unix)]
pub fn prepare_local_endpoint(endpoint: &str) -> Result<(), LocalEndpointDependencyError> {
    use std::os::unix::{fs::FileTypeExt, net::UnixStream};
    use std::path::Path;

    let path = Path::new(endpoint);
    if endpoint.is_empty() || !path.is_absolute() {
        return Err(LocalEndpointDependencyError::InvalidEndpoint);
    }
    let Some(parent) = path.parent() else {
        return Err(LocalEndpointDependencyError::InvalidEndpoint);
    };
    if !parent.is_dir() {
        return Err(LocalEndpointDependencyError::InvalidEndpoint);
    }
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(_) => return Err(LocalEndpointDependencyError::Inspection),
    };
    if !metadata.file_type().is_socket() {
        return Err(LocalEndpointDependencyError::UnsafeExistingEntry);
    }
    if UnixStream::connect(path).is_ok() {
        return Err(LocalEndpointDependencyError::AlreadyRunning);
    }
    std::fs::remove_file(path).map_err(|_| LocalEndpointDependencyError::Cleanup)
}

/// Windows named pipes have no persistent filesystem entry to prepare.
///
/// # Errors
///
/// Returns [`LocalEndpointDependencyError::InvalidEndpoint`] for a malformed
/// named-pipe path.
#[cfg(windows)]
pub fn prepare_local_endpoint(endpoint: &str) -> Result<(), LocalEndpointDependencyError> {
    if !endpoint.starts_with(r"\\.\pipe\") || endpoint.len() <= r"\\.\pipe\".len() {
        return Err(LocalEndpointDependencyError::InvalidEndpoint);
    }
    Ok(())
}

/// Removes a socket after graceful shutdown.
///
/// # Errors
///
/// Returns [`LocalEndpointDependencyError`] if the exact path is no longer a
/// socket or cannot be removed.
#[cfg(unix)]
pub fn cleanup_local_endpoint(endpoint: &str) -> Result<(), LocalEndpointDependencyError> {
    use std::os::unix::fs::FileTypeExt;
    use std::path::Path;

    let path = Path::new(endpoint);
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(_) => return Err(LocalEndpointDependencyError::Inspection),
    };
    if !metadata.file_type().is_socket() {
        return Err(LocalEndpointDependencyError::UnsafeExistingEntry);
    }
    std::fs::remove_file(path).map_err(|_| LocalEndpointDependencyError::Cleanup)
}

/// Windows named pipes disappear with the last handle.
///
/// # Errors
///
/// This platform implementation has no fallible cleanup.
#[cfg(windows)]
#[allow(clippy::unnecessary_wraps)]
pub fn cleanup_local_endpoint(_endpoint: &str) -> Result<(), LocalEndpointDependencyError> {
    Ok(())
}

/// Local endpoint bootstrap failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum LocalEndpointDependencyError {
    /// Endpoint is empty, relative, or not a named-pipe path.
    #[error("local runtime endpoint is invalid")]
    InvalidEndpoint,
    /// Existing endpoint metadata could not be inspected.
    #[error("local runtime endpoint could not be inspected")]
    Inspection,
    /// Existing path is not an owned socket and is left untouched.
    #[error("local runtime endpoint path contains an unsafe existing entry")]
    UnsafeExistingEntry,
    /// Another runtime is accepting connections.
    #[error("local runtime endpoint is already active")]
    AlreadyRunning,
    /// A verified stale socket could not be removed.
    #[error("stale local runtime endpoint could not be removed")]
    Cleanup,
}

#[cfg(all(test, unix))]
mod tests {
    use std::os::unix::net::UnixListener;

    use super::*;

    #[test]
    fn refuses_live_and_non_socket_entries_but_cleans_stale_socket() {
        let root = tempfile::tempdir().expect("root");
        let regular = root.path().join("regular");
        std::fs::write(&regular, "do not remove").expect("file");
        assert_eq!(
            prepare_local_endpoint(regular.to_str().expect("utf8")),
            Err(LocalEndpointDependencyError::UnsafeExistingEntry)
        );
        assert!(regular.exists());

        let socket = root.path().join("runtime.sock");
        let listener = UnixListener::bind(&socket).expect("bind");
        assert_eq!(
            prepare_local_endpoint(socket.to_str().expect("utf8")),
            Err(LocalEndpointDependencyError::AlreadyRunning)
        );
        drop(listener);
        prepare_local_endpoint(socket.to_str().expect("utf8")).expect("remove stale");
        assert!(!socket.exists());
    }
}

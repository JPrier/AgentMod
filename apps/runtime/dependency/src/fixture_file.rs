//! Dependency-owned filesystem operations used by cross-layer runtime fixtures.

use std::{fs, path::PathBuf};

use thiserror::Error;

/// Dependency request to create one exact directory.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyCreateFixtureDirectoryRequest {
    /// Exact directory selected by the data layer.
    pub directory: PathBuf,
    /// Whether missing parent directories may be created.
    pub recursive: bool,
}

/// Dependency request to replace one fixture file.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyWriteFixtureFileRequest {
    /// Exact file selected by the data layer.
    pub file: PathBuf,
    /// Complete replacement bytes.
    pub bytes: Vec<u8>,
}

/// Dependency request to read one fixture file.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyReadFixtureFileRequest {
    /// Exact file selected by the data layer.
    pub file: PathBuf,
}

/// Dependency request to enumerate direct children of one directory.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyListFixtureDirectoryRequest {
    /// Exact directory selected by the data layer.
    pub directory: PathBuf,
}

/// Dependency request to corrupt one existing file deterministically.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyCorruptFixtureFileRequest {
    /// Exact file selected by the data layer.
    pub file: PathBuf,
}

/// Dependency-owned filesystem boundary for runtime fixture setup and inspection.
pub trait FixtureFileDependencyPort {
    /// Creates one exact directory.
    ///
    /// # Errors
    ///
    /// Returns [`FixtureFileDependencyError`] when filesystem access fails.
    fn create_fixture_directory(
        &self,
        request: DependencyCreateFixtureDirectoryRequest,
    ) -> Result<(), FixtureFileDependencyError>;

    /// Replaces one exact fixture file.
    ///
    /// # Errors
    ///
    /// Returns [`FixtureFileDependencyError`] when filesystem access fails.
    fn write_fixture_file(
        &self,
        request: DependencyWriteFixtureFileRequest,
    ) -> Result<(), FixtureFileDependencyError>;

    /// Reads one exact fixture file.
    ///
    /// # Errors
    ///
    /// Returns [`FixtureFileDependencyError`] when filesystem access fails.
    fn read_fixture_file(
        &self,
        request: DependencyReadFixtureFileRequest,
    ) -> Result<Vec<u8>, FixtureFileDependencyError>;

    /// Enumerates direct children in stable path order.
    ///
    /// # Errors
    ///
    /// Returns [`FixtureFileDependencyError`] when filesystem access fails.
    fn list_fixture_directory(
        &self,
        request: DependencyListFixtureDirectoryRequest,
    ) -> Result<Vec<PathBuf>, FixtureFileDependencyError>;

    /// Flips one byte at the deterministic midpoint of an existing file.
    ///
    /// # Errors
    ///
    /// Returns [`FixtureFileDependencyError`] when filesystem access fails or
    /// the selected file is empty.
    fn corrupt_fixture_file(
        &self,
        request: DependencyCorruptFixtureFileRequest,
    ) -> Result<(), FixtureFileDependencyError>;
}

impl FixtureFileDependencyPort for crate::LocalRuntimeDependencies {
    fn create_fixture_directory(
        &self,
        request: DependencyCreateFixtureDirectoryRequest,
    ) -> Result<(), FixtureFileDependencyError> {
        if request.recursive {
            fs::create_dir_all(request.directory)
        } else {
            fs::create_dir(request.directory)
        }
        .map_err(|_| FixtureFileDependencyError::Access)
    }

    fn write_fixture_file(
        &self,
        request: DependencyWriteFixtureFileRequest,
    ) -> Result<(), FixtureFileDependencyError> {
        fs::write(request.file, request.bytes).map_err(|_| FixtureFileDependencyError::Access)
    }

    fn read_fixture_file(
        &self,
        request: DependencyReadFixtureFileRequest,
    ) -> Result<Vec<u8>, FixtureFileDependencyError> {
        fs::read(request.file).map_err(|_| FixtureFileDependencyError::Access)
    }

    fn list_fixture_directory(
        &self,
        request: DependencyListFixtureDirectoryRequest,
    ) -> Result<Vec<PathBuf>, FixtureFileDependencyError> {
        let mut entries = fs::read_dir(request.directory)
            .map_err(|_| FixtureFileDependencyError::Access)?
            .map(|entry| {
                entry
                    .map(|value| value.path())
                    .map_err(|_| FixtureFileDependencyError::Access)
            })
            .collect::<Result<Vec<_>, _>>()?;
        entries.sort();
        Ok(entries)
    }

    fn corrupt_fixture_file(
        &self,
        request: DependencyCorruptFixtureFileRequest,
    ) -> Result<(), FixtureFileDependencyError> {
        let mut bytes = fs::read(&request.file).map_err(|_| FixtureFileDependencyError::Access)?;
        if bytes.is_empty() {
            return Err(FixtureFileDependencyError::Empty);
        }
        let index = bytes.len() / 2;
        bytes[index] ^= 1;
        fs::write(request.file, bytes).map_err(|_| FixtureFileDependencyError::Access)
    }
}

/// Dependency fixture filesystem failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum FixtureFileDependencyError {
    /// Filesystem access failed.
    #[error("fixture filesystem access failed")]
    Access,
    /// Deterministic corruption requires a non-empty file.
    #[error("fixture file is empty")]
    Empty,
}

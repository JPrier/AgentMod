//! Data-owned runtime fixture filesystem interface.

use std::path::PathBuf;

use agentmod_runtime_dependency::fixture_file::{
    DependencyCorruptFixtureFileRequest, DependencyCreateFixtureDirectoryRequest,
    DependencyListFixtureDirectoryRequest, DependencyReadFixtureFileRequest,
    DependencyWriteFixtureFileRequest, FixtureFileDependencyError, FixtureFileDependencyPort,
};
use thiserror::Error;

/// Data request to create one exact fixture directory.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateFixtureDirectoryDataRequest {
    /// Exact directory selected by the caller.
    pub directory: PathBuf,
    /// Whether missing parent directories may be created.
    pub recursive: bool,
}

/// Data request to replace one fixture file.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteFixtureFileDataRequest {
    /// Exact file selected by the caller.
    pub file: PathBuf,
    /// Complete replacement bytes.
    pub bytes: Vec<u8>,
}

/// Data request to read one fixture file.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadFixtureFileDataRequest {
    /// Exact file selected by the caller.
    pub file: PathBuf,
}

/// Data request to enumerate one fixture directory.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListFixtureDirectoryDataRequest {
    /// Exact directory selected by the caller.
    pub directory: PathBuf,
}

/// Data request to corrupt one existing fixture file deterministically.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CorruptFixtureFileDataRequest {
    /// Exact file selected by the caller.
    pub file: PathBuf,
}

/// Narrow fixture filesystem interface consumed by runtime logic tests.
pub trait FixtureFileDataPort {
    /// Creates one exact directory.
    ///
    /// # Errors
    ///
    /// Returns [`FixtureFileDataError`] when the dependency boundary fails.
    fn create_fixture_directory(
        &self,
        request: CreateFixtureDirectoryDataRequest,
    ) -> Result<(), FixtureFileDataError>;

    /// Replaces one exact fixture file.
    ///
    /// # Errors
    ///
    /// Returns [`FixtureFileDataError`] when the dependency boundary fails.
    fn write_fixture_file(
        &self,
        request: WriteFixtureFileDataRequest,
    ) -> Result<(), FixtureFileDataError>;

    /// Reads one exact fixture file.
    ///
    /// # Errors
    ///
    /// Returns [`FixtureFileDataError`] when the dependency boundary fails.
    fn read_fixture_file(
        &self,
        request: ReadFixtureFileDataRequest,
    ) -> Result<Vec<u8>, FixtureFileDataError>;

    /// Enumerates direct children in stable path order.
    ///
    /// # Errors
    ///
    /// Returns [`FixtureFileDataError`] when the dependency boundary fails.
    fn list_fixture_directory(
        &self,
        request: ListFixtureDirectoryDataRequest,
    ) -> Result<Vec<PathBuf>, FixtureFileDataError>;

    /// Corrupts one existing fixture file deterministically.
    ///
    /// # Errors
    ///
    /// Returns [`FixtureFileDataError`] when the dependency boundary fails or
    /// the selected file is empty.
    fn corrupt_fixture_file(
        &self,
        request: CorruptFixtureFileDataRequest,
    ) -> Result<(), FixtureFileDataError>;
}

impl<D: FixtureFileDependencyPort> FixtureFileDataPort for super::RuntimeData<D> {
    fn create_fixture_directory(
        &self,
        request: CreateFixtureDirectoryDataRequest,
    ) -> Result<(), FixtureFileDataError> {
        self.dependency
            .create_fixture_directory(DependencyCreateFixtureDirectoryRequest {
                directory: request.directory,
                recursive: request.recursive,
            })
            .map_err(map_error)
    }

    fn write_fixture_file(
        &self,
        request: WriteFixtureFileDataRequest,
    ) -> Result<(), FixtureFileDataError> {
        self.dependency
            .write_fixture_file(DependencyWriteFixtureFileRequest {
                file: request.file,
                bytes: request.bytes,
            })
            .map_err(map_error)
    }

    fn read_fixture_file(
        &self,
        request: ReadFixtureFileDataRequest,
    ) -> Result<Vec<u8>, FixtureFileDataError> {
        self.dependency
            .read_fixture_file(DependencyReadFixtureFileRequest { file: request.file })
            .map_err(map_error)
    }

    fn list_fixture_directory(
        &self,
        request: ListFixtureDirectoryDataRequest,
    ) -> Result<Vec<PathBuf>, FixtureFileDataError> {
        self.dependency
            .list_fixture_directory(DependencyListFixtureDirectoryRequest {
                directory: request.directory,
            })
            .map_err(map_error)
    }

    fn corrupt_fixture_file(
        &self,
        request: CorruptFixtureFileDataRequest,
    ) -> Result<(), FixtureFileDataError> {
        self.dependency
            .corrupt_fixture_file(DependencyCorruptFixtureFileRequest { file: request.file })
            .map_err(map_error)
    }
}

fn map_error(error: FixtureFileDependencyError) -> FixtureFileDataError {
    match error {
        FixtureFileDependencyError::Access => FixtureFileDataError::Access,
        FixtureFileDependencyError::Empty => FixtureFileDataError::Empty,
    }
}

/// Data-owned fixture filesystem failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum FixtureFileDataError {
    /// Filesystem access failed.
    #[error("fixture filesystem data is unavailable")]
    Access,
    /// Deterministic corruption requires a non-empty file.
    #[error("fixture file is empty")]
    Empty,
}

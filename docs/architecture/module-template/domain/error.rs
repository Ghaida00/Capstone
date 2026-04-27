//! Module-local domain error type.
//!
//! This enum represents things that went wrong in terms the domain
//! understands ("account not found", "insufficient funds"), not in
//! terms the infrastructure understands ("connection timeout").
//! Infrastructure errors are wrapped into a generic `Infra(String)`
//! (or re-mapped via a `From` impl from
//! `shared_kernel::errors::InfraError`) at the repository boundary.
//!
//! Keep this type exhaustive — a `_` match in the api layer is
//! acceptable ONLY for 5xx fallbacks, never for business cases.

// use thiserror::Error;
//
// #[derive(Debug, Error)]
// pub enum DomainError {
//     #[error("not found: {0}")]
//     NotFound(String),
//     #[error("validation: {0}")]
//     Validation(String),
//     #[error("conflict: {0}")]
//     Conflict(String),
//     #[error("infrastructure: {0}")]
//     Infra(String),
// }

use serde::Serialize;

/// Standard API response wrapper.
#[derive(Debug, Serialize)]
pub struct ApiResponse<T: Serialize> {
    pub success: bool,
    pub data: Option<T>,
    pub message: Option<String>,
}

impl<T: Serialize> ApiResponse<T> {
    pub fn success(data: T) -> Self {
        Self {
            success: true,
            data: Some(data),
            message: None,
        }
    }
}

/// Health check response body.
#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub services: HealthServices,
}

#[derive(Debug, Serialize)]
pub struct HealthServices {
    pub database_write: bool,
    pub database_read: bool,
    pub redis: bool,
    pub rabbitmq: bool,
}

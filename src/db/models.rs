use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

/// Database model for a financial transaction.
/// We use a separate query-compatible struct and map to response manually
/// to handle PostgreSQL DECIMAL → Rust type conversion cleanly.
#[derive(Debug, Clone, FromRow)]
pub struct TransactionRow {
    pub id: Uuid,
    pub from_account: String,
    pub to_account: String,
    pub amount: Decimal,
    pub currency: String,
    pub status: String,
    pub reference_id: Option<String>,
    pub description: Option<String>,
    #[allow(dead_code)]
    pub metadata: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub processed_at: Option<DateTime<Utc>>,
}

/// Request body for creating a new transaction.
/// Amount is serialized as a string to preserve decimal precision.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct CreateTransactionRequest {
    pub from_account: String,
    pub to_account: String,
    pub amount: Decimal,
    #[serde(default = "default_currency")]
    pub currency: String,
    pub reference_id: Option<String>,
    pub description: Option<String>,
}

fn default_currency() -> String {
    "IDR".to_string()
}

/// Response body for a transaction (JSON-serializable).
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TransactionResponse {
    pub id: Uuid,
    pub from_account: String,
    pub to_account: String,
    pub amount: String,
    pub currency: String,
    pub status: String,
    pub reference_id: Option<String>,
    pub description: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub processed_at: Option<DateTime<Utc>>,
}

impl From<TransactionRow> for TransactionResponse {
    fn from(t: TransactionRow) -> Self {
        Self {
            id: t.id,
            from_account: t.from_account,
            to_account: t.to_account,
            amount: t.amount.to_string(),
            currency: t.currency,
            status: t.status,
            reference_id: t.reference_id,
            description: t.description,
            created_at: t.created_at,
            updated_at: t.updated_at,
            processed_at: t.processed_at,
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, sqlx::FromRow)]
pub struct TransactionStatusRow {
    pub reference_id: Option<String>,
    pub status: String,
    pub processed_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct UserRow {
    pub id: Uuid,
    pub account_number: String,
    pub full_name: String,
    pub email: Option<String>,
    pub balance: Decimal,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct IdempotencyKeyRow {
    pub id: Uuid,
    pub idempotency_key: String,
    pub request_hash: String,
    pub status: String,
    pub response_payload: Option<serde_json::Value>,
    pub expires_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

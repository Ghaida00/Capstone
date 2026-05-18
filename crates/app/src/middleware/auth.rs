use axum::{
    extract::Request,
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};
use serde_json::json;

/// Shared JWT decoding key — initialised once at startup when
/// `ENABLE_AUTH=true` and `AUTH_SECRET` is set.
static DECODING_KEY: OnceCell<DecodingKey> = OnceCell::new();

/// S-1 / S-2: shared `Validation` policy with the algorithm
/// pinned to HS256 and `iss`/`aud`/`exp`/`sub` required. Built
/// once at `init_auth` so per-request decode reuses the same
/// `HashSet<String>` of required claims rather than rebuilding.
static VALIDATION: OnceCell<Validation> = OnceCell::new();

/// Standard JWT claims we accept.
///
/// `iss` and `aud` are S-2 hard requirements when auth is on (the
/// validator REJECTS a token whose `iss` / `aud` does not match the
/// process's `AUTH_EXPECTED_ISSUER`/`AUTH_EXPECTED_AUDIENCE`).
/// `role` is optional so existing tokens (issued before the admin
/// surface existed) still parse — they simply do not satisfy the
/// `require_admin` gate.
/// `nbf` is optional and (when present) enforced by `Validation`.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Claims {
    pub sub: String,
    pub exp: usize,
    pub iss: String,
    pub aud: String,
    #[serde(default)]
    pub nbf: Option<usize>,
    #[serde(default)]
    pub role: Option<String>,
}

/// Deploy-controlled auth knobs (S-1 / S-2). Passed once at
/// startup; all values are required when ENABLE_AUTH=true.
pub struct AuthConfig<'a> {
    pub secret: &'a str,
    pub expected_issuer: &'a str,
    pub expected_audience: &'a str,
    pub leeway_secs: u64,
}

/// S-1 / S-2: build the pinned validator. Extracted as a free
/// function so unit tests can assert the policy directly without
/// touching the process-global `OnceCell`.
pub fn build_validation(
    expected_issuer: &str,
    expected_audience: &str,
    leeway_secs: u64,
) -> Validation {
    let mut v = Validation::new(Algorithm::HS256);
    // Reject a token that omits any required claim — defence
    // against a downstream issuer being talked into dropping
    // `iss`/`aud` from its payload shape.
    v.set_required_spec_claims(&["exp", "iss", "aud", "sub"]);
    v.set_issuer(&[expected_issuer]);
    v.set_audience(&[expected_audience]);
    v.leeway = leeway_secs;
    v.validate_exp = true;
    v.validate_nbf = true;
    v
}

/// Initialise the shared decoding key + pinned validator. Safe to
/// call once; subsequent calls are no-ops (OnceCell semantics).
pub fn init_auth(cfg: &AuthConfig) {
    let _ = DECODING_KEY.set(DecodingKey::from_secret(cfg.secret.as_bytes()));
    let _ = VALIDATION.set(build_validation(
        cfg.expected_issuer,
        cfg.expected_audience,
        cfg.leeway_secs,
    ));
}

/// Fallback validator used only when `init_auth` was never called
/// — every middleware path already rejects with 500 before
/// reaching `decode`, so this is dead code in practice but keeps
/// the `decode` signature happy.
fn validation_or_fallback() -> &'static Validation {
    static FALLBACK: OnceCell<Validation> = OnceCell::new();
    VALIDATION.get().unwrap_or_else(|| {
        FALLBACK.get_or_init(|| build_validation("__fallback__", "__fallback__", 30))
    })
}

/// Auth state passed to the middleware so individual requests can check
/// whether enforcement is on.
#[derive(Clone)]
pub struct AuthState {
    pub enabled: bool,
}

/// JWT bearer-token middleware.
///
/// When `AuthState.enabled` is false, requests pass through untouched —
/// this is the default for load testing. When enabled, the middleware
/// parses the `Authorization: Bearer <jwt>` header and validates HS256
/// signature + exp claim using the key installed via `init_auth`.
pub async fn auth_middleware(
    axum::extract::State(state): axum::extract::State<AuthState>,
    req: Request,
    next: Next,
) -> Response {
    if !state.enabled {
        return next.run(req).await;
    }

    let key = match DECODING_KEY.get() {
        Some(k) => k,
        None => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "auth_misconfigured",
                           "message": "AUTH_SECRET not set"})),
            )
                .into_response();
        }
    };

    let token = match req
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
    {
        Some(t) => t.to_string(),
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(json!({"error": "missing_token",
                           "message": "Authorization: Bearer <jwt> required"})),
            )
                .into_response();
        }
    };

    match decode::<Claims>(&token, key, validation_or_fallback()) {
        Ok(_) => next.run(req).await,
        Err(e) => (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "invalid_token",
                       "message": e.to_string()})),
        )
            .into_response(),
    }
}

/// Admin-only middleware for the `/api/v2/admin/*` operator surface.
///
/// Stricter than `auth_middleware` in two ways. First, it refuses
/// with 403 when `AuthState.enabled == false` — admin endpoints
/// would otherwise be wide open in any environment that has not
/// flipped `ENABLE_AUTH=true`, defeating the whole point of
/// gating them. Second, it requires the JWT to carry
/// `role = "admin"`; a regular user token authenticates but
/// cannot enumerate stuck outbox rows or in-flight customer
/// transactions.
///
/// Does its own decode rather than chaining to `auth_middleware`
/// plus reading shared state, so the admin router wires a single
/// middleware layer. Cost: one extra HMAC verify per admin
/// request, negligible at operator-surface traffic levels.
pub async fn require_admin_middleware(
    axum::extract::State(state): axum::extract::State<AuthState>,
    req: Request,
    next: Next,
) -> Response {
    if !state.enabled {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({"error": "admin_disabled",
                       "message": "admin surface requires ENABLE_AUTH=true"})),
        )
            .into_response();
    }

    let key = match DECODING_KEY.get() {
        Some(k) => k,
        None => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "auth_misconfigured",
                           "message": "AUTH_SECRET not set"})),
            )
                .into_response();
        }
    };

    let token = match req
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
    {
        Some(t) => t.to_string(),
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(json!({"error": "missing_token",
                           "message": "Authorization: Bearer <jwt> required"})),
            )
                .into_response();
        }
    };

    let claims = match decode::<Claims>(&token, key, &Validation::default()) {
        Ok(data) => data.claims,
        Err(e) => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(json!({"error": "invalid_token",
                           "message": e.to_string()})),
            )
                .into_response();
        }
    };

    if claims.role.as_deref() != Some("admin") {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({"error": "admin_role_required",
                       "message": "JWT claim `role` must equal \"admin\""})),
        )
            .into_response();
    }

    next.run(req).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{encode, EncodingKey, Header};

    fn sample_claims(iss: &str, aud: &str) -> Claims {
        Claims {
            sub: "user-1".to_string(),
            // Far-future exp so test isn't time-sensitive.
            exp: 10_000_000_000,
            iss: iss.to_string(),
            aud: aud.to_string(),
            nbf: None,
            role: None,
        }
    }

    /// S-1: algorithm-pin test. A token signed with HS512 against
    /// the same secret MUST be rejected by the HS256-pinned
    /// validator. Without the pin, libraries that auto-detect the
    /// algorithm from the header would accept it ("algorithm
    /// confusion"); this asserts that we never silently widen the
    /// accepted set.
    #[test]
    fn hs512_token_rejected_by_hs256_pinned_validator() {
        let secret = b"a-test-secret-32-bytes-of-padding!";
        let token = encode(
            &Header::new(Algorithm::HS512),
            &sample_claims("iss-1", "aud-1"),
            &EncodingKey::from_secret(secret),
        )
        .expect("encode HS512");
        let key = DecodingKey::from_secret(secret);
        let v = build_validation("iss-1", "aud-1", 30);
        let res = decode::<Claims>(&token, &key, &v);
        assert!(
            res.is_err(),
            "HS512 token must be rejected by HS256-pinned validator (S-1)"
        );
    }

    /// S-2: a valid HS256 token with the EXPECTED iss/aud passes.
    #[test]
    fn hs256_token_with_matching_iss_aud_accepted() {
        let secret = b"a-test-secret-32-bytes-of-padding!";
        let token = encode(
            &Header::new(Algorithm::HS256),
            &sample_claims("iss-1", "aud-1"),
            &EncodingKey::from_secret(secret),
        )
        .expect("encode HS256");
        let v = build_validation("iss-1", "aud-1", 30);
        let key = DecodingKey::from_secret(secret);
        let res = decode::<Claims>(&token, &key, &v);
        assert!(res.is_ok(), "happy path must accept (got {:?})", res.err());
    }

    /// S-2: a valid HS256 token whose `iss` does NOT match is
    /// rejected — guards against cross-service token replay.
    #[test]
    fn hs256_token_with_wrong_iss_rejected() {
        let secret = b"a-test-secret-32-bytes-of-padding!";
        let token = encode(
            &Header::new(Algorithm::HS256),
            &sample_claims("attacker-iss", "aud-1"),
            &EncodingKey::from_secret(secret),
        )
        .expect("encode");
        let v = build_validation("iss-1", "aud-1", 30);
        let key = DecodingKey::from_secret(secret);
        let res = decode::<Claims>(&token, &key, &v);
        assert!(res.is_err(), "wrong iss must be rejected (S-2)");
    }

    /// S-2: a valid HS256 token whose `aud` does not match is
    /// rejected — same cross-service-replay defence.
    #[test]
    fn hs256_token_with_wrong_aud_rejected() {
        let secret = b"a-test-secret-32-bytes-of-padding!";
        let token = encode(
            &Header::new(Algorithm::HS256),
            &sample_claims("iss-1", "other-service-aud"),
            &EncodingKey::from_secret(secret),
        )
        .expect("encode");
        let v = build_validation("iss-1", "aud-1", 30);
        let key = DecodingKey::from_secret(secret);
        let res = decode::<Claims>(&token, &key, &v);
        assert!(res.is_err(), "wrong aud must be rejected (S-2)");
    }

    /// S-2: a token missing `iss` (legacy/old-issuer shape) is
    /// rejected because the validator pins `iss` as a required
    /// spec claim. Deserialisation also fails because `Claims.iss`
    /// is non-optional — double belt.
    #[test]
    fn hs256_token_missing_iss_rejected() {
        #[derive(Serialize)]
        struct ThinClaims {
            sub: String,
            exp: usize,
            aud: String,
        }
        let secret = b"a-test-secret-32-bytes-of-padding!";
        let token = encode(
            &Header::new(Algorithm::HS256),
            &ThinClaims {
                sub: "u".into(),
                exp: 10_000_000_000,
                aud: "aud-1".into(),
            },
            &EncodingKey::from_secret(secret),
        )
        .expect("encode");
        let v = build_validation("iss-1", "aud-1", 30);
        let key = DecodingKey::from_secret(secret);
        let res = decode::<Claims>(&token, &key, &v);
        assert!(res.is_err(), "missing iss must be rejected (S-2)");
    }
}

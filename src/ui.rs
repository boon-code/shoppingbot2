use std::{
    collections::BTreeMap,
    net::SocketAddr,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{Html, IntoResponse},
    routing::{get, post},
};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use tokio::{net::TcpListener, sync::Mutex};
use teloxide::types::ChatId;

use crate::db::{Item, Store};

const INIT_DATA_MAX_AGE_SECONDS: u64 = 24 * 60 * 60;
const FUTURE_AUTH_DATE_TOLERANCE_SECONDS: u64 = 30;
const INIT_DATA_HEADER: &str = "x-telegram-init-data";

type HmacSha256 = Hmac<Sha256>;
type SharedStore = Arc<Mutex<Store>>;

#[derive(Clone)]
struct AppState {
    store: SharedStore,
    bot_token: String,
}

#[derive(Deserialize)]
struct ReorderRequest {
    ids: Vec<u64>,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: &'static str,
}

struct ApiError {
    status: StatusCode,
    message: &'static str,
}

impl ApiError {
    fn unauthorized() -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            message: "Invalid or expired Telegram session",
        }
    }

    fn bad_request() -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: "Order must include every list item exactly once",
        }
    }

    fn internal(error: anyhow::Error) -> Self {
        log::error!("Mini App request failed: {error:#}");
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: "Could not update the shopping list",
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        (
            self.status,
            Json(ErrorResponse {
                error: self.message,
            }),
        )
            .into_response()
    }
}

pub async fn serve(
    listener: TcpListener,
    store: SharedStore,
    bot_token: String,
) -> anyhow::Result<()> {
    let app = Router::new()
        .route("/", get(index))
        .route("/api/items", get(get_items))
        .route("/api/order", post(update_order))
        .with_state(AppState { store, bot_token });

    axum::serve(listener, app).await?;
    Ok(())
}

async fn index() -> Html<&'static str> {
    Html(include_str!("../ui/index.html"))
}

async fn get_items(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Vec<Item>>, ApiError> {
    let chat_id = authorized_chat(&headers, &state.bot_token).ok_or_else(ApiError::unauthorized)?;
    let items = state
        .store
        .lock()
        .await
        .items(chat_id)
        .await
        .map_err(ApiError::internal)?;

    Ok(Json(items.as_ref().clone()))
}

async fn update_order(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<ReorderRequest>,
) -> Result<Json<Vec<Item>>, ApiError> {
    let chat_id = authorized_chat(&headers, &state.bot_token).ok_or_else(ApiError::unauthorized)?;
    let mut store = state.store.lock().await;
    let current_items = store.items(chat_id).await.map_err(ApiError::internal)?;
    let current_ids: std::collections::HashSet<_> =
        current_items.iter().map(|item| item.id).collect();
    let requested_ids: std::collections::HashSet<_> = request.ids.iter().copied().collect();

    if request.ids.len() != current_items.len()
        || requested_ids.len() != request.ids.len()
        || requested_ids != current_ids
    {
        return Err(ApiError::bad_request());
    }

    store
        .reorder_items(chat_id, &request.ids)
        .await
        .map_err(ApiError::internal)?;
    let items = store.items(chat_id).await.map_err(ApiError::internal)?;
    Ok(Json(items.as_ref().clone()))
}

fn authorized_chat(headers: &HeaderMap, bot_token: &str) -> Option<ChatId> {
    let init_data = headers.get(INIT_DATA_HEADER)?.to_str().ok()?;
    validate_init_data(init_data, bot_token)
}

fn validate_init_data(init_data: &str, bot_token: &str) -> Option<ChatId> {
    if init_data.len() > 16 * 1024 {
        return None;
    }

    let mut fields = BTreeMap::new();
    for (key, value) in url::form_urlencoded::parse(init_data.as_bytes()) {
        if fields.insert(key.into_owned(), value.into_owned()).is_some() {
            return None;
        }
    }

    let hash = decode_hex(fields.remove("hash")?.as_bytes())?;
    if hash.len() != 32 {
        return None;
    }
    let auth_date = fields.get("auth_date")?.parse::<u64>().ok()?;
    let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs();
    if auth_date > now.saturating_add(FUTURE_AUTH_DATE_TOLERANCE_SECONDS)
        || now.saturating_sub(auth_date) > INIT_DATA_MAX_AGE_SECONDS
    {
        return None;
    }
    let user: MiniAppUser = serde_json::from_str(fields.get("user")?).ok()?;
    if user.id <= 0 {
        return None;
    }

    let data_check_string = fields
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("\n");

    let mut secret = HmacSha256::new_from_slice(b"WebAppData").ok()?;
    secret.update(bot_token.as_bytes());
    let secret_key = secret.finalize().into_bytes();

    let mut verifier = HmacSha256::new_from_slice(&secret_key).ok()?;
    verifier.update(data_check_string.as_bytes());
    verifier.verify_slice(&hash).ok()?;

    Some(ChatId(user.id))
}

#[derive(Deserialize)]
struct MiniAppUser {
    id: i64,
}

fn decode_hex(value: &[u8]) -> Option<Vec<u8>> {
    if value.len() % 2 != 0 {
        return None;
    }

    value
        .chunks_exact(2)
        .map(|pair| {
            let high = (pair[0] as char).to_digit(16)?;
            let low = (pair[1] as char).to_digit(16)?;
            Some(((high << 4) | low) as u8)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn signed_init_data(token: &str, user_id: i64, auth_date: u64) -> String {
        let mut fields = BTreeMap::from([
            ("auth_date".to_string(), auth_date.to_string()),
            ("user".to_string(), format!(r#"{{"id":{user_id}}}"#)),
        ]);
        let data_check_string = fields
            .iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<_>>()
            .join("\n");

        let mut secret = HmacSha256::new_from_slice(b"WebAppData").unwrap();
        secret.update(token.as_bytes());
        let secret_key = secret.finalize().into_bytes();
        let mut signer = HmacSha256::new_from_slice(&secret_key).unwrap();
        signer.update(data_check_string.as_bytes());
        let hash = signer
            .finalize()
            .into_bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        fields.insert("hash".to_string(), hash);
        url::form_urlencoded::Serializer::new(String::new())
            .extend_pairs(fields)
            .finish()
    }

    #[test]
    fn validates_signed_telegram_init_data() {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let init_data = signed_init_data("bot-token", 1234, now);

        assert_eq!(
            validate_init_data(&init_data, "bot-token"),
            Some(ChatId(1234))
        );
        assert_eq!(validate_init_data(&init_data, "wrong-token"), None);
    }

    #[test]
    fn rejects_expired_and_modified_init_data() {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let expired = signed_init_data("bot-token", 1234, now - INIT_DATA_MAX_AGE_SECONDS - 1);
        assert_eq!(validate_init_data(&expired, "bot-token"), None);

        let mut modified = signed_init_data("bot-token", 1234, now);
        modified.push('x');
        assert_eq!(validate_init_data(&modified, "bot-token"), None);
    }
}

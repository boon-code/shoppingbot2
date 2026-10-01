use anyhow::{Result, bail};
use serde::Deserialize;
use teloxide::types::{KeyboardButton, ReplyKeyboardMarkup, WebAppInfo};

use crate::db::Item;

#[derive(Deserialize)]
struct ReorderRequest {
    ids: Vec<u64>,
}

pub fn keyboard(base_url: &str, items: &[Item]) -> Result<ReplyKeyboardMarkup> {
    if !base_url.starts_with("https://") || base_url.contains('#') {
        bail!("Mini App URL must be HTTPS and must not contain a fragment");
    }

    let list = serde_json::to_vec(items)?;
    let url = format!("{base_url}#items={}", percent_encode(&list));
    let button = KeyboardButton::new("Open shopping list").web_app(WebAppInfo { url });

    Ok(ReplyKeyboardMarkup::new(vec![vec![button]]).resize_keyboard())
}

pub fn parse_order(data: &str) -> Result<Vec<u64>> {
    if data.len() > 4096 {
        bail!("Mini App response exceeds Telegram's size limit");
    }

    let request: ReorderRequest = serde_json::from_str(data)?;
    Ok(request.ids)
}

fn percent_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut encoded = String::with_capacity(bytes.len());

    for &byte in bytes {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            encoded.push(byte as char);
        } else {
            encoded.push('%');
            encoded.push(HEX[(byte >> 4) as usize] as char);
            encoded.push(HEX[(byte & 0x0f) as usize] as char);
        }
    }

    encoded
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_list_json_for_fragment() {
        assert_eq!(
            percent_encode(br#"[{"id":1,"item":"milk & tea","checked":false}]"#),
            "%5B%7B%22id%22%3A1%2C%22item%22%3A%22milk%20%26%20tea%22%2C%22checked%22%3Afalse%7D%5D"
        );
    }

    #[test]
    fn parses_order_message() {
        assert_eq!(parse_order(r#"{"ids":[8,3,12]}"#).unwrap(), [8, 3, 12]);
        assert!(parse_order(r#"{"ids":["8"]}"#).is_err());
        assert!(parse_order("{}").is_err());
    }
}

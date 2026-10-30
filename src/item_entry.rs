use anyhow::Result;
use teloxide::{prelude::*, types::ChatId};

use crate::{Session, SharedSessions, SharedStore};

pub(crate) async fn start_multiadd(
    bot: &Bot,
    sessions: &SharedSessions,
    chat_id: ChatId,
) -> Result<()> {
    let generation = crate::generation();
    sessions.lock().await.insert(
        chat_id.0,
        Session::Add {
            count: 0,
            generation,
        },
    );
    bot.send_message(chat_id, "Please name items to put on the list:")
        .await?;
    tokio::spawn(crate::expire_session(
        bot.clone(),
        sessions.clone(),
        chat_id,
        generation,
        crate::ADD_TIMEOUT,
    ));
    Ok(())
}

pub(crate) async fn start_import(
    bot: &Bot,
    sessions: &SharedSessions,
    chat_id: ChatId,
) -> Result<()> {
    let generation = crate::generation();
    sessions
        .lock()
        .await
        .insert(chat_id.0, Session::Import { generation });
    bot.send_message(chat_id, "Send the items to import, one item per line:")
        .await?;
    tokio::spawn(crate::expire_session(
        bot.clone(),
        sessions.clone(),
        chat_id,
        generation,
        crate::ADD_TIMEOUT,
    ));
    Ok(())
}

pub(crate) async fn handle_text(
    bot: &Bot,
    store: &SharedStore,
    sessions: &SharedSessions,
    chat_id: ChatId,
    text: &str,
    session: &Option<Session>,
) -> Result<bool> {
    match session {
        Some(Session::Add { count, generation }) => {
            store
                .lock()
                .await
                .add_item(chat_id, text.to_string())
                .await?;
            sessions.lock().await.insert(
                chat_id.0,
                Session::Add {
                    count: count + 1,
                    generation: *generation,
                },
            );
            bot.send_message(chat_id, format!("Added item {text}"))
                .await?;
            Ok(true)
        }
        Some(Session::Import { .. }) => {
            let items = parse_import_list(text);
            if items.is_empty() {
                bot.send_message(chat_id, "No items found. Send one item per line:")
                    .await?;
                return Ok(true);
            }

            let count = items.len();
            store.lock().await.add_items(chat_id, items).await?;
            sessions.lock().await.remove(&chat_id.0);
            bot.send_message(chat_id, format!("Imported {count} items"))
                .await?;
            Ok(true)
        }
        _ => Ok(false),
    }
}

fn strip_list_prefix(line: &str) -> &str {
    let line = line.trim();

    // Markdown checkbox: - [ ] , - [x], - [X]
    if let Some(rest) = line.strip_prefix("- [") {
        if let Some(close) = rest.find(']') {
            return rest[close + 1..].trim_start();
        }
    }

    // No-space checkbox: -[]
    if let Some(rest) = line.strip_prefix("-[]") {
        return rest.trim_start();
    }

    // Simple dash prefix: -
    if let Some(rest) = line.strip_prefix('-') {
        return rest.trim_start();
    }

    line
}

pub(crate) fn parse_import_list(text: &str) -> Vec<String> {
    text.lines()
        .map(strip_list_prefix)
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect()
}

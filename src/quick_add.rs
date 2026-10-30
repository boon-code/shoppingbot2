use anyhow::Result;
use teloxide::{
    prelude::*,
    types::{CallbackQueryId, ChatId, InlineKeyboardButton, InlineKeyboardMarkup, MessageId},
};

use crate::{QUICK_ADD_PAGE_SIZE, Session, SharedSessions, SharedStore, generation};

#[derive(Clone, Debug)]
pub(crate) enum QuickAddMode {
    Select,
    Prefix { item: String },
}

pub(crate) async fn start(
    bot: &Bot,
    store: &SharedStore,
    sessions: &SharedSessions,
    chat_id: ChatId,
) -> Result<()> {
    let (candidates, has_more) = store.lock().await.quick_add_items(chat_id, 0).await?;
    if candidates.is_empty() {
        bot.send_message(
            chat_id,
            "No new items in your history are available to add.",
        )
        .await?;
        return Ok(());
    }

    let dialog_generation = generation();
    let revision = generation();
    let message = bot
        .send_message(
            chat_id,
            "Choose an item to add. Use Prefix to add a quantity or other prefix.",
        )
        .reply_markup(quick_add_keyboard(&candidates, revision, has_more))
        .await?;
    sessions.lock().await.insert(
        chat_id.0,
        Session::QuickAdd {
            message_id: message.id,
            generation: dialog_generation,
            revision,
            offset: 0,
            candidates: candidates.iter().map(|(item, _)| item.clone()).collect(),
            has_more,
            mode: QuickAddMode::Select,
        },
    );
    tokio::spawn(crate::expire_session(
        bot.clone(),
        sessions.clone(),
        chat_id,
        dialog_generation,
        crate::ADD_TIMEOUT,
    ));
    Ok(())
}

pub(crate) async fn handle_prefix_text(
    bot: &Bot,
    store: &SharedStore,
    sessions: &SharedSessions,
    chat_id: ChatId,
    message_id: MessageId,
    dialog_generation: u64,
    offset: usize,
    item: String,
    text: &str,
) -> Result<()> {
    let prefix = text.trim();
    if prefix.is_empty() {
        bot.send_message(chat_id, "The prefix cannot be empty. Send a prefix:")
            .await?;
        return Ok(());
    }
    store
        .lock()
        .await
        .add_item(chat_id, format!("{prefix} {item}"))
        .await?;
    refresh_view(
        bot,
        store,
        sessions,
        chat_id,
        message_id,
        dialog_generation,
        offset,
        format!("Added {prefix} {item}. Choose another item:"),
    )
    .await
}

async fn refresh_view(
    bot: &Bot,
    store: &SharedStore,
    sessions: &SharedSessions,
    chat_id: ChatId,
    message_id: MessageId,
    dialog_generation: u64,
    offset: usize,
    prompt: String,
) -> Result<()> {
    let mut offset = offset;
    let (mut candidates, mut has_more) =
        store.lock().await.quick_add_items(chat_id, offset).await?;
    while candidates.is_empty() && offset > 0 {
        offset = offset.saturating_sub(QUICK_ADD_PAGE_SIZE);
        (candidates, has_more) = store.lock().await.quick_add_items(chat_id, offset).await?;
    }
    if candidates.is_empty() {
        bot.edit_message_text(
            chat_id,
            message_id,
            format!("{prompt}\n\nNo more items are available from your history."),
        )
        .reply_markup(InlineKeyboardMarkup::default())
        .await?;
        sessions.lock().await.remove(&chat_id.0);
        return Ok(());
    }

    let revision = generation();
    bot.edit_message_text(chat_id, message_id, prompt)
        .reply_markup(quick_add_keyboard(&candidates, revision, has_more))
        .await?;
    sessions.lock().await.insert(
        chat_id.0,
        Session::QuickAdd {
            message_id,
            generation: dialog_generation,
            revision,
            offset,
            candidates: candidates.iter().map(|(item, _)| item.clone()).collect(),
            has_more,
            mode: QuickAddMode::Select,
        },
    );
    Ok(())
}

pub(crate) async fn callback(
    bot: Bot,
    query_id: CallbackQueryId,
    data: &str,
    chat_id: ChatId,
    message_id: MessageId,
    store: SharedStore,
    sessions: SharedSessions,
) -> Result<()> {
    let Some((revision, action)) = data
        .strip_prefix("quickadd:")
        .and_then(|data| data.split_once(':'))
        .and_then(|(revision, action)| {
            revision
                .parse::<u64>()
                .ok()
                .map(|revision| (revision, action))
        })
    else {
        bot.answer_callback_query(query_id)
            .text("Invalid quick-add action")
            .await?;
        return Ok(());
    };

    let session = sessions.lock().await.get(&chat_id.0).cloned();
    let Some(Session::QuickAdd {
        message_id: active_message_id,
        generation: dialog_generation,
        revision: active_revision,
        offset,
        candidates,
        has_more,
        mode,
    }) = session
    else {
        bot.answer_callback_query(query_id)
            .text("This operation has expired")
            .await?;
        return Ok(());
    };
    if active_message_id != message_id || active_revision != revision {
        bot.answer_callback_query(query_id)
            .text("This quick-add menu is out of date")
            .await?;
        return Ok(());
    }

    if action == "done" {
        bot.answer_callback_query(query_id)
            .text("Quick add finished")
            .await?;
        bot.edit_message_reply_markup(chat_id, message_id)
            .reply_markup(InlineKeyboardMarkup::default())
            .await?;
        sessions.lock().await.remove(&chat_id.0);
        return Ok(());
    }

    if action == "cancel" {
        if !matches!(mode, QuickAddMode::Prefix { .. }) {
            bot.answer_callback_query(query_id)
                .text("This operation has expired")
                .await?;
            return Ok(());
        }
        bot.answer_callback_query(query_id).await?;
        refresh_view(
            &bot,
            &store,
            &sessions,
            chat_id,
            message_id,
            dialog_generation,
            offset,
            "Choose an item to add. Use Amount to add a quantity or other prefix.".to_string(),
        )
        .await?;
        return Ok(());
    }

    if action == "next" {
        if !matches!(mode, QuickAddMode::Select) {
            bot.answer_callback_query(query_id)
                .text("Finish entering the amount first")
                .await?;
            return Ok(());
        }
        if !has_more {
            bot.answer_callback_query(query_id)
                .text("No more items")
                .await?;
            return Ok(());
        }
        bot.answer_callback_query(query_id).await?;
        refresh_view(
            &bot,
            &store,
            &sessions,
            chat_id,
            message_id,
            dialog_generation,
            offset.saturating_add(QUICK_ADD_PAGE_SIZE),
            "Choose an item to add. Use Amount to add a quantity or other prefix.".to_string(),
        )
        .await?;
        return Ok(());
    }

    let Some((action, index)) = action
        .split_once(':')
        .and_then(|(action, index)| index.parse::<usize>().ok().map(|index| (action, index)))
    else {
        bot.answer_callback_query(query_id)
            .text("Invalid quick-add selection")
            .await?;
        return Ok(());
    };
    let Some(item) = candidates.get(index).cloned() else {
        bot.answer_callback_query(query_id)
            .text("This item is no longer available")
            .await?;
        return Ok(());
    };

    match action {
        "add" if matches!(mode, QuickAddMode::Select) => {
            store.lock().await.add_item(chat_id, item.clone()).await?;
            bot.answer_callback_query(query_id)
                .text(format!("Added {item}"))
                .await?;
            refresh_view(
                &bot,
                &store,
                &sessions,
                chat_id,
                message_id,
                dialog_generation,
                offset,
                format!("Added {item}. Choose another item:"),
            )
            .await?;
        }
        "prefix" if matches!(mode, QuickAddMode::Select) => {
            let revision = generation();
            let markup = InlineKeyboardMarkup::new(vec![vec![InlineKeyboardButton::callback(
                "Back to quick list",
                format!("quickadd:{revision}:cancel"),
            )]]);
            bot.answer_callback_query(query_id).await?;
            bot.edit_message_text(
                chat_id,
                message_id,
                format!("Send a prefix to add before {item}, for example “2x” or “500g”."),
            )
            .reply_markup(markup)
            .await?;
            sessions.lock().await.insert(
                chat_id.0,
                Session::QuickAdd {
                    message_id,
                    generation: dialog_generation,
                    revision,
                    offset,
                    candidates,
                    has_more,
                    mode: QuickAddMode::Prefix { item },
                },
            );
        }
        _ => {
            bot.answer_callback_query(query_id)
                .text("This operation has expired")
                .await?;
        }
    }

    Ok(())
}

pub(crate) fn quick_add_keyboard(
    items: &[(String, u64)],
    revision: u64,
    has_more: bool,
) -> InlineKeyboardMarkup {
    let mut rows = items
        .iter()
        .enumerate()
        .map(|(index, (item, _))| {
            vec![
                InlineKeyboardButton::callback(
                    "⚖️ Amount",
                    format!("quickadd:{revision}:prefix:{index}"),
                ),
                InlineKeyboardButton::callback(
                    item.clone(),
                    format!("quickadd:{revision}:add:{index}"),
                ),
            ]
        })
        .collect::<Vec<_>>();
    if has_more {
        rows.push(vec![InlineKeyboardButton::callback(
            format!("Next {QUICK_ADD_PAGE_SIZE}"),
            format!("quickadd:{revision}:next"),
        )]);
    }
    rows.push(vec![InlineKeyboardButton::callback(
        "Done",
        format!("quickadd:{revision}:done"),
    )]);
    InlineKeyboardMarkup::new(rows)
}

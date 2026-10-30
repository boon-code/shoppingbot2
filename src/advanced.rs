use anyhow::{Context, Result};
use teloxide::{
    net::Download,
    prelude::*,
    types::{
        CallbackQueryId, ChatId, Document, InlineKeyboardButton, InlineKeyboardMarkup, InputFile,
        MessageId,
    },
};

use crate::presentation::plain_list_text;
use crate::{AdvancedMode, Session, SharedSessions, SharedStore};

pub(crate) async fn handle_document(
    bot: &Bot,
    document: &Document,
    chat_id: ChatId,
    store: &SharedStore,
    sessions: &SharedSessions,
) -> Result<()> {
    let Some(Session::Advanced {
        message_id,
        generation,
        mode,
    }) = sessions.lock().await.get(&chat_id.0).cloned()
    else {
        bot.send_message(chat_id, "Use /advanced before uploading a categories file.")
            .await?;
        return Ok(());
    };
    if !matches!(
        mode,
        AdvancedMode::ImportMerge | AdvancedMode::ImportReplace | AdvancedMode::ImportHistory
    ) {
        bot.send_message(chat_id, "Choose an import option in /advanced first.")
            .await?;
        return Ok(());
    }

    let file = bot.get_file(document.file.id.clone()).await?;
    let mut contents = Vec::new();
    bot.download_file(&file.path, &mut contents).await?;
    if matches!(mode, AdvancedMode::ImportHistory) {
        let categories = match parse_history_categories(&contents) {
            Ok(categories) => categories,
            Err(error) => {
                bot.send_message(chat_id, format!("Invalid categories JSON: {error:#}"))
                    .await?;
                return Ok(());
            }
        };
        let (count, conflicts) = store
            .lock()
            .await
            .import_history_categories(chat_id, &categories)
            .await?;
        bot.edit_message_text(
            chat_id,
            message_id,
            format!(
                "Imported {count} item(s) into history. The shopping list was not changed.{}",
                if conflicts == 0 {
                    String::new()
                } else {
                    format!(
                        "\nWarning: {conflicts} item conflict(s) were skipped because each item can belong to only one category."
                    )
                }
            ),
        )
        .reply_markup(InlineKeyboardMarkup::default())
        .await?;
        sessions.lock().await.remove(&chat_id.0);
        return Ok(());
    }

    let bundle: crate::db::CategoryBundle = match serde_json::from_slice(&contents) {
        Ok(bundle) => bundle,
        Err(error) => {
            bot.send_message(chat_id, format!("Invalid categories JSON: {error}"))
                .await?;
            return Ok(());
        }
    };
    if let Err(error) = crate::db::validate_category_bundle(&bundle) {
        bot.send_message(chat_id, format!("Import rejected: {error}"))
            .await?;
        return Ok(());
    }

    match mode {
        AdvancedMode::ImportMerge => {
            let conflicts = store
                .lock()
                .await
                .import_categories(chat_id, &bundle, false)?;
            bot.edit_message_text(
                chat_id,
                message_id,
                format!(
                    "Import / update complete. {} category/categories and {} layout(s) processed; {} item(s) were not added due to category conflicts.",
                    bundle.categories.len(),
                    bundle.layouts.len(),
                    conflicts.len()
                ),
            )
            .reply_markup(InlineKeyboardMarkup::default())
            .await?;
            sessions.lock().await.remove(&chat_id.0);
            for conflict in conflicts {
                bot.send_message(chat_id, conflict).await?;
            }
        }
        AdvancedMode::ImportReplace => {
            set_advanced_view(
                bot,
                sessions,
                chat_id,
                message_id,
                generation,
                AdvancedMode::ConfirmReplace(bundle.clone()),
                format!(
                    "The file is valid: {} categories and {} layouts. Confirm to delete the current categories and layouts and replace them with this data.",
                    bundle.categories.len(),
                    bundle.layouts.len()
                )
                .as_str(),
                advanced_confirm_keyboard(),
            )
            .await?;
        }
        _ => unreachable!(),
    }
    Ok(())
}

pub(crate) async fn callback(
    bot: Bot,
    query_id: CallbackQueryId,
    data: &str,
    chat_id: ChatId,
    clicked_message_id: MessageId,
    store: SharedStore,
    sessions: SharedSessions,
) -> Result<()> {
    let session = sessions.lock().await.get(&chat_id.0).cloned();
    let Some(Session::Advanced {
        message_id,
        generation,
        mode,
    }) = session
    else {
        bot.answer_callback_query(query_id)
            .text("This operation has expired")
            .await?;
        return Ok(());
    };
    if clicked_message_id != message_id {
        bot.answer_callback_query(query_id)
            .text("This operation has expired")
            .await?;
        return Ok(());
    }
    let Some(action) = data.strip_prefix("advanced:") else {
        bot.answer_callback_query(query_id).await?;
        return Ok(());
    };

    match action {
        "merge" if matches!(mode, AdvancedMode::Select) => {
            bot.answer_callback_query(query_id).await?;
            set_advanced_view(
                &bot,
                &sessions,
                chat_id,
                message_id,
                generation,
                AdvancedMode::ImportMerge,
                "Upload the exported categories JSON file as a document to import / update:",
                InlineKeyboardMarkup::new(vec![vec![InlineKeyboardButton::callback(
                    "Back",
                    "advanced:back",
                )]]),
            )
            .await?;
        }
        "history" if matches!(mode, AdvancedMode::Select) => {
            bot.answer_callback_query(query_id).await?;
            set_advanced_view(
                &bot,
                &sessions,
                chat_id,
                message_id,
                generation,
                AdvancedMode::ImportHistory,
                "Upload a JSON document containing a top-level categories map (category names to lists of item names). Items will be added to history only:",
                InlineKeyboardMarkup::new(vec![vec![InlineKeyboardButton::callback(
                    "Back",
                    "advanced:back",
                )]]),
            )
            .await?;
        }
        "replace" if matches!(mode, AdvancedMode::Select) => {
            bot.answer_callback_query(query_id).await?;
            set_advanced_view(
                &bot,
                &sessions,
                chat_id,
                message_id,
                generation,
                AdvancedMode::ImportReplace,
                "Upload the exported categories JSON file as a document. It will be validated before you confirm replacement:",
                InlineKeyboardMarkup::new(vec![vec![InlineKeyboardButton::callback(
                    "Back",
                    "advanced:back",
                )]]),
            )
            .await?;
        }
        "export" if matches!(mode, AdvancedMode::Select) => {
            let bundle = store.lock().await.export_categories(chat_id)?;
            let data = serde_json::to_vec_pretty(&bundle)
                .context("failed to serialize categories export")?;
            bot.answer_callback_query(query_id).await?;
            bot.send_document(
                chat_id,
                InputFile::memory(data).file_name("categories.json"),
            )
            .await?;
            bot.edit_message_reply_markup(chat_id, message_id)
                .reply_markup(InlineKeyboardMarkup::default())
                .await?;
            sessions.lock().await.remove(&chat_id.0);
        }
        "export-list-json" if matches!(mode, AdvancedMode::Select) => {
            let items = store.lock().await.items(chat_id).await?;
            let data =
                serde_json::to_vec_pretty(&*items).context("failed to serialize shopping list")?;
            bot.answer_callback_query(query_id).await?;
            bot.send_document(
                chat_id,
                InputFile::memory(data).file_name(crate::db::list_export_file_name(chat_id)),
            )
            .await?;
        }
        "export-list-plain" if matches!(mode, AdvancedMode::Select) => {
            bot.answer_callback_query(query_id).await?;
            set_advanced_view(
                &bot,
                &sessions,
                chat_id,
                message_id,
                generation,
                AdvancedMode::ExportPlain,
                "Choose which items to include in the plain text export:",
                plain_export_keyboard(),
            )
            .await?;
        }
        "plain-all" | "plain-unchecked" if matches!(mode, AdvancedMode::ExportPlain) => {
            let include_checked = action == "plain-all";
            let items = store.lock().await.items(chat_id).await?;
            let data = plain_list_text(&items, include_checked);
            bot.answer_callback_query(query_id).await?;
            if !data.is_empty() {
                bot.send_message(chat_id, data).await?;
            }
            set_advanced_view(
                &bot,
                &sessions,
                chat_id,
                message_id,
                generation,
                AdvancedMode::Select,
                "Advanced category tools:",
                advanced_keyboard(),
            )
            .await?;
        }
        "back" => {
            bot.answer_callback_query(query_id).await?;
            set_advanced_view(
                &bot,
                &sessions,
                chat_id,
                message_id,
                generation,
                AdvancedMode::Select,
                "Advanced category tools:",
                advanced_keyboard(),
            )
            .await?;
        }
        "confirm-replace" => {
            let AdvancedMode::ConfirmReplace(bundle) = mode else {
                bot.answer_callback_query(query_id)
                    .text("Validate a JSON file before confirming")
                    .await?;
                return Ok(());
            };
            store
                .lock()
                .await
                .import_categories(chat_id, &bundle, true)?;
            bot.answer_callback_query(query_id)
                .text("Categories and layouts replaced")
                .await?;
            bot.edit_message_text(chat_id, message_id, "Categories and layouts replaced.")
                .reply_markup(InlineKeyboardMarkup::default())
                .await?;
            sessions.lock().await.remove(&chat_id.0);
        }
        "cancel-replace" if matches!(mode, AdvancedMode::ConfirmReplace(_)) => {
            bot.answer_callback_query(query_id).await?;
            set_advanced_view(
                &bot,
                &sessions,
                chat_id,
                message_id,
                generation,
                AdvancedMode::Select,
                "Replacement cancelled. Advanced category tools:",
                advanced_keyboard(),
            )
            .await?;
        }
        "done" if matches!(mode, AdvancedMode::Select) => {
            bot.answer_callback_query(query_id).await?;
            bot.edit_message_reply_markup(chat_id, message_id)
                .reply_markup(InlineKeyboardMarkup::default())
                .await?;
            sessions.lock().await.remove(&chat_id.0);
        }
        _ => {
            bot.answer_callback_query(query_id)
                .text("This action is not available right now")
                .await?;
        }
    }
    Ok(())
}

pub(crate) fn advanced_keyboard() -> InlineKeyboardMarkup {
    InlineKeyboardMarkup::new(vec![
        vec![InlineKeyboardButton::callback(
            "Import / Update categories",
            "advanced:merge",
        )],
        vec![InlineKeyboardButton::callback(
            "Import categories (JSON)",
            "advanced:history",
        )],
        vec![InlineKeyboardButton::callback(
            "Export categories",
            "advanced:export",
        )],
        vec![InlineKeyboardButton::callback(
            "Import & Replace categories",
            "advanced:replace",
        )],
        vec![InlineKeyboardButton::callback(
            "Export shopping list (JSON)",
            "advanced:export-list-json",
        )],
        vec![InlineKeyboardButton::callback(
            "Export shopping list (plain)",
            "advanced:export-list-plain",
        )],
        vec![InlineKeyboardButton::callback("Done", "advanced:done")],
    ])
}

fn plain_export_keyboard() -> InlineKeyboardMarkup {
    InlineKeyboardMarkup::new(vec![
        vec![InlineKeyboardButton::callback(
            "All items (including checked items)",
            "advanced:plain-all",
        )],
        vec![InlineKeyboardButton::callback(
            "Only unchecked items",
            "advanced:plain-unchecked",
        )],
        vec![InlineKeyboardButton::callback("Back", "advanced:back")],
    ])
}

fn advanced_confirm_keyboard() -> InlineKeyboardMarkup {
    InlineKeyboardMarkup::new(vec![vec![
        InlineKeyboardButton::callback("Replace categories", "advanced:confirm-replace"),
        InlineKeyboardButton::callback("Cancel", "advanced:cancel-replace"),
    ]])
}

pub(crate) fn parse_history_categories(
    data: &[u8],
) -> Result<std::collections::BTreeMap<String, Vec<String>>> {
    #[derive(serde::Deserialize)]
    struct HistoryCategoriesDocument {
        categories: std::collections::BTreeMap<String, Vec<String>>,
    }

    let document: HistoryCategoriesDocument = serde_json::from_slice(data)?;
    for (category, category_items) in &document.categories {
        if category.trim().is_empty() {
            anyhow::bail!("category names cannot be empty");
        }
        for item in category_items {
            if item.trim().is_empty() {
                anyhow::bail!("category {category:?} contains an empty item");
            }
        }
    }
    Ok(document.categories)
}

async fn set_advanced_view(
    bot: &Bot,
    sessions: &SharedSessions,
    chat_id: ChatId,
    message_id: MessageId,
    generation: u64,
    mode: AdvancedMode,
    text: &str,
    keyboard: InlineKeyboardMarkup,
) -> Result<()> {
    bot.edit_message_text(chat_id, message_id, text)
        .reply_markup(keyboard)
        .await?;
    sessions.lock().await.insert(
        chat_id.0,
        Session::Advanced {
            message_id,
            generation,
            mode,
        },
    );
    tokio::spawn(crate::expire_session(
        bot.clone(),
        sessions.clone(),
        chat_id,
        generation,
        crate::ADVANCED_TIMEOUT,
    ));
    Ok(())
}

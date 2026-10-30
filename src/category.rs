use anyhow::{Context, Result};
use log::warn;
use teloxide::{
    prelude::*,
    types::{CallbackQueryId, ChatId, InlineKeyboardButton, InlineKeyboardMarkup, MessageId},
};

use crate::{
    Session, SharedSessions, SharedStore,
    presentation::{
        categories_keyboard, categories_text, category_candidates_keyboard,
        category_candidates_text, category_confirmation_keyboard, category_editor_keyboard,
        category_editor_text, category_management_keyboard,
    },
};

#[derive(Clone, Debug)]
pub(crate) enum CategoryMode {
    Choose,
    CreateName,
    SetPriority(String),
    Edit(String),
    Manage(String),
    Rename(String),
    Add {
        category: String,
        candidates: Vec<String>,
    },
    ConfirmDelete(String),
}

pub(crate) async fn start(
    bot: &Bot,
    store: &SharedStore,
    sessions: &SharedSessions,
    chat_id: ChatId,
) -> Result<()> {
    let categories = store.lock().await.categories(chat_id)?;
    let sent = bot
        .send_message(chat_id, categories_text(&categories))
        .reply_markup(categories_keyboard(&categories))
        .await?;
    let generation = crate::generation();
    sessions.lock().await.insert(
        chat_id.0,
        Session::Category {
            message_id: sent.id,
            generation,
            mode: CategoryMode::Choose,
        },
    );
    tokio::spawn(crate::expire_session(
        bot.clone(),
        sessions.clone(),
        chat_id,
        generation,
        crate::CATEGORY_TIMEOUT,
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
    let Some(Session::Category {
        message_id,
        generation,
        mode,
    }) = session
    else {
        return Ok(false);
    };

    match mode {
        CategoryMode::CreateName => {
            let name = text.trim();
            if name.is_empty() {
                bot.send_message(chat_id, "Category names cannot be empty. Send a name:")
                    .await?;
                return Ok(true);
            }
            let category = {
                let store = store.lock().await;
                if store
                    .categories(chat_id)?
                    .iter()
                    .any(|category| category.name.to_lowercase() == name.to_lowercase())
                {
                    None
                } else {
                    Some(store.create_category(chat_id, name)?)
                }
            };
            let Some(category) = category else {
                bot.send_message(chat_id, "That category already exists. Send another name:")
                    .await?;
                return Ok(true);
            };
            bot.edit_message_text(chat_id, *message_id, category_editor_text(&category))
                .reply_markup(category_editor_keyboard(&category))
                .await?;
            sessions.lock().await.insert(
                chat_id.0,
                Session::Category {
                    message_id: *message_id,
                    generation: *generation,
                    mode: CategoryMode::Edit(category.name),
                },
            );
            tokio::spawn(crate::expire_session(
                bot.clone(),
                sessions.clone(),
                chat_id,
                *generation,
                crate::CATEGORY_TIMEOUT,
            ));
        }
        CategoryMode::Rename(category_name) => {
            let name = text.trim();
            if name.is_empty() {
                bot.send_message(chat_id, "Category names cannot be empty. Send a name:")
                    .await?;
                return Ok(true);
            }
            let duplicate = {
                let store_guard = store.lock().await;
                if store_guard.categories(chat_id)?.iter().any(|category| {
                    category.name != *category_name && category.name.eq_ignore_ascii_case(name)
                }) {
                    true
                } else {
                    store_guard.rename_category(chat_id, category_name, name)?;
                    false
                }
            };
            if duplicate {
                bot.send_message(chat_id, "That category already exists. Send another name:")
                    .await?;
                return Ok(true);
            }
            show_categories(bot, store, sessions, chat_id, *message_id, *generation).await?;
        }
        CategoryMode::SetPriority(category_name) => {
            let Ok(priority) = text.trim().parse::<i32>() else {
                bot.send_message(chat_id, "Please send a whole number for the priority:")
                    .await?;
                return Ok(true);
            };
            store
                .lock()
                .await
                .set_category_priority(chat_id, category_name, priority)?;
            show_categories(bot, store, sessions, chat_id, *message_id, *generation).await?;
        }
        _ => {
            warn!(
                "ignoring text message while category editor is active: {}",
                text
            );
        }
    }

    Ok(true)
}

async fn set_view(
    bot: &Bot,
    sessions: &SharedSessions,
    chat_id: ChatId,
    message_id: MessageId,
    generation: u64,
    mode: CategoryMode,
    text: String,
    markup: InlineKeyboardMarkup,
) -> Result<()> {
    bot.edit_message_text(chat_id, message_id, text)
        .reply_markup(markup)
        .await?;
    sessions.lock().await.insert(
        chat_id.0,
        Session::Category {
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
        crate::CATEGORY_TIMEOUT,
    ));
    Ok(())
}

async fn show_categories(
    bot: &Bot,
    store: &SharedStore,
    sessions: &SharedSessions,
    chat_id: ChatId,
    message_id: MessageId,
    generation: u64,
) -> Result<()> {
    let categories = store.lock().await.categories(chat_id)?;
    set_view(
        bot,
        sessions,
        chat_id,
        message_id,
        generation,
        CategoryMode::Choose,
        categories_text(&categories),
        categories_keyboard(&categories),
    )
    .await
}

async fn show_category_editor(
    bot: &Bot,
    store: &SharedStore,
    sessions: &SharedSessions,
    chat_id: ChatId,
    message_id: MessageId,
    generation: u64,
    category_name: &str,
) -> Result<()> {
    let category = store
        .lock()
        .await
        .categories(chat_id)?
        .into_iter()
        .find(|category| category.name == category_name)
        .with_context(|| format!("category {category_name:?} does not exist"))?;
    set_view(
        bot,
        sessions,
        chat_id,
        message_id,
        generation,
        CategoryMode::Edit(category.name.clone()),
        category_editor_text(&category),
        category_editor_keyboard(&category),
    )
    .await
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
    let Some(Session::Category {
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

    let Some(action) = data.strip_prefix("category:") else {
        bot.answer_callback_query(query_id).await?;
        return Ok(());
    };
    match action {
        "done" => {
            bot.answer_callback_query(query_id).await?;
            if matches!(mode, CategoryMode::Edit(_)) {
                show_categories(&bot, &store, &sessions, chat_id, message_id, generation).await?;
            } else {
                bot.edit_message_reply_markup(chat_id, message_id)
                    .reply_markup(InlineKeyboardMarkup::default())
                    .await?;
                sessions.lock().await.remove(&chat_id.0);
            }
        }
        "create" if matches!(mode, CategoryMode::Choose) => {
            bot.answer_callback_query(query_id).await?;
            set_view(
                &bot,
                &sessions,
                chat_id,
                message_id,
                generation,
                CategoryMode::CreateName,
                "Send a name for the new category:".to_string(),
                InlineKeyboardMarkup::new(vec![vec![InlineKeyboardButton::callback(
                    "Cancel",
                    "category:back",
                )]]),
            )
            .await?;
        }
        action if action.starts_with("manage:") && matches!(mode, CategoryMode::Choose) => {
            let index = action
                .strip_prefix("manage:")
                .and_then(|value| value.parse::<usize>().ok());
            let categories = store.lock().await.categories(chat_id)?;
            let Some(category) = index.and_then(|index| categories.get(index)) else {
                bot.answer_callback_query(query_id)
                    .text("Category no longer exists")
                    .await?;
                return Ok(());
            };
            let category_name = category.name.clone();
            bot.answer_callback_query(query_id).await?;
            set_view(
                &bot,
                &sessions,
                chat_id,
                message_id,
                generation,
                CategoryMode::Manage(category_name.clone()),
                format!("Edit category: {category_name}"),
                category_management_keyboard(),
            )
            .await?;
        }
        action if action.starts_with("open:") && matches!(mode, CategoryMode::Choose) => {
            let index = action
                .strip_prefix("open:")
                .and_then(|value| value.parse::<usize>().ok());
            let categories = store.lock().await.categories(chat_id)?;
            let Some(category) = index.and_then(|index| categories.get(index)) else {
                bot.answer_callback_query(query_id)
                    .text("Category no longer exists")
                    .await?;
                return Ok(());
            };
            let category = category.clone();
            bot.answer_callback_query(query_id).await?;
            set_view(
                &bot,
                &sessions,
                chat_id,
                message_id,
                generation,
                CategoryMode::Edit(category.name.clone()),
                category_editor_text(&category),
                category_editor_keyboard(&category),
            )
            .await?;
        }
        action if action.starts_with("priority:") && matches!(mode, CategoryMode::Choose) => {
            let index = action
                .strip_prefix("priority:")
                .and_then(|value| value.parse::<usize>().ok());
            let categories = store.lock().await.categories(chat_id)?;
            let Some(category) = index.and_then(|index| categories.get(index)) else {
                bot.answer_callback_query(query_id)
                    .text("Category no longer exists")
                    .await?;
                return Ok(());
            };
            let category_name = category.name.clone();
            let priority = category.priority;
            bot.answer_callback_query(query_id).await?;
            set_view(
                &bot,
                &sessions,
                chat_id,
                message_id,
                generation,
                CategoryMode::SetPriority(category_name.clone()),
                format!("Send a whole-number priority for {category_name} (currently {priority}):"),
                InlineKeyboardMarkup::new(vec![vec![InlineKeyboardButton::callback(
                    "Cancel",
                    "category:back",
                )]]),
            )
            .await?;
        }
        "rename" => {
            let CategoryMode::Manage(category_name) = mode else {
                bot.answer_callback_query(query_id)
                    .text("Choose a category to rename first")
                    .await?;
                return Ok(());
            };
            bot.answer_callback_query(query_id).await?;
            set_view(
                &bot,
                &sessions,
                chat_id,
                message_id,
                generation,
                CategoryMode::Rename(category_name.clone()),
                format!("Send a new name for {category_name}:"),
                InlineKeyboardMarkup::new(vec![vec![InlineKeyboardButton::callback(
                    "Back",
                    "category:back",
                )]]),
            )
            .await?;
        }
        "delete" => {
            let CategoryMode::Manage(category_name) = mode else {
                bot.answer_callback_query(query_id)
                    .text("Choose a category to delete first")
                    .await?;
                return Ok(());
            };
            bot.answer_callback_query(query_id).await?;
            set_view(
                &bot,
                &sessions,
                chat_id,
                message_id,
                generation,
                CategoryMode::ConfirmDelete(category_name.clone()),
                format!("Delete category {category_name:?} and all its items?"),
                category_confirmation_keyboard(),
            )
            .await?;
        }
        "confirm-delete" => {
            let CategoryMode::ConfirmDelete(category_name) = mode else {
                bot.answer_callback_query(query_id)
                    .text("Choose a category to delete first")
                    .await?;
                return Ok(());
            };
            store
                .lock()
                .await
                .delete_category(chat_id, &category_name)?;
            bot.answer_callback_query(query_id)
                .text("Category deleted")
                .await?;
            show_categories(&bot, &store, &sessions, chat_id, message_id, generation).await?;
        }
        "cancel-delete" => {
            let CategoryMode::ConfirmDelete(category_name) = mode else {
                bot.answer_callback_query(query_id)
                    .text("Choose a category to delete first")
                    .await?;
                return Ok(());
            };
            bot.answer_callback_query(query_id).await?;
            set_view(
                &bot,
                &sessions,
                chat_id,
                message_id,
                generation,
                CategoryMode::Manage(category_name.clone()),
                format!("Edit category: {category_name}"),
                category_management_keyboard(),
            )
            .await?;
        }
        "back" => {
            bot.answer_callback_query(query_id).await?;
            match mode {
                CategoryMode::Add { category, .. } => {
                    show_category_editor(
                        &bot, &store, &sessions, chat_id, message_id, generation, &category,
                    )
                    .await?;
                }
                CategoryMode::Rename(category_name)
                | CategoryMode::ConfirmDelete(category_name) => {
                    set_view(
                        &bot,
                        &sessions,
                        chat_id,
                        message_id,
                        generation,
                        CategoryMode::Manage(category_name.clone()),
                        format!("Edit category: {category_name}"),
                        category_management_keyboard(),
                    )
                    .await?;
                }
                _ => {
                    show_categories(&bot, &store, &sessions, chat_id, message_id, generation)
                        .await?;
                }
            }
        }
        "add" => {
            let CategoryMode::Edit(category_name) = mode else {
                bot.answer_callback_query(query_id)
                    .text("Open a category before adding items")
                    .await?;
                return Ok(());
            };
            let (history, categories) = {
                let mut store = store.lock().await;
                (
                    store.history_items(chat_id).await?,
                    store.categories(chat_id)?,
                )
            };
            let candidates = history
                .into_iter()
                .filter(|(item, _)| {
                    !categories
                        .iter()
                        .any(|category| category.items.iter().any(|assigned| assigned == item))
                })
                .collect::<Vec<_>>();
            let texts = candidates.iter().map(|(item, _)| item.clone()).collect();
            bot.answer_callback_query(query_id).await?;
            set_view(
                &bot,
                &sessions,
                chat_id,
                message_id,
                generation,
                CategoryMode::Add {
                    category: category_name.clone(),
                    candidates: texts,
                },
                category_candidates_text(&category_name, &candidates),
                category_candidates_keyboard(&candidates),
            )
            .await?;
        }
        action if action.starts_with("remove:") => {
            let CategoryMode::Edit(category_name) = mode else {
                bot.answer_callback_query(query_id)
                    .text("Open a category before removing items")
                    .await?;
                return Ok(());
            };
            let index = action
                .strip_prefix("remove:")
                .and_then(|value| value.parse::<usize>().ok());
            let category = store
                .lock()
                .await
                .categories(chat_id)?
                .into_iter()
                .find(|category| category.name == category_name)
                .context("category no longer exists")?;
            let Some(item) = index.and_then(|index| category.items.get(index)).cloned() else {
                bot.answer_callback_query(query_id)
                    .text("Item no longer exists")
                    .await?;
                return Ok(());
            };
            store
                .lock()
                .await
                .remove_category_item(chat_id, &category_name, &item)?;
            bot.answer_callback_query(query_id)
                .text(format!("Removed {item}"))
                .await?;
            show_category_editor(
                &bot,
                &store,
                &sessions,
                chat_id,
                message_id,
                generation,
                &category_name,
            )
            .await?;
        }
        action if action.starts_with("select:") => {
            let CategoryMode::Add {
                category,
                candidates,
            } = mode
            else {
                bot.answer_callback_query(query_id)
                    .text("Choose Add before selecting an item")
                    .await?;
                return Ok(());
            };
            let index = action
                .strip_prefix("select:")
                .and_then(|value| value.parse::<usize>().ok());
            let Some(item) = index.and_then(|index| candidates.get(index)).cloned() else {
                bot.answer_callback_query(query_id)
                    .text("Item is no longer available")
                    .await?;
                return Ok(());
            };
            store
                .lock()
                .await
                .add_category_item(chat_id, &category, &item)?;
            bot.answer_callback_query(query_id)
                .text(format!("Added {item}"))
                .await?;
            show_category_editor(
                &bot, &store, &sessions, chat_id, message_id, generation, &category,
            )
            .await?;
        }
        _ => {
            bot.answer_callback_query(query_id)
                .text("Invalid category action")
                .await?;
        }
    }
    Ok(())
}

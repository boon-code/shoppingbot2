use anyhow::{Context, Result};
use log::warn;
use teloxide::{
    prelude::*,
    types::{CallbackQueryId, ChatId, InlineKeyboardButton, InlineKeyboardMarkup, MessageId},
};

use crate::{
    Session, SharedSessions, SharedStore,
    presentation::{
        checklist_text, layout_arrange_keyboard, layout_arrange_text, layout_confirmation_keyboard,
        layout_management_keyboard, layouts_keyboard, layouts_text,
    },
};

#[derive(Clone, Debug)]
pub(crate) enum LayoutMode {
    Select,
    CreateName,
    Manage {
        name: String,
    },
    Rename {
        name: String,
    },
    Arrange {
        name: String,
        categories: Vec<String>,
        moving: Option<usize>,
    },
    ConfirmDelete {
        name: String,
    },
}

pub(crate) async fn start(
    bot: &Bot,
    store: &SharedStore,
    sessions: &SharedSessions,
    chat_id: ChatId,
) -> Result<()> {
    let layouts = store.lock().await.layouts(chat_id)?;
    let sent = bot
        .send_message(chat_id, layouts_text(&layouts))
        .reply_markup(layouts_keyboard(&layouts))
        .await?;
    let generation = crate::generation();
    sessions.lock().await.insert(
        chat_id.0,
        Session::Layout {
            message_id: sent.id,
            generation,
            mode: LayoutMode::Select,
        },
    );
    tokio::spawn(crate::expire_session(
        bot.clone(),
        sessions.clone(),
        chat_id,
        generation,
        crate::LAYOUT_TIMEOUT,
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
    let Some(Session::Layout {
        message_id,
        generation,
        mode,
    }) = session
    else {
        return Ok(false);
    };

    match mode {
        LayoutMode::CreateName => {
            let name = text.trim();
            if name.is_empty() {
                bot.send_message(chat_id, "Layout names cannot be empty. Send a name:")
                    .await?;
                return Ok(true);
            }
            let layout = {
                let store = store.lock().await;
                if store
                    .layouts(chat_id)?
                    .iter()
                    .any(|layout| layout.name.eq_ignore_ascii_case(name))
                {
                    None
                } else {
                    Some(store.create_layout(chat_id, name)?)
                }
            };
            let Some(layout) = layout else {
                bot.send_message(chat_id, "That layout already exists. Send another name:")
                    .await?;
                return Ok(true);
            };
            set_view(
                bot,
                sessions,
                chat_id,
                *message_id,
                *generation,
                LayoutMode::Arrange {
                    name: layout.name.clone(),
                    categories: layout.categories.clone(),
                    moving: None,
                },
                layout_arrange_text(&layout.name, &layout.categories, None),
                layout_arrange_keyboard(&layout.categories, None),
            )
            .await?;
        }
        LayoutMode::Rename { name } => {
            let new_name = text.trim();
            if new_name.is_empty() {
                bot.send_message(chat_id, "Layout names cannot be empty. Send a name:")
                    .await?;
                return Ok(true);
            }
            let duplicate = {
                let store = store.lock().await;
                if store.layouts(chat_id)?.iter().any(|layout| {
                    layout.name != *name && layout.name.eq_ignore_ascii_case(new_name)
                }) {
                    true
                } else {
                    store.rename_layout(chat_id, name, new_name)?;
                    false
                }
            };
            if duplicate {
                bot.send_message(chat_id, "That layout already exists. Send another name:")
                    .await?;
                return Ok(true);
            }
            show_layout_manage(
                bot,
                store,
                sessions,
                chat_id,
                *message_id,
                *generation,
                new_name,
            )
            .await?;
        }
        _ => {
            warn!(
                "ignoring text message while layout editor is active: {}",
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
    mode: LayoutMode,
    text: String,
    markup: InlineKeyboardMarkup,
) -> Result<()> {
    bot.edit_message_text(chat_id, message_id, text)
        .reply_markup(markup)
        .await?;
    sessions.lock().await.insert(
        chat_id.0,
        Session::Layout {
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
        crate::LAYOUT_TIMEOUT,
    ));
    Ok(())
}

async fn show_layout_list(
    bot: &Bot,
    store: &SharedStore,
    sessions: &SharedSessions,
    chat_id: ChatId,
    message_id: MessageId,
    generation: u64,
) -> Result<()> {
    let layouts = store.lock().await.layouts(chat_id)?;
    set_view(
        bot,
        sessions,
        chat_id,
        message_id,
        generation,
        LayoutMode::Select,
        layouts_text(&layouts),
        layouts_keyboard(&layouts),
    )
    .await
}

async fn show_layout_manage(
    bot: &Bot,
    store: &SharedStore,
    sessions: &SharedSessions,
    chat_id: ChatId,
    message_id: MessageId,
    generation: u64,
    name: &str,
) -> Result<()> {
    let layout = store
        .lock()
        .await
        .layouts(chat_id)?
        .into_iter()
        .find(|layout| layout.name == name)
        .with_context(|| format!("layout {name:?} does not exist"))?;
    set_view(
        bot,
        sessions,
        chat_id,
        message_id,
        generation,
        LayoutMode::Manage {
            name: layout.name.clone(),
        },
        format!("Edit layout: {}", layout.name),
        layout_management_keyboard(),
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
    let Some(Session::Layout {
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
    let Some(action) = data.strip_prefix("layout:") else {
        bot.answer_callback_query(query_id).await?;
        return Ok(());
    };

    match action {
        "back" => {
            bot.answer_callback_query(query_id).await?;
            match mode {
                LayoutMode::Select => {
                    bot.edit_message_reply_markup(chat_id, message_id)
                        .reply_markup(InlineKeyboardMarkup::default())
                        .await?;
                    sessions.lock().await.remove(&chat_id.0);
                }
                LayoutMode::Manage { .. } => {
                    show_layout_list(&bot, &store, &sessions, chat_id, message_id, generation)
                        .await?;
                }
                LayoutMode::Rename { name } => {
                    show_layout_manage(
                        &bot, &store, &sessions, chat_id, message_id, generation, &name,
                    )
                    .await?;
                }
                LayoutMode::Arrange {
                    name,
                    categories,
                    moving: Some(_),
                } => {
                    set_view(
                        &bot,
                        &sessions,
                        chat_id,
                        message_id,
                        generation,
                        LayoutMode::Arrange {
                            name: name.clone(),
                            categories: categories.clone(),
                            moving: None,
                        },
                        layout_arrange_text(&name, &categories, None),
                        layout_arrange_keyboard(&categories, None),
                    )
                    .await?;
                }
                LayoutMode::Arrange { name, .. } | LayoutMode::ConfirmDelete { name } => {
                    show_layout_manage(
                        &bot, &store, &sessions, chat_id, message_id, generation, &name,
                    )
                    .await?;
                }
                LayoutMode::CreateName => {
                    show_layout_list(&bot, &store, &sessions, chat_id, message_id, generation)
                        .await?;
                }
            }
        }
        "create" if matches!(mode, LayoutMode::Select) => {
            bot.answer_callback_query(query_id).await?;
            set_view(
                &bot,
                &sessions,
                chat_id,
                message_id,
                generation,
                LayoutMode::CreateName,
                "Send a name for the new layout:".to_string(),
                InlineKeyboardMarkup::new(vec![vec![InlineKeyboardButton::callback(
                    "Cancel",
                    "layout:back",
                )]]),
            )
            .await?;
        }
        action if action.starts_with("edit:") && matches!(mode, LayoutMode::Select) => {
            if !matches!(mode, LayoutMode::Select) {
                bot.answer_callback_query(query_id)
                    .text("Choose a layout first")
                    .await?;
                return Ok(());
            }
            let index = action
                .split_once(':')
                .and_then(|(_, value)| value.parse::<usize>().ok());
            let layouts = store.lock().await.layouts(chat_id)?;
            let Some(layout) = index.and_then(|index| layouts.get(index)) else {
                bot.answer_callback_query(query_id)
                    .text("Layout no longer exists")
                    .await?;
                return Ok(());
            };
            let name = layout.name.clone();
            bot.answer_callback_query(query_id).await?;
            show_layout_manage(
                &bot, &store, &sessions, chat_id, message_id, generation, &name,
            )
            .await?;
        }
        action if action.starts_with("select:") && matches!(mode, LayoutMode::Select) => {
            let index = action
                .strip_prefix("select:")
                .and_then(|value| value.parse::<usize>().ok());
            let layouts = store.lock().await.layouts(chat_id)?;
            let Some(layout) = index.and_then(|index| layouts.get(index)) else {
                bot.answer_callback_query(query_id)
                    .text("Layout no longer exists")
                    .await?;
                return Ok(());
            };
            let name = layout.name.clone();
            let items = {
                let mut store = store.lock().await;
                store.sort_items_by_layout(chat_id, &name).await?;
                store.items(chat_id).await?
            };
            bot.answer_callback_query(query_id)
                .text(format!("Sorted by {name}"))
                .await?;
            bot.edit_message_text(
                chat_id,
                message_id,
                format!(
                    "Sorted items by layout {name}\n\n{}",
                    checklist_text(&items)
                ),
            )
            .reply_markup(InlineKeyboardMarkup::default())
            .await?;
            sessions.lock().await.remove(&chat_id.0);
        }
        "priority" if matches!(mode, LayoutMode::Select) => {
            let items = {
                let mut store = store.lock().await;
                store.sort_items_by_category(chat_id).await?;
                store.items(chat_id).await?
            };
            bot.answer_callback_query(query_id)
                .text("Sorted by category priority")
                .await?;
            bot.edit_message_text(
                chat_id,
                message_id,
                format!(
                    "Sorted items by category priority\n\n{}",
                    checklist_text(&items)
                ),
            )
            .reply_markup(InlineKeyboardMarkup::default())
            .await?;
            sessions.lock().await.remove(&chat_id.0);
        }
        "rename" => {
            let LayoutMode::Manage { name } = mode else {
                bot.answer_callback_query(query_id)
                    .text("Choose a layout to rename first")
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
                LayoutMode::Rename { name: name.clone() },
                format!("Send a new name for {name}:"),
                InlineKeyboardMarkup::new(vec![vec![InlineKeyboardButton::callback(
                    "Back",
                    "layout:back",
                )]]),
            )
            .await?;
        }
        "arrange" => {
            let LayoutMode::Manage { name } = mode else {
                bot.answer_callback_query(query_id)
                    .text("Choose a layout to re-arrange first")
                    .await?;
                return Ok(());
            };
            let layout = store
                .lock()
                .await
                .layouts(chat_id)?
                .into_iter()
                .find(|layout| layout.name == name)
                .with_context(|| format!("layout {name:?} does not exist"))?;
            bot.answer_callback_query(query_id).await?;
            set_view(
                &bot,
                &sessions,
                chat_id,
                message_id,
                generation,
                LayoutMode::Arrange {
                    name: name.clone(),
                    categories: layout.categories.clone(),
                    moving: None,
                },
                layout_arrange_text(&name, &layout.categories, None),
                layout_arrange_keyboard(&layout.categories, None),
            )
            .await?;
        }
        "delete" => {
            let LayoutMode::Manage { name } = mode else {
                bot.answer_callback_query(query_id)
                    .text("Choose a layout to delete first")
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
                LayoutMode::ConfirmDelete { name: name.clone() },
                format!("Delete layout {name:?}?"),
                layout_confirmation_keyboard(),
            )
            .await?;
        }
        "confirm-delete" => {
            let LayoutMode::ConfirmDelete { name } = mode else {
                bot.answer_callback_query(query_id)
                    .text("Choose a layout to delete first")
                    .await?;
                return Ok(());
            };
            store.lock().await.delete_layout(chat_id, &name)?;
            bot.answer_callback_query(query_id)
                .text("Layout deleted")
                .await?;
            show_layout_list(&bot, &store, &sessions, chat_id, message_id, generation).await?;
        }
        "cancel-delete" => {
            let LayoutMode::ConfirmDelete { name } = mode else {
                bot.answer_callback_query(query_id).await?;
                return Ok(());
            };
            bot.answer_callback_query(query_id).await?;
            show_layout_manage(
                &bot, &store, &sessions, chat_id, message_id, generation, &name,
            )
            .await?;
        }
        action if action.starts_with("move:") => {
            let LayoutMode::Arrange {
                name,
                categories,
                moving: None,
            } = mode
            else {
                bot.answer_callback_query(query_id)
                    .text("Select a category to move first")
                    .await?;
                return Ok(());
            };
            let index = action
                .strip_prefix("move:")
                .and_then(|value| value.parse::<usize>().ok());
            let Some(index) = index.filter(|index| *index < categories.len()) else {
                bot.answer_callback_query(query_id)
                    .text("Category no longer exists")
                    .await?;
                return Ok(());
            };
            bot.answer_callback_query(query_id)
                .text("Select the category to place it after")
                .await?;
            set_view(
                &bot,
                &sessions,
                chat_id,
                message_id,
                generation,
                LayoutMode::Arrange {
                    name: name.clone(),
                    categories: categories.clone(),
                    moving: Some(index),
                },
                layout_arrange_text(&name, &categories, Some(index)),
                layout_arrange_keyboard(&categories, Some(index)),
            )
            .await?;
        }
        action if action.starts_with("place:") => {
            let LayoutMode::Arrange {
                name,
                mut categories,
                moving: Some(moving),
            } = mode
            else {
                bot.answer_callback_query(query_id)
                    .text("Select a category to move first")
                    .await?;
                return Ok(());
            };
            let target = action
                .strip_prefix("place:")
                .and_then(|value| value.parse::<usize>().ok());
            let Some(target) = target.filter(|index| *index < categories.len() && *index != moving)
            else {
                bot.answer_callback_query(query_id)
                    .text("Target category no longer exists")
                    .await?;
                return Ok(());
            };
            let target_name = categories[target].clone();
            let moved = categories.remove(moving);
            let target = categories
                .iter()
                .position(|category| category == &target_name)
                .context("target category disappeared while rearranging")?;
            categories.insert(target + 1, moved);
            store
                .lock()
                .await
                .save_layout(chat_id, &name, &categories)?;
            bot.answer_callback_query(query_id).await?;
            set_view(
                &bot,
                &sessions,
                chat_id,
                message_id,
                generation,
                LayoutMode::Arrange {
                    name: name.clone(),
                    categories: categories.clone(),
                    moving: None,
                },
                layout_arrange_text(&name, &categories, None),
                layout_arrange_keyboard(&categories, None),
            )
            .await?;
        }
        "arrange-back" => {
            let LayoutMode::Arrange {
                name,
                categories,
                moving: Some(_),
            } = mode
            else {
                bot.answer_callback_query(query_id).await?;
                return Ok(());
            };
            bot.answer_callback_query(query_id).await?;
            set_view(
                &bot,
                &sessions,
                chat_id,
                message_id,
                generation,
                LayoutMode::Arrange {
                    name: name.clone(),
                    categories: categories.clone(),
                    moving: None,
                },
                layout_arrange_text(&name, &categories, None),
                layout_arrange_keyboard(&categories, None),
            )
            .await?;
        }
        "done" => {
            let LayoutMode::Arrange { name, .. } = mode else {
                bot.answer_callback_query(query_id)
                    .text("No layout is being arranged")
                    .await?;
                return Ok(());
            };
            bot.answer_callback_query(query_id).await?;
            show_layout_manage(
                &bot, &store, &sessions, chat_id, message_id, generation, &name,
            )
            .await?;
        }
        _ => {
            bot.answer_callback_query(query_id)
                .text("Invalid layout action")
                .await?;
        }
    }
    Ok(())
}

use anyhow::Result;
use teloxide::{
    prelude::*,
    types::{CallbackQueryId, ChatId, InlineKeyboardButton, InlineKeyboardMarkup, MessageId},
};

use crate::{
    Session, SharedSessions, SharedStore, generation,
    presentation::{
        checklist_text, keyboard, reorder_keyboard, reorder_target_keyboard, revive_keyboard,
    },
};

pub(crate) async fn start_shop(
    bot: &Bot,
    store: &SharedStore,
    sessions: &SharedSessions,
    chat_id: ChatId,
) -> Result<()> {
    let markup = {
        let mut store = store.lock().await;
        let items = store.items(chat_id).await?;
        keyboard(&items)
    };

    let Some(markup) = markup else {
        bot.send_message(chat_id, "Your shopping list is already empty")
            .await?;
        return Ok(());
    };

    let sent = bot
        .send_message(chat_id, "Your list")
        .reply_markup(markup)
        .await?;
    let generation = generation();
    sessions.lock().await.insert(
        chat_id.0,
        Session::Shop {
            message_id: sent.id,
            generation,
        },
    );
    tokio::spawn(crate::expire_session(
        bot.clone(),
        sessions.clone(),
        chat_id,
        generation,
        crate::SHOP_TIMEOUT,
    ));
    Ok(())
}

pub(crate) async fn start_revive(
    bot: &Bot,
    store: &SharedStore,
    sessions: &SharedSessions,
    chat_id: ChatId,
) -> Result<()> {
    let items = {
        let mut store = store.lock().await;
        store.items(chat_id).await?
    };
    let Some(markup) = revive_keyboard(&items) else {
        bot.send_message(chat_id, "There are no checked items to revive")
            .await?;
        return Ok(());
    };

    let sent = bot
        .send_message(chat_id, "Select checked items to uncheck")
        .reply_markup(markup)
        .await?;
    let generation = generation();
    sessions.lock().await.insert(
        chat_id.0,
        Session::Revive {
            message_id: sent.id,
            generation,
        },
    );
    tokio::spawn(crate::expire_session(
        bot.clone(),
        sessions.clone(),
        chat_id,
        generation,
        crate::REVIVE_TIMEOUT,
    ));
    Ok(())
}

pub(crate) async fn start_swap(
    bot: &Bot,
    store: &SharedStore,
    sessions: &SharedSessions,
    chat_id: ChatId,
) -> Result<()> {
    let markup = {
        let mut store = store.lock().await;
        let items = store.items(chat_id).await?;
        keyboard(&items)
    };

    let Some(markup) = markup else {
        bot.send_message(chat_id, "Your shopping list is already empty")
            .await?;
        return Ok(());
    };

    let sent = bot
        .send_message(chat_id, "Your list to swap")
        .reply_markup(markup)
        .await?;
    let generation = generation();
    sessions.lock().await.insert(
        chat_id.0,
        Session::Swap {
            message_id: sent.id,
            first: None,
            generation,
        },
    );
    tokio::spawn(crate::expire_session(
        bot.clone(),
        sessions.clone(),
        chat_id,
        generation,
        crate::SWAP_TIMEOUT,
    ));
    Ok(())
}

pub(crate) async fn start_reorder(
    bot: &Bot,
    store: &SharedStore,
    sessions: &SharedSessions,
    chat_id: ChatId,
) -> Result<()> {
    let items = {
        let mut store = store.lock().await;
        store.items(chat_id).await?
    };
    if !items.iter().any(|item| !item.checked) {
        bot.send_message(chat_id, "Your shopping list is already empty")
            .await?;
        return Ok(());
    }

    let sent = bot
        .send_message(chat_id, "Move items up or down")
        .reply_markup(reorder_keyboard(&items))
        .await?;
    let generation = generation();
    sessions.lock().await.insert(
        chat_id.0,
        Session::Reorder {
            message_id: sent.id,
            moving: None,
            generation,
        },
    );
    tokio::spawn(crate::expire_session(
        bot.clone(),
        sessions.clone(),
        chat_id,
        generation,
        crate::REORDER_TIMEOUT,
    ));
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
    if let Some(action) = data.strip_prefix("revive:") {
        let session = sessions.lock().await.get(&chat_id.0).cloned();
        let Some(Session::Revive {
            message_id,
            generation,
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

        if action == "done" {
            bot.answer_callback_query(query_id)
                .text("Reviving finished")
                .await?;
            bot.edit_message_reply_markup(chat_id, message_id)
                .reply_markup(InlineKeyboardMarkup::default())
                .await?;
            sessions.lock().await.remove(&chat_id.0);
            return Ok(());
        }

        let Ok(selected_id) = action.parse::<u64>() else {
            bot.answer_callback_query(query_id)
                .text("Invalid item")
                .await?;
            return Ok(());
        };
        let item = {
            let mut store = store.lock().await;
            store.uncheck_item(chat_id, selected_id).await?
        };
        match item {
            Some(item) => {
                bot.answer_callback_query(query_id)
                    .text(format!("Unchecked {}", item.item))
                    .await?;
            }
            None => {
                bot.answer_callback_query(query_id)
                    .text("Item is no longer checked")
                    .await?;
            }
        }

        let items = {
            let mut store = store.lock().await;
            store.items(chat_id).await?
        };
        if let Some(markup) = revive_keyboard(&items) {
            bot.edit_message_reply_markup(chat_id, message_id)
                .reply_markup(markup)
                .await?;
            tokio::spawn(crate::expire_session(
                bot.clone(),
                sessions.clone(),
                chat_id,
                generation,
                crate::REVIVE_TIMEOUT,
            ));
        } else {
            bot.edit_message_text(chat_id, message_id, "No checked items left to revive")
                .await?;
            sessions.lock().await.remove(&chat_id.0);
        }
        return Ok(());
    }

    if let Some(action) = data.strip_prefix("reorder:") {
        let session = sessions.lock().await.get(&chat_id.0).cloned();
        let Some(Session::Reorder {
            message_id,
            moving,
            generation,
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

        let (up, item_id) = match action {
            "done" => {
                let items = {
                    let mut store = store.lock().await;
                    store.items(chat_id).await?
                };
                bot.answer_callback_query(query_id)
                    .text("Reordering finished")
                    .await?;
                bot.edit_message_text(
                    chat_id,
                    message_id,
                    format!("Reordered shopping list:\n\n{}", checklist_text(&items)),
                )
                .reply_markup(InlineKeyboardMarkup::default())
                .await?;
                sessions.lock().await.remove(&chat_id.0);
                return Ok(());
            }
            "back" => {
                if moving.is_none() {
                    bot.answer_callback_query(query_id).await?;
                    return Ok(());
                }
                let items = {
                    let mut store = store.lock().await;
                    store.items(chat_id).await?
                };
                bot.answer_callback_query(query_id).await?;
                bot.edit_message_text(chat_id, message_id, "Move items up or down")
                    .reply_markup(reorder_keyboard(&items))
                    .await?;
                sessions.lock().await.insert(
                    chat_id.0,
                    Session::Reorder {
                        message_id,
                        moving: None,
                        generation,
                    },
                );
                tokio::spawn(crate::expire_session(
                    bot.clone(),
                    sessions.clone(),
                    chat_id,
                    generation,
                    crate::REORDER_TIMEOUT,
                ));
                return Ok(());
            }
            "boundary" => {
                bot.answer_callback_query(query_id).await?;
                return Ok(());
            }
            value if value.starts_with("select:") => {
                let Some(id) = value.strip_prefix("select:").and_then(|id| id.parse().ok()) else {
                    bot.answer_callback_query(query_id)
                        .text("Invalid item")
                        .await?;
                    return Ok(());
                };
                if moving.is_some() {
                    bot.answer_callback_query(query_id)
                        .text("Finish selecting an item first")
                        .await?;
                    return Ok(());
                }
                let items = {
                    let mut store = store.lock().await;
                    store.items(chat_id).await?
                };
                if !items.iter().any(|item| item.id == id && !item.checked) {
                    bot.answer_callback_query(query_id)
                        .text("Item no longer exists")
                        .await?;
                    return Ok(());
                }
                bot.answer_callback_query(query_id)
                    .text("Select the item to move it below")
                    .await?;
                bot.edit_message_text(chat_id, message_id, "Select the item to move below")
                    .reply_markup(reorder_target_keyboard(&items, id))
                    .await?;
                sessions.lock().await.insert(
                    chat_id.0,
                    Session::Reorder {
                        message_id,
                        moving: Some(id),
                        generation,
                    },
                );
                tokio::spawn(crate::expire_session(
                    bot.clone(),
                    sessions.clone(),
                    chat_id,
                    generation,
                    crate::REORDER_TIMEOUT,
                ));
                return Ok(());
            }
            value if value.starts_with("place:") => {
                let Some(id) = value.strip_prefix("place:").and_then(|id| id.parse().ok()) else {
                    bot.answer_callback_query(query_id)
                        .text("Invalid item")
                        .await?;
                    return Ok(());
                };
                let Some(moving_id) = moving else {
                    bot.answer_callback_query(query_id)
                        .text("Select an item to move first")
                        .await?;
                    return Ok(());
                };
                let movement = {
                    let mut store = store.lock().await;
                    store.move_item_after(chat_id, moving_id, id).await?
                };
                if movement.is_none() {
                    bot.answer_callback_query(query_id)
                        .text("Item no longer exists")
                        .await?;
                } else {
                    bot.answer_callback_query(query_id).await?;
                }
                let items = {
                    let mut store = store.lock().await;
                    store.items(chat_id).await?
                };
                bot.edit_message_text(chat_id, message_id, "Move items up or down")
                    .reply_markup(reorder_keyboard(&items))
                    .await?;
                sessions.lock().await.insert(
                    chat_id.0,
                    Session::Reorder {
                        message_id,
                        moving: None,
                        generation,
                    },
                );
                tokio::spawn(crate::expire_session(
                    bot.clone(),
                    sessions.clone(),
                    chat_id,
                    generation,
                    crate::REORDER_TIMEOUT,
                ));
                return Ok(());
            }
            value => {
                if moving.is_some() {
                    bot.answer_callback_query(query_id)
                        .text("Select an item from the list")
                        .await?;
                    return Ok(());
                }
                let Some((direction, id)) = value.split_once(':') else {
                    bot.answer_callback_query(query_id)
                        .text("Invalid item")
                        .await?;
                    return Ok(());
                };
                let up = match direction {
                    "up" => true,
                    "down" => false,
                    _ => {
                        bot.answer_callback_query(query_id)
                            .text("Invalid item")
                            .await?;
                        return Ok(());
                    }
                };
                let Ok(id) = id.parse::<u64>() else {
                    bot.answer_callback_query(query_id)
                        .text("Invalid item")
                        .await?;
                    return Ok(());
                };
                (up, id)
            }
        };

        let movement = {
            let mut store = store.lock().await;
            store.move_item(chat_id, item_id, up).await?
        };

        match movement {
            None => {
                bot.answer_callback_query(query_id)
                    .text("Item no longer exists")
                    .await?;
                return Ok(());
            }
            Some(false) => {
                bot.answer_callback_query(query_id)
                    .text(if up {
                        "Item is already at the top"
                    } else {
                        "Item is already at the bottom"
                    })
                    .await?;
                return Ok(());
            }
            Some(true) => {}
        }

        let items = {
            let mut store = store.lock().await;
            store.items(chat_id).await?
        };
        bot.answer_callback_query(query_id).await?;
        bot.edit_message_reply_markup(chat_id, message_id)
            .reply_markup(reorder_keyboard(&items))
            .await?;

        tokio::spawn(crate::expire_session(
            bot.clone(),
            sessions.clone(),
            chat_id,
            generation,
            crate::REORDER_TIMEOUT,
        ));
        return Ok(());
    }

    let selected_id: u64 = match data.parse() {
        Ok(id) => id,
        Err(_) => {
            bot.answer_callback_query(query_id)
                .text("Invalid item")
                .await?;

            return Ok(());
        }
    };

    let session = sessions.lock().await.get(&chat_id.0).cloned();

    match session {
        Some(Session::Shop {
            message_id,
            generation,
        }) => {
            let result = {
                let mut store = store.lock().await;
                store.check_item(chat_id, selected_id).await?
            };

            let Some(item) = result else {
                bot.answer_callback_query(query_id)
                    .text("Item no longer exists")
                    .await?;

                return Ok(());
            };

            bot.answer_callback_query(query_id)
                .text(format!("Ticked off {}", item.item))
                .await?;

            let (new_keyboard, remaining) = {
                let mut store = store.lock().await;
                let items = store.items(chat_id).await?;
                let remaining = items.iter().filter(|item| !item.checked).count();
                (keyboard(&items), remaining)
            };

            if remaining > 0 {
                bot.edit_message_reply_markup(chat_id, message_id)
                    .reply_markup(new_keyboard.unwrap())
                    .await?;
            } else {
                let checked = {
                    let mut store = store.lock().await;
                    store
                        .items(chat_id)
                        .await?
                        .iter()
                        .filter(|item| item.checked)
                        .map(|item| format!("- {}", item.item))
                        .collect::<Vec<_>>()
                };

                let text = format!("Shopping list done\n\n{}", checked.join("\n"));

                {
                    let mut store = store.lock().await;
                    store.remove_checked(chat_id).await?;
                }

                bot.edit_message_text(chat_id, message_id, text).await?;

                sessions.lock().await.remove(&chat_id.0);
            }

            tokio::spawn(crate::expire_session(
                bot.clone(),
                sessions.clone(),
                chat_id,
                generation,
                crate::SHOP_TIMEOUT,
            ));
        }

        Some(Session::Swap {
            message_id,
            first,
            generation,
        }) => match first {
            None => {
                bot.answer_callback_query(query_id)
                    .text(format!("Select {selected_id}"))
                    .await?;

                let new_keyboard = {
                    let mut store = store.lock().await;
                    let items = store.items(chat_id).await?;
                    let rows = items
                        .iter()
                        .filter(|item| !item.checked && item.id != selected_id)
                        .map(|item| {
                            vec![InlineKeyboardButton::callback(
                                item.item.clone(),
                                item.id.to_string(),
                            )]
                        })
                        .collect::<Vec<_>>();

                    InlineKeyboardMarkup::new(rows)
                };

                bot.edit_message_reply_markup(chat_id, message_id)
                    .reply_markup(new_keyboard)
                    .await?;

                sessions.lock().await.insert(
                    chat_id.0,
                    Session::Swap {
                        message_id,
                        first: Some(selected_id),
                        generation,
                    },
                );
            }

            Some(first_id) => {
                if first_id == selected_id {
                    bot.answer_callback_query(query_id)
                        .text("Abort swap command")
                        .await?;

                    return Ok(());
                }

                bot.answer_callback_query(query_id.clone())
                    .text(format!("Swap {first_id} and {selected_id}"))
                    .await?;

                let result = {
                    let mut store = store.lock().await;
                    store.swap_items(chat_id, first_id, selected_id).await
                };

                if let Err(err) = result {
                    bot.answer_callback_query(query_id)
                        .text("Could not swap items")
                        .await?;

                    return Err(err);
                }

                let new_keyboard = {
                    let mut store = store.lock().await;
                    let items = store.items(chat_id).await?;
                    keyboard(&items)
                };

                if let Some(new_keyboard) = new_keyboard {
                    bot.edit_message_reply_markup(chat_id, message_id)
                        .reply_markup(new_keyboard)
                        .await?;
                } else {
                    bot.edit_message_reply_markup(chat_id, message_id)
                        .reply_markup(InlineKeyboardMarkup::default())
                        .await?;
                }

                sessions.lock().await.insert(
                    chat_id.0,
                    Session::Swap {
                        message_id,
                        first: None,
                        generation,
                    },
                );

                tokio::spawn(crate::expire_session(
                    bot.clone(),
                    sessions.clone(),
                    chat_id,
                    generation,
                    crate::SWAP_TIMEOUT,
                ));
            }
        },

        Some(Session::QuickAdd { .. }) => {
            bot.answer_callback_query(query_id)
                .text("Please use the quick-add buttons")
                .await?;
        }

        Some(Session::Add { .. } | Session::Import { .. }) => {
            bot.answer_callback_query(query_id)
                .text("Please send the item list as a message")
                .await?;
        }

        Some(Session::Reorder { .. }) => {
            bot.answer_callback_query(query_id)
                .text("Please use the reorder arrows")
                .await?;
        }

        Some(Session::Revive { .. }) => {
            bot.answer_callback_query(query_id)
                .text("Please select a checked item")
                .await?;
        }

        Some(Session::Category { .. }) => {
            bot.answer_callback_query(query_id)
                .text("Please use the category editor buttons")
                .await?;
        }

        Some(Session::Layout { .. }) => {
            bot.answer_callback_query(query_id)
                .text("Please use the layout editor buttons")
                .await?;
        }

        Some(Session::Advanced { .. }) => {
            bot.answer_callback_query(query_id)
                .text("Please use the advanced category menu")
                .await?;
        }

        None => {
            bot.answer_callback_query(query_id)
                .text("This operation has expired")
                .await?;
        }
    }

    Ok(())
}

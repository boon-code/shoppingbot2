use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result};
use clap::Parser;
use db::{Category, CategoryBundle, Item, Layout, Store};
use log::{debug, info, warn};
use teloxide::{
    dispatching::UpdateFilterExt,
    net::Download,
    prelude::*,
    types::{
        CallbackQuery, ChatId, InlineKeyboardButton, InlineKeyboardMarkup, InputFile, Message,
        MessageId, Update,
    },
    utils::command::BotCommands,
};
use tokio::{fs, sync::Mutex, time::sleep};

mod db;

const QUICK_ADD_PAGE_SIZE: usize = 30;
const ADD_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const SHOP_TIMEOUT: Duration = Duration::from_secs(2 * 60 * 60);
const SWAP_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const REORDER_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const REVIVE_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const CATEGORY_TIMEOUT: Duration = Duration::from_secs(10 * 60);
const LAYOUT_TIMEOUT: Duration = Duration::from_secs(10 * 60);
const ADVANCED_TIMEOUT: Duration = Duration::from_secs(10 * 60);

type SharedStore = Arc<Mutex<Store>>;
type SharedSessions = Arc<Mutex<HashMap<i64, Session>>>;

static NEXT_GENERATION: AtomicU64 = AtomicU64::new(1);

#[derive(Parser, Debug)]
#[command(name = "shoppingbot")]
#[command(about = "Telegram shopping list bot")]
struct Args {
    /// Telegram bot token, or a path to a file containing the token.
    token: String,

    /// Enable debug logging.
    #[arg(long)]
    verbose: bool,

    /// Only log errors.
    #[arg(long)]
    quiet: bool,

    /// Path to the list data directory, or a legacy JSON database file to import.
    #[arg(long, default_value = "lists.json")]
    database: PathBuf,
}

#[derive(Clone, Debug, BotCommands)]
#[command(rename_rule = "lowercase", description = "Shopping List Bot")]
enum Command {
    #[command(description = "Show current shopping list")]
    List,

    #[command(description = "Add multiple items to list")]
    Multiadd,

    #[command(description = "Quickly add items from your history")]
    Quickadd,

    #[command(rename = "import", description = "Import newline-separated items")]
    Import,

    #[command(description = "Start shopping")]
    Shop,

    #[command(description = "Uncheck items on your shopping list")]
    Revive,

    #[command(description = "Cancel current operation")]
    Cancel,

    #[command(description = "Swap items on list")]
    Swap,

    #[command(description = "Reorder items on list")]
    Reorder,

    #[command(description = "Sort items by category or shop layout")]
    Categorize,

    #[command(description = "Remove checked items from list")]
    Cleanup,

    #[command(description = "Edit item categories")]
    Category,

    #[command(description = "Import, export, or replace categories")]
    Advanced,

    #[command(description = "Show help text")]
    Help,

    #[command(rename = "cmd", description = "List all supported commands")]
    Cmd,
}

#[derive(Clone, Debug)]
enum Session {
    Add {
        count: usize,
        generation: u64,
    },

    QuickAdd {
        message_id: MessageId,
        generation: u64,
        revision: u64,
        offset: usize,
        candidates: Vec<String>,
        has_more: bool,
        mode: QuickAddMode,
    },

    Import {
        generation: u64,
    },

    Shop {
        message_id: MessageId,
        generation: u64,
    },

    Swap {
        message_id: MessageId,
        first: Option<u64>,
        generation: u64,
    },

    Reorder {
        message_id: MessageId,
        moving: Option<u64>,
        generation: u64,
    },

    Revive {
        message_id: MessageId,
        generation: u64,
    },

    Category {
        message_id: MessageId,
        generation: u64,
        mode: CategoryMode,
    },

    Layout {
        message_id: MessageId,
        generation: u64,
        mode: LayoutMode,
    },

    Advanced {
        message_id: MessageId,
        generation: u64,
        mode: AdvancedMode,
    },
}

#[derive(Clone, Debug)]
enum QuickAddMode {
    Select,
    Prefix { item: String },
}

#[derive(Clone, Debug)]
enum AdvancedMode {
    Select,
    ImportMerge,
    ImportReplace,
    ImportHistory,
    ExportPlain,
    ConfirmReplace(CategoryBundle),
}

#[derive(serde::Deserialize)]
struct HistoryCategoriesDocument {
    categories: std::collections::BTreeMap<String, Vec<String>>,
}

#[derive(Clone, Debug)]
enum LayoutMode {
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

#[derive(Clone, Debug)]
enum CategoryMode {
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

impl Session {
    fn generation(&self) -> u64 {
        match self {
            Session::Add { generation, .. }
            | Session::QuickAdd { generation, .. }
            | Session::Import { generation }
            | Session::Shop { generation, .. }
            | Session::Swap { generation, .. }
            | Session::Reorder { generation, .. }
            | Session::Revive { generation, .. }
            | Session::Category { generation, .. }
            | Session::Layout { generation, .. }
            | Session::Advanced { generation, .. } => *generation,
        }
    }

    fn message_id(&self) -> Option<MessageId> {
        match self {
            Session::Shop { message_id, .. }
            | Session::QuickAdd { message_id, .. }
            | Session::Swap { message_id, .. }
            | Session::Reorder { message_id, .. }
            | Session::Revive { message_id, .. }
            | Session::Category { message_id, .. }
            | Session::Layout { message_id, .. }
            | Session::Advanced { message_id, .. } => Some(*message_id),
            Session::Add { .. } | Session::Import { .. } => None,
        }
    }
}

fn checklist_text(items: &[Item]) -> String {
    if items.is_empty() {
        return "Your shopping list is empty 😃".to_string();
    }

    let lines = items
        .iter()
        .map(|item| {
            if item.checked {
                format!(" - [x] {}", item.item)
            } else {
                format!(" - [ ] {}", item.item)
            }
        })
        .collect::<Vec<_>>();

    format!("Your shopping list:\n\n{}", lines.join("\n"))
}

fn plain_list_text(items: &[Item], include_checked: bool) -> String {
    items
        .iter()
        .filter(|item| include_checked || !item.checked)
        .map(|item| {
            if item.checked {
                format!("- [x] {}", item.item)
            } else {
                format!("- [ ] {}", item.item)
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn keyboard(items: &[Item]) -> Option<InlineKeyboardMarkup> {
    let rows = items
        .iter()
        .filter(|item| !item.checked)
        .map(|item| {
            vec![InlineKeyboardButton::callback(
                item.item.clone(),
                item.id.to_string(),
            )]
        })
        .collect::<Vec<_>>();

    if rows.is_empty() {
        None
    } else {
        Some(InlineKeyboardMarkup::new(rows))
    }
}

fn revive_keyboard(items: &[Item]) -> Option<InlineKeyboardMarkup> {
    let mut rows = items
        .iter()
        .filter(|item| item.checked)
        .map(|item| {
            vec![InlineKeyboardButton::callback(
                item.item.clone(),
                format!("revive:{}", item.id),
            )]
        })
        .collect::<Vec<_>>();

    if rows.is_empty() {
        return None;
    }
    rows.push(vec![InlineKeyboardButton::callback("DONE", "revive:done")]);
    Some(InlineKeyboardMarkup::new(rows))
}

fn quick_add_keyboard(
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

fn categories_keyboard(categories: &[Category]) -> InlineKeyboardMarkup {
    let mut rows = categories
        .iter()
        .enumerate()
        .map(|(index, category)| {
            vec![
                InlineKeyboardButton::callback(
                    format!("{} ({})", category.name, category.items.len()),
                    format!("category:open:{index}"),
                ),
                InlineKeyboardButton::callback(
                    format!("⭐ {}", category.priority),
                    format!("category:priority:{index}"),
                ),
                InlineKeyboardButton::callback("✏️ Edit", format!("category:manage:{index}")),
            ]
        })
        .collect::<Vec<_>>();
    rows.push(vec![InlineKeyboardButton::callback(
        "Create category",
        "category:create",
    )]);
    rows.push(vec![InlineKeyboardButton::callback(
        "Done",
        "category:done",
    )]);
    InlineKeyboardMarkup::new(rows)
}

fn categories_text(categories: &[Category]) -> String {
    if categories.is_empty() {
        "No categories yet. Create one to get started.".to_string()
    } else {
        format!(
            "Categories:\n{}",
            categories
                .iter()
                .map(|category| format!("• {} ({} items)", category.name, category.items.len()))
                .collect::<Vec<_>>()
                .join("\n")
        )
    }
}

fn layouts_text(layouts: &[Layout]) -> String {
    if layouts.is_empty() {
        return "No shop layouts are defined.".to_string();
    }
    format!(
        "Choose a layout to sort your list, or edit one:\n{}",
        layouts
            .iter()
            .map(|layout| format!("• {}", layout.name))
            .collect::<Vec<_>>()
            .join("\n")
    )
}

fn layouts_keyboard(layouts: &[Layout]) -> InlineKeyboardMarkup {
    let mut rows = layouts
        .iter()
        .enumerate()
        .map(|(index, layout)| {
            vec![
                InlineKeyboardButton::callback(
                    layout.name.clone(),
                    format!("layout:select:{index}"),
                ),
                InlineKeyboardButton::callback("✏️", format!("layout:edit:{index}")),
            ]
        })
        .collect::<Vec<_>>();
    rows.push(vec![InlineKeyboardButton::callback(
        "Sort by category priority",
        "layout:priority",
    )]);
    rows.push(vec![InlineKeyboardButton::callback(
        "Create layout",
        "layout:create",
    )]);
    rows.push(vec![InlineKeyboardButton::callback("Back", "layout:back")]);
    InlineKeyboardMarkup::new(rows)
}

fn advanced_keyboard() -> InlineKeyboardMarkup {
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

fn layout_management_keyboard() -> InlineKeyboardMarkup {
    InlineKeyboardMarkup::new(vec![
        vec![InlineKeyboardButton::callback("Rename", "layout:rename")],
        vec![InlineKeyboardButton::callback(
            "Re-arrange",
            "layout:arrange",
        )],
        vec![InlineKeyboardButton::callback("Delete", "layout:delete")],
        vec![InlineKeyboardButton::callback("Back", "layout:back")],
    ])
}

fn layout_confirmation_keyboard() -> InlineKeyboardMarkup {
    InlineKeyboardMarkup::new(vec![vec![
        InlineKeyboardButton::callback("Delete", "layout:confirm-delete"),
        InlineKeyboardButton::callback("Cancel", "layout:cancel-delete"),
    ]])
}

fn layout_arrange_text(name: &str, categories: &[String], moving: Option<usize>) -> String {
    let instruction = if moving.is_some() {
        "Choose the category to place it after:"
    } else {
        "Choose a category to move, then choose the category to place it after:"
    };
    let order = categories
        .iter()
        .enumerate()
        .map(|(index, category)| format!("{}. {category}", index + 1))
        .collect::<Vec<_>>()
        .join("\n");
    format!("Arrange layout {name}\n\n{instruction}\n\n{order}")
}

fn layout_arrange_keyboard(categories: &[String], moving: Option<usize>) -> InlineKeyboardMarkup {
    let mut rows = categories
        .iter()
        .enumerate()
        .filter(|(index, _)| moving != Some(*index))
        .map(|(index, category)| {
            vec![InlineKeyboardButton::callback(
                category.clone(),
                if moving.is_some() {
                    format!("layout:place:{index}")
                } else {
                    format!("layout:move:{index}")
                },
            )]
        })
        .collect::<Vec<_>>();
    rows.push(vec![InlineKeyboardButton::callback("Done", "layout:done")]);
    if moving.is_some() {
        rows.push(vec![InlineKeyboardButton::callback(
            "Back",
            "layout:arrange-back",
        )]);
    }
    InlineKeyboardMarkup::new(rows)
}

fn category_editor_text(category: &Category) -> String {
    if category.items.is_empty() {
        return format!("Category: {}\n\nNo items yet.", category.name);
    }
    format!(
        "Category: {}\n\n{}",
        category.name,
        category
            .items
            .iter()
            .map(|item| format!("• {item}"))
            .collect::<Vec<_>>()
            .join("\n")
    )
}

fn category_editor_keyboard(category: &Category) -> InlineKeyboardMarkup {
    let mut rows = category
        .items
        .iter()
        .enumerate()
        .map(|(index, item)| {
            vec![InlineKeyboardButton::callback(
                format!("🗑 {item}"),
                format!("category:remove:{index}"),
            )]
        })
        .collect::<Vec<_>>();
    rows.push(vec![
        InlineKeyboardButton::callback("Add", "category:add"),
        InlineKeyboardButton::callback("Done", "category:done"),
    ]);
    InlineKeyboardMarkup::new(rows)
}

fn category_confirmation_keyboard() -> InlineKeyboardMarkup {
    InlineKeyboardMarkup::new(vec![vec![
        InlineKeyboardButton::callback("Delete", "category:confirm-delete"),
        InlineKeyboardButton::callback("Cancel", "category:cancel-delete"),
    ]])
}

fn category_management_keyboard() -> InlineKeyboardMarkup {
    InlineKeyboardMarkup::new(vec![
        vec![InlineKeyboardButton::callback("Rename", "category:rename")],
        vec![InlineKeyboardButton::callback("Delete", "category:delete")],
        vec![InlineKeyboardButton::callback("Back", "category:back")],
    ])
}

fn category_candidates_text(category: &str, candidates: &[(String, u64)]) -> String {
    if candidates.is_empty() {
        format!("Category: {category}\n\nNo unassigned history items to add.")
    } else {
        format!("Choose an item to add to {category}:")
    }
}

fn category_candidates_keyboard(candidates: &[(String, u64)]) -> InlineKeyboardMarkup {
    let mut rows = candidates
        .iter()
        .enumerate()
        .map(|(index, (item, count))| {
            vec![InlineKeyboardButton::callback(
                format!("{item} ({count})"),
                format!("category:select:{index}"),
            )]
        })
        .collect::<Vec<_>>();
    rows.push(vec![InlineKeyboardButton::callback(
        "Back",
        "category:back",
    )]);
    InlineKeyboardMarkup::new(rows)
}

fn reorder_keyboard(items: &[Item]) -> InlineKeyboardMarkup {
    let items = items
        .iter()
        .filter(|item| !item.checked)
        .collect::<Vec<_>>();
    let mut rows = items
        .iter()
        .enumerate()
        .map(|(index, item)| {
            let up = if index == 0 {
                InlineKeyboardButton::callback("⬆️", "reorder:boundary")
            } else {
                InlineKeyboardButton::callback("⬆️", format!("reorder:up:{}", item.id))
            };
            let down = if index + 1 == items.len() {
                InlineKeyboardButton::callback("⬇️", "reorder:boundary")
            } else {
                InlineKeyboardButton::callback("⬇️", format!("reorder:down:{}", item.id))
            };

            vec![
                up,
                down,
                InlineKeyboardButton::callback(
                    item.item.clone(),
                    format!("reorder:select:{}", item.id),
                ),
            ]
        })
        .collect::<Vec<Vec<_>>>();

    rows.push(vec![InlineKeyboardButton::callback("Done", "reorder:done")]);

    InlineKeyboardMarkup::new(rows)
}

fn reorder_target_keyboard(items: &[Item], moving_id: u64) -> InlineKeyboardMarkup {
    let mut rows = items
        .iter()
        .filter(|item| !item.checked && item.id != moving_id)
        .map(|item| {
            vec![InlineKeyboardButton::callback(
                item.item.clone(),
                format!("reorder:place:{}", item.id),
            )]
        })
        .collect::<Vec<_>>();
    rows.push(vec![InlineKeyboardButton::callback("Back", "reorder:back")]);
    InlineKeyboardMarkup::new(rows)
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

fn rstrip_slash<'a>(s: &'a String) -> &'a str {
    match s.find('/') {
        Some(idx) => &s[(idx + 1)..],
        None => &s,
    }
}

fn parse_import_list(text: &str) -> Vec<String> {
    text.lines()
        .map(strip_list_prefix)
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect()
}

fn parse_history_categories(
    data: &[u8],
) -> Result<std::collections::BTreeMap<String, Vec<String>>> {
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

async fn get_chat_id(message: &Message) -> Option<ChatId> {
    Some(message.chat.id)
}

async fn close_session(bot: Bot, sessions: SharedSessions, chat_id: ChatId) -> Result<()> {
    let old = sessions.lock().await.remove(&chat_id.0);

    if let Some(session) = old {
        if let Some(message_id) = session.message_id() {
            if let Err(err) = bot
                .edit_message_reply_markup(chat_id, message_id)
                .reply_markup(InlineKeyboardMarkup::default())
                .await
            {
                debug!("could not remove old keyboard: {err}");
            }
        }
    }

    Ok(())
}

async fn expire_session(
    bot: Bot,
    sessions: SharedSessions,
    chat_id: ChatId,
    generation: u64,
    timeout: Duration,
) {
    sleep(timeout).await;

    let session = sessions.lock().await;

    let still_active = session
        .get(&chat_id.0)
        .map(|s| s.generation() == generation)
        .unwrap_or(false);

    drop(session);

    if !still_active {
        return;
    }

    info!("session expired for chat {}", chat_id.0);

    let old = sessions.lock().await.remove(&chat_id.0);

    if let Some(session) = old {
        if let Some(message_id) = session.message_id() {
            let _ = bot
                .edit_message_reply_markup(chat_id, message_id)
                .reply_markup(InlineKeyboardMarkup::default())
                .await;
        }
    }
}

async fn command_handler(
    bot: Bot,
    msg: Message,
    cmd: Command,
    store: SharedStore,
    sessions: SharedSessions,
) -> Result<()> {
    let chat_id = get_chat_id(&msg).await.context("message has no chat")?;

    // Every command cancels the current dialog, matching the Python implementation.
    close_session(bot.clone(), sessions.clone(), chat_id).await?;

    match cmd {
        Command::List => {
            let items = store.lock().await.items(chat_id).await?;

            bot.send_message(chat_id, checklist_text(&items)).await?;
        }

        Command::Multiadd => {
            let generation = generation();

            {
                let mut sessions = sessions.lock().await;
                sessions.insert(
                    chat_id.0,
                    Session::Add {
                        count: 0,
                        generation,
                    },
                );
            }

            bot.send_message(chat_id, "Please name items to put on the list:")
                .await?;

            tokio::spawn(expire_session(
                bot.clone(),
                sessions.clone(),
                chat_id,
                generation,
                ADD_TIMEOUT,
            ));
        }

        Command::Quickadd => {
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
            tokio::spawn(expire_session(
                bot.clone(),
                sessions.clone(),
                chat_id,
                dialog_generation,
                ADD_TIMEOUT,
            ));
        }

        Command::Import => {
            let generation = generation();

            sessions
                .lock()
                .await
                .insert(chat_id.0, Session::Import { generation });

            bot.send_message(chat_id, "Send the items to import, one item per line:")
                .await?;

            tokio::spawn(expire_session(
                bot.clone(),
                sessions.clone(),
                chat_id,
                generation,
                ADD_TIMEOUT,
            ));
        }

        Command::Shop => {
            let keyboard = {
                let mut store = store.lock().await;
                let items = store.items(chat_id).await?;
                keyboard(&items)
            };

            let Some(keyboard) = keyboard else {
                bot.send_message(chat_id, "Your shopping list is already empty")
                    .await?;

                return Ok(());
            };

            let sent = bot
                .send_message(chat_id, "Your list")
                .reply_markup(keyboard)
                .await?;

            let generation = generation();

            {
                let mut sessions = sessions.lock().await;
                sessions.insert(
                    chat_id.0,
                    Session::Shop {
                        message_id: sent.id,
                        generation,
                    },
                );
            }

            tokio::spawn(expire_session(
                bot.clone(),
                sessions.clone(),
                chat_id,
                generation,
                SHOP_TIMEOUT,
            ));
        }

        Command::Revive => {
            let items = {
                let mut store = store.lock().await;
                store.items(chat_id).await?
            };
            let Some(revive_keyboard) = revive_keyboard(&items) else {
                bot.send_message(chat_id, "There are no checked items to revive")
                    .await?;
                return Ok(());
            };

            let sent = bot
                .send_message(chat_id, "Select checked items to uncheck")
                .reply_markup(revive_keyboard)
                .await?;
            let generation = generation();
            sessions.lock().await.insert(
                chat_id.0,
                Session::Revive {
                    message_id: sent.id,
                    generation,
                },
            );
            tokio::spawn(expire_session(
                bot.clone(),
                sessions.clone(),
                chat_id,
                generation,
                REVIVE_TIMEOUT,
            ));
        }

        Command::Swap => {
            let keyboard = {
                let mut store = store.lock().await;
                let items = store.items(chat_id).await?;
                keyboard(&items)
            };

            let Some(keyboard) = keyboard else {
                bot.send_message(chat_id, "Your shopping list is already empty")
                    .await?;

                return Ok(());
            };

            let sent = bot
                .send_message(chat_id, "Your list to swap")
                .reply_markup(keyboard)
                .await?;

            let generation = generation();

            {
                let mut sessions = sessions.lock().await;
                sessions.insert(
                    chat_id.0,
                    Session::Swap {
                        message_id: sent.id,
                        first: None,
                        generation,
                    },
                );
            }

            tokio::spawn(expire_session(
                bot.clone(),
                sessions.clone(),
                chat_id,
                generation,
                SWAP_TIMEOUT,
            ));
        }

        Command::Reorder => {
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

            tokio::spawn(expire_session(
                bot.clone(),
                sessions.clone(),
                chat_id,
                generation,
                REORDER_TIMEOUT,
            ));
        }

        Command::Categorize => {
            let layouts = store.lock().await.layouts(chat_id)?;
            let sent = bot
                .send_message(chat_id, layouts_text(&layouts))
                .reply_markup(layouts_keyboard(&layouts))
                .await?;
            let generation = generation();
            sessions.lock().await.insert(
                chat_id.0,
                Session::Layout {
                    message_id: sent.id,
                    generation,
                    mode: LayoutMode::Select,
                },
            );
            tokio::spawn(expire_session(
                bot.clone(),
                sessions.clone(),
                chat_id,
                generation,
                LAYOUT_TIMEOUT,
            ));
        }

        Command::Cancel => {
            // Already closed above.
        }

        Command::Cleanup => {
            let mut store = store.lock().await;
            store.remove_checked(chat_id).await?;

            bot.send_message(chat_id, "Cleaned up your shopping list")
                .await?;
        }

        Command::Category => {
            let categories = store.lock().await.categories(chat_id)?;
            let sent = bot
                .send_message(chat_id, categories_text(&categories))
                .reply_markup(categories_keyboard(&categories))
                .await?;
            let generation = generation();
            sessions.lock().await.insert(
                chat_id.0,
                Session::Category {
                    message_id: sent.id,
                    generation,
                    mode: CategoryMode::Choose,
                },
            );
            tokio::spawn(expire_session(
                bot.clone(),
                sessions.clone(),
                chat_id,
                generation,
                CATEGORY_TIMEOUT,
            ));
        }

        Command::Advanced => {
            let sent = bot
                .send_message(chat_id, "Advanced category tools:")
                .reply_markup(advanced_keyboard())
                .await?;
            let generation = generation();
            sessions.lock().await.insert(
                chat_id.0,
                Session::Advanced {
                    message_id: sent.id,
                    generation,
                    mode: AdvancedMode::Select,
                },
            );
            tokio::spawn(expire_session(
                bot.clone(),
                sessions.clone(),
                chat_id,
                generation,
                ADVANCED_TIMEOUT,
            ));
        }

        Command::Help => {
            bot.send_message(chat_id, Command::descriptions().to_string())
                .await?;
        }

        Command::Cmd => {
            let cmds: Vec<_> = Command::bot_commands()
                .iter()
                .map(|x| format!("{} - {}", rstrip_slash(&x.command), x.description))
                .collect();
            bot.send_message(chat_id, cmds.join("\n")).await?;
        }
    }

    Ok(())
}

async fn text_handler(
    bot: Bot,
    msg: Message,
    store: SharedStore,
    sessions: SharedSessions,
) -> Result<()> {
    let chat_id = msg.chat.id;

    if let Some(document) = msg.document() {
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

        let bundle: CategoryBundle = match serde_json::from_slice(&contents) {
            Ok(bundle) => bundle,
            Err(error) => {
                bot.send_message(chat_id, format!("Invalid categories JSON: {error}"))
                    .await?;
                return Ok(());
            }
        };
        if let Err(error) = db::validate_category_bundle(&bundle) {
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
                    &bot,
                    &sessions,
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
        return Ok(());
    }

    let Some(text) = msg.text() else {
        bot.send_message(chat_id, "Unsupported content type")
            .await?;

        return Ok(());
    };

    let session = sessions.lock().await.get(&chat_id.0).cloned();

    match session {
        Some(Session::QuickAdd {
            message_id,
            generation,
            offset,
            mode: QuickAddMode::Prefix { item },
            ..
        }) => {
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
            refresh_quick_add_view(
                &bot,
                &store,
                &sessions,
                chat_id,
                message_id,
                generation,
                offset,
                format!("Added {prefix} {item}. Choose another item:"),
            )
            .await?;
        }

        Some(Session::Add { count, generation }) => {
            {
                let mut store = store.lock().await;
                store.add_item(chat_id, text.to_string()).await?;
            }

            {
                let mut sessions = sessions.lock().await;

                sessions.insert(
                    chat_id.0,
                    Session::Add {
                        count: count + 1,
                        generation,
                    },
                );
            }

            bot.send_message(chat_id, format!("Added item {text}"))
                .await?;
        }

        Some(Session::Import { .. }) => {
            let items = parse_import_list(text);

            if items.is_empty() {
                bot.send_message(chat_id, "No items found. Send one item per line:")
                    .await?;
                return Ok(());
            }

            let count = items.len();
            {
                let mut store = store.lock().await;
                store.add_items(chat_id, items).await?;
            }
            sessions.lock().await.remove(&chat_id.0);

            bot.send_message(chat_id, format!("Imported {count} items"))
                .await?;
        }

        Some(Session::Category {
            message_id,
            generation,
            mode: CategoryMode::CreateName,
        }) => {
            let name = text.trim();
            if name.is_empty() {
                bot.send_message(chat_id, "Category names cannot be empty. Send a name:")
                    .await?;
                return Ok(());
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
                return Ok(());
            };
            bot.edit_message_text(chat_id, message_id, category_editor_text(&category))
                .reply_markup(category_editor_keyboard(&category))
                .await?;
            sessions.lock().await.insert(
                chat_id.0,
                Session::Category {
                    message_id,
                    generation,
                    mode: CategoryMode::Edit(category.name),
                },
            );
            tokio::spawn(expire_session(
                bot.clone(),
                sessions.clone(),
                chat_id,
                generation,
                CATEGORY_TIMEOUT,
            ));
        }

        Some(Session::Category {
            message_id,
            generation,
            mode: CategoryMode::Rename(category_name),
        }) => {
            let name = text.trim();
            if name.is_empty() {
                bot.send_message(chat_id, "Category names cannot be empty. Send a name:")
                    .await?;
                return Ok(());
            }
            let duplicate = {
                let store_guard = store.lock().await;
                if store_guard.categories(chat_id)?.iter().any(|category| {
                    category.name != category_name && category.name.eq_ignore_ascii_case(name)
                }) {
                    true
                } else {
                    store_guard.rename_category(chat_id, &category_name, name)?;
                    false
                }
            };
            if duplicate {
                bot.send_message(chat_id, "That category already exists. Send another name:")
                    .await?;
                return Ok(());
            }
            show_categories(&bot, &store, &sessions, chat_id, message_id, generation).await?;
        }

        Some(Session::Category {
            message_id,
            generation,
            mode: CategoryMode::SetPriority(category_name),
        }) => {
            let Ok(priority) = text.trim().parse::<i32>() else {
                bot.send_message(chat_id, "Please send a whole number for the priority:")
                    .await?;
                return Ok(());
            };
            store
                .lock()
                .await
                .set_category_priority(chat_id, &category_name, priority)?;
            show_categories(&bot, &store, &sessions, chat_id, message_id, generation).await?;
        }

        Some(Session::Layout {
            message_id,
            generation,
            mode: LayoutMode::CreateName,
        }) => {
            let name = text.trim();
            if name.is_empty() {
                bot.send_message(chat_id, "Layout names cannot be empty. Send a name:")
                    .await?;
                return Ok(());
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
                return Ok(());
            };
            set_layout_view(
                &bot,
                &sessions,
                chat_id,
                message_id,
                generation,
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

        Some(Session::Layout {
            message_id,
            generation,
            mode: LayoutMode::Rename { name },
        }) => {
            let new_name = text.trim();
            if new_name.is_empty() {
                bot.send_message(chat_id, "Layout names cannot be empty. Send a name:")
                    .await?;
                return Ok(());
            }
            let duplicate =
                {
                    let store = store.lock().await;
                    if store.layouts(chat_id)?.iter().any(|layout| {
                        layout.name != name && layout.name.eq_ignore_ascii_case(new_name)
                    }) {
                        true
                    } else {
                        store.rename_layout(chat_id, &name, new_name)?;
                        false
                    }
                };
            if duplicate {
                bot.send_message(chat_id, "That layout already exists. Send another name:")
                    .await?;
                return Ok(());
            }
            show_layout_manage(
                &bot, &store, &sessions, chat_id, message_id, generation, new_name,
            )
            .await?;
        }

        Some(Session::Category { .. }) => {
            warn!(
                "ignoring text message while category editor is active: {}",
                text
            );
        }

        Some(Session::Layout { .. }) => {
            warn!(
                "ignoring text message while layout editor is active: {}",
                text
            );
        }

        Some(Session::Advanced { .. }) => {
            bot.send_message(
                chat_id,
                "Please upload the categories JSON file as a document.",
            )
            .await?;
        }

        Some(_) => {
            warn!(
                "ignoring text message while callback-based dialog is active: {}",
                text
            );
        }

        None => {
            debug!("ignoring message without active dialog");
        }
    }

    Ok(())
}

async fn set_category_view(
    bot: &Bot,
    sessions: &SharedSessions,
    chat_id: ChatId,
    message_id: MessageId,
    generation: u64,
    mode: CategoryMode,
    text: String,
    keyboard: InlineKeyboardMarkup,
) -> Result<()> {
    bot.edit_message_text(chat_id, message_id, text)
        .reply_markup(keyboard)
        .await?;
    sessions.lock().await.insert(
        chat_id.0,
        Session::Category {
            message_id,
            generation,
            mode,
        },
    );
    tokio::spawn(expire_session(
        bot.clone(),
        sessions.clone(),
        chat_id,
        generation,
        CATEGORY_TIMEOUT,
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
    set_category_view(
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
    set_category_view(
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

async fn set_layout_view(
    bot: &Bot,
    sessions: &SharedSessions,
    chat_id: ChatId,
    message_id: MessageId,
    generation: u64,
    mode: LayoutMode,
    text: String,
    keyboard: InlineKeyboardMarkup,
) -> Result<()> {
    bot.edit_message_text(chat_id, message_id, text)
        .reply_markup(keyboard)
        .await?;
    sessions.lock().await.insert(
        chat_id.0,
        Session::Layout {
            message_id,
            generation,
            mode,
        },
    );
    tokio::spawn(expire_session(
        bot.clone(),
        sessions.clone(),
        chat_id,
        generation,
        LAYOUT_TIMEOUT,
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
    set_layout_view(
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
    set_layout_view(
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
    tokio::spawn(expire_session(
        bot.clone(),
        sessions.clone(),
        chat_id,
        generation,
        ADVANCED_TIMEOUT,
    ));
    Ok(())
}

async fn advanced_callback(
    bot: Bot,
    query_id: teloxide::types::CallbackQueryId,
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
                InputFile::memory(data).file_name(db::list_export_file_name(chat_id)),
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

async fn layout_callback(
    bot: Bot,
    query_id: teloxide::types::CallbackQueryId,
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
                    set_layout_view(
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
            set_layout_view(
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
            set_layout_view(
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
            set_layout_view(
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
            set_layout_view(
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
            set_layout_view(
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
            set_layout_view(
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
            set_layout_view(
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

async fn category_callback(
    bot: Bot,
    query_id: teloxide::types::CallbackQueryId,
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
            set_category_view(
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
            set_category_view(
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
            set_category_view(
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
            set_category_view(
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
            set_category_view(
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
            set_category_view(
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
            set_category_view(
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
                    set_category_view(
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
            set_category_view(
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

async fn refresh_quick_add_view(
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

async fn quick_add_callback(
    bot: Bot,
    query_id: teloxide::types::CallbackQueryId,
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
        refresh_quick_add_view(
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
        refresh_quick_add_view(
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
            refresh_quick_add_view(
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

async fn callback_handler(
    bot: Bot,
    query: CallbackQuery,
    store: SharedStore,
    sessions: SharedSessions,
) -> Result<()> {
    let Some(message) = query.message.as_ref() else {
        return Ok(());
    };

    let chat_id = message.chat().id;

    let Some(data) = query.data.as_deref() else {
        return Ok(());
    };

    if data.starts_with("quickadd:") {
        return quick_add_callback(bot, query.id, data, chat_id, message.id(), store, sessions)
            .await;
    }

    if data.starts_with("category:") {
        return category_callback(bot, query.id, data, chat_id, message.id(), store, sessions)
            .await;
    }

    if data.starts_with("layout:") {
        return layout_callback(bot, query.id, data, chat_id, message.id(), store, sessions).await;
    }

    if data.starts_with("advanced:") {
        return advanced_callback(bot, query.id, data, chat_id, message.id(), store, sessions)
            .await;
    }

    if let Some(action) = data.strip_prefix("revive:") {
        let session = sessions.lock().await.get(&chat_id.0).cloned();
        let Some(Session::Revive {
            message_id,
            generation,
        }) = session
        else {
            bot.answer_callback_query(query.id)
                .text("This operation has expired")
                .await?;
            return Ok(());
        };
        if message.id() != message_id {
            bot.answer_callback_query(query.id)
                .text("This operation has expired")
                .await?;
            return Ok(());
        }

        if action == "done" {
            bot.answer_callback_query(query.id)
                .text("Reviving finished")
                .await?;
            bot.edit_message_reply_markup(chat_id, message_id)
                .reply_markup(InlineKeyboardMarkup::default())
                .await?;
            sessions.lock().await.remove(&chat_id.0);
            return Ok(());
        }

        let Ok(selected_id) = action.parse::<u64>() else {
            bot.answer_callback_query(query.id)
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
                bot.answer_callback_query(query.id)
                    .text(format!("Unchecked {}", item.item))
                    .await?;
            }
            None => {
                bot.answer_callback_query(query.id)
                    .text("Item is no longer checked")
                    .await?;
            }
        }

        let items = {
            let mut store = store.lock().await;
            store.items(chat_id).await?
        };
        if let Some(revive_keyboard) = revive_keyboard(&items) {
            bot.edit_message_reply_markup(chat_id, message_id)
                .reply_markup(revive_keyboard)
                .await?;
            tokio::spawn(expire_session(
                bot.clone(),
                sessions.clone(),
                chat_id,
                generation,
                REVIVE_TIMEOUT,
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
            bot.answer_callback_query(query.id)
                .text("This operation has expired")
                .await?;
            return Ok(());
        };

        if message.id() != message_id {
            bot.answer_callback_query(query.id)
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
                bot.answer_callback_query(query.id)
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
                    bot.answer_callback_query(query.id).await?;
                    return Ok(());
                }
                let items = {
                    let mut store = store.lock().await;
                    store.items(chat_id).await?
                };
                bot.answer_callback_query(query.id).await?;
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
                tokio::spawn(expire_session(
                    bot.clone(),
                    sessions.clone(),
                    chat_id,
                    generation,
                    REORDER_TIMEOUT,
                ));
                return Ok(());
            }
            "boundary" => {
                bot.answer_callback_query(query.id).await?;
                return Ok(());
            }
            value if value.starts_with("select:") => {
                let Some(id) = value.strip_prefix("select:").and_then(|id| id.parse().ok()) else {
                    bot.answer_callback_query(query.id)
                        .text("Invalid item")
                        .await?;
                    return Ok(());
                };
                if moving.is_some() {
                    bot.answer_callback_query(query.id)
                        .text("Finish selecting an item first")
                        .await?;
                    return Ok(());
                }
                let items = {
                    let mut store = store.lock().await;
                    store.items(chat_id).await?
                };
                if !items.iter().any(|item| item.id == id && !item.checked) {
                    bot.answer_callback_query(query.id)
                        .text("Item no longer exists")
                        .await?;
                    return Ok(());
                }
                bot.answer_callback_query(query.id)
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
                tokio::spawn(expire_session(
                    bot.clone(),
                    sessions.clone(),
                    chat_id,
                    generation,
                    REORDER_TIMEOUT,
                ));
                return Ok(());
            }
            value if value.starts_with("place:") => {
                let Some(id) = value.strip_prefix("place:").and_then(|id| id.parse().ok()) else {
                    bot.answer_callback_query(query.id)
                        .text("Invalid item")
                        .await?;
                    return Ok(());
                };
                let Some(moving_id) = moving else {
                    bot.answer_callback_query(query.id)
                        .text("Select an item to move first")
                        .await?;
                    return Ok(());
                };
                let movement = {
                    let mut store = store.lock().await;
                    store.move_item_after(chat_id, moving_id, id).await?
                };
                if movement.is_none() {
                    bot.answer_callback_query(query.id)
                        .text("Item no longer exists")
                        .await?;
                } else {
                    bot.answer_callback_query(query.id).await?;
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
                tokio::spawn(expire_session(
                    bot.clone(),
                    sessions.clone(),
                    chat_id,
                    generation,
                    REORDER_TIMEOUT,
                ));
                return Ok(());
            }
            value => {
                if moving.is_some() {
                    bot.answer_callback_query(query.id)
                        .text("Select an item from the list")
                        .await?;
                    return Ok(());
                }
                let Some((direction, id)) = value.split_once(':') else {
                    bot.answer_callback_query(query.id)
                        .text("Invalid item")
                        .await?;
                    return Ok(());
                };
                let up = match direction {
                    "up" => true,
                    "down" => false,
                    _ => {
                        bot.answer_callback_query(query.id)
                            .text("Invalid item")
                            .await?;
                        return Ok(());
                    }
                };
                let Ok(id) = id.parse::<u64>() else {
                    bot.answer_callback_query(query.id)
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
                bot.answer_callback_query(query.id)
                    .text("Item no longer exists")
                    .await?;
                return Ok(());
            }
            Some(false) => {
                bot.answer_callback_query(query.id)
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
        bot.answer_callback_query(query.id).await?;
        bot.edit_message_reply_markup(chat_id, message_id)
            .reply_markup(reorder_keyboard(&items))
            .await?;

        tokio::spawn(expire_session(
            bot.clone(),
            sessions.clone(),
            chat_id,
            generation,
            REORDER_TIMEOUT,
        ));
        return Ok(());
    }

    let selected_id: u64 = match data.parse() {
        Ok(id) => id,
        Err(_) => {
            bot.answer_callback_query(query.id)
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
                bot.answer_callback_query(query.id)
                    .text("Item no longer exists")
                    .await?;

                return Ok(());
            };

            bot.answer_callback_query(query.id)
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

            // Keep the original timeout alive while the dialog is being used.
            tokio::spawn(expire_session(
                bot.clone(),
                sessions.clone(),
                chat_id,
                generation,
                SHOP_TIMEOUT,
            ));
        }

        Some(Session::Swap {
            message_id,
            first,
            generation,
        }) => match first {
            None => {
                bot.answer_callback_query(query.id)
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

                {
                    let mut sessions = sessions.lock().await;

                    sessions.insert(
                        chat_id.0,
                        Session::Swap {
                            message_id,
                            first: Some(selected_id),
                            generation,
                        },
                    );
                }
            }

            Some(first_id) => {
                if first_id == selected_id {
                    bot.answer_callback_query(query.id)
                        .text("Abort swap command")
                        .await?;

                    return Ok(());
                }

                bot.answer_callback_query(query.id.clone())
                    .text(format!("Swap {first_id} and {selected_id}"))
                    .await?;

                let result = {
                    let mut store = store.lock().await;
                    store.swap_items(chat_id, first_id, selected_id).await
                };

                if let Err(err) = result {
                    bot.answer_callback_query(query.id)
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

                {
                    let mut sessions = sessions.lock().await;

                    sessions.insert(
                        chat_id.0,
                        Session::Swap {
                            message_id,
                            first: None,
                            generation,
                        },
                    );
                }

                tokio::spawn(expire_session(
                    bot.clone(),
                    sessions.clone(),
                    chat_id,
                    generation,
                    SWAP_TIMEOUT,
                ));
            }
        },

        Some(Session::QuickAdd { .. }) => {
            bot.answer_callback_query(query.id)
                .text("Please use the quick-add buttons")
                .await?;
        }

        Some(Session::Add { .. } | Session::Import { .. }) => {
            bot.answer_callback_query(query.id)
                .text("Please send the item list as a message")
                .await?;
        }

        Some(Session::Reorder { .. }) => {
            bot.answer_callback_query(query.id)
                .text("Please use the reorder arrows")
                .await?;
        }

        Some(Session::Revive { .. }) => {
            bot.answer_callback_query(query.id)
                .text("Please select a checked item")
                .await?;
        }

        Some(Session::Category { .. }) => {
            bot.answer_callback_query(query.id)
                .text("Please use the category editor buttons")
                .await?;
        }

        Some(Session::Layout { .. }) => {
            bot.answer_callback_query(query.id)
                .text("Please use the layout editor buttons")
                .await?;
        }

        Some(Session::Advanced { .. }) => {
            bot.answer_callback_query(query.id)
                .text("Please use the advanced category menu")
                .await?;
        }

        None => {
            bot.answer_callback_query(query.id)
                .text("This operation has expired")
                .await?;
        }
    }

    Ok(())
}

fn generation() -> u64 {
    NEXT_GENERATION.fetch_add(1, Ordering::Relaxed)
}

async fn load_token(value: &str) -> Result<String> {
    let path = Path::new(value);

    if path.exists() {
        match fs::read_to_string(path).await {
            Ok(token) => {
                debug!("read Telegram token from {}", path.display());

                let token = token.trim().to_string();

                if token.is_empty() {
                    anyhow::bail!("token file is empty");
                }

                return Ok(token);
            }

            Err(err) => {
                warn!(
                    "could not read '{}' as token file: {err}; \
                     treating argument as token",
                    value
                );
            }
        }
    }

    Ok(value.to_string())
}

fn init_logging(args: &Args) {
    let level = if args.verbose {
        "debug"
    } else if args.quiet {
        "error"
    } else {
        "info"
    };

    unsafe {
        std::env::set_var("RUST_LOG", level);
    }

    pretty_env_logger::init();
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    init_logging(&args);

    info!("Shopping List Bot is starting up");

    let token = load_token(&args.token).await?;

    let store = Store::load(&args.database)
        .await
        .with_context(|| format!("failed to load database {}", args.database.display()))?;

    let store: SharedStore = Arc::new(Mutex::new(store));
    let sessions: SharedSessions = Arc::new(Mutex::new(HashMap::new()));

    let bot = Bot::new(token);
    bot.set_my_commands(Command::bot_commands()).await?;

    info!("Bot initialized");

    let handler = dptree::entry()
        .branch(
            Update::filter_message()
                .branch(teloxide::filter_command::<Command, _>().endpoint(command_handler))
                .branch(dptree::endpoint(text_handler)),
        )
        .branch(Update::filter_callback_query().endpoint(callback_handler));

    Dispatcher::builder(bot, handler)
        .dependencies(dptree::deps![store.clone(), sessions])
        .enable_ctrlc_handler()
        .default_handler(|update| async move {
            debug!("Unhandled update: {update:?}");
        })
        .error_handler(LoggingErrorHandler::with_custom_text(
            "An error occurred while processing an update",
        ))
        .build()
        .dispatch()
        .await;

    store.lock().await.shutdown()?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::rstrip_slash;

    use super::{
        Item, parse_history_categories, parse_import_list, plain_list_text, quick_add_keyboard,
    };

    #[test]
    fn plain_list_export_has_no_header_and_can_exclude_checked_items() {
        let items = vec![
            Item {
                id: 1,
                item: "milk".to_string(),
                checked: false,
            },
            Item {
                id: 2,
                item: "eggs".to_string(),
                checked: true,
            },
        ];

        assert_eq!(plain_list_text(&items, true), "- [ ] milk\n- [x] eggs");
        assert_eq!(plain_list_text(&items, false), "- [ ] milk");
        assert_eq!(plain_list_text(&[], true), "");
    }

    #[test]
    fn parses_newline_separated_items_and_ignores_blank_lines() {
        assert_eq!(
            parse_import_list("  milk \r\n\n eggs\n  bread  \n"),
            vec!["milk", "eggs", "bread"]
        );
    }

    #[test]
    fn parses_category_item_map_for_history_import() {
        assert_eq!(
            parse_history_categories(
                br#"{"categories":{"Produce":["apples","carrots"],"Bakery":["bread"]}}"#
            )
            .unwrap(),
            std::collections::BTreeMap::from([
                ("Bakery".to_string(), vec!["bread".to_string()]),
                (
                    "Produce".to_string(),
                    vec!["apples".to_string(), "carrots".to_string()],
                ),
            ])
        );
    }

    #[test]
    fn rejects_invalid_history_category_documents() {
        for data in [
            br#"{"categories":{"Produce":"apples"}}"#.as_slice(),
            br#"{"categories":{"   ":["apples"]}}"#.as_slice(),
            br#"{"categories":{"Produce":[" "]}}"#.as_slice(),
            br#"{"items":["apples"]}"#.as_slice(),
        ] {
            assert!(parse_history_categories(data).is_err());
        }
    }

    #[test]
    fn strips_list_prefixes() {
        assert_eq!(
            parse_import_list("- [ ] milk\n- [x] eggs\n- [X] bread\n-[] butter\n- cheese"),
            vec!["milk", "eggs", "bread", "butter", "cheese"]
        );
    }

    #[test]
    fn strips_leading_spaces_before_prefix() {
        assert_eq!(
            parse_import_list("  - [ ] milk\n    - eggs\n  -[] bread"),
            vec!["milk", "eggs", "bread"]
        );
    }

    #[test]
    fn rstrip_slash_test() {
        assert_eq!("cmd", rstrip_slash(&"/cmd".to_string()));
        assert_eq!("", rstrip_slash(&"/".to_string()));
        assert_eq!("cmd", rstrip_slash(&"cmd".to_string()));
    }

    #[test]
    fn reorder_keyboard_has_arrows_and_a_move_selection_button_per_row() {
        let items = vec![
            Item {
                id: 1,
                item: "milk".to_string(),
                checked: false,
            },
            Item {
                id: 2,
                item: "checked item".to_string(),
                checked: true,
            },
            Item {
                id: 3,
                item: "eggs".to_string(),
                checked: false,
            },
        ];

        let keyboard = super::reorder_keyboard(&items);
        let keyboard = serde_json::to_value(keyboard).unwrap();
        let rows = keyboard["inline_keyboard"].as_array().unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].as_array().unwrap().len(), 3);
        assert_eq!(
            rows[0][0]["callback_data"].as_str(),
            Some("reorder:boundary")
        );
        assert_eq!(rows[0][1]["callback_data"].as_str(), Some("reorder:down:1"));
        assert_eq!(
            rows[0][2]["callback_data"].as_str(),
            Some("reorder:select:1")
        );
        assert_eq!(rows[1][0]["callback_data"].as_str(), Some("reorder:up:3"));
        assert_eq!(
            rows[1][1]["callback_data"].as_str(),
            Some("reorder:boundary")
        );
        assert_eq!(rows[1][2]["text"].as_str(), Some("eggs"));
        assert_eq!(rows[2][0]["text"].as_str(), Some("Done"));
        assert_eq!(rows[2][0]["callback_data"].as_str(), Some("reorder:done"));
    }

    #[test]
    fn reorder_target_keyboard_lists_other_unchecked_items_and_back() {
        let items = vec![
            Item {
                id: 1,
                item: "milk".to_string(),
                checked: false,
            },
            Item {
                id: 2,
                item: "checked item".to_string(),
                checked: true,
            },
            Item {
                id: 3,
                item: "eggs".to_string(),
                checked: false,
            },
        ];
        let keyboard = serde_json::to_value(super::reorder_target_keyboard(&items, 1)).unwrap();
        let rows = keyboard["inline_keyboard"].as_array().unwrap();

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0]["text"].as_str(), Some("eggs"));
        assert_eq!(
            rows[0][0]["callback_data"].as_str(),
            Some("reorder:place:3")
        );
        assert_eq!(rows[1][0]["text"].as_str(), Some("Back"));
        assert_eq!(rows[1][0]["callback_data"].as_str(), Some("reorder:back"));
    }

    #[test]
    fn revive_keyboard_lists_checked_items_and_a_done_button() {
        let items = vec![
            Item {
                id: 1,
                item: "milk".to_string(),
                checked: false,
            },
            Item {
                id: 2,
                item: "eggs".to_string(),
                checked: true,
            },
        ];
        let keyboard = serde_json::to_value(super::revive_keyboard(&items).unwrap()).unwrap();
        let rows = keyboard["inline_keyboard"].as_array().unwrap();

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0]["text"].as_str(), Some("eggs"));
        assert_eq!(rows[0][0]["callback_data"].as_str(), Some("revive:2"));
        assert_eq!(rows[1][0]["text"].as_str(), Some("DONE"));
        assert_eq!(rows[1][0]["callback_data"].as_str(), Some("revive:done"));
        assert!(super::revive_keyboard(&items[..1]).is_none());
    }

    #[test]
    fn quick_add_keyboard_shows_pagination_for_available_pages() {
        let items = vec![("Milk".to_string(), 3)];
        let first_page = serde_json::to_value(quick_add_keyboard(&items, 7, true)).unwrap();
        let first_rows = first_page["inline_keyboard"].as_array().unwrap();
        assert_eq!(first_rows[0][0]["text"].as_str(), Some("⚖️ Amount"));
        assert_eq!(
            first_rows[1][0]["text"].as_str(),
            Some(format!("Next {}", super::QUICK_ADD_PAGE_SIZE).as_str())
        );
        assert_eq!(
            first_rows[1][0]["callback_data"].as_str(),
            Some("quickadd:7:next")
        );

        let later_page = serde_json::to_value(quick_add_keyboard(&items, 8, false)).unwrap();
        let later_rows = later_page["inline_keyboard"].as_array().unwrap();
        assert_eq!(later_rows.len(), 2);
        assert_eq!(later_rows[1][0]["text"].as_str(), Some("Done"));
    }

    #[test]
    fn categorize_menu_offers_priority_sort_and_layout_creation_without_existing_layouts() {
        let keyboard = serde_json::to_value(super::layouts_keyboard(&[])).unwrap();
        let rows = keyboard["inline_keyboard"].as_array().unwrap();

        assert_eq!(rows.len(), 3);
        assert_eq!(
            rows[0][0]["callback_data"].as_str(),
            Some("layout:priority")
        );
        assert_eq!(rows[1][0]["callback_data"].as_str(), Some("layout:create"));
        assert_eq!(rows[2][0]["callback_data"].as_str(), Some("layout:back"));
    }
}

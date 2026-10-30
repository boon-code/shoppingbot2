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
use db::{CategoryBundle, Store};
use log::{debug, info, warn};
use teloxide::{
    dispatching::UpdateFilterExt,
    prelude::*,
    types::{CallbackQuery, ChatId, InlineKeyboardMarkup, Message, MessageId, Update},
    utils::command::BotCommands,
};
use tokio::{fs, sync::Mutex, time::sleep};

mod advanced;
mod category;
mod db;
mod layout;
mod presentation;
mod quick_add;
mod shopping_actions;

use presentation::checklist_text;

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
        mode: quick_add::QuickAddMode,
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
        mode: category::CategoryMode,
    },

    Layout {
        message_id: MessageId,
        generation: u64,
        mode: layout::LayoutMode,
    },

    Advanced {
        message_id: MessageId,
        generation: u64,
        mode: AdvancedMode,
    },
}

#[derive(Clone, Debug)]
pub(crate) enum AdvancedMode {
    Select,
    ImportMerge,
    ImportReplace,
    ImportHistory,
    ExportPlain,
    ConfirmReplace(CategoryBundle),
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
            quick_add::start(&bot, &store, &sessions, chat_id).await?;
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
            shopping_actions::start_shop(&bot, &store, &sessions, chat_id).await?;
        }

        Command::Revive => {
            shopping_actions::start_revive(&bot, &store, &sessions, chat_id).await?;
        }

        Command::Swap => {
            shopping_actions::start_swap(&bot, &store, &sessions, chat_id).await?;
        }

        Command::Reorder => {
            shopping_actions::start_reorder(&bot, &store, &sessions, chat_id).await?;
        }

        Command::Categorize => {
            layout::start(&bot, &store, &sessions, chat_id).await?;
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
            category::start(&bot, &store, &sessions, chat_id).await?;
        }

        Command::Advanced => {
            let sent = bot
                .send_message(chat_id, "Advanced category tools:")
                .reply_markup(advanced::advanced_keyboard())
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
        advanced::handle_document(&bot, document, chat_id, &store, &sessions).await?;
        return Ok(());
    }

    let Some(text) = msg.text() else {
        bot.send_message(chat_id, "Unsupported content type")
            .await?;

        return Ok(());
    };

    let session = sessions.lock().await.get(&chat_id.0).cloned();
    if category::handle_text(&bot, &store, &sessions, chat_id, text, &session).await? {
        return Ok(());
    }
    if layout::handle_text(&bot, &store, &sessions, chat_id, text, &session).await? {
        return Ok(());
    }

    match session {
        Some(Session::QuickAdd {
            message_id,
            generation,
            offset,
            mode: quick_add::QuickAddMode::Prefix { item },
            ..
        }) => {
            quick_add::handle_prefix_text(
                &bot, &store, &sessions, chat_id, message_id, generation, offset, item, text,
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
        return quick_add::callback(bot, query.id, data, chat_id, message.id(), store, sessions)
            .await;
    }

    if data.starts_with("category:") {
        return category::callback(bot, query.id, data, chat_id, message.id(), store, sessions)
            .await;
    }

    if data.starts_with("layout:") {
        return layout::callback(bot, query.id, data, chat_id, message.id(), store, sessions).await;
    }

    if data.starts_with("advanced:") {
        return advanced::callback(bot, query.id, data, chat_id, message.id(), store, sessions)
            .await;
    }

    shopping_actions::callback(bot, query.id, data, chat_id, message.id(), store, sessions).await
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

    use super::parse_import_list;

    use crate::advanced::parse_history_categories;
    use crate::db::Item;
    use crate::presentation::{
        layouts_keyboard, plain_list_text, reorder_keyboard, reorder_target_keyboard,
        revive_keyboard,
    };
    use crate::quick_add::quick_add_keyboard;
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

        let keyboard = reorder_keyboard(&items);
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
        let keyboard = serde_json::to_value(reorder_target_keyboard(&items, 1)).unwrap();
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
        let keyboard = serde_json::to_value(revive_keyboard(&items).unwrap()).unwrap();
        let rows = keyboard["inline_keyboard"].as_array().unwrap();

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0]["text"].as_str(), Some("eggs"));
        assert_eq!(rows[0][0]["callback_data"].as_str(), Some("revive:2"));
        assert_eq!(rows[1][0]["text"].as_str(), Some("DONE"));
        assert_eq!(rows[1][0]["callback_data"].as_str(), Some("revive:done"));
        assert!(revive_keyboard(&items[..1]).is_none());
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
        let keyboard = serde_json::to_value(layouts_keyboard(&[])).unwrap();
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

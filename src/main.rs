use std::{
    collections::HashMap,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result};
use clap::Parser;
use db::{Item, Store};
use log::{debug, info, warn};
use teloxide::{
    dispatching::UpdateFilterExt,
    prelude::*,
    types::{
        CallbackQuery, ChatId, InlineKeyboardButton, InlineKeyboardMarkup, Message, MessageId,
        Update, WebAppInfo,
    },
    utils::command::BotCommands,
};
use tokio::{fs, net::TcpListener, sync::Mutex, time::sleep};

mod db;
mod ui;

const ADD_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const SHOP_TIMEOUT: Duration = Duration::from_secs(2 * 60 * 60);
const SWAP_TIMEOUT: Duration = Duration::from_secs(5 * 60);

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

    /// Public HTTPS URL at which the Telegram Mini App is served.
    #[arg(long)]
    web_app_url: Option<String>,

    /// Address on which to serve the Telegram Mini App.
    #[arg(long, default_value = "0.0.0.0:8080")]
    web_app_bind: SocketAddr,
}

#[derive(Clone, Debug, BotCommands)]
#[command(rename_rule = "lowercase", description = "Shopping List Bot")]
enum Command {
    #[command(description = "Show current shopping list")]
    List,

    #[command(description = "Open the shopping list Mini App")]
    App,

    #[command(description = "Add multiple items to list")]
    Multiadd,

    #[command(rename = "import", description = "Import newline-separated items")]
    Import,

    #[command(description = "Start shopping")]
    Shop,

    #[command(description = "Cancel current operation")]
    Cancel,

    #[command(description = "Swap items on list")]
    Swap,

    #[command(description = "Remove checked items from list")]
    Cleanup,

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
}

impl Session {
    fn generation(&self) -> u64 {
        match self {
            Session::Add { generation, .. }
            | Session::Import { generation }
            | Session::Shop { generation, .. }
            | Session::Swap { generation, .. } => *generation,
        }
    }

    fn message_id(&self) -> Option<MessageId> {
        match self {
            Session::Shop { message_id, .. } | Session::Swap { message_id, .. } => {
                Some(*message_id)
            }
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
    web_app_url: Option<String>,
) -> Result<()> {
    let chat_id = get_chat_id(&msg).await.context("message has no chat")?;

    // Every command cancels the current dialog, matching the Python implementation.
    close_session(bot.clone(), sessions.clone(), chat_id).await?;

    match cmd {
        Command::List => {
            let items = store.lock().await.items(chat_id).await?;

            bot.send_message(chat_id, checklist_text(&items)).await?;
        }

        Command::App => {
            if let Some(url) = web_app_url {
                let keyboard = InlineKeyboardMarkup::new(vec![vec![
                    InlineKeyboardButton::web_app("Open shopping list", WebAppInfo::new(url)),
                ]]);
                bot.send_message(chat_id, "View and arrange your shopping list:")
                    .reply_markup(keyboard)
                    .await?;
            } else {
                bot.send_message(
                    chat_id,
                    "The Mini App is not configured. Start the bot with --web-app-url \
                     https://your-domain.example/",
                )
                .await?;
            }
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

        Command::Cancel => {
            // Already closed above.
        }

        Command::Cleanup => {
            let mut store = store.lock().await;
            store.remove_checked(chat_id).await?;

            bot.send_message(chat_id, "Cleaned up your shopping list")
                .await?;
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

    let Some(text) = msg.text() else {
        bot.send_message(chat_id, "Unsupported content type")
            .await?;

        return Ok(());
    };

    let session = sessions.lock().await.get(&chat_id.0).cloned();

    match session {
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

        Some(Session::Add { .. } | Session::Import { .. }) => {
            bot.answer_callback_query(query.id)
                .text("Please send the item list as a message")
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

    let web_app_url = args.web_app_url.clone();
    let web_app_listener = if let Some(url) = web_app_url.as_deref() {
        let parsed_url = url::Url::parse(url).context("invalid Mini App URL")?;
        if parsed_url.scheme() != "https"
            || parsed_url.host_str().is_none()
            || parsed_url.path() != "/"
            || parsed_url.query().is_some()
            || parsed_url.fragment().is_some()
        {
            anyhow::bail!("--web-app-url must be an HTTPS origin with an optional trailing slash");
        }

        Some(
            TcpListener::bind(args.web_app_bind)
                .await
                .with_context(|| format!("failed to bind Mini App server to {}", args.web_app_bind))?,
        )
    } else {
        None
    };

    let bot = Bot::new(token.clone());

    info!("Bot initialized");

    let handler = dptree::entry()
        .branch(
            Update::filter_message()
                .branch(teloxide::filter_command::<Command, _>().endpoint(command_handler))
                .branch(dptree::endpoint(text_handler)),
        )
        .branch(Update::filter_callback_query().endpoint(callback_handler));

    Dispatcher::builder(bot, handler)
        .dependencies(dptree::deps![store.clone(), sessions, web_app_url])
        .enable_ctrlc_handler()
        .default_handler(|update| async move {
            debug!("Unhandled update: {update:?}");
        })
        .error_handler(LoggingErrorHandler::with_custom_text(
            "An error occurred while processing an update",
        ))
        .build();

    let ui_result = if let Some(listener) = web_app_listener {
        info!("Mini App server listening on {}", args.web_app_bind);
        tokio::select! {
            result = ui::serve(listener, store.clone(), token) => Some(result),
            _ = dispatcher.dispatch() => None,
        }
    } else {
        dispatcher.dispatch().await;
        None
    };

    store.lock().await.shutdown()?;
    if let Some(result) = ui_result {
        result.context("Mini App server stopped unexpectedly")?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::rstrip_slash;

    use super::parse_import_list;

    #[test]
    fn parses_newline_separated_items_and_ignores_blank_lines() {
        assert_eq!(
            parse_import_list("  milk \r\n\n eggs\n  bread  \n"),
            vec!["milk", "eggs", "bread"]
        );
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
}

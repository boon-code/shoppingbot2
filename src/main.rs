use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, atomic::{AtomicU64, Ordering}},
    time::Duration,
};

use anyhow::{Context, Result};
use clap::Parser;
use log::{debug, info, warn};
use serde::{Deserialize, Serialize};
use teloxide::{
    dispatching::UpdateFilterExt,
    prelude::*,
    types::{
        CallbackQuery, ChatId, InlineKeyboardButton, InlineKeyboardMarkup, Message, MessageId,
        Update,
    },
    utils::command::BotCommands,
};
use tokio::{fs, sync::Mutex, time::sleep};

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

    /// Path to the persistent JSON database.
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
}

#[derive(Clone, Debug)]
enum Session {
    Add {
        count: usize,
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
            | Session::Shop { generation, .. }
            | Session::Swap { generation, .. } => *generation,
        }
    }

    fn message_id(&self) -> Option<MessageId> {
        match self {
            Session::Shop { message_id, .. } | Session::Swap { message_id, .. } => {
                Some(*message_id)
            }
            Session::Add { .. } => None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Item {
    id: u64,
    item: String,
    checked: bool,
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct Database {
    next_id: u64,
    chats: HashMap<String, Vec<Item>>,
}

struct Store {
    path: PathBuf,
    db: Database,
}

impl Store {
    async fn load(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();

        if !Path::new(&path).exists() {
            return Ok(Self {
                path,
                db: Database {
                    next_id: 1,
                    chats: HashMap::new(),
                },
            });
        }

        let contents = fs::read_to_string(&path)
            .await
            .with_context(|| format!("failed to read {}", path.display()))?;

        let mut db: Database =
            serde_json::from_str(&contents).context("invalid shopping list database")?;

        if db.next_id == 0 {
            db.next_id = db.chats.values().flatten().map(|i| i.id).max().unwrap_or(0) + 1;
        }

        Ok(Self { path, db })
    }

    async fn save(&self) -> Result<()> {
        let tmp = self.path.with_extension("json.tmp");

        let data = serde_json::to_vec_pretty(&self.db).context("failed to serialize database")?;

        fs::write(&tmp, data)
            .await
            .with_context(|| format!("failed to write {}", tmp.display()))?;

        fs::rename(&tmp, &self.path)
            .await
            .with_context(|| format!("failed to replace database {}", self.path.display()))?;

        Ok(())
    }

    fn items(&self, chat_id: ChatId) -> &[Item] {
        self.db
            .chats
            .get(&chat_id.0.to_string())
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    fn items_mut(&mut self, chat_id: ChatId) -> &mut Vec<Item> {
        self.db.chats.entry(chat_id.0.to_string()).or_default()
    }

    async fn add_item(&mut self, chat_id: ChatId, text: String) -> Result<()> {
        let id = self.db.next_id;
        self.db.next_id += 1;

        self.items_mut(chat_id).push(Item {
            id,
            item: text,
            checked: false,
        });

        self.save().await
    }

    async fn check_item(&mut self, chat_id: ChatId, id: u64) -> Result<Option<Item>> {
        let item = self
            .items_mut(chat_id)
            .iter_mut()
            .find(|item| item.id == id);

        let Some(item) = item else {
            return Ok(None);
        };

        if item.checked {
            return Ok(Some(item.clone()));
        }

        item.checked = true;

        let result = item.clone();

        self.save().await?;

        Ok(Some(result))
    }

    async fn swap_items(&mut self, chat_id: ChatId, first: u64, second: u64) -> Result<()> {
        if first == second {
            return Ok(());
        }

        let items = self.items_mut(chat_id);

        let first_index = items
            .iter()
            .position(|item| item.id == first)
            .context("first item does not exist")?;

        let second_index = items
            .iter()
            .position(|item| item.id == second)
            .context("second item does not exist")?;

        if items[first_index].checked || items[second_index].checked {
            anyhow::bail!("cannot swap checked items");
        }

        items.swap(first_index, second_index);

        self.save().await
    }

    async fn remove_checked(&mut self, chat_id: ChatId) -> Result<()> {
        self.items_mut(chat_id).retain(|item| !item.checked);
        self.save().await
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
            let store = store.lock().await;
            let items = store.items(chat_id);

            bot.send_message(chat_id, checklist_text(items)).await?;
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

        Command::Shop => {
            let keyboard = {
                let store = store.lock().await;
                keyboard(store.items(chat_id))
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
                let store = store.lock().await;
                keyboard(store.items(chat_id))
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
                let store = store.lock().await;

                (
                    keyboard(store.items(chat_id)),
                    store
                        .items(chat_id)
                        .iter()
                        .filter(|item| !item.checked)
                        .count(),
                )
            };

            if remaining > 0 {
                bot.edit_message_reply_markup(chat_id, message_id)
                    .reply_markup(new_keyboard.unwrap())
                    .await?;
            } else {
                let checked = {
                    let store = store.lock().await;

                    store
                        .items(chat_id)
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
                    let store = store.lock().await;

                    let rows = store
                        .items(chat_id)
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
                    let store = store.lock().await;
                    keyboard(store.items(chat_id))
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

        Some(Session::Add { .. }) => {
            bot.answer_callback_query(query.id)
                .text("Please enter an item name")
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

    info!("Bot initialized");

    let handler = dptree::entry()
        .branch(
            Update::filter_message()
                .branch(teloxide::filter_command::<Command, _>().endpoint(command_handler))
                .branch(dptree::endpoint(text_handler)),
        )
        .branch(Update::filter_callback_query().endpoint(callback_handler));

    Dispatcher::builder(bot, handler)
        .dependencies(dptree::deps![store, sessions])
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

    Ok(())
}

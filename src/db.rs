use super::ChatId;
use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex as StdMutex,
        mpsc::{self, Receiver, RecvTimeoutError, Sender},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};
use tokio::fs as async_fs;

const CACHE_CAPACITY: usize = 100;
const WRITE_DEBOUNCE: Duration = Duration::from_millis(75);
const MAX_PARALLEL_WRITES: usize = 8;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Item {
    pub id: u64,
    pub item: String,
    pub checked: bool,
}

#[derive(Deserialize)]
struct LegacyDatabase {
    #[serde(default)]
    chats: HashMap<String, Vec<Item>>,
}

struct CacheEntry {
    items: Arc<Vec<Item>>,
    next_id: u64,
}

type PendingWrites = Arc<StdMutex<HashMap<String, (u64, Arc<Vec<Item>>)>>>;

enum WriterMessage {
    Save {
        chat_id: String,
        version: u64,
        items: Arc<Vec<Item>>,
        completion_sender: Sender<WriterMessage>,
    },
    Done {
        chat_id: String,
        result: Result<(), String>,
    },
    Shutdown,
}

pub struct Store {
    path: PathBuf,
    cache: HashMap<String, CacheEntry>,
    lru: VecDeque<String>,
    pending_writes: PendingWrites,
    writer_sender: Option<Sender<WriterMessage>>,
    writer_thread: Option<JoinHandle<Result<()>>>,
}

impl Store {
    pub async fn load(path: impl Into<PathBuf>) -> Result<Self> {
        let requested_path = path.into();
        let is_legacy_file = requested_path.is_file();
        let data_path = if requested_path.is_dir() {
            requested_path.clone()
        } else if requested_path.extension().is_some() {
            requested_path.with_extension("")
        } else {
            requested_path.clone()
        };

        fs::create_dir_all(&data_path)
            .with_context(|| format!("failed to create data directory {}", data_path.display()))?;

        if is_legacy_file {
            Self::import_legacy(&requested_path, &data_path)?;
        }

        let (writer_sender, receiver) = mpsc::channel();
        let pending_writes = Arc::new(StdMutex::new(HashMap::new()));
        let writer_path = data_path.clone();
        let writer_pending_writes = Arc::clone(&pending_writes);
        let writer_thread = thread::Builder::new()
            .name("shopping-list-writer".to_string())
            .spawn(move || writer_loop(receiver, writer_path, writer_pending_writes))
            .context("failed to start shopping list writer")?;

        Ok(Self {
            path: data_path,
            cache: HashMap::new(),
            lru: VecDeque::new(),
            pending_writes,
            writer_sender: Some(writer_sender),
            writer_thread: Some(writer_thread),
        })
    }

    fn import_legacy(legacy_path: &Path, data_path: &Path) -> Result<()> {
        let marker = data_path.join(".legacy_imported");
        if marker.exists() {
            return Ok(());
        }

        let contents = fs::read_to_string(legacy_path)
            .with_context(|| format!("failed to read legacy database {}", legacy_path.display()))?;
        let legacy: LegacyDatabase =
            serde_json::from_str(&contents).context("invalid legacy shopping list database")?;

        for (chat_id, items) in legacy.chats {
            let target = data_path.join(list_file_name(&chat_id));
            if !target.exists() {
                write_list_atomically(data_path, &chat_id, &items)?;
            }
        }

        let mut marker_file = File::create(&marker)
            .with_context(|| format!("failed to create {}", marker.display()))?;
        marker_file
            .write_all(b"imported\n")
            .and_then(|()| marker_file.sync_all())
            .with_context(|| format!("failed to sync {}", marker.display()))?;
        sync_directory(data_path)?;
        Ok(())
    }

    async fn ensure_cached(&mut self, chat_id: ChatId) -> Result<String> {
        let chat_id = chat_id.0.to_string();
        if self.cache.contains_key(&chat_id) {
            self.touch(&chat_id);
            return Ok(chat_id);
        }

        let pending_items = self
            .pending_writes
            .lock()
            .map_err(|_| anyhow!("pending shopping list state is unavailable"))?
            .get(&chat_id)
            .map(|(_, items)| Arc::clone(items));
        let items = if let Some(items) = pending_items {
            Arc::try_unwrap(items).unwrap_or_else(|items| (*items).clone())
        } else {
            let file_path = self.path.join(list_file_name(&chat_id));
            match async_fs::read(&file_path).await {
                Ok(contents) => serde_json::from_slice::<Vec<Item>>(&contents)
                    .with_context(|| format!("invalid shopping list {}", file_path.display()))?,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => Vec::new(),
                Err(err) => {
                    return Err(err)
                        .with_context(|| format!("failed to read {}", file_path.display()));
                }
            }
        };
        let next_id = items
            .iter()
            .map(|item| item.id)
            .max()
            .unwrap_or(0)
            .checked_add(1)
            .context("shopping list item id overflow")?;

        if self.cache.len() == CACHE_CAPACITY {
            if let Some(evicted) = self.lru.pop_front() {
                self.cache.remove(&evicted);
            }
        }

        self.cache.insert(
            chat_id.clone(),
            CacheEntry {
                items: Arc::new(items),
                next_id,
            },
        );
        self.touch(&chat_id);
        Ok(chat_id)
    }

    fn touch(&mut self, chat_id: &str) {
        if let Some(index) = self.lru.iter().position(|cached| cached == chat_id) {
            self.lru.remove(index);
        }
        self.lru.push_back(chat_id.to_string());
    }

    fn queue_save(&self, chat_id: String, items: Arc<Vec<Item>>) -> Result<()> {
        let sender = self
            .writer_sender
            .as_ref()
            .context("shopping list writer is shut down")?;
        let version = {
            let mut pending_writes = self
                .pending_writes
                .lock()
                .map_err(|_| anyhow!("pending shopping list state is unavailable"))?;
            let version = pending_writes
                .get(&chat_id)
                .map(|(version, _)| version.saturating_add(1))
                .unwrap_or(1);
            pending_writes.insert(chat_id.clone(), (version, Arc::clone(&items)));
            version
        };
        sender
            .send(WriterMessage::Save {
                chat_id,
                version,
                items,
                completion_sender: sender.clone(),
            })
            .context("shopping list writer has stopped")
    }

    pub async fn items(&mut self, chat_id: ChatId) -> Result<Arc<Vec<Item>>> {
        let chat_id = self.ensure_cached(chat_id).await?;
        Ok(Arc::clone(&self.cache[&chat_id].items))
    }

    pub async fn add_item(&mut self, chat_id: ChatId, text: String) -> Result<()> {
        let chat_key = self.ensure_cached(chat_id).await?;
        let entry = self.cache.get_mut(&chat_key).expect("cached list exists");
        let id = entry.next_id;
        entry.next_id = id
            .checked_add(1)
            .context("shopping list item id overflow")?;
        Arc::make_mut(&mut entry.items).push(Item {
            id,
            item: text,
            checked: false,
        });
        let items = Arc::clone(&entry.items);
        self.queue_save(chat_key, items)
    }

    pub async fn add_items(&mut self, chat_id: ChatId, texts: Vec<String>) -> Result<()> {
        if texts.is_empty() {
            return Ok(());
        }

        let chat_key = self.ensure_cached(chat_id).await?;
        let entry = self.cache.get_mut(&chat_key).expect("cached list exists");
        let start_id = entry.next_id;
        let end_id = entry
            .next_id
            .checked_add(texts.len() as u64)
            .context("shopping list item id overflow")?;
        let items = Arc::make_mut(&mut entry.items);
        for (offset, text) in texts.into_iter().enumerate() {
            items.push(Item {
                id: start_id + offset as u64,
                item: text,
                checked: false,
            });
        }
        entry.next_id = end_id;
        let items = Arc::clone(&entry.items);
        self.queue_save(chat_key, items)
    }

    pub async fn check_item(&mut self, chat_id: ChatId, id: u64) -> Result<Option<Item>> {
        let chat_key = self.ensure_cached(chat_id).await?;
        let entry = self.cache.get_mut(&chat_key).expect("cached list exists");
        let Some(item) = Arc::make_mut(&mut entry.items)
            .iter_mut()
            .find(|item| item.id == id)
        else {
            return Ok(None);
        };

        if item.checked {
            return Ok(Some(item.clone()));
        }

        item.checked = true;
        let result = item.clone();
        let items = Arc::clone(&entry.items);
        self.queue_save(chat_key, items)?;
        Ok(Some(result))
    }

    pub async fn swap_items(&mut self, chat_id: ChatId, first: u64, second: u64) -> Result<()> {
        if first == second {
            return Ok(());
        }

        let chat_key = self.ensure_cached(chat_id).await?;
        let entry = self.cache.get_mut(&chat_key).expect("cached list exists");
        let items = Arc::make_mut(&mut entry.items);
        let first_index = items
            .iter()
            .position(|item| item.id == first)
            .context("first item does not exist")?;
        let second_index = items
            .iter()
            .position(|item| item.id == second)
            .context("second item does not exist")?;

        if items[first_index].checked || items[second_index].checked {
            bail!("cannot swap checked items");
        }

        items.swap(first_index, second_index);
        let items = Arc::clone(&entry.items);
        self.queue_save(chat_key, items)
    }

    pub async fn reorder_items(&mut self, chat_id: ChatId, order: &[u64]) -> Result<()> {
        let chat_key = self.ensure_cached(chat_id).await?;
        let entry = self.cache.get_mut(&chat_key).expect("cached list exists");
        let items = Arc::make_mut(&mut entry.items);
        let current_ids: HashSet<_> = items.iter().map(|item| item.id).collect();
        let requested_ids: HashSet<_> = order.iter().copied().collect();

        if order.len() != items.len()
            || requested_ids.len() != order.len()
            || requested_ids != current_ids
        {
            bail!("requested order must contain every shopping list item exactly once");
        }

        if items.iter().map(|item| item.id).eq(order.iter().copied()) {
            return Ok(());
        }

        let mut by_id: HashMap<_, _> = std::mem::take(items)
            .into_iter()
            .map(|item| (item.id, item))
            .collect();
        *items = order
            .iter()
            .map(|id| by_id.remove(id).expect("validated item id exists"))
            .collect();

        let items = Arc::clone(&entry.items);
        self.queue_save(chat_key, items)
    }

    pub async fn remove_checked(&mut self, chat_id: ChatId) -> Result<()> {
        let chat_key = self.ensure_cached(chat_id).await?;
        let entry = self.cache.get_mut(&chat_key).expect("cached list exists");
        let items = Arc::make_mut(&mut entry.items);
        let original_len = items.len();
        items.retain(|item| !item.checked);

        if items.len() != original_len {
            let items = Arc::clone(&entry.items);
            self.queue_save(chat_key, items)?;
        }
        Ok(())
    }

    pub fn shutdown(&mut self) -> Result<()> {
        if let Some(sender) = self.writer_sender.take() {
            sender
                .send(WriterMessage::Shutdown)
                .context("shopping list writer has stopped")?;
        }

        if let Some(writer_thread) = self.writer_thread.take() {
            writer_thread
                .join()
                .map_err(|_| anyhow!("shopping list writer thread panicked"))??;
        }
        Ok(())
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        if self.writer_thread.is_some() {
            if let Err(err) = self.shutdown() {
                log::error!("failed to flush shopping lists during store drop: {err:#}");
            }
        }
    }
}

fn list_file_name(chat_id: &str) -> String {
    let hex_id = chat_id
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("list_{hex_id}.json")
}

fn write_list_atomically(directory: &Path, chat_id: &str, items: &[Item]) -> Result<()> {
    let file_name = list_file_name(chat_id);
    let destination = directory.join(&file_name);
    let temporary = directory.join(format!(".tmp_{file_name}"));
    let data = serde_json::to_vec_pretty(items).context("failed to serialize shopping list")?;

    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)
        .with_context(|| format!("failed to create {}", temporary.display()))?;
    file.write_all(&data)
        .and_then(|()| file.sync_all())
        .with_context(|| format!("failed to sync {}", temporary.display()))?;
    drop(file);

    fs::rename(&temporary, &destination).with_context(|| {
        format!(
            "failed to replace shopping list {} with {}",
            destination.display(),
            temporary.display()
        )
    })?;
    sync_directory(directory)?;
    Ok(())
}

fn sync_directory(directory: &Path) -> Result<()> {
    File::open(directory)
        .and_then(|directory| directory.sync_all())
        .with_context(|| format!("failed to sync data directory {}", directory.display()))
}

fn writer_loop(
    receiver: Receiver<WriterMessage>,
    directory: PathBuf,
    pending_writes: PendingWrites,
) -> Result<()> {
    let mut pending: HashMap<String, (u64, Arc<Vec<Item>>, Sender<WriterMessage>)> = HashMap::new();
    let mut active = HashSet::new();
    let mut errors = Vec::new();
    let mut next_flush = None;
    let mut shutting_down = false;

    loop {
        let timeout =
            next_flush.map(|deadline: Instant| deadline.saturating_duration_since(Instant::now()));
        let message = match timeout {
            Some(timeout) => receiver.recv_timeout(timeout),
            None => receiver.recv().map_err(|_| RecvTimeoutError::Disconnected),
        };

        match message {
            Ok(WriterMessage::Save {
                chat_id,
                version,
                items,
                completion_sender,
            }) => {
                pending.insert(chat_id, (version, items, completion_sender));
                next_flush = Some(Instant::now() + WRITE_DEBOUNCE);
            }
            Ok(WriterMessage::Done { chat_id, result }) => {
                active.remove(&chat_id);
                if let Err(error) = result {
                    errors.push(error);
                }
                if shutting_down && !pending.is_empty() {
                    next_flush = Some(Instant::now());
                } else if !pending.is_empty() {
                    next_flush = Some(Instant::now() + WRITE_DEBOUNCE);
                }
            }
            Ok(WriterMessage::Shutdown) => {
                shutting_down = true;
                next_flush = Some(Instant::now());
            }
            Err(RecvTimeoutError::Timeout) => {
                next_flush = None;
                start_pending_writes(
                    &mut pending,
                    &mut active,
                    &mut errors,
                    MAX_PARALLEL_WRITES,
                    &directory,
                    &pending_writes,
                );
            }
            Err(RecvTimeoutError::Disconnected) => {
                if !shutting_down {
                    errors.push("writer channel closed without shutdown".to_string());
                    shutting_down = true;
                }
                next_flush = Some(Instant::now());
            }
        }

        if next_flush.is_some_and(|deadline| deadline <= Instant::now()) {
            next_flush = None;
            start_pending_writes(
                &mut pending,
                &mut active,
                &mut errors,
                MAX_PARALLEL_WRITES,
                &directory,
                &pending_writes,
            );
        }

        if shutting_down && pending.is_empty() && active.is_empty() {
            break;
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        bail!("failed to write shopping list(s): {}", errors.join("; "))
    }
}

fn start_pending_writes(
    pending: &mut HashMap<String, (u64, Arc<Vec<Item>>, Sender<WriterMessage>)>,
    active: &mut HashSet<String>,
    errors: &mut Vec<String>,
    max_parallel: usize,
    directory: &Path,
    pending_writes: &PendingWrites,
) {
    let available_slots = max_parallel.saturating_sub(active.len());
    let ready_ids = pending
        .keys()
        .filter(|chat_id| !active.contains(*chat_id))
        .take(available_slots)
        .cloned()
        .collect::<Vec<_>>();

    for chat_id in ready_ids {
        let (version, items, completion_sender) =
            pending.remove(&chat_id).expect("pending write exists");
        active.insert(chat_id.clone());
        let directory = directory.to_path_buf();
        let pending_writes = Arc::clone(pending_writes);
        let thread_chat_id = chat_id.clone();
        if let Err(error) = thread::Builder::new()
            .name(format!("shopping-list-{chat_id}"))
            .spawn(move || {
                let result = write_list_atomically(&directory, &thread_chat_id, &items)
                    .map_err(|error| format!("{error:#}"));
                if result.is_ok() {
                    match pending_writes.lock() {
                        Ok(mut pending) => {
                            if pending
                                .get(&thread_chat_id)
                                .is_some_and(|(pending_version, _)| *pending_version == version)
                            {
                                pending.remove(&thread_chat_id);
                            }
                        }

                        Err(_) => log::error!(
                            "pending shopping list state was poisoned for {thread_chat_id}"
                        ),
                    }
                }
                if let Err(error) = &result {
                    log::error!("failed to persist shopping list {thread_chat_id}: {error}");
                }
                let _ = completion_sender.send(WriterMessage::Done {
                    chat_id: thread_chat_id,
                    result,
                });
            })
        {
            active.remove(&chat_id);
            errors.push(format!(
                "failed to start writer for list {chat_id}: {error}"
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_TEST_DIRECTORY: AtomicU64 = AtomicU64::new(0);

    fn test_directory(label: &str) -> PathBuf {
        let id = NEXT_TEST_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("shoppingbot2-{label}-{}-{id}", std::process::id()))
    }

    #[test]
    fn chat_id_is_encoded_as_hex_of_its_string() {
        assert_eq!(list_file_name("1234"), "list_31323334.json");
        assert_eq!(list_file_name("-100"), "list_2d313030.json");
    }

    #[tokio::test]
    async fn writes_independent_lists_and_flushes_latest_state() {
        let directory = test_directory("lists");
        let chat_id = ChatId(1234);
        let mut store = Store::load(&directory).await.unwrap();

        store.add_item(chat_id, "milk".to_string()).await.unwrap();
        store
            .add_items(chat_id, vec!["eggs".to_string(), "bread".to_string()])
            .await
            .unwrap();
        store.check_item(chat_id, 1).await.unwrap();
        store
            .add_item(ChatId(1235), "coffee".to_string())
            .await
            .unwrap();
        store.shutdown().unwrap();

        let first: Vec<Item> =
            serde_json::from_slice(&fs::read(directory.join("list_31323334.json")).unwrap())
                .unwrap();
        let second: Vec<Item> =
            serde_json::from_slice(&fs::read(directory.join("list_31323335.json")).unwrap())
                .unwrap();
        assert_eq!(
            first
                .iter()
                .map(|item| (item.item.as_str(), item.checked))
                .collect::<Vec<_>>(),
            vec![("milk", true), ("eggs", false), ("bread", false)]
        );
        assert_eq!(second[0].item, "coffee");
        assert!(!directory.join(".tmp_list_31323334.json").exists());

        drop(store);
        fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn evicted_list_uses_its_latest_pending_snapshot() {
        let directory = test_directory("cache");
        let mut store = Store::load(&directory).await.unwrap();

        for id in 1000..1000 + CACHE_CAPACITY as i64 + 1 {
            store
                .add_item(ChatId(id), format!("item {id}"))
                .await
                .unwrap();
        }

        assert_eq!(store.cache.len(), CACHE_CAPACITY);
        assert_eq!(
            store.items(ChatId(1000)).await.unwrap()[0].item,
            "item 1000"
        );
        store.shutdown().unwrap();
        drop(store);

        let mut reopened = Store::load(&directory).await.unwrap();
        assert_eq!(
            reopened.items(ChatId(1000)).await.unwrap()[0].item,
            "item 1000"
        );
        reopened.shutdown().unwrap();
        drop(reopened);
        fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn imports_existing_single_file_database() {
        let parent = test_directory("legacy");
        fs::create_dir_all(&parent).unwrap();
        let legacy_path = parent.join("legacy.json");
        fs::write(
            &legacy_path,
            r#"{"next_id":2,"chats":{"1234":[{"id":1,"item":"milk","checked":false}]}}"#,
        )
        .unwrap();

        let mut store = Store::load(&legacy_path).await.unwrap();
        assert_eq!(store.items(ChatId(1234)).await.unwrap()[0].item, "milk");
        store.shutdown().unwrap();
        drop(store);

        assert!(parent.join("legacy").join("list_31323334.json").exists());
        assert!(legacy_path.exists());
        fs::remove_dir_all(parent).unwrap();
    }
}

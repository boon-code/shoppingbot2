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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct HistoryEntry {
    text: String,
    count: u64,
    #[serde(default)]
    spelling: Vec<HashMap<String, HistorySpellingCount>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct HistorySpellingCount {
    count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Category {
    pub name: String,
    pub items: Vec<String>,
    #[serde(default)]
    pub priority: i32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Layout {
    pub name: String,
    pub categories: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CategoryBundle {
    pub categories: Vec<Category>,
    pub layouts: Vec<Layout>,
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
    history: HashMap<String, Vec<HistoryEntry>>,
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
            history: HashMap::new(),
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

    pub fn categories(&self, chat_id: ChatId) -> Result<Vec<Category>> {
        let directory = self
            .path
            .join(categories_directory_name(&chat_id.0.to_string()));
        if !directory.exists() {
            return Ok(Vec::new());
        }

        let mut categories: Vec<Category> = fs::read_dir(&directory)
            .with_context(|| format!("failed to read {}", directory.display()))?
            .map(|entry| {
                let entry =
                    entry.with_context(|| format!("failed to read {}", directory.display()))?;
                let path = entry.path();
                if !path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with("category_") && name.ends_with(".json"))
                {
                    return Ok(None);
                }
                let contents = fs::read(&path)
                    .with_context(|| format!("failed to read {}", path.display()))?;
                serde_json::from_slice(&contents)
                    .with_context(|| format!("invalid category {}", path.display()))
                    .map(Some)
            })
            .collect::<Result<Vec<Option<Category>>>>()?
            .into_iter()
            .flatten()
            .collect();
        categories.sort_by(|left, right| {
            left.priority
                .cmp(&right.priority)
                .then_with(|| left.name.to_lowercase().cmp(&right.name.to_lowercase()))
        });
        Ok(categories)
    }

    pub async fn sort_items_by_category(&mut self, chat_id: ChatId) -> Result<()> {
        let mut category_order = HashMap::new();
        for category in self.categories(chat_id)? {
            for item in category.items {
                let canonical = normalize_history_text(&item);
                if canonical.is_empty() {
                    continue;
                }
                category_order
                    .entry(canonical)
                    .and_modify(|priority: &mut i32| *priority = (*priority).min(category.priority))
                    .or_insert(category.priority);
            }
        }

        let chat_key = self.ensure_cached(chat_id).await?;
        let entry = self.cache.get_mut(&chat_key).expect("cached list exists");
        let items = Arc::make_mut(&mut entry.items);
        items.sort_by_key(|item| {
            let priority = category_order.get(&normalize_history_text(&item.item));
            (priority.is_none(), priority.copied().unwrap_or_default())
        });
        let items = Arc::clone(&entry.items);
        self.queue_save(chat_key, items)
    }

    pub async fn sort_items_by_layout(&mut self, chat_id: ChatId, name: &str) -> Result<()> {
        let layout = self
            .layouts(chat_id)?
            .into_iter()
            .find(|layout| layout.name == name)
            .with_context(|| format!("layout {name:?} does not exist"))?;
        let categories = self.categories(chat_id)?;
        let mut category_order = HashMap::new();
        for (order, category_name) in layout.categories.iter().enumerate() {
            if let Some(category) = categories
                .iter()
                .find(|category| category.name == *category_name)
            {
                for item in &category.items {
                    let canonical = normalize_history_text(item);
                    if canonical.is_empty() {
                        continue;
                    }
                    category_order
                        .entry(canonical)
                        .and_modify(|current: &mut usize| *current = (*current).min(order))
                        .or_insert(order);
                }
            }
        }

        let chat_key = self.ensure_cached(chat_id).await?;
        let entry = self.cache.get_mut(&chat_key).expect("cached list exists");
        let items = Arc::make_mut(&mut entry.items);
        items.sort_by_key(|item| {
            let order = category_order.get(&normalize_history_text(&item.item));
            (order.is_none(), order.copied().unwrap_or_default())
        });
        let items = Arc::clone(&entry.items);
        self.queue_save(chat_key, items)
    }

    pub fn layouts(&self, chat_id: ChatId) -> Result<Vec<Layout>> {
        let directory = self
            .path
            .join(categories_directory_name(&chat_id.0.to_string()));
        if !directory.exists() {
            return Ok(Vec::new());
        }
        let categories = self.categories(chat_id)?;
        let category_names = categories
            .iter()
            .map(|category| category.name.clone())
            .collect::<Vec<_>>();
        let mut layouts = fs::read_dir(&directory)
            .with_context(|| format!("failed to read {}", directory.display()))?
            .map(|entry| {
                let entry =
                    entry.with_context(|| format!("failed to read {}", directory.display()))?;
                let path = entry.path();
                if !path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with("layout_") && name.ends_with(".json"))
                {
                    return Ok(None);
                }
                let name = layout_name_from_path(&path)
                    .with_context(|| format!("invalid layout filename {}", path.display()))?;
                let contents = fs::read(&path)
                    .with_context(|| format!("failed to read {}", path.display()))?;
                let stored: Vec<String> = serde_json::from_slice(&contents)
                    .with_context(|| format!("invalid layout {}", path.display()))?;
                Ok(Some(Layout {
                    name,
                    categories: stored,
                }))
            })
            .collect::<Result<Vec<Option<Layout>>>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        layouts.sort_by(|left, right| left.name.to_lowercase().cmp(&right.name.to_lowercase()));

        for layout in &mut layouts {
            let normalized = normalize_layout_categories(&layout.categories, &category_names);
            if normalized != layout.categories {
                self.write_layout(chat_id, &layout.name, &normalized)?;
                layout.categories = normalized;
            }
        }
        Ok(layouts)
    }

    pub fn export_categories(&self, chat_id: ChatId) -> Result<CategoryBundle> {
        let bundle = CategoryBundle {
            categories: self.categories(chat_id)?,
            layouts: self.layouts(chat_id)?,
        };
        validate_category_bundle(&bundle)?;
        Ok(bundle)
    }

    pub fn import_categories(
        &self,
        chat_id: ChatId,
        bundle: &CategoryBundle,
        replace: bool,
    ) -> Result<Vec<String>> {
        validate_category_bundle(bundle)?;
        let directory = self
            .path
            .join(categories_directory_name(&chat_id.0.to_string()));
        fs::create_dir_all(&directory)
            .with_context(|| format!("failed to create {}", directory.display()))?;

        if replace {
            let mut updates = Vec::new();
            if directory.exists() {
                for entry in fs::read_dir(&directory)
                    .with_context(|| format!("failed to read {}", directory.display()))?
                {
                    let path = entry
                        .with_context(|| format!("failed to read {}", directory.display()))?
                        .path();
                    if is_category_or_layout_file(&path) {
                        updates.push((path, None));
                    }
                }
            }
            for category in &bundle.categories {
                updates.push((
                    directory.join(category_file_name(&category.name)),
                    Some(
                        serde_json::to_vec_pretty(category)
                            .context("failed to serialize shopping category")?,
                    ),
                ));
            }
            for layout in &bundle.layouts {
                updates.push((
                    directory.join(layout_file_name(&layout.name)),
                    Some(
                        serde_json::to_vec_pretty(&layout.categories)
                            .context("failed to serialize shop layout")?,
                    ),
                ));
            }
            replace_files_atomically(&directory, updates)?;
            return Ok(Vec::new());
        }

        let existing_categories = self.categories(chat_id)?;
        let mut categories = existing_categories.clone();
        let mut imported_names = HashMap::new();
        let mut messages = Vec::new();

        for imported in &bundle.categories {
            let existing_index = categories
                .iter()
                .position(|category| category.name.eq_ignore_ascii_case(&imported.name));
            let index = existing_index.unwrap_or_else(|| {
                categories.push(Category {
                    name: imported.name.clone(),
                    items: Vec::new(),
                    priority: imported.priority,
                });
                categories.len() - 1
            });
            let actual_name = categories[index].name.clone();
            categories[index].priority = imported.priority;
            for item in &imported.items {
                let key = item_key(item);
                if categories[index]
                    .items
                    .iter()
                    .any(|existing| item_key(existing) == key)
                {
                    continue;
                }
                if let Some(owner) = categories.iter().find(|category| {
                    category.name != actual_name
                        && category
                            .items
                            .iter()
                            .any(|existing| item_key(existing) == key)
                }) {
                    messages.push(format!(
                        "Item {item:?} wasn't added because it is already in category {:?}.",
                        owner.name
                    ));
                } else {
                    categories[index].items.push(item.clone());
                }
            }
            categories[index].items.sort();
            imported_names.insert(imported.name.to_lowercase(), actual_name);
        }

        let existing_layouts = self.layouts(chat_id)?;
        let mut layouts = existing_layouts.clone();
        for imported in &bundle.layouts {
            let incoming_categories = imported
                .categories
                .iter()
                .map(|name| {
                    imported_names
                        .get(&name.to_lowercase())
                        .cloned()
                        .or_else(|| {
                            categories
                                .iter()
                                .find(|category| category.name.eq_ignore_ascii_case(name))
                                .map(|category| category.name.clone())
                        })
                        .unwrap_or_else(|| name.clone())
                })
                .collect::<Vec<_>>();
            if let Some(existing) = layouts
                .iter_mut()
                .find(|layout| layout.name.eq_ignore_ascii_case(&imported.name))
            {
                for name in incoming_categories {
                    if !existing.categories.contains(&name) {
                        existing.categories.push(name);
                    }
                }
            } else {
                layouts.push(Layout {
                    name: imported.name.clone(),
                    categories: incoming_categories,
                });
            }
        }

        let mut updates = categories
            .iter()
            .map(|category| {
                Ok((
                    directory.join(category_file_name(&category.name)),
                    Some(
                        serde_json::to_vec_pretty(category)
                            .context("failed to serialize shopping category")?,
                    ),
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        updates.extend(
            layouts
                .iter()
                .map(|layout| {
                    Ok((
                        directory.join(layout_file_name(&layout.name)),
                        Some(
                            serde_json::to_vec_pretty(&layout.categories)
                                .context("failed to serialize shop layout")?,
                        ),
                    ))
                })
                .collect::<Result<Vec<_>>>()?,
        );
        replace_files_atomically(&directory, updates)?;
        Ok(messages)
    }

    pub fn create_layout(&self, chat_id: ChatId, name: &str) -> Result<Layout> {
        let name = name.trim();
        if name.is_empty() {
            bail!("layout name cannot be empty");
        }
        if self
            .layouts(chat_id)?
            .iter()
            .any(|layout| layout.name.eq_ignore_ascii_case(name))
        {
            bail!("layout already exists");
        }
        let categories = self
            .categories(chat_id)?
            .into_iter()
            .map(|category| category.name)
            .collect::<Vec<_>>();
        self.write_layout(chat_id, name, &categories)?;
        Ok(Layout {
            name: name.to_string(),
            categories,
        })
    }

    pub fn save_layout(&self, chat_id: ChatId, name: &str, categories: &[String]) -> Result<()> {
        let current = self
            .layouts(chat_id)?
            .into_iter()
            .find(|layout| layout.name == name)
            .with_context(|| format!("layout {name:?} does not exist"))?;
        let valid_categories = self
            .categories(chat_id)?
            .into_iter()
            .map(|category| category.name)
            .collect::<Vec<_>>();
        let normalized = normalize_layout_categories(categories, &valid_categories);
        self.write_layout(chat_id, &current.name, &normalized)
    }

    pub fn rename_layout(&self, chat_id: ChatId, name: &str, new_name: &str) -> Result<()> {
        let new_name = new_name.trim();
        if new_name.is_empty() {
            bail!("layout name cannot be empty");
        }
        let layouts = self.layouts(chat_id)?;
        let layout = layouts
            .iter()
            .find(|layout| layout.name == name)
            .with_context(|| format!("layout {name:?} does not exist"))?;
        if layouts
            .iter()
            .any(|layout| layout.name != name && layout.name.eq_ignore_ascii_case(new_name))
        {
            bail!("layout already exists");
        }
        let directory = self
            .path
            .join(categories_directory_name(&chat_id.0.to_string()));
        let old_file = directory.join(layout_file_name(name));
        let new_file = directory.join(layout_file_name(new_name));
        if old_file != new_file {
            let data = serde_json::to_vec_pretty(&layout.categories)
                .context("failed to serialize shop layout")?;
            write_file_atomically(&directory, &new_file, &data)?;
            fs::remove_file(&old_file)
                .with_context(|| format!("failed to remove {}", old_file.display()))?;
            sync_directory(&directory)?;
        }
        Ok(())
    }

    pub fn delete_layout(&self, chat_id: ChatId, name: &str) -> Result<()> {
        let layout = self
            .layouts(chat_id)?
            .into_iter()
            .find(|layout| layout.name == name)
            .with_context(|| format!("layout {name:?} does not exist"))?;
        let directory = self
            .path
            .join(categories_directory_name(&chat_id.0.to_string()));
        let file = directory.join(layout_file_name(&layout.name));
        fs::remove_file(&file).with_context(|| format!("failed to remove {}", file.display()))?;
        sync_directory(&directory)
    }

    fn write_layout(&self, chat_id: ChatId, name: &str, categories: &[String]) -> Result<()> {
        let directory = self
            .path
            .join(categories_directory_name(&chat_id.0.to_string()));
        fs::create_dir_all(&directory)
            .with_context(|| format!("failed to create {}", directory.display()))?;
        let data =
            serde_json::to_vec_pretty(categories).context("failed to serialize shop layout")?;
        write_file_atomically(&directory, &directory.join(layout_file_name(name)), &data)
    }

    pub async fn history_items(&mut self, chat_id: ChatId) -> Result<Vec<(String, u64)>> {
        self.load_history(chat_id).await?;
        let mut items = self.history[&chat_id.0.to_string()]
            .iter()
            .map(|entry| (entry.text.clone(), entry.count))
            .collect::<Vec<_>>();
        items.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
        Ok(items)
    }

    pub fn create_category(&self, chat_id: ChatId, name: &str) -> Result<Category> {
        let name = name.trim();
        if name.is_empty() {
            bail!("category name cannot be empty");
        }
        if self
            .categories(chat_id)?
            .iter()
            .any(|category| category.name.to_lowercase() == name.to_lowercase())
        {
            bail!("category already exists");
        }

        let category = Category {
            name: name.to_string(),
            items: Vec::new(),
            priority: 0,
        };
        write_category(&self.path, chat_id, &category)?;
        self.layouts(chat_id)?;
        Ok(category)
    }

    pub fn set_category_priority(
        &self,
        chat_id: ChatId,
        category_name: &str,
        priority: i32,
    ) -> Result<()> {
        let mut category = self.category(chat_id, category_name)?;
        category.priority = priority;
        write_category(&self.path, chat_id, &category)
    }

    pub fn rename_category(
        &self,
        chat_id: ChatId,
        category_name: &str,
        new_name: &str,
    ) -> Result<()> {
        let new_name = new_name.trim();
        if new_name.is_empty() {
            bail!("category name cannot be empty");
        }

        let mut category = self.category(chat_id, category_name)?;
        if self.categories(chat_id)?.iter().any(|existing| {
            existing.name != category.name && existing.name.eq_ignore_ascii_case(new_name)
        }) {
            bail!("category already exists");
        }

        let old_file = self
            .path
            .join(categories_directory_name(&chat_id.0.to_string()))
            .join(category_file_name(&category.name));
        let directory = old_file
            .parent()
            .context("category file path has no parent")?
            .to_path_buf();
        let mut layouts = self.layouts(chat_id)?;
        for layout in &mut layouts {
            for name in &mut layout.categories {
                if name == &category.name {
                    *name = new_name.to_string();
                }
            }
        }
        category.name = new_name.to_string();
        let new_file = directory.join(category_file_name(&category.name));
        let category_data = serde_json::to_vec_pretty(&category)
            .context("failed to serialize shopping category")?;
        let mut updates = vec![(new_file.clone(), Some(category_data))];
        for layout in layouts {
            updates.push((
                directory.join(layout_file_name(&layout.name)),
                Some(
                    serde_json::to_vec_pretty(&layout.categories)
                        .context("failed to serialize shop layout")?,
                ),
            ));
        }
        if old_file != new_file {
            updates.push((old_file, None));
        }
        replace_files_atomically(&directory, updates)
    }

    pub fn add_category_item(
        &self,
        chat_id: ChatId,
        category_name: &str,
        item: &str,
    ) -> Result<()> {
        if self.categories(chat_id)?.iter().any(|category| {
            category.name != category_name && category.items.iter().any(|existing| existing == item)
        }) {
            bail!("item is already assigned to another category");
        }
        let mut category = self.category(chat_id, category_name)?;
        if !category.items.iter().any(|existing| existing == item) {
            category.items.push(item.to_string());
            category.items.sort();
            write_category(&self.path, chat_id, &category)?;
        }
        Ok(())
    }

    pub fn remove_category_item(
        &self,
        chat_id: ChatId,
        category_name: &str,
        item: &str,
    ) -> Result<()> {
        let mut category = self.category(chat_id, category_name)?;
        if let Some(index) = category.items.iter().position(|existing| existing == item) {
            category.items.remove(index);
            write_category(&self.path, chat_id, &category)?;
        }
        Ok(())
    }

    pub fn delete_category(&self, chat_id: ChatId, category_name: &str) -> Result<()> {
        let category = self.category(chat_id, category_name)?;
        let path = self
            .path
            .join(categories_directory_name(&chat_id.0.to_string()));
        let file = path.join(category_file_name(&category.name));
        fs::remove_file(&file).with_context(|| format!("failed to remove {}", file.display()))?;
        sync_directory(&path)?;
        self.layouts(chat_id)?;
        Ok(())
    }

    fn category(&self, chat_id: ChatId, category_name: &str) -> Result<Category> {
        self.categories(chat_id)?
            .into_iter()
            .find(|category| category.name == category_name)
            .with_context(|| format!("category {category_name:?} does not exist"))
    }

    async fn load_history(&mut self, chat_id: ChatId) -> Result<()> {
        let chat_key = chat_id.0.to_string();
        if self.history.contains_key(&chat_key) {
            return Ok(());
        }

        let file_path = self.path.join(history_file_name(&chat_key));
        let history = match async_fs::read_to_string(&file_path).await {
            Ok(contents) => contents
                .lines()
                .enumerate()
                .filter(|(_, line)| !line.trim().is_empty())
                .map(|(index, line)| {
                    serde_json::from_str(line).with_context(|| {
                        format!(
                            "invalid history entry at line {} in {}",
                            index + 1,
                            file_path.display()
                        )
                    })
                })
                .collect::<Result<Vec<HistoryEntry>>>()?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("failed to read {}", file_path.display()));
            }
        };
        self.history.insert(chat_key, history);
        Ok(())
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
            item: text.clone(),
            checked: false,
        });
        let items = Arc::clone(&entry.items);
        self.queue_save(chat_key.clone(), items)?;
        self.track_history(&chat_key, &[text]).await
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
        for (offset, text) in texts.iter().enumerate() {
            items.push(Item {
                id: start_id + offset as u64,
                item: text.clone(),
                checked: false,
            });
        }
        entry.next_id = end_id;
        let items = Arc::clone(&entry.items);
        self.queue_save(chat_key.clone(), items)?;
        self.track_history(&chat_key, &texts).await
    }

    async fn track_history(&mut self, chat_id: &str, texts: &[String]) -> Result<()> {
        self.load_history(ChatId(chat_id.parse()?)).await?;

        let mut updated_history = self.history[chat_id].clone();
        for text in texts {
            let spelling = normalize_history_spelling(text);
            let normalized = spelling.to_lowercase();
            if normalized.is_empty() {
                continue;
            }

            if let Some(entry) = updated_history
                .iter_mut()
                .find(|entry| entry.text == normalized)
            {
                entry.count = entry
                    .count
                    .checked_add(1)
                    .context("shopping history count overflow")?;
                if let Some(spelling_entry) = entry
                    .spelling
                    .iter_mut()
                    .find_map(|spellings| spellings.get_mut(&spelling))
                {
                    spelling_entry.count = spelling_entry
                        .count
                        .checked_add(1)
                        .context("shopping history spelling count overflow")?;
                } else {
                    entry.spelling.push(HashMap::from([(
                        spelling,
                        HistorySpellingCount { count: 1 },
                    )]));
                }
            } else {
                updated_history.push(HistoryEntry {
                    text: normalized,
                    count: 1,
                    spelling: vec![HashMap::from([(
                        spelling,
                        HistorySpellingCount { count: 1 },
                    )])],
                });
            }
        }

        write_history_atomically(&self.path, chat_id, &updated_history)?;
        self.history.insert(chat_id.to_string(), updated_history);
        Ok(())
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

    pub async fn uncheck_item(&mut self, chat_id: ChatId, id: u64) -> Result<Option<Item>> {
        let chat_key = self.ensure_cached(chat_id).await?;
        let entry = self.cache.get_mut(&chat_key).expect("cached list exists");
        let Some(item) = Arc::make_mut(&mut entry.items)
            .iter_mut()
            .find(|item| item.id == id && item.checked)
        else {
            return Ok(None);
        };

        item.checked = false;
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

    pub async fn move_item(&mut self, chat_id: ChatId, id: u64, up: bool) -> Result<Option<bool>> {
        let chat_key = self.ensure_cached(chat_id).await?;
        let entry = self.cache.get_mut(&chat_key).expect("cached list exists");
        let items = Arc::make_mut(&mut entry.items);
        let Some(index) = items.iter().position(|item| item.id == id) else {
            return Ok(None);
        };
        if items[index].checked {
            return Ok(None);
        }
        let Some(target_index) = (if up {
            items[..index].iter().rposition(|item| !item.checked)
        } else {
            items[index + 1..]
                .iter()
                .position(|item| !item.checked)
                .map(|offset| index + 1 + offset)
        }) else {
            return Ok(Some(false));
        };

        items.swap(index, target_index);
        let items = Arc::clone(&entry.items);
        self.queue_save(chat_key, items)?;
        Ok(Some(true))
    }

    pub async fn move_item_after(
        &mut self,
        chat_id: ChatId,
        id: u64,
        target_id: u64,
    ) -> Result<Option<bool>> {
        if id == target_id {
            return Ok(Some(false));
        }

        let chat_key = self.ensure_cached(chat_id).await?;
        let entry = self.cache.get_mut(&chat_key).expect("cached list exists");
        let items = Arc::make_mut(&mut entry.items);
        let Some(source_index) = items.iter().position(|item| item.id == id) else {
            return Ok(None);
        };
        let Some(target_index) = items.iter().position(|item| item.id == target_id) else {
            return Ok(None);
        };
        if items[source_index].checked || items[target_index].checked {
            return Ok(None);
        }

        let unchecked_indices = items
            .iter()
            .enumerate()
            .filter_map(|(index, item)| (!item.checked).then_some(index))
            .collect::<Vec<_>>();
        let mut unchecked_items = unchecked_indices
            .iter()
            .map(|&index| items[index].clone())
            .collect::<Vec<_>>();
        let source_position = unchecked_items
            .iter()
            .position(|item| item.id == id)
            .expect("source item is unchecked");
        let target_position = unchecked_items
            .iter()
            .position(|item| item.id == target_id)
            .expect("target item is unchecked");

        if source_position == target_position + 1 {
            return Ok(Some(false));
        }

        let moving = unchecked_items.remove(source_position);
        let target_position = unchecked_items
            .iter()
            .position(|item| item.id == target_id)
            .expect("target item remains in the list");
        unchecked_items.insert(target_position + 1, moving);

        for (index, item) in unchecked_indices.into_iter().zip(unchecked_items) {
            items[index] = item;
        }

        let items = Arc::clone(&entry.items);
        self.queue_save(chat_key, items)?;
        Ok(Some(true))
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
    format!("list_{}.json", hex_encode(chat_id))
}

fn history_file_name(chat_id: &str) -> String {
    format!("history_{}", hex_encode(chat_id))
}

fn categories_directory_name(chat_id: &str) -> String {
    format!("categories_{}", hex_encode(chat_id))
}

fn category_file_name(name: &str) -> String {
    format!("category_{}.json", hex_encode(&name.to_lowercase()))
}

fn layout_file_name(name: &str) -> String {
    format!("layout_{}.json", hex_encode(name))
}

fn layout_name_from_path(path: &Path) -> Result<String> {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .context("layout filename is not valid UTF-8")?;
    let encoded = file_name
        .strip_prefix("layout_")
        .and_then(|name| name.strip_suffix(".json"))
        .context("layout filename has an invalid format")?;
    if encoded.len() % 2 != 0 || !encoded.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("layout filename contains invalid hex");
    }
    let bytes = encoded
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            u8::from_str_radix(std::str::from_utf8(pair).expect("ASCII hex"), 16)
                .context("layout filename contains invalid hex")
        })
        .collect::<Result<Vec<_>>>()?;
    String::from_utf8(bytes).context("layout name is not valid UTF-8")
}

fn hex_encode(value: &str) -> String {
    value
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn normalize_layout_categories(stored: &[String], categories: &[String]) -> Vec<String> {
    let mut normalized = stored
        .iter()
        .filter(|name| categories.iter().any(|category| category == *name))
        .fold(Vec::new(), |mut result, name| {
            if !result.contains(name) {
                result.push(name.clone());
            }
            result
        });
    for category in categories {
        if !normalized.contains(category) {
            normalized.push(category.clone());
        }
    }
    normalized
}

fn item_key(item: &str) -> String {
    item.trim().to_lowercase()
}

fn is_category_or_layout_file(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| {
            (name.starts_with("category_") || name.starts_with("layout_"))
                && name.ends_with(".json")
        })
}

pub fn validate_category_bundle(bundle: &CategoryBundle) -> Result<()> {
    let mut categories = HashMap::new();
    let mut assigned_items = HashMap::new();

    for category in &bundle.categories {
        let name = category.name.trim();
        if name.is_empty() {
            bail!("category name cannot be empty");
        }
        let key = name.to_lowercase();
        if categories.insert(key, category.name.as_str()).is_some() {
            bail!("duplicate category name {:?}", category.name);
        }

        let mut category_items = HashSet::new();
        for item in &category.items {
            let item_name = item.trim();
            if item_name.is_empty() {
                bail!("category {:?} contains an empty item", category.name);
            }
            let item_key = item_key(item_name);
            if !category_items.insert(item_key.clone()) {
                bail!(
                    "category {:?} contains duplicate item {:?}",
                    category.name,
                    item
                );
            }
            if let Some(owner) = assigned_items.insert(item_key, category.name.as_str()) {
                bail!(
                    "item {:?} is assigned to both {:?} and {:?}",
                    item,
                    owner,
                    category.name
                );
            }
        }
    }

    let mut layout_names = HashSet::new();
    for layout in &bundle.layouts {
        let name = layout.name.trim();
        if name.is_empty() {
            bail!("layout name cannot be empty");
        }
        if !layout_names.insert(name.to_lowercase()) {
            bail!("duplicate layout name {:?}", layout.name);
        }
        let mut layout_categories = HashSet::new();
        for category in &layout.categories {
            let key = category.to_lowercase();
            if !categories.contains_key(&key) {
                bail!(
                    "layout {:?} references unknown category {:?}",
                    layout.name,
                    category
                );
            }
            if !layout_categories.insert(key) {
                bail!(
                    "layout {:?} contains duplicate category {:?}",
                    layout.name,
                    category
                );
            }
        }
        for category in &bundle.categories {
            if !layout_categories.contains(&category.name.trim().to_lowercase()) {
                bail!(
                    "layout {:?} is missing category {:?}",
                    layout.name,
                    category.name
                );
            }
        }
    }
    Ok(())
}

fn replace_files_atomically(
    directory: &Path,
    updates: Vec<(PathBuf, Option<Vec<u8>>)>,
) -> Result<()> {
    let mut staged = Vec::new();
    let mut originals = Vec::new();
    let staging = (|| -> Result<()> {
        for (index, (destination, contents)) in updates.iter().enumerate() {
            if destination.parent() != Some(directory) {
                bail!("transaction file is outside {}", directory.display());
            }
            let original = match fs::read(destination) {
                Ok(contents) => Some(contents),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("failed to read {}", destination.display()));
                }
            };
            originals.push((destination.clone(), original));
            if let Some(contents) = contents {
                let file_name = destination
                    .file_name()
                    .and_then(|name| name.to_str())
                    .context("transaction filename is not valid UTF-8")?;
                let temporary = directory.join(format!(".tmp_txn_{index}_{file_name}"));
                staged.push((temporary.clone(), destination.clone()));
                let mut file = OpenOptions::new()
                    .create(true)
                    .truncate(true)
                    .write(true)
                    .open(&temporary)
                    .with_context(|| format!("failed to create {}", temporary.display()))?;
                file.write_all(contents)
                    .and_then(|()| file.sync_all())
                    .with_context(|| format!("failed to sync {}", temporary.display()))?;
            }
        }
        Ok(())
    })();
    if let Err(error) = staging {
        for (temporary, _) in staged {
            let _ = fs::remove_file(temporary);
        }
        return Err(error);
    }

    let commit = (|| -> Result<()> {
        let mut staged_index = 0;
        for (destination, contents) in &updates {
            if contents.is_some() {
                let (temporary, staged_destination) = &staged[staged_index];
                debug_assert_eq!(staged_destination, destination);
                fs::rename(temporary, destination).with_context(|| {
                    format!(
                        "failed to replace {} with {}",
                        destination.display(),
                        temporary.display()
                    )
                })?;
                staged_index += 1;
            } else if destination.exists() {
                fs::remove_file(destination)
                    .with_context(|| format!("failed to remove {}", destination.display()))?;
            }
        }
        sync_directory(directory)
    })();
    if let Err(error) = commit {
        let mut rollback_errors = Vec::new();
        for (destination, original) in originals {
            let result = match original {
                Some(contents) => write_file_atomically(directory, &destination, &contents),
                None => match fs::remove_file(&destination) {
                    Ok(()) => Ok(()),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
                    Err(error) => Err(error)
                        .with_context(|| format!("failed to remove {}", destination.display())),
                },
            };
            if let Err(rollback_error) = result {
                rollback_errors.push(format!("{rollback_error:#}"));
            }
        }
        for (temporary, _) in staged {
            let _ = fs::remove_file(temporary);
        }
        if rollback_errors.is_empty() {
            return Err(error).context("file transaction failed and was rolled back");
        }
        bail!(
            "file transaction failed: {error:#}; rollback errors: {}",
            rollback_errors.join("; ")
        );
    }
    Ok(())
}

fn write_file_atomically(directory: &Path, destination: &Path, data: &[u8]) -> Result<()> {
    let file_name = destination
        .file_name()
        .and_then(|name| name.to_str())
        .context("destination filename is not valid UTF-8")?;
    let temporary = directory.join(format!(".tmp_{file_name}"));
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)
        .with_context(|| format!("failed to create {}", temporary.display()))?;
    file.write_all(data)
        .and_then(|()| file.sync_all())
        .with_context(|| format!("failed to sync {}", temporary.display()))?;
    drop(file);
    fs::rename(&temporary, destination).with_context(|| {
        format!(
            "failed to replace {} with {}",
            destination.display(),
            temporary.display()
        )
    })?;
    sync_directory(directory)
}

fn write_category(directory: &Path, chat_id: ChatId, category: &Category) -> Result<()> {
    let directory = directory.join(categories_directory_name(&chat_id.0.to_string()));
    fs::create_dir_all(&directory)
        .with_context(|| format!("failed to create {}", directory.display()))?;
    let file_name = category_file_name(&category.name);
    let destination = directory.join(&file_name);
    let temporary = directory.join(format!(".tmp_{file_name}"));
    let data =
        serde_json::to_vec_pretty(category).context("failed to serialize shopping category")?;

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
            "failed to replace category {} with {}",
            destination.display(),
            temporary.display()
        )
    })?;
    sync_directory(&directory)
}

fn normalize_history_text(text: &str) -> String {
    normalize_history_spelling(text).to_lowercase()
}

fn normalize_history_spelling(text: &str) -> String {
    let mut words: Vec<&str> = text.split_whitespace().collect();

    let Some(first) = words.first().cloned() else {
        return String::new();
    };
    let lower_first = first.to_lowercase();
    let Some((number_end, suffix)) = amount_token(&lower_first) else {
        return words.join(" ");
    };
    let suffix = suffix.to_string();

    if number_end == 0 {
        return words.join(" ");
    }

    words.remove(0);
    if suffix.is_empty() {
        if words
            .first()
            .is_some_and(|word| word.eq_ignore_ascii_case("x"))
        {
            words.remove(0);
        }
        if words
            .first()
            .is_some_and(|word| is_amount_unit(&word.to_lowercase()))
        {
            words.remove(0);
        }
    } else if suffix == "x" {
        if words
            .first()
            .is_some_and(|word| word.eq_ignore_ascii_case("x"))
        {
            words.remove(0);
        }
    }

    while words
        .first()
        .is_some_and(|word| is_package_prefix(&word.to_lowercase()))
    {
        words.remove(0);
    }
    if words
        .first()
        .is_some_and(|word| word.eq_ignore_ascii_case("of") || word.eq_ignore_ascii_case("von"))
    {
        words.remove(0);
    }

    words.join(" ")
}

fn amount_token(word: &str) -> Option<(usize, &str)> {
    let number_end = word
        .char_indices()
        .find(|(_, character)| {
            !character.is_ascii_digit() && *character != '.' && *character != ','
        })
        .map(|(index, _)| index)
        .unwrap_or(word.len());
    if number_end == 0
        || !word[..number_end]
            .chars()
            .any(|character| character.is_ascii_digit())
    {
        return None;
    }

    let suffix = &word[number_end..];
    if suffix.is_empty() || suffix == "x" || is_amount_unit(suffix) {
        Some((number_end, suffix))
    } else {
        None
    }
}

fn is_amount_unit(word: &str) -> bool {
    matches!(
        word,
        "g" | "kg"
            | "mg"
            | "ml"
            | "cl"
            | "dl"
            | "l"
            | "oz"
            | "lb"
            | "lbs"
            | "pc"
            | "pcs"
            | "piece"
            | "pieces"
            | "stk"
            | "st"
            | "stück"
            | "stueck"
    )
}

fn is_package_prefix(word: &str) -> bool {
    matches!(
        word,
        "pkg"
            | "pack"
            | "package"
            | "packet"
            | "box"
            | "bag"
            | "bottle"
            | "can"
            | "tin"
            | "jar"
            | "bunch"
            | "dose"
            | "beutel"
            | "flasche"
            | "glas"
    )
}

fn write_history_atomically(
    directory: &Path,
    chat_id: &str,
    history: &[HistoryEntry],
) -> Result<()> {
    let file_name = history_file_name(chat_id);
    let destination = directory.join(&file_name);
    let temporary = directory.join(format!(".tmp_{file_name}"));
    let mut data = Vec::new();
    for entry in history {
        serde_json::to_writer(&mut data, entry).context("failed to serialize history entry")?;
        data.push(b'\n');
    }

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
            "failed to replace history {} with {}",
            destination.display(),
            temporary.display()
        )
    })?;
    sync_directory(directory)?;
    Ok(())
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
        assert_eq!(history_file_name("1234"), "history_31323334");
    }

    #[test]
    fn normalizes_common_amount_and_package_prefixes() {
        assert_eq!(normalize_history_text("Tomaten"), "tomaten");
        assert_eq!(normalize_history_text("1x pkg Tomaten"), "tomaten");
        assert_eq!(normalize_history_text("500g Tomaten"), "tomaten");
        assert_eq!(normalize_history_text("3 Tomaten"), "tomaten");
        assert_eq!(normalize_history_text("1 x pack of TOMATEN"), "tomaten");
        assert_eq!(normalize_history_text("500 g Tomaten"), "tomaten");
        assert_eq!(normalize_history_spelling("1 x pack of TOMATEN"), "TOMATEN");
        assert_eq!(normalize_history_spelling("500ML Tomaten"), "Tomaten");
    }

    #[tokio::test]
    async fn persists_categories_per_chat_and_prevents_duplicate_assignments() {
        let directory = test_directory("categories");
        let mut store = Store::load(&directory).await.unwrap();
        let first_chat = ChatId(1234);
        let second_chat = ChatId(1235);

        store.create_category(first_chat, "Produce").unwrap();
        store
            .add_category_item(first_chat, "Produce", "tomaten")
            .unwrap();
        store.create_category(first_chat, "Pantry").unwrap();
        assert!(
            store
                .add_category_item(first_chat, "Pantry", "tomaten")
                .is_err()
        );
        store.create_category(second_chat, "Produce").unwrap();
        assert_eq!(store.categories(first_chat).unwrap()[0].priority, 0);
        store
            .set_category_priority(first_chat, "Produce", 7)
            .unwrap();

        let categories = store.categories(first_chat).unwrap();
        assert_eq!(categories.len(), 2);
        assert_eq!(categories[0].name, "Pantry");
        assert_eq!(categories[0].priority, 0);
        assert_eq!(categories[1].items, vec!["tomaten"]);
        assert_eq!(categories[1].priority, 7);
        assert!(
            directory
                .join("categories_31323334")
                .join("category_70726f64756365.json")
                .exists()
        );
        assert_eq!(store.categories(second_chat).unwrap().len(), 1);

        store
            .remove_category_item(first_chat, "Produce", "tomaten")
            .unwrap();
        store.delete_category(first_chat, "Pantry").unwrap();
        assert_eq!(
            store.categories(first_chat).unwrap()[0].items,
            Vec::<String>::new()
        );

        store.shutdown().unwrap();
        drop(store);

        let reopened = Store::load(&directory).await.unwrap();
        let category = reopened
            .categories(first_chat)
            .unwrap()
            .into_iter()
            .find(|category| category.name == "Produce")
            .unwrap();
        assert_eq!(category.priority, 7);
        drop(reopened);
        fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn merges_category_imports_without_overwriting_or_reassigning_items() {
        let directory = test_directory("merge-category-import");
        let store = Store::load(&directory).await.unwrap();
        let chat_id = ChatId(1234);
        store.create_category(chat_id, "Fruits").unwrap();
        store.add_category_item(chat_id, "Fruits", "Apple").unwrap();
        store.set_category_priority(chat_id, "Fruits", 9).unwrap();
        store.create_category(chat_id, "Pantry").unwrap();
        store.add_category_item(chat_id, "Pantry", "Beans").unwrap();
        store.create_layout(chat_id, "Shop").unwrap();
        store
            .save_layout(
                chat_id,
                "Shop",
                &["Fruits".to_string(), "Pantry".to_string()],
            )
            .unwrap();

        let bundle = CategoryBundle {
            categories: vec![
                Category {
                    name: "FRUITS".to_string(),
                    items: vec![
                        "apple".to_string(),
                        "Banana".to_string(),
                        "Beans".to_string(),
                    ],
                    priority: -5,
                },
                Category {
                    name: "Snacks".to_string(),
                    items: vec!["Toffee".to_string()],
                    priority: 2,
                },
            ],
            layouts: vec![
                Layout {
                    name: "Shop".to_string(),
                    categories: vec!["Snacks".to_string(), "FRUITS".to_string()],
                },
                Layout {
                    name: "Weekend".to_string(),
                    categories: vec!["Snacks".to_string(), "FRUITS".to_string()],
                },
            ],
        };
        let conflicts = store.import_categories(chat_id, &bundle, false).unwrap();

        let categories = store.categories(chat_id).unwrap();
        let fruits = categories
            .iter()
            .find(|category| category.name == "Fruits")
            .unwrap();
        assert_eq!(fruits.items, vec!["Apple", "Banana"]);
        assert_eq!(fruits.priority, -5);
        assert_eq!(
            categories
                .iter()
                .find(|category| category.name == "Snacks")
                .unwrap()
                .items,
            vec!["Toffee"]
        );
        assert_eq!(conflicts.len(), 1);
        assert!(conflicts[0].contains("already in category \"Pantry\""));

        let layouts = store.layouts(chat_id).unwrap();
        let shop = layouts.iter().find(|layout| layout.name == "Shop").unwrap();
        assert_eq!(shop.categories, vec!["Fruits", "Pantry", "Snacks"]);
        assert!(
            layouts.iter().any(|layout| {
                layout.name == "Weekend" && layout.categories == ["Snacks", "Fruits", "Pantry"]
            }),
            "{layouts:?}"
        );
        drop(store);
        fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn replaces_all_categories_and_layouts_after_validation() {
        let directory = test_directory("replace-category-import");
        let store = Store::load(&directory).await.unwrap();
        let chat_id = ChatId(1234);
        store.create_category(chat_id, "Old").unwrap();
        store
            .add_category_item(chat_id, "Old", "stale item")
            .unwrap();
        store.create_layout(chat_id, "Old layout").unwrap();

        let invalid = CategoryBundle {
            categories: vec![Category {
                name: "Fresh".to_string(),
                items: vec!["shared".to_string()],
                priority: 0,
            }],
            layouts: vec![Layout {
                name: "Bad layout".to_string(),
                categories: vec!["Missing".to_string()],
            }],
        };
        assert!(store.import_categories(chat_id, &invalid, true).is_err());
        assert_eq!(store.categories(chat_id).unwrap()[0].name, "Old");

        let replacement = CategoryBundle {
            categories: vec![Category {
                name: "Fresh".to_string(),
                items: vec!["new item".to_string()],
                priority: 4,
            }],
            layouts: vec![Layout {
                name: "Fresh layout".to_string(),
                categories: vec!["Fresh".to_string()],
            }],
        };
        store
            .import_categories(chat_id, &replacement, true)
            .unwrap();
        assert_eq!(store.categories(chat_id).unwrap(), replacement.categories);
        assert_eq!(store.layouts(chat_id).unwrap(), replacement.layouts);
        let category_directory = directory.join(categories_directory_name("1234"));
        assert!(!category_directory.join(category_file_name("Old")).exists());
        assert!(
            !category_directory
                .join(layout_file_name("Old layout"))
                .exists()
        );
        drop(store);
        fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn renames_category_without_losing_items_or_priority() {
        let directory = test_directory("rename-category");
        let mut store = Store::load(&directory).await.unwrap();
        let chat_id = ChatId(1234);
        store.create_category(chat_id, "Produce").unwrap();
        store
            .add_category_item(chat_id, "Produce", "tomaten")
            .unwrap();
        store.set_category_priority(chat_id, "Produce", 7).unwrap();
        store.create_category(chat_id, "Pantry").unwrap();

        store
            .rename_category(chat_id, "Produce", "Fresh Produce")
            .unwrap();
        let categories = store.categories(chat_id).unwrap();
        let renamed = categories
            .iter()
            .find(|category| category.name == "Fresh Produce")
            .unwrap();
        assert_eq!(renamed.items, vec!["tomaten"]);
        assert_eq!(renamed.priority, 7);
        assert!(!categories.iter().any(|category| category.name == "Produce"));
        assert!(
            store
                .rename_category(chat_id, "Fresh Produce", "Pantry")
                .is_err()
        );

        store.shutdown().unwrap();
        drop(store);
        fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn layouts_follow_category_renames_and_append_new_categories() {
        let directory = test_directory("layout-category-consistency");
        let store = Store::load(&directory).await.unwrap();
        let chat_id = ChatId(1234);
        store.create_category(chat_id, "First").unwrap();
        store.create_category(chat_id, "Second").unwrap();
        store.set_category_priority(chat_id, "First", 3).unwrap();
        store.set_category_priority(chat_id, "Second", 1).unwrap();
        let layout = store.create_layout(chat_id, "Weekly shop").unwrap();
        assert_eq!(layout.categories, vec!["Second", "First"]);
        assert!(
            directory
                .join("categories_31323334")
                .join("layout_5765656b6c792073686f70.json")
                .exists()
        );

        store
            .save_layout(
                chat_id,
                "Weekly shop",
                &["First".to_string(), "Second".to_string()],
            )
            .unwrap();
        store.create_category(chat_id, "Third").unwrap();
        store.rename_category(chat_id, "Second", "Renamed").unwrap();
        assert_eq!(
            store.layouts(chat_id).unwrap()[0].categories,
            vec!["First", "Renamed", "Third"]
        );

        let layout_path = directory
            .join("categories_31323334")
            .join("layout_5765656b6c792073686f70.json");
        fs::write(&layout_path, r#"["stale","Third","Third"]"#).unwrap();
        assert_eq!(
            store.layouts(chat_id).unwrap()[0].categories,
            vec!["Third", "Renamed", "First"]
        );
        drop(store);
        fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn sorts_items_by_layout_order_and_keeps_unmatched_items_stable() {
        let directory = test_directory("sort-by-layout");
        let mut store = Store::load(&directory).await.unwrap();
        let chat_id = ChatId(1234);
        store.create_category(chat_id, "First").unwrap();
        store.create_category(chat_id, "Second").unwrap();
        store.set_category_priority(chat_id, "First", 10).unwrap();
        store.set_category_priority(chat_id, "Second", -10).unwrap();
        store.add_category_item(chat_id, "First", "Apples").unwrap();
        store.add_category_item(chat_id, "Second", "Bread").unwrap();
        store.create_layout(chat_id, "Custom").unwrap();
        store
            .save_layout(
                chat_id,
                "Custom",
                &["First".to_string(), "Second".to_string()],
            )
            .unwrap();
        store
            .add_items(
                chat_id,
                vec![
                    "Bread".to_string(),
                    "unmatched one".to_string(),
                    "Apples".to_string(),
                    "unmatched two".to_string(),
                ],
            )
            .await
            .unwrap();
        store.sort_items_by_layout(chat_id, "Custom").await.unwrap();
        let items = store.items(chat_id).await.unwrap();
        let names = items
            .iter()
            .map(|item| item.item.clone())
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            vec!["Apples", "Bread", "unmatched one", "unmatched two"]
        );
        store.shutdown().unwrap();
        drop(store);
        fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn sorts_category_matches_by_priority_and_keeps_unmatched_items_in_order() {
        let directory = test_directory("sort-by-category");
        let mut store = Store::load(&directory).await.unwrap();
        let chat_id = ChatId(1234);
        store.create_category(chat_id, "Later").unwrap();
        store.set_category_priority(chat_id, "Later", 5).unwrap();
        store.add_category_item(chat_id, "Later", "Milk").unwrap();
        store.create_category(chat_id, "First").unwrap();
        store.set_category_priority(chat_id, "First", -2).unwrap();
        store.add_category_item(chat_id, "First", "Bread").unwrap();
        store
            .add_items(
                chat_id,
                vec![
                    "unmatched one".to_string(),
                    "Milk".to_string(),
                    "2x pkg Bread".to_string(),
                    "unmatched two".to_string(),
                    "Another milk".to_string(),
                ],
            )
            .await
            .unwrap();

        store.sort_items_by_category(chat_id).await.unwrap();
        let items = store.items(chat_id).await.unwrap();
        let item_names = items
            .iter()
            .map(|item| item.item.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            item_names,
            vec![
                "2x pkg Bread",
                "Milk",
                "unmatched one",
                "unmatched two",
                "Another milk"
            ]
        );

        store.shutdown().unwrap();
        drop(store);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn category_priority_defaults_to_zero_for_existing_files() {
        let category: Category =
            serde_json::from_str(r#"{"name":"Produce","items":["tomaten"]}"#).unwrap();
        assert_eq!(category.priority, 0);
    }

    #[tokio::test]
    async fn tracks_normalized_history_per_chat() {
        let directory = test_directory("history");
        let chat_id = ChatId(1234);
        let mut store = Store::load(&directory).await.unwrap();

        store
            .add_items(
                chat_id,
                vec![
                    "Tomaten".to_string(),
                    "1x pkg Tomaten".to_string(),
                    "500g tomaten".to_string(),
                    "3 TOMATEN".to_string(),
                    "Kartoffeln".to_string(),
                    "Kartoffeln".to_string(),
                ],
            )
            .await
            .unwrap();
        store
            .add_item(ChatId(1235), "Tomaten".to_string())
            .await
            .unwrap();
        store.shutdown().unwrap();

        let entries: Vec<HistoryEntry> = fs::read_to_string(directory.join("history_31323334"))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(
            entries,
            vec![
                HistoryEntry {
                    text: "tomaten".to_string(),
                    count: 4,
                    spelling: vec![
                        HashMap::from([
                            ("Tomaten".to_string(), HistorySpellingCount { count: 2 },)
                        ]),
                        HashMap::from([
                            ("tomaten".to_string(), HistorySpellingCount { count: 1 },)
                        ]),
                        HashMap::from([
                            ("TOMATEN".to_string(), HistorySpellingCount { count: 1 },)
                        ]),
                    ],
                },
                HistoryEntry {
                    text: "kartoffeln".to_string(),
                    count: 2,
                    spelling: vec![HashMap::from([(
                        "Kartoffeln".to_string(),
                        HistorySpellingCount { count: 2 },
                    )])],
                },
            ]
        );

        let other_chat: Vec<HistoryEntry> = fs::read_to_string(directory.join("history_31323335"))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(other_chat[0].count, 1);
        let frequency = store.history_items(chat_id).await.unwrap();
        assert_eq!(
            frequency,
            vec![("tomaten".to_string(), 4), ("kartoffeln".to_string(), 2)]
        );

        drop(store);

        let mut reopened = Store::load(&directory).await.unwrap();
        reopened
            .add_item(chat_id, "1kg Tomaten".to_string())
            .await
            .unwrap();
        reopened.shutdown().unwrap();
        drop(reopened);
        let persisted: Vec<HistoryEntry> = fs::read_to_string(directory.join("history_31323334"))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(persisted[0].count, 5);
        assert_eq!(persisted[1].count, 2);
        assert_eq!(persisted[0].spelling[0]["Tomaten"].count, 3);

        fs::remove_dir_all(directory).unwrap();
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
    async fn moves_items_up_and_down_and_persists_order() {
        let directory = test_directory("reorder");
        let chat_id = ChatId(1234);
        let mut store = Store::load(&directory).await.unwrap();
        store
            .add_items(
                chat_id,
                vec![
                    "milk".to_string(),
                    "checked item".to_string(),
                    "eggs".to_string(),
                    "bread".to_string(),
                ],
            )
            .await
            .unwrap();
        store.check_item(chat_id, 2).await.unwrap();

        assert_eq!(
            store.move_item(chat_id, 1, true).await.unwrap(),
            Some(false)
        );
        assert_eq!(store.move_item(chat_id, 2, true).await.unwrap(), None);
        assert_eq!(store.move_item(chat_id, 4, true).await.unwrap(), Some(true));
        assert_eq!(
            store.move_item(chat_id, 1, false).await.unwrap(),
            Some(true)
        );
        assert_eq!(store.move_item(chat_id, 99, false).await.unwrap(), None);
        assert_eq!(
            store
                .items(chat_id)
                .await
                .unwrap()
                .iter()
                .filter(|item| !item.checked)
                .map(|item| item.item.as_str())
                .collect::<Vec<_>>(),
            vec!["bread", "milk", "eggs"]
        );
        assert!(store.items(chat_id).await.unwrap()[1].checked);

        store.shutdown().unwrap();
        drop(store);

        let mut reopened = Store::load(&directory).await.unwrap();
        assert_eq!(
            reopened
                .items(chat_id)
                .await
                .unwrap()
                .iter()
                .filter(|item| !item.checked)
                .map(|item| item.item.as_str())
                .collect::<Vec<_>>(),
            vec!["bread", "milk", "eggs"]
        );
        reopened.shutdown().unwrap();
        drop(reopened);
        fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn moves_an_item_after_another_without_moving_checked_items() {
        let directory = test_directory("move-after");
        let chat_id = ChatId(1234);
        let mut store = Store::load(&directory).await.unwrap();
        store
            .add_items(
                chat_id,
                vec![
                    "milk".to_string(),
                    "checked item".to_string(),
                    "eggs".to_string(),
                    "bread".to_string(),
                ],
            )
            .await
            .unwrap();
        store.check_item(chat_id, 2).await.unwrap();

        assert_eq!(
            store.move_item_after(chat_id, 4, 1).await.unwrap(),
            Some(true)
        );
        assert_eq!(
            store.move_item_after(chat_id, 4, 1).await.unwrap(),
            Some(false)
        );
        assert_eq!(store.move_item_after(chat_id, 99, 1).await.unwrap(), None);
        assert_eq!(
            store
                .items(chat_id)
                .await
                .unwrap()
                .iter()
                .map(|item| (item.item.as_str(), item.checked))
                .collect::<Vec<_>>(),
            vec![
                ("milk", false),
                ("checked item", true),
                ("bread", false),
                ("eggs", false)
            ]
        );

        store.shutdown().unwrap();
        drop(store);
        fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn unchecks_an_item_and_persists_the_change() {
        let directory = test_directory("revive");
        let chat_id = ChatId(1234);
        let mut store = Store::load(&directory).await.unwrap();
        store.add_item(chat_id, "milk".to_string()).await.unwrap();

        assert!(store.uncheck_item(chat_id, 1).await.unwrap().is_none());
        store.check_item(chat_id, 1).await.unwrap();
        assert_eq!(
            store.uncheck_item(chat_id, 1).await.unwrap().unwrap().item,
            "milk"
        );
        assert!(store.uncheck_item(chat_id, 1).await.unwrap().is_none());
        assert!(!store.items(chat_id).await.unwrap()[0].checked);

        store.shutdown().unwrap();
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

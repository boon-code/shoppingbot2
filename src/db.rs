use super::ChatId;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, path::{Path, PathBuf}};
use tokio::fs;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Item {
    pub id: u64,
    pub item: String,
    pub checked: bool,
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct Database {
    next_id: u64,
    chats: HashMap<String, Vec<Item>>,
}

pub struct Store {
    path: PathBuf,
    db: Database,
}

impl Store {
    pub async fn load(path: impl Into<PathBuf>) -> Result<Self> {
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

    pub async fn save(&self) -> Result<()> {
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

    pub fn items(&self, chat_id: ChatId) -> &[Item] {
        self.db
            .chats
            .get(&chat_id.0.to_string())
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    pub fn items_mut(&mut self, chat_id: ChatId) -> &mut Vec<Item> {
        self.db.chats.entry(chat_id.0.to_string()).or_default()
    }

    pub async fn add_item(&mut self, chat_id: ChatId, text: String) -> Result<()> {
        let id = self.db.next_id;
        self.db.next_id += 1;

        self.items_mut(chat_id).push(Item {
            id,
            item: text,
            checked: false,
        });

        self.save().await
    }

    pub async fn add_items(&mut self, chat_id: ChatId, texts: Vec<String>) -> Result<()> {
        let mut new_items = Vec::with_capacity(texts.len());
        for text in texts {
            let id = self.db.next_id;
            self.db.next_id += 1;
            new_items.push(Item {
                id,
                item: text,
                checked: false,
            });
        }

        self.items_mut(chat_id).extend(new_items);
        self.save().await
    }

    pub async fn check_item(&mut self, chat_id: ChatId, id: u64) -> Result<Option<Item>> {
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

    pub async fn swap_items(&mut self, chat_id: ChatId, first: u64, second: u64) -> Result<()> {
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

    pub async fn remove_checked(&mut self, chat_id: ChatId) -> Result<()> {
        self.items_mut(chat_id).retain(|item| !item.checked);
        self.save().await
    }
}

use crate::db::{Category, Item, Layout};
use teloxide::types::{InlineKeyboardButton, InlineKeyboardMarkup};

pub(crate) fn checklist_text(items: &[Item]) -> String {
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

pub(crate) fn plain_list_text(items: &[Item], include_checked: bool) -> String {
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

pub(crate) fn keyboard(items: &[Item]) -> Option<InlineKeyboardMarkup> {
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

pub(crate) fn revive_keyboard(items: &[Item]) -> Option<InlineKeyboardMarkup> {
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

pub(crate) fn categories_keyboard(categories: &[Category]) -> InlineKeyboardMarkup {
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

pub(crate) fn categories_text(categories: &[Category]) -> String {
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

pub(crate) fn layouts_text(layouts: &[Layout]) -> String {
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

pub(crate) fn layouts_keyboard(layouts: &[Layout]) -> InlineKeyboardMarkup {
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

pub(crate) fn layout_management_keyboard() -> InlineKeyboardMarkup {
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

pub(crate) fn layout_confirmation_keyboard() -> InlineKeyboardMarkup {
    InlineKeyboardMarkup::new(vec![vec![
        InlineKeyboardButton::callback("Delete", "layout:confirm-delete"),
        InlineKeyboardButton::callback("Cancel", "layout:cancel-delete"),
    ]])
}

pub(crate) fn layout_arrange_text(
    name: &str,
    categories: &[String],
    moving: Option<usize>,
) -> String {
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

pub(crate) fn layout_arrange_keyboard(
    categories: &[String],
    moving: Option<usize>,
) -> InlineKeyboardMarkup {
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

pub(crate) fn category_editor_text(category: &Category) -> String {
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

pub(crate) fn category_editor_keyboard(category: &Category) -> InlineKeyboardMarkup {
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

pub(crate) fn category_confirmation_keyboard() -> InlineKeyboardMarkup {
    InlineKeyboardMarkup::new(vec![vec![
        InlineKeyboardButton::callback("Delete", "category:confirm-delete"),
        InlineKeyboardButton::callback("Cancel", "category:cancel-delete"),
    ]])
}

pub(crate) fn category_management_keyboard() -> InlineKeyboardMarkup {
    InlineKeyboardMarkup::new(vec![
        vec![InlineKeyboardButton::callback("Rename", "category:rename")],
        vec![InlineKeyboardButton::callback("Delete", "category:delete")],
        vec![InlineKeyboardButton::callback("Back", "category:back")],
    ])
}

pub(crate) fn category_candidates_text(category: &str, candidates: &[(String, u64)]) -> String {
    if candidates.is_empty() {
        format!("Category: {category}\n\nNo unassigned history items to add.")
    } else {
        format!("Choose an item to add to {category}:")
    }
}

pub(crate) fn category_candidates_keyboard(candidates: &[(String, u64)]) -> InlineKeyboardMarkup {
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

pub(crate) fn reorder_keyboard(items: &[Item]) -> InlineKeyboardMarkup {
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

pub(crate) fn reorder_target_keyboard(items: &[Item], moving_id: u64) -> InlineKeyboardMarkup {
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

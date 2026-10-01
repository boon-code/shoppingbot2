#!/usr/bin/env python3

"""
Convert shoppingbot's old TinyDB-style list.json into shoppingbot2's
per-chat JSON files.

Old format:

{
    "_default": {
        "420": {
            "cid": "-123456789",
            "item": "Eier"
        },
        "422": {
            "cid": "-123456789",
            "item": "Milch"
        }
    }
}

New format:

    output_directory/
        list_2d313233343536373839.json

containing:

[
    {
        "id": 420,
        "item": "Eier",
        "checked": false
    },
    {
        "id": 422,
        "item": "Milch",
        "checked": false
    }
]

The filename is the hexadecimal representation of the UTF-8 bytes of
the chat ID, matching shoppingbot2/src/db.rs.
"""

import argparse
import json
import os
import sys
import tempfile
from pathlib import Path


def list_filename(chat_id: str) -> str:
    """Return the shoppingbot2 filename for a chat ID."""
    hex_id = chat_id.encode("utf-8").hex()
    return f"list_{hex_id}.json"


def load_old_database(path: Path) -> dict:
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        raise RuntimeError(f"Input file does not exist: {path}")
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"Invalid JSON in {path}: {exc}"
        ) from exc

    if not isinstance(data, dict):
        raise RuntimeError("Old database must contain a JSON object.")

    if "_default" not in data:
        raise RuntimeError(
            "Old database does not contain the expected '_default' table."
        )

    if not isinstance(data["_default"], dict):
        raise RuntimeError("The '_default' table must be a JSON object.")

    return data


def convert(data: dict) -> dict[str, list[dict]]:
    """Convert old TinyDB records into per-chat item lists."""

    chats: dict[str, list[dict]] = {}

    for document_id, record in data["_default"].items():
        if not isinstance(record, dict):
            raise RuntimeError(
                f"Record {document_id!r} is not a JSON object."
            )

        if "cid" not in record:
            raise RuntimeError(
                f"Record {document_id!r} has no 'cid' field."
            )

        if "item" not in record:
            raise RuntimeError(
                f"Record {document_id!r} has no 'item' field."
            )

        cid = str(record["cid"])
        item = record["item"]

        if not isinstance(item, str):
            raise RuntimeError(
                f"Record {document_id!r} has a non-string 'item' field."
            )

        # TinyDB's document IDs are numeric in the old database.
        try:
            item_id = int(document_id)
        except ValueError as exc:
            raise RuntimeError(
                f"Record key {document_id!r} is not a numeric TinyDB ID."
            ) from exc

        if item_id < 0:
            raise RuntimeError(
                f"Record {document_id!r} has a negative ID."
            )

        new_item = {
            "id": item_id,
            "item": item,
            "checked": False,
        }

        chats.setdefault(cid, []).append(new_item)

    return chats


def write_json_atomically(path: Path, data) -> None:
    """Write JSON through a temporary file and replace the destination."""

    path.parent.mkdir(parents=True, exist_ok=True)

    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
        text=True,
    )

    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(
                data,
                f,
                ensure_ascii=False,
                indent=4,
            )
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())

        os.replace(temporary_name, path)

    except Exception:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass
        raise


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Convert shoppingbot's old list.json into the "
            "shoppingbot2 per-chat database format."
        )
    )

    parser.add_argument(
        "input",
        type=Path,
        help="Old TinyDB list.json",
    )

    parser.add_argument(
        "output",
        type=Path,
        help=(
            "Output directory for shoppingbot2 lists "
            "(e.g. ./lists)"
        ),
    )

    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing list files.",
    )

    args = parser.parse_args()

    try:
        old_data = load_old_database(args.input)
        chats = convert(old_data)

        args.output.mkdir(parents=True, exist_ok=True)

        total_items = 0

        for cid, items in chats.items():
            filename = list_filename(cid)
            destination = args.output / filename

            if destination.exists() and not args.force:
                raise RuntimeError(
                    f"Refusing to overwrite existing file: "
                    f"{destination}\n"
                    f"Use --force if this is intentional."
                )

            write_json_atomically(destination, items)

            print(
                f"{cid}: {len(items)} item(s) -> {destination}"
            )

            total_items += len(items)

        print()
        print(
            f"Converted {total_items} item(s) "
            f"for {len(chats)} chat(s)."
        )

        return 0

    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    except OSError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())

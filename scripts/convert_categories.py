#!/usr/bin/env python3
"""Convert category-style INI files to JSON, with optional section remapping."""

import argparse
import json
import sys
from collections import OrderedDict
from pathlib import Path


def read_categories(filename):
    """Read sections and their bare-line entries from an INI-like file."""
    categories = OrderedDict()
    current_section = None

    with open(filename, "r", encoding="utf-8") as f:
        for line_number, raw_line in enumerate(f, start=1):
            line = raw_line.strip()

            # Ignore blank lines and comments.
            if not line or line.startswith(("#", ";")):
                continue

            # Section header.
            if line.startswith("[") and line.endswith("]"):
                current_section = line[1:-1].strip()
                if not current_section:
                    raise ValueError(
                        f"{filename}:{line_number}: empty section name"
                    )
                categories.setdefault(current_section, [])
                continue

            if current_section is None:
                raise ValueError(
                    f"{filename}:{line_number}: entry outside a section"
                )

            categories[current_section].append(line)

    return categories


def read_remapping(filename):
    """Read target sections and the source sections to combine."""
    return read_categories(filename)


def apply_remapping(categories, remapping):
    """Combine source sections into their mapped target sections."""
    result = OrderedDict()
    remapped_sources = set()

    for target, sources in remapping.items():
        result[target] = []
        for source in sources:
            if source not in categories:
                raise ValueError(
                    f"Remapping target {target!r} references "
                    f"unknown section {source!r}"
                )
            if source in remapped_sources:
                raise ValueError(
                    f"Section {source!r} is mapped more than once"
                )
            remapped_sources.add(source)
            result[target].extend(categories[source])

    # Keep sections that were not remapped.
    for section, entries in categories.items():
        if section not in remapped_sources:
            if section in result:
                raise ValueError(
                    f"Output section name collision: {section!r}"
                )
            result[section] = entries

    return result


def main():
    parser = argparse.ArgumentParser(
        description="Convert category-style INI files to JSON."
    )
    parser.add_argument("input", help="Input INI-like file")
    parser.add_argument(
        "-r", "--remap",
        help="Optional section remapping file"
    )
    parser.add_argument(
        "-o", "--output",
        help="Output JSON file (default: stdout)"
    )
    args = parser.parse_args()

    try:
        categories = read_categories(args.input)

        if args.remap:
            remapping = read_remapping(args.remap)
            categories = apply_remapping(categories, remapping)

        output = json.dumps(
            {"categories": categories},
            ensure_ascii=False,
            indent=2,
        )

        if args.output:
            Path(args.output).write_text(output + "\n", encoding="utf-8")
        else:
            print(output)

    except (OSError, ValueError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())

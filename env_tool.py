#!/usr/bin/env python3
"""Merge local env values into an example env file.

Reads a local configuration (e.g. ``env.local``) and an example file
(e.g. ``env.example``), replaces the value of every active ``KEY=VALUE``
line in the example with the matching value from the local file, and writes
the merged result to the output file, overwriting it if it exists.
Comment lines and commented-out keys are preserved untouched.

Keys present in the local file but not (active) in the example are ignored.

Usage:
    env-tool.py EXAMPLE LOCAL -o OUTPUT
"""

import argparse
import re
import sys
from pathlib import Path

KEY_RE = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*)=(.*)$")


def parse_local(path: Path) -> dict[str, str]:
    local: dict[str, str] = {}
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        m = KEY_RE.match(line)
        if m:
            local[m.group(1)] = m.group(2)
        else:
            print(f"env-tool: warning: skipping unparsed local line: {line}", file=sys.stderr)
    return local


def merge(example: Path, local_path: Path, output: Path) -> None:
    local = parse_local(local_path)
    example_text = example.read_text()

    out_lines = []
    for line in example_text.splitlines():
        m = KEY_RE.match(line)
        if m and m.group(1) in local:
            key = m.group(1)
            out_lines.append(f"{key}={local[key]}")
        else:
            out_lines.append(line)

    result = "\n".join(out_lines)
    if example_text.endswith("\n"):
        result += "\n"
    output.write_text(result)
    print(f"merged: {example} + {local_path} -> {output}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Overlay a local env file onto an example env file.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("example", type=Path, help="example file with defaults")
    parser.add_argument("local", type=Path, help="local configuration file (wins)")
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        required=True,
        help="output file to write (overwritten if it exists)",
    )
    args = parser.parse_args()

    for label, path in (("example", args.example), ("local", args.local)):
        if not path.is_file():
            print(f"env-tool: error: {label} file not found: {path}", file=sys.stderr)
            sys.exit(1)

    if args.output.resolve() == args.local.resolve():
        print(f"env-tool: error: output would overwrite the local file: {args.output}", file=sys.stderr)
        sys.exit(1)

    merge(args.example, args.local, args.output)


if __name__ == "__main__":
    main()

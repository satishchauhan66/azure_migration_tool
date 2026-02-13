#!/usr/bin/env python
# Author: Sa-tish Chauhan

"""
One-time script to add author/company headers to all Python files that don't have them.
Run from repo root: python azure_migration_tool/add_author_headers.py
"""
from pathlib import Path

HEADER_LINES = [
    "# Author: Satish Chauhan",
    "",
]
SKIP_DIRS = {"__pycache__", ".git", "venv", ".venv", "env", ".env"}


def repo_root() -> Path:
    p = Path(__file__).resolve().parent
    for _ in range(5):
        if (p / "azure_migration_tool").is_dir() or (p / ".git").is_dir():
            return p
        p = p.parent
    return Path(__file__).resolve().parent.parent


def should_skip(path: Path) -> bool:
    if path.suffix != ".py":
        return True
    for part in path.parts:
        if part in SKIP_DIRS:
            return True
    return False


def already_has_header(content: str) -> bool:
    first_lines = "\n".join(content.splitlines()[:8])
    return "Satish Chauhan" in first_lines and "Satish Chauhan" in first_lines


def add_header(content: str, path: Path) -> str:
    lines = content.splitlines(keepends=True)
    if not lines:
        return "\n".join(HEADER_LINES) + "\n"
    insert_at = 0
    if lines[0].strip().startswith("#!") and len(lines) >= 1:
        insert_at = 1
    header = "\n".join(HEADER_LINES) + "\n"
    if insert_at == 0:
        return header + content
    return "".join(lines[:insert_at]) + header + "".join(lines[insert_at:])


def main() -> None:
    root = repo_root()
    py_files = [p for p in root.rglob("*.py") if not should_skip(p)]
    updated = 0
    for path in sorted(py_files):
        content = path.read_text(encoding="utf-8", errors="replace")
        if already_has_header(content):
            continue
        new_content = add_header(content, path)
        path.write_text(new_content, encoding="utf-8", newline="")
        print(path.relative_to(root))
        updated += 1
    print(f"Added headers to {updated} file(s).")


if __name__ == "__main__":
    main()

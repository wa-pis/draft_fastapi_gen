from __future__ import annotations

import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
VERSION_FILE = ROOT / "VERSION"
PYPROJECT_FILE = ROOT / "pyproject.toml"
VERSION_RE = re.compile(r'^version = "(\d+)\.(\d+)\.(\d+)"$', re.MULTILINE)


def next_version(version: str, part: str) -> str:
    major, minor, patch = (int(piece) for piece in version.split("."))

    if part == "patch":
        patch += 1
    elif part == "minor":
        minor += 1
        patch = 0
    else:
        raise SystemExit("Usage: python scripts/bump_version.py [patch|minor]")

    return f"{major}.{minor}.{patch}"


def main() -> None:
    part = sys.argv[1] if len(sys.argv) == 2 else ""
    current = VERSION_FILE.read_text(encoding="utf-8").strip()
    updated = next_version(current, part)

    pyproject = PYPROJECT_FILE.read_text(encoding="utf-8")
    pyproject, replacements = VERSION_RE.subn(f'version = "{updated}"', pyproject, count=1)
    if replacements != 1:
        raise SystemExit("Could not update [project] version in pyproject.toml")

    VERSION_FILE.write_text(f"{updated}\n", encoding="utf-8")
    PYPROJECT_FILE.write_text(pyproject, encoding="utf-8")
    print(f"{current} -> {updated}")


if __name__ == "__main__":
    main()

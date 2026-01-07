#!/usr/bin/env python3

import importlib.metadata
from pathlib import Path

OUTPUT = Path("../NOTICE")
SEPARATOR = "-" * 64

# Build / tooling packages — never part of app redistribution
EXCLUDE_PACKAGES = {
    "setuptools",
    "pip",
    "wheel",
    "distutils",
}

NOTICE_NAMES = ("notice", "notice.txt", "notice.md")
LICENSE_CANDIDATES = (
    "license", "license.txt", "license.md",
    "copying", "copying.txt",
)

def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace")

def looks_like_apache2(text: str) -> bool:
    t = text.lower()
    return "apache license" in t and "version 2.0" in t

def is_apache2(dist: importlib.metadata.Distribution) -> bool:
    # First try license metadata
    meta = (
        (dist.metadata.get("License") or "") +
        " ".join(dist.metadata.get_all("Classifier", []) or [])
    ).lower()

    if "apache" in meta and ("2.0" in meta or "version 2.0" in meta):
        return True

    # Fallback: inspect license files shipped in the dist
    for f in dist.files or []:
        name = f.name.lower()
        if any(k in name for k in LICENSE_CANDIDATES):
            try:
                if looks_like_apache2(read_text(dist.locate_file(f))):
                    return True
            except Exception:
                pass

    return False

def find_notice_files(dist):
    notices = []
    for f in dist.files or []:
        name = f.name.lower()
        if name in NOTICE_NAMES or name.endswith("/notice") or name.endswith("/notice.txt"):
            notices.append(dist.locate_file(f))
    return notices

def main():
    sections = []
    sections.append("This product includes third-party software components.\n")

    for dist in importlib.metadata.distributions():
        name = (dist.metadata.get("Name") or "").lower()

        if name in EXCLUDE_PACKAGES:
            continue

        if not is_apache2(dist):
            continue

        notices = find_notice_files(dist)
        if not notices:
            continue

        for notice_path in notices:
            try:
                content = read_text(notice_path).strip()
            except Exception:
                continue

            if not content:
                continue

            sections.append(SEPARATOR)
            sections.append(dist.metadata.get("Name", "UNKNOWN"))
            sections.append(content)
            sections.append(SEPARATOR)
            sections.append("")

    if len(sections) <= 1:
        print("No Apache NOTICE files found.")
        return

    OUTPUT.write_text("\n".join(sections) + "\n", encoding="utf-8")
    print("NOTICE file generated successfully.")

if __name__ == "__main__":
    main()

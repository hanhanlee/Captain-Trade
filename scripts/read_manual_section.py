"""
read_manual_section.py

依章節標題關鍵字，從「使用說明手冊.md」擷取單一章節內容。

用法：
  python scripts/read_manual_section.py 回測模組
  python scripts/read_manual_section.py 資料管理
"""
from __future__ import annotations

import sys
from pathlib import Path

from build_manual_index import MANUAL_PATH, build_index


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    if len(sys.argv) < 2:
        print("請提供章節關鍵字，例如: python scripts/read_manual_section.py 回測模組")
        return 1

    query = sys.argv[1].strip().lower()
    index = build_index()
    sections = index["sections"]
    target = next((s for s in sections if query in s["title"].lower()), None)

    if not target:
        print(f"找不到章節: {sys.argv[1]}")
        return 1

    lines = MANUAL_PATH.read_text(encoding="utf-8").splitlines()
    chunk = lines[target["start_line"] - 1: target["end_line"]]

    print(f"# {target['title']}")
    print(f"# lines: {target['start_line']}-{target['end_line']}")
    print()
    print("\n".join(chunk))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

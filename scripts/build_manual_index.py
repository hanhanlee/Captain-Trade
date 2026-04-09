"""
build_manual_index.py

從「使用說明手冊.md」產生兩份索引：
1. ref/manual_index.json  機器可讀，供程式或 LLM 先讀索引再精準抓章節
2. ref/manual_index.md    人類可讀，方便快速瀏覽
"""
from __future__ import annotations

import json
import re
import shutil
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MANUAL_PATH = ROOT / "使用說明手冊.md"
INDEX_JSON_PATH = ROOT / "ref" / "manual_index.json"
INDEX_MD_PATH = ROOT / "ref" / "manual_index.md"
SECTIONS_DIR = ROOT / "ref" / "manual_sections"


HEADING_RE = re.compile(r"^(#{1,3})\s+(.+?)\s*$")
VERSION_RE = re.compile(r"^\>\s+\*\*版本：\*\*\s*(.+?)\s+\*\*\最後更新：\*\*\s*(.+?)\s*$")


def slugify_heading(text: str) -> str:
    text = text.strip().lower()
    text = text.replace("：", "").replace(":", "")
    text = text.replace("（", "").replace("）", "")
    text = text.replace("(", "").replace(")", "")
    text = text.replace("，", "").replace(",", "")
    text = text.replace("。", "").replace(".", "")
    text = text.replace("、", "")
    text = text.replace("／", "").replace("/", "")
    text = text.replace("？", "").replace("?", "")
    text = text.replace("！", "").replace("!", "")
    text = text.replace("　", " ").replace(" ", "-")
    text = re.sub(r"-{2,}", "-", text)
    return text.strip("-")


def safe_filename(text: str) -> str:
    name = slugify_heading(text)
    name = re.sub(r"[^0-9a-zA-Z\u4e00-\u9fff\-_]+", "_", name)
    name = re.sub(r"_+", "_", name)
    return name.strip("_") or "section"


def extract_preview(lines: list[str], start_idx: int, end_idx: int) -> str:
    for i in range(start_idx, min(end_idx, start_idx + 20)):
        line = lines[i].strip()
        if not line:
            continue
        if line.startswith("#"):
            continue
        if line == "---":
            continue
        if line.startswith(">"):
            line = line.lstrip("> ").strip()
        if line.startswith("- "):
            line = line[2:].strip()
        if re.match(r"^\d+\. ", line):
            line = re.sub(r"^\d+\. ", "", line)
        return line[:120]
    return ""


def build_index() -> dict:
    text = MANUAL_PATH.read_text(encoding="utf-8")
    lines = text.splitlines()

    version = ""
    last_updated = ""
    for line in lines[:10]:
        m = VERSION_RE.match(line)
        if m:
            version = m.group(1).strip()
            last_updated = m.group(2).strip()
            break

    headings: list[dict] = []
    for idx, line in enumerate(lines):
        m = HEADING_RE.match(line)
        if not m:
            continue
        level = len(m.group(1))
        title = m.group(2).strip()
        headings.append(
            {
                "level": level,
                "title": title,
                "anchor": slugify_heading(title),
                "start_line": idx + 1,
                "_start_idx": idx,
            }
        )

    for i, item in enumerate(headings):
        next_idx = len(lines)
        for candidate in headings[i + 1:]:
            if candidate["level"] <= item["level"]:
                next_idx = candidate["_start_idx"]
                break
        item["end_line"] = next_idx
        item["preview"] = extract_preview(lines, item["_start_idx"] + 1, next_idx)
        item["line_span"] = item["end_line"] - item["start_line"] + 1
        item["path"] = str(MANUAL_PATH)
        del item["_start_idx"]

    chapter_count = sum(1 for h in headings if h["level"] == 2)

    return {
        "document": {
            "title": "台股交易輔助工具 — 完整使用說明手冊",
            "path": str(MANUAL_PATH),
            "version": version,
            "last_updated": last_updated,
            "total_lines": len(lines),
            "chapter_count": chapter_count,
        },
        "usage_hint": [
            "先讀這份 index，確認要看的章節標題與行號。",
            "只擷取需要的章節，不要整份手冊一起讀。",
            "更新手冊後重新執行 scripts/build_manual_index.py。",
        ],
        "sections": headings,
    }


def write_json(index: dict) -> None:
    INDEX_JSON_PATH.write_text(
        json.dumps(index, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def write_markdown(index: dict) -> None:
    doc = index["document"]
    lines = [
        "# 使用說明手冊索引",
        "",
        f"- 文件: `{doc['title']}`",
        f"- 路徑: `{doc['path']}`",
        f"- 版本: `{doc['version']}`",
        f"- 最後更新: `{doc['last_updated']}`",
        f"- 總行數: `{doc['total_lines']}`",
        f"- 章節數: `{doc['chapter_count']}`",
        "",
        "## 使用方式",
        "",
    ]

    for hint in index["usage_hint"]:
        lines.append(f"- {hint}")

    lines.extend(["", "## 章節索引", ""])

    for section in index["sections"]:
        indent = "  " * (section["level"] - 1)
        preview = f" — {section['preview']}" if section["preview"] else ""
        lines.append(
            f"{indent}- L{section['start_line']}-L{section['end_line']} "
            f"`{section['title']}`{preview}"
        )

    INDEX_MD_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_section_files(index: dict) -> None:
    if SECTIONS_DIR.exists():
        shutil.rmtree(SECTIONS_DIR)
    SECTIONS_DIR.mkdir(parents=True, exist_ok=True)

    manual_lines = MANUAL_PATH.read_text(encoding="utf-8").splitlines()
    chapter_no = 0

    for section in index["sections"]:
        if section["level"] != 2:
            continue

        chapter_no += 1
        start = section["start_line"] - 1
        end = section["end_line"]
        content = "\n".join(manual_lines[start:end]).strip() + "\n"
        filename = f"{chapter_no:02d}_{safe_filename(section['title'])}.md"
        out_path = SECTIONS_DIR / filename
        out_path.write_text(content, encoding="utf-8")
        section["section_file"] = str(out_path)


def main() -> None:
    index = build_index()
    write_section_files(index)
    write_json(index)
    write_markdown(index)
    print(f"已產生: {INDEX_JSON_PATH}")
    print(f"已產生: {INDEX_MD_PATH}")
    print(f"已產生章節目錄: {SECTIONS_DIR}")


if __name__ == "__main__":
    main()

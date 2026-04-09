"""
sync_to_notion.py — 將使用說明手冊同步到 Notion 頁面

支援兩種模式：
1. 全量同步：整份手冊同步到同一頁，並在最前面附上章節索引
2. 單章同步：只同步指定章節到指定頁面，適合 Notion 分章維護

建議流程：
1. 先執行 scripts/build_manual_index.py
2. 全量同步首頁：python sync_to_notion.py
3. 單章同步某頁：python sync_to_notion.py --section "回測模組"
"""
from __future__ import annotations

import argparse
import json
import os
import re
import time
from pathlib import Path

import requests

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
PAGE_ID = os.getenv("NOTION_PAGE_ID")
FILE_PATH = Path("使用說明手冊.md")
INDEX_JSON_PATH = Path("ref/manual_index.json")

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}


def load_index() -> dict:
    if not INDEX_JSON_PATH.exists():
        raise FileNotFoundError(
            f"找不到 {INDEX_JSON_PATH}，請先執行 python scripts/build_manual_index.py"
        )
    return json.loads(INDEX_JSON_PATH.read_text(encoding="utf-8"))


def clear_page(page_id: str):
    url = f"https://api.notion.com/v1/blocks/{page_id}/children"
    while True:
        r = requests.get(url, headers=HEADERS)
        r.raise_for_status()
        data = r.json()
        blocks = data.get("results", [])
        if not blocks:
            break
        for block in blocks:
            requests.delete(
                f"https://api.notion.com/v1/blocks/{block['id']}",
                headers=HEADERS,
            )
        if not data.get("has_more"):
            break
        time.sleep(0.3)
    print("✅ 頁面已清空")


def rich_text(text: str) -> list:
    result = []
    pattern = re.compile(r"(\*\*[^*]+\*\*|`[^`]+`)")
    parts = pattern.split(text)
    for part in parts:
        if part.startswith("**") and part.endswith("**"):
            result.append(
                {
                    "type": "text",
                    "text": {"content": part[2:-2]},
                    "annotations": {"bold": True},
                }
            )
        elif part.startswith("`") and part.endswith("`"):
            result.append(
                {
                    "type": "text",
                    "text": {"content": part[1:-1]},
                    "annotations": {"code": True},
                }
            )
        elif part:
            result.append({"type": "text", "text": {"content": part}})
    return result if result else [{"type": "text", "text": {"content": ""}}]


def make_block(btype: str, text: str, **extra) -> dict:
    return {
        "object": "block",
        "type": btype,
        btype: {"rich_text": rich_text(text), **extra},
    }


def md_to_blocks(md_text: str) -> list:
    blocks = []
    lines = md_text.splitlines()
    i = 0
    in_code = False
    code_lines = []
    code_lang = ""

    while i < len(lines):
        line = lines[i]

        if line.startswith("```"):
            if not in_code:
                in_code = True
                code_lang = line[3:].strip() or "plain text"
                code_lines = []
            else:
                in_code = False
                blocks.append(
                    {
                        "object": "block",
                        "type": "code",
                        "code": {
                            "rich_text": [
                                {
                                    "type": "text",
                                    "text": {"content": "\n".join(code_lines)},
                                }
                            ],
                            "language": code_lang if code_lang else "plain text",
                        },
                    }
                )
                code_lines = []
            i += 1
            continue

        if in_code:
            code_lines.append(line)
            i += 1
            continue

        if re.match(r"^---+$", line.strip()):
            blocks.append({"object": "block", "type": "divider", "divider": {}})
            i += 1
            continue

        if line.startswith("### "):
            blocks.append(make_block("heading_3", line[4:].strip()))
        elif line.startswith("## "):
            blocks.append(make_block("heading_2", line[3:].strip()))
        elif line.startswith("# "):
            blocks.append(make_block("heading_1", line[2:].strip()))
        elif re.match(r"^\d+\. ", line):
            text = re.sub(r"^\d+\. ", "", line)
            blocks.append(make_block("numbered_list_item", text))
        elif line.startswith("- ") or line.startswith("* "):
            blocks.append(make_block("bulleted_list_item", line[2:]))
        elif line.startswith("> "):
            blocks.append(make_block("quote", line[2:]))
        elif line.startswith("|"):
            if re.match(r"^\|[\s\-\|:]+\|$", line):
                i += 1
                continue
            cells = [c.strip() for c in line.strip("|").split("|")]
            content = "　".join(cells)
            blocks.append(make_block("bulleted_list_item", content))
        elif line.strip():
            blocks.append(make_block("paragraph", line.strip()))

        i += 1

    return blocks


def append_blocks(page_id: str, blocks: list):
    url = f"https://api.notion.com/v1/blocks/{page_id}/children"
    total = len(blocks)
    for start in range(0, total, 100):
        batch = blocks[start:start + 100]
        r = requests.patch(url, headers=HEADERS, json={"children": batch})
        if r.status_code != 200:
            print(f"❌ 上傳失敗 (block {start}): {r.text}")
            r.raise_for_status()
        print(f"  上傳進度：{min(start + 100, total)}/{total} blocks")
        time.sleep(0.3)


def build_index_blocks(index: dict) -> list:
    doc = index["document"]
    blocks = [
        make_block("paragraph", f"**版本：** {doc['version']}"),
        make_block("paragraph", f"**最後更新：** {doc['last_updated']}"),
        make_block("divider", "") if False else {"object": "block", "type": "divider", "divider": {}},
        make_block("heading_2", "章節索引"),
    ]

    for section in index["sections"]:
        if section["level"] != 2:
            continue
        summary = (
            f"{section['title']} "
            f"(L{section['start_line']}-L{section['end_line']})"
        )
        if section.get("preview"):
            summary += f" — {section['preview']}"
        blocks.append(make_block("bulleted_list_item", summary))

    blocks.append({"object": "block", "type": "divider", "divider": {}})
    return blocks


def load_section_markdown(index: dict, query: str) -> tuple[dict, str]:
    target = next(
        (
            section
            for section in index["sections"]
            if section["level"] == 2 and query.lower() in section["title"].lower()
        ),
        None,
    )
    if not target:
        raise ValueError(f"找不到章節：{query}")

    section_file = target.get("section_file")
    if not section_file:
        raise ValueError("索引中沒有 section_file，請重新執行 build_manual_index.py")

    md_text = Path(section_file).read_text(encoding="utf-8")
    return target, md_text


def sync_full(page_id: str, index: dict):
    print(f"📖 讀取 {FILE_PATH} ...")
    md_text = FILE_PATH.read_text(encoding="utf-8")
    print(f"📚 使用手冊索引：{INDEX_JSON_PATH}")

    print("🔄 清空 Notion 頁面 ...")
    clear_page(page_id)

    print("🔨 建立首頁索引區塊 ...")
    index_blocks = build_index_blocks(index)

    print("🔨 轉換完整手冊 Markdown → Notion blocks ...")
    manual_blocks = md_to_blocks(md_text)
    blocks = index_blocks + manual_blocks
    print(f"  共 {len(blocks)} 個 block")

    print("🚀 上傳至 Notion ...")
    append_blocks(page_id, blocks)
    print("✅ 完整手冊同步完成")


def sync_single_section(page_id: str, index: dict, query: str):
    target, md_text = load_section_markdown(index, query)
    print(f"📖 同步單一章節：{target['title']}")
    print(f"📄 來源：{target['section_file']}")

    print("🔄 清空 Notion 頁面 ...")
    clear_page(page_id)

    preface = [
        make_block("paragraph", f"**章節：** {target['title']}"),
        make_block("paragraph", f"**來源行號：** L{target['start_line']}-L{target['end_line']}"),
        make_block("paragraph", "這是從完整使用說明手冊中抽出的單章內容，適合在 Notion 分章維護。"),
        {"object": "block", "type": "divider", "divider": {}},
    ]
    section_blocks = md_to_blocks(md_text)
    blocks = preface + section_blocks
    print(f"  共 {len(blocks)} 個 block")

    print("🚀 上傳至 Notion ...")
    append_blocks(page_id, blocks)
    print("✅ 單章同步完成")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--section",
        help="只同步指定章節，例如 --section 回測模組",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    if not NOTION_TOKEN or not PAGE_ID:
        print("❌ 找不到 NOTION_TOKEN 或 NOTION_PAGE_ID 環境變數")
        return
    if not FILE_PATH.exists():
        print(f"❌ 找不到檔案：{FILE_PATH}")
        return

    index = load_index()

    if args.section:
        sync_single_section(PAGE_ID, index, args.section)
    else:
        sync_full(PAGE_ID, index)


if __name__ == "__main__":
    main()

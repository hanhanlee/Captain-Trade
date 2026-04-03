"""
sync_to_notion.py — 將使用說明手冊.md 同步到 Notion 頁面

使用官方 notion-client（requests 直接呼叫），不依賴 md2notion。
支援：標題 h1/h2/h3、段落、項目清單、有序清單、程式碼區塊、分隔線、引用、表格
"""
import os
import re
import time
import requests

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
PAGE_ID = os.getenv("NOTION_PAGE_ID")
FILE_PATH = "使用說明手冊.md"

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}


# ── 清空頁面 ──────────────────────────────────────────────────

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


# ── Markdown → Notion Block 轉換 ──────────────────────────────

def rich_text(text: str) -> list:
    """將含 **bold** 和 `code` 的文字轉成 rich_text 陣列"""
    result = []
    pattern = re.compile(r'(\*\*[^*]+\*\*|`[^`]+`)')
    parts = pattern.split(text)
    for part in parts:
        if part.startswith("**") and part.endswith("**"):
            result.append({
                "type": "text",
                "text": {"content": part[2:-2]},
                "annotations": {"bold": True},
            })
        elif part.startswith("`") and part.endswith("`"):
            result.append({
                "type": "text",
                "text": {"content": part[1:-1]},
                "annotations": {"code": True},
            })
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

        # 程式碼區塊
        if line.startswith("```"):
            if not in_code:
                in_code = True
                code_lang = line[3:].strip() or "plain text"
                code_lines = []
            else:
                in_code = False
                blocks.append({
                    "object": "block",
                    "type": "code",
                    "code": {
                        "rich_text": [{"type": "text", "text": {"content": "\n".join(code_lines)}}],
                        "language": code_lang if code_lang else "plain text",
                    },
                })
                code_lines = []
            i += 1
            continue

        if in_code:
            code_lines.append(line)
            i += 1
            continue

        # 分隔線
        if re.match(r'^---+$', line.strip()):
            blocks.append({"object": "block", "type": "divider", "divider": {}})
            i += 1
            continue

        # 標題
        if line.startswith("### "):
            blocks.append(make_block("heading_3", line[4:].strip()))
        elif line.startswith("## "):
            blocks.append(make_block("heading_2", line[3:].strip()))
        elif line.startswith("# "):
            blocks.append(make_block("heading_1", line[2:].strip()))

        # 有序清單
        elif re.match(r'^\d+\. ', line):
            text = re.sub(r'^\d+\. ', '', line)
            blocks.append(make_block("numbered_list_item", text))

        # 項目清單
        elif line.startswith("- ") or line.startswith("* "):
            blocks.append(make_block("bulleted_list_item", line[2:]))

        # 引用
        elif line.startswith("> "):
            blocks.append(make_block("quote", line[2:]))

        # 表格（跳過分隔行，其餘轉為 bulleted list）
        elif line.startswith("|"):
            if re.match(r'^\|[\s\-\|:]+\|$', line):
                i += 1
                continue
            cells = [c.strip() for c in line.strip("|").split("|")]
            content = "　".join(cells)
            blocks.append(make_block("bulleted_list_item", content))

        # 一般段落
        elif line.strip():
            blocks.append(make_block("paragraph", line.strip()))

        i += 1

    return blocks


# ── 分批上傳（Notion 每次上限 100 個 block）────────────────────

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


# ── 主程式 ────────────────────────────────────────────────────

def sync_to_notion():
    if not NOTION_TOKEN or not PAGE_ID:
        print("❌ 找不到 NOTION_TOKEN 或 NOTION_PAGE_ID 環境變數")
        return

    if not os.path.exists(FILE_PATH):
        print(f"❌ 找不到檔案：{FILE_PATH}")
        return

    print(f"📖 讀取 {FILE_PATH} ...")
    with open(FILE_PATH, "r", encoding="utf-8") as f:
        md_text = f.read()

    print("🔄 清空 Notion 頁面 ...")
    clear_page(PAGE_ID)

    print("🔨 轉換 Markdown → Notion blocks ...")
    blocks = md_to_blocks(md_text)
    print(f"  共 {len(blocks)} 個 block")

    print("🚀 上傳至 Notion ...")
    append_blocks(PAGE_ID, blocks)

    print("✅ 同步完成！")


if __name__ == "__main__":
    sync_to_notion()

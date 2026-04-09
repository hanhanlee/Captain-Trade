# 手冊索引使用方式

這份工具的目標是：更新手冊或查手冊時，不需要每次都讀完整份 `使用說明手冊.md`。

## 建議流程

### 1. 手冊更新後，先重建索引

```bash
python scripts/build_manual_index.py
```

會更新這些檔案：

- `ref/manual_index.json`
- `ref/manual_index.md`
- `ref/manual_sections/*.md`

### 2. 查某一章內容時，不直接讀整份手冊

先看：

- `ref/manual_index.md`

再只抓需要的章節：

```bash
python scripts/read_manual_section.py 回測模組
python scripts/read_manual_section.py 資料管理
python scripts/read_manual_section.py 交易日誌
```

### 3. 同步到 Notion

整份同步到同一頁：

```bash
python sync_to_notion.py
```

只同步單一章節到指定頁面：

```bash
python sync_to_notion.py --section 回測模組
```

## 為什麼這樣比較省 token

- `manual_index.md/json` 很小，適合先讀索引
- `manual_sections/*.md` 是分章檔案，只拿需要的一章
- 不需要每次都把 8 萬多字的完整手冊送進上下文
- Notion 若採分章頁面，也可以只更新單章

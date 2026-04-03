import os
import requests
from md2notion.upload import upload
from notion_client import Client # 建議安裝 notion-client 處理刪除更穩定

# 取得環境變數
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
PAGE_ID = os.getenv("NOTION_PAGE_ID")
FILE_PATH = "使用說明手冊.md"

def clear_page(token, page_id):
    """清空 Notion 頁面內的所有內容"""
    headers = {
        "Authorization": f"Bearer {token}",
        "Notion-Version": "2022-06-28",
    }
    
    # 取得頁面下的所有 Block
    url = f"https://api.notion.com/v1/blocks/{page_id}/children"
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        blocks = response.json().get("results", [])
        for block in blocks:
            block_id = block["id"]
            del_url = f"https://api.notion.com/v1/blocks/{block_id}"
            requests.delete(del_url, headers=headers)
        print(f"🧹 已成功清空頁面內容 (共 {len(blocks)} 個區塊)")
    else:
        print(f"❌ 無法讀取頁面內容: {response.text}")

def sync_to_notion():
    if not NOTION_TOKEN or not PAGE_ID:
        print("❌ 錯誤：找不到 NOTION_TOKEN 或 NOTION_PAGE_ID 環境變數")
        return

    # 1. 先清空現有內容
    clear_page(NOTION_TOKEN, PAGE_ID)

    # 2. 使用 md2notion 上傳最新手冊
    try:
        # 這裡需要用到 notion-client 的 Client 物件
        from notion_client import Client
        notion = Client(auth=NOTION_TOKEN)
        
        with open(FILE_PATH, "r", encoding="utf-8") as f:
            # md2notion 的 upload 功能會處理標題、列表、表格轉換
            # 它會將內容上傳至指定的 page_id 作為 children
            upload(f, notion, PAGE_ID)
            
        print(f"🚀 【交易隊長】手冊同步成功！版本更新日期：2026-04-03")
    except Exception as e:
        print(f"❌ 同步過程發生錯誤: {e}")

if __name__ == "__main__":
    sync_to_notion()
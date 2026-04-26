import requests
import logging
import time
from typing import List, Dict, Optional

logging.basicConfig(level=logging.INFO, format='%(message)s')

def fetch_capital_etf_weights(etf_id: str) -> Optional[List[Dict]]:
    """
    獲取群益投信 ETF 每日持股權重
    支持: 00982A (內部代號 399), 00992A (內部代號 500)
    """
    # 1. 群益內部代號映射
    etf_map = {
        "00982A": "399",
        "00992A": "500",
        "00919":  "195"  # 順手幫你把熱門的 00919 也補上
    }
    
    internal_id = etf_map.get(etf_id)
    if not internal_id:
        logging.error(f"❌ 找不到 {etf_id} 對應的群益內部代號！")
        return None

    # 使用 Session 模擬瀏覽器行為，應對 Imperva 防火牆
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Origin": "https://www.capitalfund.com.tw"
    })

    try:
        # 第一步：先刷一下產品頁，讓伺服器種下必要的 Cookie
        landing_url = f"https://www.capitalfund.com.tw/etf/product/detail/{internal_id}/buyback"
        session.get(landing_url, timeout=10)
        
        # 稍微緩衝，模擬人類閱讀網頁
        time.sleep(1)

        # 第二步：呼叫真正的資料 API
        api_url = "https://www.capitalfund.com.tw/CFWeb/api/etf/buyback"
        payload = {
            "fundId": internal_id,
            "date": None  # 抓最新
        }
        
        response = session.post(api_url, json=payload, timeout=15)
        response.raise_for_status()
        json_data = response.json()
        
        # 依照 Preview 看到的結構進行解析
        # 結構: data -> stocks -> list
        data_node = json_data.get("data", {})
        stock_list = data_node.get("stocks", [])
        
        if not stock_list:
            logging.warning(f"⚠️ {etf_id} 查無持股明細資料。")
            return None

        # 取得資料日期（從 API 內的 date1 欄位拿，格式通常是 2026-04-27）
        raw_date = data_node.get("pcf", {}).get("date1", "")
        clean_date = raw_date.split(' ')[0].replace('-', '') if raw_date else "unknown"

        result_list = []
        for item in stock_list:
            # 依照抓包到的欄位名稱: stocNo, stocName, weight
            code = item.get("stocNo")
            name = item.get("stocName")
            weight = item.get("weight")
            
            if not code or weight is None:
                continue
                
            result_list.append({
                "date": clean_date,
                "etf_id": etf_id,
                "stock_id": str(code).strip(),
                "stock_name": str(name).strip(),
                "weight": float(weight)
            })
            
        return result_list

    except Exception as e:
        logging.error(f"❌ 抓取群益 ETF {etf_id} 失敗: {e}")
        return None

# ==========================================
# 測試執行區塊
# ==========================================
if __name__ == "__main__":
    # 我們同時測試兩檔！
    target_etfs = ["00982A", "00992A"]
    
    for etf in target_etfs:
        print(f"\n🚀 正在處理: {etf}...")
        data = fetch_capital_etf_weights(etf)
        
        if data:
            print(f"✅ 成功！取得 {len(data)} 檔成分股。日期: {data[0]['date']}")
            print("前 3 大持股：")
            # 依權重排序
            sorted_data = sorted(data, key=lambda x: x['weight'], reverse=True)
            for i, item in enumerate(sorted_data[:3], 1):
                print(f"  {i}. {item['stock_id']} {item['stock_name']}: {item['weight']}%")
        
        # 禮貌爬蟲：間隔 2 秒抓下一檔
        time.sleep(2)
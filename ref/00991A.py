import requests
import logging
from typing import List, Dict, Optional

logging.basicConfig(level=logging.INFO, format='%(message)s')

def fetch_fuhhwa_etf_weights(etf_id: str, target_date: str) -> Optional[List[Dict]]:
    """
    獲取復華投信 ETF 每日持股權重
    支持: 00991A (內部代號 ETF23)
    """
    etf_map = {
        "00991A": "ETF23",
        # 未來若有其他復華 ETF 可在此擴增
    }
    
    internal_id = etf_map.get(etf_id)
    if not internal_id:
        logging.error(f"❌ 找不到 {etf_id} 對應的復華內部代號！")
        return None

    # 將 YYYY-MM-DD 轉為 YYYY/MM/DD
    api_date = target_date.replace('-', '/')

    url = f"https://www.fhtrust.com.tw/api/assets"
    params = {
        "fundID": internal_id,
        "qDate": api_date
    }

    headers = {
        "Accept": "*/*",
        "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": "https://www.fhtrust.com.tw/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36"
    }

    try:
        response = requests.get(url, params=params, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        # 依照剛才解析的結構：result[0] -> detail
        results = data.get("result", [])
        if not results:
            logging.warning(f"⚠️ {etf_id} 查無結果資料 (可能非交易日)。")
            return None
            
        details = results[0].get("detail", [])
        if not details:
            logging.warning(f"⚠️ {etf_id} 查無股票明細 (detail為空)。")
            return None

        result_list = []
        for item in details:
            # 復華有區分資產類別，我們只要「股票」
            if item.get("ftype") != "股票":
                continue
                
            code = item.get("stockid")
            name = item.get("stockname")
            
            # 直接鎖定正確的權重欄位
            weight_raw = item.get("prate_addaccint", "0")
            
            if not code:
                continue
                
            # 處理帶有 '%' 的字串，轉換為浮點數
            try:
                if isinstance(weight_raw, str):
                    weight_val = float(weight_raw.replace('%', '').strip())
                else:
                    weight_val = float(weight_raw)
            except ValueError:
                weight_val = 0.0

            result_list.append({
                "date": target_date.replace('-', ''), # 輸出如: 20260423
                "etf_id": etf_id,
                "stock_id": str(code).strip(),
                "stock_name": str(name).strip(),
                "weight": weight_val
            })
            
        return result_list

    except Exception as e:
        logging.error(f"❌ 抓取復華 ETF {etf_id} 失敗: {e}")
        return None

# ==========================================
# 測試執行區塊
# ==========================================
if __name__ == "__main__":
    test_etf = "00991A"
    # 上次截圖顯示 2026/04/23 有資料
    test_date = "2026-04-23" 
    
    print(f"🚀 正在處理復華投信: {test_etf}...")
    data = fetch_fuhhwa_etf_weights(test_etf, test_date)
    
    if data:
        print(f"✅ 成功！取得 {len(data)} 檔成分股。")
        # 如果權重是 0，代表我們觸發了盲盒
        if data[0]['weight'] == 0.0:
            print("⚠️ 權重欄位名稱未知，請查看上方盲盒內容替換代碼。")
        else:
            print("前 5 大持股：")
            sorted_data = sorted(data, key=lambda x: x['weight'], reverse=True)
            for i, item in enumerate(sorted_data[:5], 1):
                print(f"  {i}. {item['stock_id']} {item['stock_name']}: {item['weight']}%")
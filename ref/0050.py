import requests
import logging
from typing import List, Dict, Optional

# 設定基本的 logging 格式
logging.basicConfig(level=logging.INFO, format='%(message)s')

def fetch_yuanta_etf_weights(etf_id: str, target_date: str) -> Optional[List[Dict]]:
    """
    獲取元大投信 ETF 每日持股權重
    
    :param etf_id: ETF 代號 (例如: '0050', '0056')
    :param target_date: 查詢日期，格式 'YYYYMMDD' (例如 '20260424')
    :return: 格式化後的持股清單，若失敗則回傳 None
    """
    url = "https://etfapi.yuantaetfs.com/ectranslation/api/bridge"
    
    params = {
        "APIType": "ETFAPI",
        "CompanyName": "YUANTAFUNDS",
        "PageName": f"/tradeInfo/pcf/{etf_id}",
        "DeviceId": "e7a29639-f413-493c-96f0-d6d4cfbedf4e",
        "FuncId": "PCF/Daily",
        "AppName": "ETF",
        "Device": "3",
        "Platform": "ETF",
        "ticker": etf_id,
        "date": target_date 
    }

    headers = {
        "accept": "application/json, text/plain, */*",
        "origin": "https://www.yuantaetfs.com",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36"
    }

    try:
        response = requests.get(url, params=params, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        # 依照剛才開箱的結構，精準定位到 StockWeights
        fund_weights = data.get("FundWeights", {})
        stock_weights = fund_weights.get("StockWeights", [])
        
        if not stock_weights:
            logging.warning(f"⚠️ {etf_id} 在 {target_date} 沒有持股資料 (可能非交易日或資料已清除)。")
            return None

        result_list = []
        for item in stock_weights:
            code = item.get("code")
            weight = item.get("weights")
            
            # 過濾掉沒有代號或沒有權重的無效資料
            if not code or weight is None:
                continue
                
            result_list.append({
                "date": target_date,
                "etf_id": etf_id,
                "stock_id": code,
                "stock_name": item.get("name", ""),
                "weight": float(weight)
            })
            
        return result_list

    except requests.exceptions.RequestException as e:
        logging.error(f"❌ API 網路請求失敗 ({etf_id}): {e}")
        return None
    except Exception as e:
        logging.error(f"❌ 解析發生未知錯誤 ({etf_id}): {e}")
        return None

# ==========================================
# 測試執行區塊
# ==========================================
if __name__ == "__main__":
    test_etf = "0050"
    test_date = "20260424" # 記得使用 YYYYMMDD 格式
    
    print(f"🚀 開始抓取元大 {test_etf} 權重資料...")
    weights_data = fetch_yuanta_etf_weights(test_etf, test_date)
    
    if weights_data:
        print(f"✅ 成功抓取！共取得 {len(weights_data)} 檔成分股。")
        print("前 5 大持股權重：")
        # 稍微排版印出前 5 名
        for i, w in enumerate(weights_data[:5], 1):
            print(f"  {i}. {w['stock_id']} {w['stock_name']:<6}: {w['weight']}%")
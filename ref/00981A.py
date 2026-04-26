import requests
import logging
from typing import List, Dict, Optional

logging.basicConfig(level=logging.INFO, format='%(message)s')

def fetch_uni_etf_weights(etf_id: str, target_date: str) -> Optional[List[Dict]]:
    """
    獲取統一投信 ETF 每日持股權重
    """
    etf_map = {
        "00981A": "61YTW",
    }
    
    internal_code = etf_map.get(etf_id)
    if not internal_code:
        logging.error(f"❌ 找不到 {etf_id} 對應的統一投信內部代號！")
        return None

    try:
        y, m, d = target_date.split('-')
        minguo_y = int(y) - 1911
        api_date = f"{minguo_y}/{m}/{d}"
    except ValueError:
        logging.error(f"❌ 日期格式錯誤，請使用 YYYY-MM-DD")
        return None

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
        "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
    })

    try:
        pcf_page_url = "https://www.ezmoney.com.tw/ETF/Transaction/PCF"
        session.get(pcf_page_url, timeout=10)

        api_url = "https://www.ezmoney.com.tw/ETF/Transaction/GetPCF"
        payload = {
            "fundCode": internal_code,
            "date": api_date,
            "specificDate": True
        }
        
        post_headers = {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Content-Type": "application/json; charset=UTF-8",
            "Origin": "https://www.ezmoney.com.tw",
            "Referer": pcf_page_url,
            "X-Requested-With": "XMLHttpRequest"
        }

        response = session.post(api_url, json=payload, headers=post_headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        assets = data.get("asset", [])
        stock_details = []
        
        for asset in assets:
            if asset.get("AssetName") == "股票":
                stock_details = asset.get("Details", [])
                break
                
        if not stock_details:
            logging.warning(f"⚠️ {etf_id} 在 {target_date} 沒有股票持股資料。")
            return None

        result_list = []
        for item in stock_details:
            # 🌟 換上正確的欄位名稱
            code = item.get("DetailCode")
            name = item.get("DetailName")
            weight = item.get("NavRate")
            
            if not code or weight is None:
                continue
                
            clean_name = str(name).split('.')[0].strip() if name else ""

            result_list.append({
                "date": target_date.replace('-', ''),
                "etf_id": etf_id,
                "stock_id": str(code).strip(),
                "stock_name": clean_name,
                "weight": float(weight)
            })
            
        return result_list

    except requests.exceptions.RequestException as e:
        logging.error(f"❌ 網路連線錯誤 ({etf_id}): {e}")
        return None
    except Exception as e:
        logging.error(f"❌ 解析發生未知錯誤 ({etf_id}): {e}")
        return None

if __name__ == "__main__":
    test_etf = "00981A"
    test_date = "2026-04-24" 
    
    print(f"🚀 開始抓取統一 {test_etf} 於 {test_date} 的權重資料...")
    weights_data = fetch_uni_etf_weights(test_etf, test_date)
    
    if weights_data:
        print(f"✅ 成功抓取！共取得 {len(weights_data)} 檔成分股。")
        print("前 5 大持股權重：")
        sorted_weights = sorted(weights_data, key=lambda x: x['weight'], reverse=True)
        for i, w in enumerate(sorted_weights[:5], 1):
            print(f"  {i}. {w['stock_id']:<8} {w['stock_name']:<15}: {w['weight']}%")
import os
import time
import logging
import importlib.util

logging.basicConfig(level=logging.INFO, format='%(message)s')

def load_function(file_name, func_name):
    """動態載入爬蟲腳本"""
    try:
        file_path = os.path.join(os.getcwd(), file_name)
        if not os.path.exists(file_path):
            logging.warning(f"⚠️ 找不到檔案: {file_name}")
            return None
            
        spec = importlib.util.spec_from_file_location("dynamic_module", file_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return getattr(module, func_name)
    except Exception as e:
        logging.error(f"❌ 載入 {file_name} 失敗: {e}")
        return None

def main():
    # 🌟 設定一個「歷史日期」來驗證群益投信是否能穿越時空
    target_date = "2026-04-10" 
    
    print(f"🚀 啟動歷史資料抓取測試 - 目標日期: {target_date}")
    print("-" * 50)
    
    # 掛載你的武器庫
    scrapers = {
        "0050":   load_function("0050.py", "fetch_yuanta_etf_weights"),
        "00981A": load_function("00981A.py", "fetch_uni_etf_weights"),
        "00982A": load_function("00982A_00992A.py", "fetch_capital_etf_weights"),
        "00992A": load_function("00982A_00992A.py", "fetch_capital_etf_weights"),
        "00991A": load_function("00991A.py", "fetch_fuhhwa_etf_weights")
    }

    for etf_id, fetch_func in scrapers.items():
        if fetch_func is None:
            continue
            
        print(f"\n🔄 正在測試: {etf_id} (查詢日期: {target_date})...")
        
        try:
            # 傳入歷史日期給爬蟲
            data = fetch_func(etf_id, target_date)
        except TypeError:
            # 如果某支爬蟲還沒支援 target_date 參數，就先不傳日期
            data = fetch_func(etf_id)
            
        if data:
            # 驗證 API 實際回傳的日期，確保不是回傳最新的一天
            actual_date = data[0]['date']
            print(f"✅ 成功！取得 {len(data)} 檔成分股。API 實際回傳日期: {actual_date}")
            
            print("   前 3 大持股預覽：")
            sorted_data = sorted(data, key=lambda x: x['weight'], reverse=True)
            for i, item in enumerate(sorted_data[:3], 1):
                print(f"     {i}. {item['stock_id']:<6} {item['stock_name']:<10}: {item['weight']}%")
        else:
            print("⚠️ 抓取失敗或該日無資料。")
            
        time.sleep(2)

if __name__ == "__main__":
    main()
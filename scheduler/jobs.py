"""
排程任務
盤後自動執行：選股掃描 + 持股警示 → LINE 推播

執行方式：python scheduler/jobs.py
或在背景常駐：搭配 run_scheduler.py
"""
import time
import logging
from datetime import datetime, time as dtime

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from data.finmind_client import get_stock_list, get_daily_price
from modules.scanner import run_scan
from modules.portfolio import run_portfolio_check
from modules.journal import get_all_trades, calc_performance
from db.database import init_db, get_session
from db.models import Portfolio
from notifications.line_notify import send_multicast, send_scan_results

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def job_daily_scan(top_n: int = 5, scan_count: int = 200):
    """
    盤後選股掃描任務（預設掃描前 200 檔）
    每日 14:45 執行（盤後 15 分鐘）
    """
    logger.info("開始盤後選股掃描...")
    try:
        stock_list = get_stock_list()
        if stock_list.empty:
            logger.warning("無法取得股票清單")
            return

        sample_ids = stock_list["stock_id"].head(scan_count).tolist()
        price_data = {}
        for sid in sample_ids:
            try:
                df = get_daily_price(sid, days=120)
                if not df.empty:
                    price_data[sid] = df
                time.sleep(0.05)
            except Exception:
                pass

        result_df, _, _ = run_scan(price_data=price_data, stock_info=stock_list)

        if result_df.empty:
            send_multicast("📊 今日選股雷達：無符合條件的股票")
        else:
            results = result_df.to_dict("records")
            send_scan_results(results, top_n=top_n)
            logger.info(f"選股完成，找到 {len(result_df)} 檔，已推播前 {top_n} 名")

    except Exception as e:
        logger.error(f"選股任務失敗：{e}")
        send_multicast(f"⚠️ 選股掃描失敗：{e}")


def job_portfolio_check():
    """
    持股警示任務
    每日 13:30（盤中）、14:35（盤後）執行
    """
    logger.info("開始持股警示檢查...")
    try:
        with get_session() as sess:
            rows = sess.query(Portfolio).all()
            holdings = [{
                "stock_id": r.stock_id,
                "stock_name": r.stock_name or "",
                "shares": r.shares,
                "cost_price": r.cost_price,
                "stop_loss": r.stop_loss,
                "take_profit": r.take_profit,
            } for r in rows]

        if not holdings:
            logger.info("持股清單為空，跳過")
            return

        price_data = {}
        for h in holdings:
            try:
                df = get_daily_price(h["stock_id"], days=90)
                if not df.empty:
                    price_data[h["stock_id"]] = df
                time.sleep(0.05)
            except Exception:
                pass

        stats_list, all_alerts = run_portfolio_check(holdings, price_data)

        if not all_alerts:
            logger.info("持股無警示")
            return

        from modules.portfolio import AlertLevel
        lines = ["💼 持股監控警示"]
        for a in all_alerts[:8]:
            emoji = "🔴" if a.level == AlertLevel.DANGER else "🟡"
            lines.append(f"\n{emoji} {a.stock_id} {a.stock_name}")
            lines.append(f"   {a.reason}")
            lines.append(f"   現價 {a.current_price} 元  損益 {a.pnl_pct:+.1f}%")

        send_multicast("\n".join(lines))
        logger.info(f"推播 {len(all_alerts)} 則警示")

    except Exception as e:
        logger.error(f"持股警示任務失敗：{e}")


def job_intraday_monitor():
    """
    盤中持股監控任務
    週一到週五 09:00–13:30，每分鐘執行一次。
    CronTrigger 設 hour="9-13"，函式內自行截止在 13:30。
    """
    now = datetime.now().time()
    if now > dtime(13, 30):
        return

    from modules.intraday_monitor import run_intraday_check
    try:
        sent = run_intraday_check()
        if sent:
            logger.info(f"盤中監控：推播 {sent} 則警示")
    except Exception as e:
        logger.error(f"盤中監控任務失敗：{e}")


def job_weekly_holding_shares():
    """
    每週五 22:00 更新持股分佈資料（大戶/散戶比例）
    只更新 Portfolio 中有的股票，抓近 180 天。
    """
    logger.info("開始更新持股分佈資料...")
    try:
        from data.finmind_client import fetch_holding_shares_from_finmind
        from db.holding_shares_cache import save_holding_shares
        from datetime import date, timedelta

        with get_session() as sess:
            rows = sess.query(Portfolio).all()
            stock_ids = list({r.stock_id for r in rows})

        if not stock_ids:
            logger.info("持股清單為空，跳過持股分佈更新")
            return

        end_date = date.today().isoformat()
        start_date = (date.today() - timedelta(days=180)).isoformat()
        updated = 0

        for sid in stock_ids:
            try:
                data = fetch_holding_shares_from_finmind(sid, start_date=start_date, end_date=end_date)
                if data:
                    save_holding_shares(data)
                    updated += 1
                time.sleep(0.5)
            except Exception as e:
                logger.warning(f"持股分佈更新失敗 {sid}: {e}")

        logger.info(f"持股分佈更新完成，共更新 {updated}/{len(stock_ids)} 檔")
    except Exception as e:
        logger.error(f"持股分佈更新任務失敗：{e}")


def job_weekly_performance():
    """
    每週五收盤後推播績效摘要
    """
    logger.info("產生每週績效摘要...")
    try:
        df = get_all_trades()
        perf = calc_performance(df)
        if not perf:
            return

        msg = (
            f"📈 本週績效摘要\n"
            f"\n勝率：{perf['win_rate']}%（{perf['win_trades']}勝/{perf['loss_trades']}敗）"
            f"\n盈虧比：{perf['profit_factor']}"
            f"\n累積損益：{perf['total_pnl']:+,.0f} 元"
            f"\n最佳交易：{perf['best_trade']:+,.0f} 元"
            f"\n最差交易：{perf['worst_trade']:+,.0f} 元"
        )
        send_multicast(msg)
    except Exception as e:
        logger.error(f"週報任務失敗：{e}")


def run_scheduler():
    """啟動排程器（blocking，適合獨立程序常駐）"""
    init_db()
    scheduler = BlockingScheduler(timezone="Asia/Taipei")

    # 盤後選股：週一到週五 14:45
    scheduler.add_job(
        job_daily_scan,
        CronTrigger(day_of_week="mon-fri", hour=14, minute=45, timezone="Asia/Taipei"),
        id="daily_scan",
        name="盤後選股掃描",
    )

    # 盤中持股警示：週一到週五 13:30
    scheduler.add_job(
        job_portfolio_check,
        CronTrigger(day_of_week="mon-fri", hour=13, minute=30, timezone="Asia/Taipei"),
        id="portfolio_mid",
        name="盤中持股警示",
    )

    # 盤中分K監控：週一到週五 09:00–13:30，每分鐘一次
    scheduler.add_job(
        job_intraday_monitor,
        CronTrigger(day_of_week="mon-fri", hour="9-13", minute="*", timezone="Asia/Taipei"),
        id="intraday_monitor",
        name="盤中分K監控",
    )

    # 盤後持股警示：週一到週五 14:35
    scheduler.add_job(
        job_portfolio_check,
        CronTrigger(day_of_week="mon-fri", hour=14, minute=35, timezone="Asia/Taipei"),
        id="portfolio_close",
        name="盤後持股警示",
    )

    # 每週五績效摘要：15:10
    scheduler.add_job(
        job_weekly_performance,
        CronTrigger(day_of_week="fri", hour=15, minute=10, timezone="Asia/Taipei"),
        id="weekly_report",
        name="週報",
    )

    # 每週五持股分佈更新：22:00
    scheduler.add_job(
        job_weekly_holding_shares,
        CronTrigger(day_of_week="fri", hour=22, minute=0, timezone="Asia/Taipei"),
        id="weekly_holding_shares",
        name="週五持股分佈更新",
    )

    logger.info("排程器啟動，等待任務觸發...")
    logger.info("排程時間：")
    logger.info("  盤中分K監控：週一至週五 09:00–13:30（每分鐘）")
    logger.info("  盤後選股：週一至週五 14:45")
    logger.info("  持股警示：週一至週五 13:30 / 14:35")
    logger.info("  週績效報告：週五 15:10")
    logger.info("  持股分佈更新：週五 22:00")

    try:
        scheduler.start()
    except KeyboardInterrupt:
        logger.info("排程器已停止")


if __name__ == "__main__":
    run_scheduler()

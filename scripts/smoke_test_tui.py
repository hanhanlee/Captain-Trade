# TUI headless 煙霧測試（唯讀：只跑狀態/健康刷新，不按任何按鈕）
import asyncio

from srock.config import load_config
from srock.tui import SrockTui


async def main():
    app = SrockTui(load_config(), profile="full")
    async with app.run_test(size=(110, 34)) as pilot:
        await pilot.pause(1.0)   # 讓 on_mount 的 status/health worker 跑完
        await pilot.pause(1.0)
        urlbar = app.query_one("#urlbar").render()
        rows = app.query(".service-row")
        print("rows:", len(rows))
        print("urlbar:", str(urlbar)[:90])
        for row in rows:
            dot = row.query_one(".dot")
            detail = row.query_one(".svc-detail").render()
            print(f"  {row.kind:<10} dot={dot.render()} {str(detail)[:60]}")
        health = str(app.query_one("#health").render())
        print("health panel:")
        for line in health.splitlines():
            print("   ", line)
    print("SMOKE OK")


asyncio.run(main())


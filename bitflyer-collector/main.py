import time
import asyncio
from datetime import datetime, timezone

import httpx
import psycopg

# 設定
DB_CONFIG = {
    "dbname": "trading_db",
    "user": "bot_user",
    "password": "gP2kh7zl4Wy8",
    "host": "db", # Docker Compose内ならサービス名
    "port": "5432"
}
SYMBOL = "FX_BTC_JPY"
INTERVAL = "1m"

db_queue = asyncio.Queue()

async def db_writer():
    """DB書き込みを一手に引き受ける専用タスク"""
    while True:
        data = await db_queue.get()
        # ここで DB 書き込みを実行（to_thread を使うと安全）
        await asyncio.to_thread(save_to_db, data)
        db_queue.task_done()

async def stream_listener():
    while True:
        await asyncio.sleep(100)

async def periodic_fetcher():
    while True:
        # 10分おきの取得
        data = await async_fetch()
        if data:
            await db_queue.put(data)
        await asyncio.sleep(600)

async def blank_fetcher():
    last_newest_blank_time = 0
    await asyncio.sleep(30)
    while True:
        newest_blank_time = await asyncio.to_thread(search_newest_blank_time)
        if newest_blank_time:
            print(f"最新の空白時間: {newest_blank_time}")
            to_fetch = True
            if last_newest_blank_time and newest_blank_time == last_newest_blank_time:
                print("空白時間に変化なし。スキップします。")
                to_fetch = False

            if to_fetch:
                data = await async_fetch(before=newest_blank_time - 60*1000) # 空白時間の終了時刻の1分前を指定してAPIを呼び出す
                if data:
                    await db_queue.put(data)
                last_newest_blank_time = newest_blank_time

        await asyncio.sleep(600)

def search_newest_blank_time() -> int:
    # DB から最新の空白時間を検索する
    # 返すのは空白時間の終了時刻（ミリ秒）

    sql = """
    SELECT time
    FROM (
        SELECT time,
               LAG(time) OVER (ORDER BY time) AS prev_time
        FROM ohlcv_data
        WHERE symbol = %s AND interval = %s
    ) t
    WHERE time - prev_time > INTERVAL '50 minute 30 seconds'
    and time < '2026-04-22 12:00:00+00'
    ORDER BY time DESC
    LIMIT 1;
    """

    try:
        with psycopg.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, ("bitflyer:" + SYMBOL, INTERVAL))
                row = cur.fetchone()
                if row:
                    return int(row[0].timestamp() * 1000)
    except Exception as e:
        print(f"search_newest_blank_time エラー: {e}")

    return 0

def save_to_db(data):
    with psycopg.connect(**DB_CONFIG) as conn:
        try:
            # 有効なデータ（open/highが存在する行）のみ抽出
            valid_data = [row for row in data if row[1] and row[2]]
            if not valid_data:
                print(f"[{datetime.now()}] 有効なデータがありませんでした。")
                return

            with conn.cursor() as cur:
                upsert_sql = """
                INSERT INTO ohlcv_data (time, symbol, interval, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (time, symbol, interval) 
                DO UPDATE SET 
                    open = EXCLUDED.open, high = EXCLUDED.high, 
                    low = EXCLUDED.low, close = EXCLUDED.close, 
                    volume = EXCLUDED.volume;
                """

                cnt = 0
                for i, row in enumerate(valid_data):
                    ts = datetime.fromtimestamp(row[0] / 1000, tz=timezone.utc)
                    params = (ts, "bitflyer:" + SYMBOL, INTERVAL, row[1], row[2], row[3], row[4], row[5])
                    cur.execute(upsert_sql, params)
                    cnt += cur.rowcount
                
                conn.commit()
                print(f"[{datetime.now()}] {cnt}件のデータを同期しました。")

        except Exception as e:
            print(f"エラー発生: {e}")
            print(e.args)
            conn.rollback()
        finally:
            conn.close()

async def async_fetch(before: int = 0, after_min: int = 0):
    after = 0
    if before == 0:
        now_ms = int(time.time() * 1000)
        before = now_ms + 5 * 60 * 1000 # 5分後

    # before を1分単位に丸める（APIの仕様に合わせるため）
    before = (before // 60000) * 60000

    url = f"https://lightchart.bitflyer.com/api/ohlc?symbol={SYMBOL}&period=m&before={before}&type=full&grouping=1"
    if after_min > 0:
        after = before - after_min * 60 * 1000
        url += f"&after={after}"

    # afterとbeforeを指定すれば特定の期間のデータも取得可能っぽいがどうも怪しい
    # 手元で実験したところ、afterを指定してもAPIが正しく動作せず、結局はbeforeの値だけでデータが返ってきているように見える
    # なので、afterは指定せずにbeforeだけでAPIを呼び出す形にしている
    # 件数は、beforeの値に応じて変わるが、特定の時刻を起点として、そこから before までのデータが返ってくるように見える
    # （ただし、APIの挙動が不安定なため、常に同じ結果が得られるわけではない）
    # ここでいう「特定の時刻」は毎日 21:05(JST) だが、これは計測時点のAPIの挙動であり、信頼すべき情報ではない

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36',
        'Referer': 'https://lightning.bitflyer.com/',
    }

    # 非同期用のクライアントを作成（セッションのように機能します）
    async with httpx.AsyncClient(follow_redirects=True) as client:
        try:
            # await をつけるだけで非同期リクエストになる
            print(f"APIリクエスト: {url}")
            response = await client.get(url, headers=headers, timeout=10.0)
            
            # ステータスコードのチェック
            if response.status_code != 200:
                print(f"APIリクエスト失敗: {response.status_code} - {response.text}")
                return None
                
            return response.json()  # 成功時
            
        except httpx.RequestError as e:
            print(f"通信エラーが発生しました: {e}")
            return None

async def main():
    tasks = [
        asyncio.create_task(db_writer()),
        asyncio.create_task(stream_listener()),
        asyncio.create_task(periodic_fetcher()),
        asyncio.create_task(blank_fetcher())
    ]
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        print("タスクがキャンセルされました。")
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        print("全てのタスクが安全に終了しました。")

if __name__ == "__main__":
    asyncio.run(main())

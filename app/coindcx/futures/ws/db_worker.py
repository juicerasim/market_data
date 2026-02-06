from queue import Queue
from threading import Thread
from app.repository.cdx_repo import insert_cdx_candle

# ⭐ global queue
CDX_DB_QUEUE = Queue(maxsize=10000)


def db_worker():
    print("🚀 DB worker started")

    while True:
        payload, is_closed = CDX_DB_QUEUE.get()

        try:
            insert_cdx_candle(payload, is_closed)
        except Exception as e:
            print("❌ Worker insert error:", e)

        CDX_DB_QUEUE.task_done()


def start_db_worker():
    t = Thread(target=db_worker, daemon=True)
    t.start()

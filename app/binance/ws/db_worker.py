import signal
from queue import Empty
from collections import defaultdict

from app.binance.ws.queue import candle_queue
from app.binance.repo import insert_candle
from app.config import TIMEFRAMES

RUNNING = True

# --------------------------------------------------
# HTF Aggregation State
# Structure:
# {
#   "15m": {
#       "BTCUSDT": current_bucket_payload
#   }
# }
# --------------------------------------------------
aggregation_state = defaultdict(dict)


def shutdown_handler(sig, frame):
    global RUNNING
    print("[DB] Shutdown signal received")
    RUNNING = False


signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)


# --------------------------------------------------
# HTF Aggregation Logic (Derived From 1m)
# --------------------------------------------------
def process_htf(symbol, base_payload):
    """
    Aggregate closed 1m candle into higher timeframes.
    """

    open_time = base_payload["open_time"]

    for tf, config in TIMEFRAMES.items():

        # Skip 1m (source-of-truth)
        if tf == "1m":
            continue

        tf_ms = config["tf_ms"]

        # Compute bucket start time
        bucket_open = (open_time // tf_ms) * tf_ms

        state = aggregation_state[tf].get(symbol)

        # --------------------------------------------------
        # If bucket changed → finalize previous bucket
        # --------------------------------------------------
        if not state or state["open_time"] != bucket_open:

            if state:
                print(f"[AGG] Finalizing {symbol} {tf}")
                insert_candle(tf, state)

            # Start new bucket
            aggregation_state[tf][symbol] = {
                **base_payload,
                "open_time": bucket_open,
            }

            continue

        # --------------------------------------------------
        # Update existing bucket
        # --------------------------------------------------
        state["high_price"] = max(
            state["high_price"], base_payload["high_price"]
        )
        state["low_price"] = min(
            state["low_price"], base_payload["low_price"]
        )
        state["close_price"] = base_payload["close_price"]
        state["base_volume"] += base_payload["base_volume"]
        state["quote_volume"] += base_payload["quote_volume"]
        state["taker_buy_base_volume"] += base_payload["taker_buy_base_volume"]
        state["taker_buy_quote_volume"] += base_payload["taker_buy_quote_volume"]
        state["trade_count"] += base_payload["trade_count"]


# --------------------------------------------------
# DB Worker Loop
# --------------------------------------------------
def run():
    print("[DB] Worker started")

    while RUNNING:
        try:
            tf, payload = candle_queue.get(timeout=1)

            print(f"[DB] Inserting candle → {payload['symbol']} {tf}")

            # --------------------------------------------------
            # Always insert original candle
            # --------------------------------------------------
            insert_candle(tf, payload)

            # --------------------------------------------------
            # Only aggregate from 1m
            # --------------------------------------------------
            if tf == "1m":
                process_htf(payload["symbol"], payload)

        except Empty:
            continue

        except Exception as e:
            print("[DB] ERROR:", e)

    print("[DB] Worker stopped")

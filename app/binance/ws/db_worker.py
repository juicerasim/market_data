import signal
from queue import Empty
from collections import defaultdict
from datetime import datetime, timezone

from app.binance.ws.queue import candle_queue
from app.binance.repo import insert_candle
from app.config import TIMEFRAMES

RUNNING = True


# --------------------------------------------------
# Aggregation State (IN-MEMORY ONLY)
# --------------------------------------------------
# Structure:
# {
#   "15m": {
#       "BTCUSDT": current_bucket_payload
#   },
#   "1h": {
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


def ms_to_utc(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


# --------------------------------------------------
# HTF Aggregation Logic (Derived FROM 1m ONLY)
# --------------------------------------------------
def process_htf(symbol, base_payload):
    """
    Aggregate CLOSED 1m candle into higher timeframes.
    """

    open_time = base_payload["open_time"]

    print(
        f"\n[AGG INPUT] symbol={symbol} "
        f"1m_open_ms={open_time} "
        f"utc={ms_to_utc(open_time)}"
    )

    for tf, config in TIMEFRAMES.items():

        if tf == "1m":
            continue

        tf_ms = config["tf_ms"]

        # ------------------------------------------
        # Compute bucket start time
        # ------------------------------------------
        bucket_open = (open_time // tf_ms) * tf_ms

        print(
            f"[AGG CALC] symbol={symbol} "
            f"tf={tf} "
            f"tf_ms={tf_ms} "
            f"bucket_ms={bucket_open} "
            f"bucket_utc={ms_to_utc(bucket_open)}"
        )

        state = aggregation_state[tf].get(symbol)

        # ------------------------------------------
        # NEW BUCKET → finalize previous
        # ------------------------------------------
        if not state or state["open_time"] != bucket_open:

            if state:
                print(
                    f"[AGG FINALIZE] symbol={symbol} "
                    f"tf={tf} "
                    f"final_bucket={ms_to_utc(state['open_time'])}"
                )

                insert_candle(tf, state)

            print(
                f"[AGG NEW BUCKET] symbol={symbol} "
                f"tf={tf} "
                f"bucket={ms_to_utc(bucket_open)}"
            )

            aggregation_state[tf][symbol] = {
                **base_payload,
                "open_time": bucket_open,
                "interval": tf,  # CRITICAL FIX
            }

            continue

        # ------------------------------------------
        # UPDATE EXISTING BUCKET
        # ------------------------------------------
        print(
            f"[AGG UPDATE] symbol={symbol} "
            f"tf={tf} "
            f"bucket={ms_to_utc(bucket_open)}"
        )

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

            print(
                f"\n[DB INSERT] symbol={payload['symbol']} "
                f"tf={tf} "
                f"open_ms={payload['open_time']} "
                f"utc={ms_to_utc(payload['open_time'])} "
                f"interval={payload.get('interval')}"
            )

            # ------------------------------------------
            # Insert original candle
            # ------------------------------------------
            insert_candle(tf, payload)

            # ------------------------------------------
            # Aggregate ONLY from 1m
            # ------------------------------------------
            if tf == "1m":
                process_htf(payload["symbol"], payload)

        except Empty:
            continue

        except Exception as e:
            print("[DB ERROR]:", e)

    print("[DB] Worker stopped")

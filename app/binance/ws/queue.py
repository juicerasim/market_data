from queue import Queue

QUEUE_MAXSIZE = 10000

# Global candle queue
candle_queue = Queue(maxsize=QUEUE_MAXSIZE)

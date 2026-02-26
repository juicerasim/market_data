import logging
import os
import sys


def setup_logging():
    level_name = os.environ.get("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    root = logging.getLogger()
    if root.handlers:
        return

    fmt = "[%(asctime)s] %(levelname)s %(name)s %(module)s:%(lineno)d - %(message)s"
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(fmt))
    root.setLevel(level)
    root.addHandler(handler)


def get_logger(name: str):
    return logging.getLogger(name)


def install_exception_hook():
    def handle_exception(exc_type, exc, exc_tb):
        logger = logging.getLogger("uncaught")
        if logger.handlers:
            logger.exception("Uncaught exception", exc_info=(exc_type, exc, exc_tb))
        else:
            # fallback to stderr
            sys.__stderr__.write("Uncaught exception:\n")
            import traceback

            traceback.print_exception(exc_type, exc, exc_tb)

    sys.excepthook = handle_exception

import asyncio
import logging
import signal
import sys
import time
from logging.handlers import RotatingFileHandler
import config
import database as db
from ws_collector import run_ws_collector, set_agg_queue, metrics
from aggregator import run_aggregator
from rest_client import load_symbols, load_historical_candles, run_rest_poller

def setup_logging():
    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    level = getattr(logging, config.LOG_LEVEL, logging.INFO)
    handlers = [logging.StreamHandler(sys.stdout)]
    try:
        fh = RotatingFileHandler(config.LOG_FILE, maxBytes=50*1024*1024, backupCount=5, encoding="utf-8")
        handlers.append(fh)
    except Exception as e:
        print(f"Warning: cant write log: {e}")
    logging.basicConfig(level=level, format=fmt, handlers=handlers)
    logging.getLogger("websockets").setLevel(logging.WARNING)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)

logger = logging.getLogger("axonize.main")
_shutdown_event = asyncio.Event()

def _handle_signal(sig):
    logger.info(f"Signal {sig.name} received, shutting down...")
    _shutdown_event.set()

async def run_monitor(symbols_count):
    while True:
        await asyncio.sleep(60)
        try:
            logger.info(
                f"[MONITOR] uptime={metrics.uptime_sec}s | "
                f"msgs/s={metrics.msgs_per_sec:.1f} | "
                f"writes={metrics.writes_total} | "
                f"errors={metrics.errors_total} | "
                f"reconnects={metrics.reconnects}"
            )
        except Exception as e:
            logger.error(f"Monitor error: {e}")

async def main():
    setup_logging()
    logger.info("=" * 60)
    logger.info("  AXONIZE BingX Collector starting")
    logger.info("=" * 60)

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda s=sig: _handle_signal(s))

    logger.info("Connecting to PostgreSQL...")
    await db.init_pool()
    logger.info("DB pool ready")

    logger.info("Fetching top symbols from BingX...")
    try:
        symbols = await load_symbols()
    except Exception as e:
        logger.error(f"Failed to load symbols: {e}")
        symbols = await db.get_active_symbols()
        if not symbols:
            logger.critical("No symbols available.")
            sys.exit(1)

    logger.info(f"Symbols loaded: {len(symbols)}")

    agg_queue = asyncio.Queue(maxsize=10000)
    set_agg_queue(agg_queue)

    tasks = [
        asyncio.create_task(run_ws_collector(symbols), name="ws_collector"),
        asyncio.create_task(run_aggregator(agg_queue), name="aggregator"),
        asyncio.create_task(run_rest_poller(symbols), name="rest_poller"),
        asyncio.create_task(run_monitor(len(symbols)), name="monitor"),
        asyncio.create_task(load_historical_candles(symbols), name="historical"),
    ]

    logger.info(f"All tasks started. Collecting {len(symbols)} symbols.")

    await _shutdown_event.wait()

    logger.info("Shutting down...")
    for task in tasks:
        if not task.done():
            task.cancel()

    await asyncio.gather(*tasks, return_exceptions=True)
    await db.close_pool()
    logger.info("Collector stopped.")

if __name__ == "__main__":
    asyncio.run(main())

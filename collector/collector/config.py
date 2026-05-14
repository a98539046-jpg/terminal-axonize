import os
from dotenv import load_dotenv
load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", 5432))
DB_NAME = os.getenv("DB_NAME", "trading_terminal")
DB_USER = os.getenv("DB_USER", "axonize")
DB_PASSWORD = os.getenv("DB_PASSWORD", "Axonize2025x")
DB_POOL_MIN = int(os.getenv("DB_POOL_MIN", 5))
DB_POOL_MAX = int(os.getenv("DB_POOL_MAX", 20))
DB_DSN = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

BINGX_WS_URL = "wss://open-api-swap.bingx.com/swap-market"
BINGX_REST_BASE = "https://open-api.bingx.com"

TOP_SYMBOLS_COUNT = int(os.getenv("TOP_SYMBOLS_COUNT", 20))
RECONNECT_DELAY_MS = int(os.getenv("RECONNECT_DELAY_MS", 1000))
WS_PING_INTERVAL_MS = int(os.getenv("WS_PING_INTERVAL_MS", 20000))
HISTORY_DAYS = int(os.getenv("HISTORY_DAYS", 7))
AGG_TIMEFRAMES = os.getenv("AGG_TIMEFRAMES", "5m,15m,1h,4h").split(",")
SYMBOLS_PER_WS = int(os.getenv("SYMBOLS_PER_WS", 1))

OI_POLL_INTERVAL_SEC = 3600
FUNDING_POLL_INTERVAL_SEC = 3600

TF_MINUTES = {"1m":1,"5m":5,"15m":15,"30m":30,"1h":60,"4h":240,"1d":1440}

LOG_LEVEL = os.getenv("LOG_LEVEL", "info").upper()
LOG_FILE = os.getenv("LOG_FILE", "/var/log/axonize-collector.log")

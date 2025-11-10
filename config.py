import os
import sqlite3

DB_PATH = os.environ.get(
    "QUEUECTL_DB_PATH",
    os.path.join(os.path.expanduser("~"), ".queuectl", "queue.db"),
)

def _get_conn():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    return conn

def init_config_table():
    conn = _get_conn()
    with conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS config (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )
    conn.close()

class Config:
    DEFAULTS = {
        "max_retries": "3",
        "backoff_base": "2",
        "worker_poll_interval": "1",
        "worker_heartbeat_interval": "2",
    }

    def __init__(self):
        init_config_table()

    def get(self, key: str):
        conn = _get_conn()
        try:
            row = conn.execute(
                "SELECT value FROM config WHERE key = ?", (key,)
            ).fetchone()
            if row:
                return row["value"]
            return self.DEFAULTS.get(key)
        finally:
            conn.close()

    def set(self, key: str, value: str):
        conn = _get_conn()
        try:
            with conn:
                conn.execute(
                    """
                    INSERT INTO config(key, value)
                    VALUES(?, ?)
                    ON CONFLICT(key) DO UPDATE SET value=excluded.value
                    """,
                    (key, str(value)),
                )
        finally:
            conn.close()

    def get_int(self, key: str):
        v = self.get(key)
        return int(v) if v is not None else None

    def get_float(self, key: str):
        v = self.get(key)
        return float(v) if v is not None else None

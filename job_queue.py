import os
import sqlite3
from datetime import datetime, timedelta
from config import DB_PATH, Config

ISO_FMT = "%Y-%m-%dT%H:%M:%S.%fZ"

def utcnow():
    return datetime.utcnow()

def iso(dt: datetime) -> str:
    return dt.strftime(ISO_FMT)

def _get_conn():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = _get_conn()
    with conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                id          TEXT PRIMARY KEY,
                command     TEXT NOT NULL,
                state       TEXT NOT NULL,
                attempts    INTEGER NOT NULL,
                max_retries INTEGER NOT NULL,
                created_at  TEXT NOT NULL,
                updated_at  TEXT NOT NULL,
                next_run_at TEXT NOT NULL,
                last_error  TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS workers (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                pid            INTEGER NOT NULL,
                name           TEXT,
                last_heartbeat TEXT NOT NULL,
                started_at     TEXT NOT NULL
            )
            """
        )
    conn.close()

class JobQueue:
    def __init__(self):
        init_db()
        self.cfg = Config()

    # ---------------- Enqueue ---------------- #

    def enqueue(self, job_payload: dict) -> dict:
        now = utcnow()
        job_id = job_payload.get("id") or f"job-{int(now.timestamp() * 1000)}"
        max_retries = job_payload.get(
            "max_retries", self.cfg.get_int("max_retries") or 3
        )

        job = {
            "id": job_id,
            "command": job_payload["command"],
            "state": "pending",
            "attempts": int(job_payload.get("attempts", 0)),
            "max_retries": int(max_retries),
            "created_at": job_payload.get("created_at") or iso(now),
            "updated_at": job_payload.get("updated_at") or iso(now),
            "next_run_at": iso(now),
            "last_error": job_payload.get("last_error"),
        }

        conn = _get_conn()
        try:
            with conn:
                conn.execute(
                    """
                    INSERT INTO jobs(
                        id, command, state,
                        attempts, max_retries,
                        created_at, updated_at,
                        next_run_at, last_error
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        job["id"],
                        job["command"],
                        job["state"],
                        job["attempts"],
                        job["max_retries"],
                        job["created_at"],
                        job["updated_at"],
                        job["next_run_at"],
                        job["last_error"],
                    ),
                )
        finally:
            conn.close()

        return job

    # ---------------- Reservation (for workers) ---------------- #

    def reserve_job(self):
        """
        Atomically pick a runnable job and mark it as 'processing'.
        Prevents duplicate picks across multiple workers using a single UPDATE.
        Returns job dict or None.
        """
        now_str = iso(utcnow())
        conn = _get_conn()
        try:
            with conn:
                # Claim one job if available
                cur = conn.execute(
                    """
                    UPDATE jobs
                    SET state='processing',
                        updated_at=?,
                        next_run_at=?
                    WHERE id = (
                        SELECT id FROM jobs
                        WHERE state IN ('pending', 'failed')
                          AND next_run_at <= ?
                        ORDER BY created_at
                        LIMIT 1
                    )
                    """,
                    (now_str, now_str, now_str),
                )

                if cur.rowcount == 0:
                    return None

                # Fetch the job we just updated
                row = conn.execute(
                    """
                    SELECT *
                    FROM jobs
                    WHERE state='processing'
                      AND updated_at=?
                    ORDER BY created_at
                    LIMIT 1
                    """,
                    (now_str,),
                ).fetchone()

                if not row:
                    return None

                return dict(row)
        finally:
            conn.close()

    # ---------------- State transitions ---------------- #

    def mark_completed(self, job_id: str):
        conn = _get_conn()
        try:
            with conn:
                conn.execute(
                    """
                    UPDATE jobs
                    SET state='completed',
                        updated_at=?
                    WHERE id=?
                    """,
                    (iso(utcnow()), job_id),
                )
        finally:
            conn.close()

    def mark_failed(self, job_id: str, attempts: int, max_retries: int, error_msg: str):
        """
        Apply exponential backoff and DLQ transition.
        delay = backoff_base ^ attempts (after increment).
        """
        now = utcnow()
        cfg = self.cfg
        backoff_base = cfg.get_int("backoff_base") or 2

        attempts += 1

        if attempts > max_retries:
            # DLQ
            new_state = "dead"
            next_run_at = iso(now)
        else:
            new_state = "failed"
            delay = backoff_base ** attempts
            next_run_at = iso(now + timedelta(seconds=delay))

        conn = _get_conn()
        try:
            with conn:
                conn.execute(
                    """
                    UPDATE jobs
                    SET state=?,
                        attempts=?,
                        updated_at=?,
                        next_run_at=?,
                        last_error=?
                    WHERE id=?
                    """,
                    (
                        new_state,
                        attempts,
                        iso(now),
                        next_run_at,
                        (error_msg or "")[:512],
                        job_id,
                    ),
                )
        finally:
            conn.close()

    # ---------------- Query helpers ---------------- #

    def list_jobs(self, state: str | None = None):
        conn = _get_conn()
        try:
            if state:
                rows = conn.execute(
                    """
                    SELECT *
                    FROM jobs
                    WHERE state=?
                    ORDER BY created_at
                    """,
                    (state,),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM jobs ORDER BY created_at"
                ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def get_status(self):
        conn = _get_conn()
        try:
            states = ["pending", "processing", "completed", "failed", "dead"]
            job_counts = {}
            for st in states:
                c = conn.execute(
                    "SELECT COUNT(*) AS c FROM jobs WHERE state=?", (st,)
                ).fetchone()["c"]
                job_counts[st] = c

            # active workers = heartbeats in last 10s
            cutoff = iso(utcnow() - timedelta(seconds=10))
            w = conn.execute(
                """
                SELECT COUNT(*) AS c
                FROM workers
                WHERE last_heartbeat >= ?
                """,
                (cutoff,),
            ).fetchone()["c"]

            return {
                "jobs": job_counts,
                "workers": {"active": w},
            }
        finally:
            conn.close()

    # ---------------- DLQ ---------------- #

    def retry_dead_job(self, job_id: str):
        now_str = iso(utcnow())
        conn = _get_conn()
        try:
            with conn:
                conn.execute(
                    """
                    UPDATE jobs
                    SET state='pending',
                        attempts=0,
                        updated_at=?,
                        next_run_at=?,
                        last_error=NULL
                    WHERE id=? AND state='dead'
                    """,
                    (now_str, now_str, job_id),
                )
        finally:
            conn.close()

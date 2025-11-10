import os
import subprocess
import time
from multiprocessing import Process, current_process
from job_queue import JobQueue, _get_conn, iso, utcnow
from config import Config, DB_PATH

STOP_FILE = os.path.join(os.path.dirname(DB_PATH), "workers.stop")

# ---------- Worker registration helpers ---------- #

def register_worker(pid: int, name: str):
    conn = _get_conn()
    now = iso(utcnow())
    try:
        with conn:
            conn.execute(
                """
                INSERT INTO workers(pid, name, last_heartbeat, started_at)
                VALUES (?, ?, ?, ?)
                """,
                (pid, name, now, now),
            )
    finally:
        conn.close()

def update_heartbeat(pid: int):
    conn = _get_conn()
    now = iso(utcnow())
    try:
        with conn:
            conn.execute(
                "UPDATE workers SET last_heartbeat=? WHERE pid=?",
                (now, pid),
            )
    finally:
        conn.close()

def unregister_worker(pid: int):
    conn = _get_conn()
    try:
        with conn:
            conn.execute("DELETE FROM workers WHERE pid=?", (pid,))
    finally:
        conn.close()

# ---------- Worker loop ---------- #

def worker_loop():
    q = JobQueue()
    cfg = Config()
    poll_interval = int(cfg.get("worker_poll_interval") or 1)

    pid = os.getpid()
    name = current_process().name

    register_worker(pid, name)

    try:
        while True:
            # Check for stop signal (graceful)
            if os.path.exists(STOP_FILE):
                break

            update_heartbeat(pid)

            job = q.reserve_job()
            if not job:
                time.sleep(poll_interval)
                continue

            cmd = job["command"]

            try:
                result = subprocess.run(cmd, shell=True)
                if result.returncode == 0:
                    q.mark_completed(job["id"])
                else:
                    q.mark_failed(
                        job_id=job["id"],
                        attempts=job["attempts"],
                        max_retries=job["max_retries"],
                        error_msg=f"Exit code {result.returncode}",
                    )
            except FileNotFoundError as e:
                q.mark_failed(
                    job_id=job["id"],
                    attempts=job["attempts"],
                    max_retries=job["max_retries"],
                    error_msg=str(e),
                )
            except Exception as e:
                q.mark_failed(
                    job_id=job["id"],
                    attempts=job["attempts"],
                    max_retries=job["max_retries"],
                    error_msg=str(e),
                )
    finally:
        unregister_worker(pid)

# ---------- Manager ---------- #

class WorkerManager:
    def __init__(self):
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    def start_workers(self, count: int):
        # clear previous stop signal
        if os.path.exists(STOP_FILE):
            os.remove(STOP_FILE)

        procs: list[Process] = []

        for i in range(count):
            p = Process(target=worker_loop, name=f"worker-{i+1}")
            p.start()
            procs.append(p)

        # Run in foreground; in real-world use, you'd daemonize or use a supervisor.
        try:
            for p in procs:
                p.join()
        except KeyboardInterrupt:
            # Ctrl+C triggers graceful stop
            self.stop_workers()
            for p in procs:
                p.join()

    def stop_workers(self):
        # Workers poll this file and exit after finishing current loop/job.
        with open(STOP_FILE, "w", encoding="utf-8") as f:
            f.write("stop")

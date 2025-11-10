import argparse
import json
from job_queue import JobQueue
from worker import WorkerManager
from config import Config

def main():
    parser = argparse.ArgumentParser(
        prog="queuectl",
        description="CLI-based background job queue with workers, retries, and DLQ.",
    )

    subparsers = parser.add_subparsers(dest="command")

    # ---------- enqueue ---------- #
    enqueue_p = subparsers.add_parser("enqueue", help="Enqueue a new job from JSON")
    enqueue_p.add_argument(
        "job_json",
        help="Job JSON string or @/path/to/file.json",
    )

    # ---------- worker ---------- #
    worker_p = subparsers.add_parser("worker", help="Worker management")
    worker_sub = worker_p.add_subparsers(dest="worker_cmd")

    worker_start = worker_sub.add_parser("start", help="Start one or more workers")
    worker_start.add_argument(
        "--count",
        type=int,
        default=1,
        help="Number of worker processes to start",
    )

    worker_sub.add_parser("stop", help="Stop running workers gracefully")

    # ---------- status ---------- #
    subparsers.add_parser("status", help="Show job & worker status summary")

    # ---------- list ---------- #
    list_p = subparsers.add_parser("list", help="List jobs")
    list_p.add_argument(
        "--state",
        choices=["pending", "processing", "completed", "failed", "dead"],
        help="Filter jobs by state",
    )

    # ---------- dlq ---------- #
    dlq_p = subparsers.add_parser("dlq", help="Dead Letter Queue operations")
    dlq_sub = dlq_p.add_subparsers(dest="dlq_cmd")

    dlq_sub.add_parser("list", help="List DLQ jobs (state=dead)")

    dlq_retry = dlq_sub.add_parser("retry", help="Retry a DLQ job by id")
    dlq_retry.add_argument("job_id")

    # ---------- config ---------- #
    config_p = subparsers.add_parser("config", help="Config management")
    config_sub = config_p.add_subparsers(dest="config_cmd")

    c_set = config_sub.add_parser("set", help="Set config key")
    c_set.add_argument("key")
    c_set.add_argument("value")

    c_get = config_sub.add_parser("get", help="Get config key")
    c_get.add_argument("key")

    args = parser.parse_args()

    # Shared helpers
    q = JobQueue()
    cfg = Config()

    # ---------- dispatch ---------- #

    if args.command == "enqueue":
        # Inline JSON or @file.json
        if args.job_json.startswith("@"):
            with open(args.job_json[1:], "r", encoding="utf-8") as f:
                payload = json.load(f)
        else:
            payload = json.loads(args.job_json)

        if "command" not in payload:
            raise SystemExit("Job JSON must contain 'command'")

        job = q.enqueue(payload)
        print(f"Enqueued job {job['id']} (state={job['state']})")

    elif args.command == "worker":
        wm = WorkerManager()
        if args.worker_cmd == "start":
            wm.start_workers(args.count)
        elif args.worker_cmd == "stop":
            wm.stop_workers()
            print("Signalled workers to stop gracefully.")
        else:
            parser.error("Missing worker subcommand. Use 'start' or 'stop'.")

    elif args.command == "status":
        status = q.get_status()
        print("Job counts:")
        for st, c in status["jobs"].items():
            print(f"  {st:10s}: {c}")
        print("Workers:")
        print(f"  active   : {status['workers']['active']}")

    elif args.command == "list":
        jobs = q.list_jobs(state=args.state)
        for j in jobs:
            print(
                f"{j['id']} | {j['state']:10s} | attempts={j['attempts']} "
                f"| max={j['max_retries']} | cmd={j['command']}"
            )

    elif args.command == "dlq":
        if args.dlq_cmd == "list":
            jobs = q.list_jobs(state="dead")
            for j in jobs:
                print(
                    f"{j['id']} | attempts={j['attempts']} | cmd={j['command']} "
                    f"| last_error={j.get('last_error','')}"
                )
        elif args.dlq_cmd == "retry":
            q.retry_dead_job(args.job_id)
            print(f"Moved dead job {args.job_id} back to pending.")
        else:
            parser.error("Missing dlq subcommand. Use 'list' or 'retry <job_id>'.")

    elif args.command == "config":
        if args.config_cmd == "set":
            cfg.set(args.key, args.value)
            print(f"Config {args.key} set to {args.value}")
        elif args.config_cmd == "get":
            val = cfg.get(args.key)
            if val is None:
                print(f"{args.key} not set")
            else:
                print(f"{args.key}={val}")
        else:
            parser.error("Missing config subcommand. Use 'set' or 'get'.")

    else:
        parser.print_help()

if __name__ == "__main__":
    main()

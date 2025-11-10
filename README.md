 QueueCTL — CLI Background Job Queue System

QueueCTL is a lightweight, Python-based command-line job queue system that can manage background jobs, run multiple worker processes, automatically retry failed jobs with exponential backoff, and move permanently failed jobs into a Dead Letter Queue (DLQ). It uses a persistent SQLite database for job storage and works entirely with Python’s standard library — no external dependencies required.

The project is written in Python 3.10 or later and runs on all major platforms including Windows and Linux. It relies on sqlite3, multiprocessing, and argparse to provide job persistence, concurrency, and a clean CLI interface.

Setup and Running

Clone or copy the project folder and open a terminal inside the QueueCTL directory.
You can verify that the CLI works by running:

python cli.py --help


This displays all available commands such as enqueue, worker, status, list, dlq, and config.

All job data is stored automatically in a persistent SQLite database located at
C:\Users\<yourname>\.queuectl\queue.db on Windows (or ~/.queuectl/queue.db on Linux/Mac).
The file will be created the first time you enqueue a job.

How Jobs Work

Each job record contains a unique ID, the shell command to execute, its current state, number of attempts, and timestamps. Example:

{
  "id": "demo1",
  "command": "echo Hello from QueueCTL",
  "state": "pending",
  "attempts": 0,
  "max_retries": 3
}


The job can be in one of five states — pending (waiting for a worker), processing (currently running), completed (finished successfully), failed (temporarily failed but retryable), or dead (moved to DLQ after all retries fail).
Retry delays follow an exponential backoff formula: delay = base ^ attempts, where the base value can be configured.

Enqueuing and Processing Jobs

To enqueue a new job, create a file job.json containing:

{ "id": "demo1", "command": "echo Hello from QueueCTL" }


Then run:

python cli.py enqueue "@job.json"


The system responds with Enqueued job demo1 (state=pending).
You can view the job list using python cli.py list and check overall status with python cli.py status.

To start workers that execute the queued jobs, run:

python cli.py worker start --count 1


You’ll see the command output printed (Hello from QueueCTL), after which the job’s state becomes completed.
You can also start multiple workers in parallel with python cli.py worker start --count 3.

To gracefully stop all running workers, open another terminal and execute:

python cli.py worker stop


This signals the workers to finish their current job and exit cleanly.

Handling Failures and DLQ

If a job fails, the worker automatically retries it with exponential backoff until it reaches the maximum retry count.
When a job exceeds its max_retries, it is marked as dead and moved to the Dead Letter Queue.

You can view all dead jobs using:

python cli.py dlq list


and retry a specific one with:

python cli.py dlq retry <job_id>


For example:

python cli.py dlq retry fail1


moves the job back to pending.

 Configuration

QueueCTL allows you to configure retry and backoff parameters from the CLI.
To change the maximum number of retries or backoff base, use:

python cli.py config set max-retries 5
python cli.py config set backoff_base 3


You can confirm values with:

python cli.py config get max-retries

 Testing the System

You can test everything manually in this order:

Enqueue a success job:
python cli.py enqueue "@job.json"

Start a worker:
python cli.py worker start --count 1

Check status:
python cli.py status

Enqueue a failing job (example):
python cli.py enqueue @'{ "id": "fail1", "command": "exit 3" }'

Run worker again — the job will retry and eventually appear in the DLQ.

List dead jobs:
python cli.py dlq list

Retry them:
python cli.py dlq retry fail1

All jobs and their states will persist across restarts because they are stored in the SQLite database.

 Architecture Overview

The system is modular:

cli.py — handles command parsing and routing.

job_queue.py — defines the database, job states, and lifecycle methods.

worker.py — manages worker processes and job execution logic.

config.py — stores and retrieves global configuration.
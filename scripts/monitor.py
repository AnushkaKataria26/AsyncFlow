import argparse
import time
import os
import sys

from sqlalchemy import create_engine, text

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--interval", type=float, default=2.0)
    parser.add_argument("--db-path", type=str, default="asyncflow.db")
    args = parser.parse_args()

    db_url = f"sqlite:///{args.db_path}"
    engine = create_engine(db_url)

    job_status_query = text("""
        SELECT status, COUNT(*) as count 
        FROM (
            SELECT status FROM jobs
            UNION ALL
            SELECT status FROM dead_letter_jobs
        )
        GROUP BY status
    """)
    worker_status_query = text("SELECT status, COUNT(*) as count FROM workers GROUP BY status")

    try:
        while True:
            with engine.connect() as conn:
                job_results = conn.execute(job_status_query).fetchall()
                worker_results = conn.execute(worker_status_query).fetchall()

            job_counts = {r[0]: r[1] for r in job_results}
            worker_counts = {r[0]: r[1] for r in worker_results}

            pending = job_counts.get("PENDING", 0)
            in_progress = job_counts.get("IN_PROGRESS", 0)
            completed = job_counts.get("COMPLETED", 0)
            failed = job_counts.get("FAILED", 0)
            dead_letter = job_counts.get("DEAD_LETTER", 0)

            active_workers = worker_counts.get("ACTIVE", 0)
            dead_workers = worker_counts.get("DEAD", 0)

            # Clear line
            sys.stdout.write("\r\033[K")
            sys.stdout.write(
                f"PENDING: {pending} | IN_PROGRESS: {in_progress} | COMPLETED: {completed} | "
                f"FAILED: {failed} | DEAD_LETTER: {dead_letter} | "
                f"Workers ACTIVE: {active_workers} DEAD: {dead_workers}"
            )
            sys.stdout.flush()

            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nExiting.")

if __name__ == "__main__":
    main()

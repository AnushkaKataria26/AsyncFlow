import os
from typing import Dict, Any, Optional
from sqlalchemy import text
from db.db import engine

def collect_metrics(db_path: str = None) -> Dict[str, Any]:
    """
    Collects metrics from the database.
    """
    metrics = {
        "jobs": {"PENDING": 0, "IN_PROGRESS": 0, "COMPLETED": 0, "FAILED": 0, "DEAD_LETTER": 0, "total": 0},
        "workers": {"ACTIVE": 0, "DEAD": 0},
        "dead_letter_jobs": {"count": 0},
        "avg_completion_time_seconds": None,
        "retry_rate": 0.0,
        "failure_rate": 0.0
    }
    
    with engine.connect() as conn:
        # Jobs statuses
        res = conn.execute(text("SELECT status, count(*) FROM jobs GROUP BY status")).all()
        for status, count in res:
            if status in metrics["jobs"]:
                metrics["jobs"][status] = count
            metrics["jobs"]["total"] += count
            
        # Dead letter jobs
        dlq_res = conn.execute(text("SELECT count(*) FROM dead_letter_jobs")).scalar()
        if dlq_res:
            metrics["jobs"]["DEAD_LETTER"] = dlq_res
            metrics["dead_letter_jobs"]["count"] = dlq_res
            metrics["jobs"]["total"] += dlq_res
            
        # Workers statuses
        res = conn.execute(text("SELECT status, count(*) FROM workers GROUP BY status")).all()
        for status, count in res:
            if status in metrics["workers"]:
                metrics["workers"][status] = count
                
        # Average completion time
        # updated_at and created_at in SQLite are strings by default if using CURRENT_TIMESTAMP.
        # But wait, SQLite datetime functions can compute the difference.
        is_sqlite = engine.url.drivername == "sqlite"
        if is_sqlite:
            avg_res = conn.execute(text("SELECT AVG(julianday(updated_at) - julianday(created_at)) * 86400 FROM jobs WHERE status = 'COMPLETED'")).scalar()
        else:
            avg_res = conn.execute(text("SELECT EXTRACT(EPOCH FROM AVG(updated_at - created_at)) FROM jobs WHERE status = 'COMPLETED'")).scalar()
            
        metrics["avg_completion_time_seconds"] = float(avg_res) if avg_res else None
        
        # Retry rate
        completed = metrics["jobs"]["COMPLETED"]
        if completed > 0:
            retried_completed = conn.execute(text("SELECT count(*) FROM jobs WHERE status = 'COMPLETED' AND retry_count > 0")).scalar()
            metrics["retry_rate"] = retried_completed / completed
            
        # Failure rate
        total_jobs = metrics["jobs"]["total"]
        if total_jobs > 0:
            metrics["failure_rate"] = (metrics["jobs"]["FAILED"] + metrics["jobs"]["DEAD_LETTER"]) / total_jobs
            
    return metrics

def print_metrics_table(metrics: Dict[str, Any]) -> None:
    print("=== AsyncFlow Metrics Report ===")
    print(f"Jobs Total: {metrics['jobs']['total']}")
    print("Job Statuses:")
    for status in ["PENDING", "IN_PROGRESS", "COMPLETED", "FAILED", "DEAD_LETTER"]:
        print(f"  {status}: {metrics['jobs'].get(status, 0)}")
    
    print("\nWorkers:")
    for status in ["ACTIVE", "DEAD"]:
        print(f"  {status}: {metrics['workers'].get(status, 0)}")
        
    avg_time = f"{metrics['avg_completion_time_seconds']:.2f}s" if metrics["avg_completion_time_seconds"] is not None else "N/A"
    print(f"\nAvg Completion Time: {avg_time}")
    print(f"Retry Rate: {metrics['retry_rate'] * 100:.2f}%")
    print(f"Failure Rate: {metrics['failure_rate'] * 100:.2f}%")

import os
from dataclasses import dataclass

@dataclass
class SchedulerConfig:
    database_url: str = "sqlite:///asyncflow.db"
    queue_host: str = "localhost"
    queue_port: int = 9000
    scheduler_interval_seconds: float = 5.0
    lease_grace_period_seconds: int = 5
    dead_worker_timeout_seconds: int = 30
    max_requeue_batch_size: int = 100

    @classmethod
    def from_env(cls) -> "SchedulerConfig":
        return cls(
            database_url=os.environ.get("DATABASE_URL", "sqlite:///asyncflow.db"),
            queue_host=os.environ.get("QUEUE_HOST", "localhost"),
            queue_port=int(os.environ.get("QUEUE_PORT", "9000")),
            scheduler_interval_seconds=float(os.environ.get("SCHEDULER_INTERVAL_SECONDS", "5.0")),
            lease_grace_period_seconds=int(os.environ.get("LEASE_GRACE_PERIOD_SECONDS", "5")),
            dead_worker_timeout_seconds=int(os.environ.get("DEAD_WORKER_TIMEOUT_SECONDS", "30")),
            max_requeue_batch_size=int(os.environ.get("MAX_REQUEUE_BATCH_SIZE", "100"))
        )

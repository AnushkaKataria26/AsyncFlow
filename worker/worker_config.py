import os
import uuid
from dataclasses import dataclass, field

@dataclass
class WorkerConfig:
    worker_id: str = field(default_factory=lambda: os.environ.get("WORKER_ID", str(uuid.uuid4())))
    database_url: str = field(default_factory=lambda: os.environ.get("DATABASE_URL", "sqlite:///asyncflow.db"))
    queue_host: str = field(default_factory=lambda: os.environ.get("QUEUE_HOST", "localhost"))
    queue_port: int = field(default_factory=lambda: int(os.environ.get("QUEUE_PORT", 9000)))
    lease_duration_seconds: int = field(default_factory=lambda: int(os.environ.get("LEASE_DURATION_SECONDS", 30)))
    heartbeat_interval_seconds: int = field(default_factory=lambda: int(os.environ.get("HEARTBEAT_INTERVAL_SECONDS", 10)))
    poll_interval_seconds: float = field(default_factory=lambda: float(os.environ.get("POLL_INTERVAL_SECONDS", 1.0)))
    max_consecutive_errors: int = field(default_factory=lambda: int(os.environ.get("MAX_CONSECUTIVE_ERRORS", 5)))

    def __post_init__(self):
        if self.heartbeat_interval_seconds >= self.lease_duration_seconds:
            raise ValueError(
                f"Heartbeat interval ({self.heartbeat_interval_seconds}s) must be strictly less than "
                f"lease duration ({self.lease_duration_seconds}s)"
            )

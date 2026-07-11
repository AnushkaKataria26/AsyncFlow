import time
import logging
import threading
import uuid
from datetime import datetime, timedelta, timezone

from db import db
from shared.queue_client import send_command, QueueUnavailableError, QueueTimeoutError, QueueAuthError
from scheduler.scheduler_config import SchedulerConfig

logger = logging.getLogger(__name__)

class Scheduler:
    def __init__(self, config: SchedulerConfig):
        self.config = config
        self._stop_event = threading.Event()
        
        self.scheduler_id = f"scheduler_{uuid.uuid4().hex}"
        self.auth_token = str(uuid.uuid4())
        self.is_registered = False
        
        self.total_requeues = 0
        self.total_retries_scheduled = 0
        self.total_moved_to_dlq = 0
        self.total_dead_workers_detected = 0

    def run(self) -> None:
        logger.info("Scheduler starting")
        while not self._stop_event.is_set():
            try:
                self._sweep()
            except QueueAuthError:
                logger.warning("Auth token rejected by queue server — attempting re-registration")
                self.is_registered = False
            except Exception as e:
                logger.critical("Unhandled exception in scheduler sweep: %s", e, exc_info=True)
            
            self._stop_event.wait(timeout=self.config.scheduler_interval_seconds)
            
        logger.info("Scheduler exiting. Stats: requeues=%d, retries_scheduled=%d, dlq_moves=%d, dead_workers_detected=%d",
                    self.total_requeues, self.total_retries_scheduled, self.total_moved_to_dlq, self.total_dead_workers_detected)

    def stop(self) -> None:
        self._stop_event.set()

    def _sweep(self) -> None:
        if not self.is_registered:
            try:
                send_command(f"REGISTER {self.scheduler_id} {self.auth_token}", self.config.queue_host, self.config.queue_port)
                self.is_registered = True
            except Exception as e:
                logger.error("Failed to register scheduler with queue server: %s", e)
                
        start_ms = time.time() * 1000
        
        # Step 1: Detect dead workers
        dead_workers = db.mark_dead_workers(self.config.dead_worker_timeout_seconds)
        for worker_id in dead_workers:
            logger.warning("Worker %s declared dead (heartbeat timeout)", worker_id)
            self.total_dead_workers_detected += 1
            
        # Step 2: Requeue expired leases
        expired_jobs = db.find_expired_leases()
        
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        grace_period = timedelta(seconds=self.config.lease_grace_period_seconds)
        
        processed_requeue = 0
        for job in expired_jobs:
            if processed_requeue >= self.config.max_requeue_batch_size:
                break
                
            job_id = job["job_id"]
            worker_id = job["worker_id"]
            
            lease_expires_at = job.get("lease_expires_at")
            if lease_expires_at:
                if isinstance(lease_expires_at, str):
                    lease_time = datetime.fromisoformat(lease_expires_at.replace("Z", ""))
                else:
                    lease_time = lease_expires_at
                
                if lease_time.tzinfo is not None:
                    lease_time = lease_time.replace(tzinfo=None)
                        
                if lease_time > now - grace_period:
                    continue
            
            logger.warning("Job %s lease expired (held by worker %s), requeuing", job_id, worker_id)
            
            if job["retry_count"] >= job["max_retries"]:
                self._move_to_dlq(job, "Lease expired after max retries exhausted")
                continue
                
            db.requeue_job(job_id)
            try:
                send_command(f"ENQUEUE {job_id} {self.auth_token}", self.config.queue_host, self.config.queue_port)
            except QueueUnavailableError:
                logger.error("Failed to enqueue %s after lease expiry — job is PENDING in DB but not in queue", job_id)
            
            self.total_requeues += 1
            processed_requeue += 1
            
        # Step 3: Schedule retries for FAILED jobs
        failed_jobs = db.get_retryable_failed_jobs()
        retries_in_sweep = 0
        for job in failed_jobs:
            job_id = job["job_id"]
            updated_at = job.get("updated_at")
            
            if updated_at:
                if isinstance(updated_at, str):
                    updated_at_time = datetime.fromisoformat(updated_at.replace("Z", ""))
                else:
                    updated_at_time = updated_at
                    
                if updated_at_time.tzinfo is not None:
                    updated_at_time = updated_at_time.replace(tzinfo=None)
            else:
                updated_at_time = now
                    
            backoff_seconds = min(2 ** job["retry_count"], 300)
            next_scheduled_time = updated_at_time + timedelta(seconds=backoff_seconds)
            
            if now < next_scheduled_time:
                continue
                
            db.requeue_job(job_id)
            try:
                send_command(f"ENQUEUE {job_id} {self.auth_token}", self.config.queue_host, self.config.queue_port)
            except QueueUnavailableError:
                logger.error("Failed to enqueue %s after retry scheduling — job is PENDING in DB but not in queue", job_id)
            
            logger.info("Job %s scheduled for retry %d/%d after backoff", job_id, job["retry_count"], job["max_retries"])
            self.total_retries_scheduled += 1
            retries_in_sweep += 1
                
        # Step 4: Move exhausted FAILED jobs to DLQ
        dlq_candidates = db.get_dlq_candidates()
        dlq_in_sweep = 0
        for job in dlq_candidates:
            self._move_to_dlq(job, "Max retries exceeded")
            dlq_in_sweep += 1
            
        # Step 5: Reconciliation pass
        pending_jobs = db.get_pending_jobs()
        for job in pending_jobs:
            job_id = job["job_id"]
            try:
                resp = send_command(f"ENQUEUE {job_id} {self.auth_token}", self.config.queue_host, self.config.queue_port)
                if resp == "DUPLICATE":
                    logger.debug("Reconciliation: Job %s is already in queue", job_id)
            except QueueUnavailableError:
                logger.error("Queue unavailable during reconciliation, aborting loop")
                break
                
        end_ms = time.time() * 1000
        logger.info("Sweep complete — requeued: %d, retries: %d, DLQ: %d, dead workers: %d, duration: %.2fms",
                    processed_requeue, retries_in_sweep, dlq_in_sweep, len(dead_workers), end_ms - start_ms)

    def _move_to_dlq(self, job: dict, failure_reason: str) -> None:
        job_id = job["job_id"]
        db.move_to_dead_letter(job_id, failure_reason)
        logger.error("Job %s moved to dead-letter queue: %s", job_id, failure_reason)
        self.total_moved_to_dlq += 1

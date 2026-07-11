import logging
import threading
import uuid
import time
import json
from datetime import datetime, timedelta

from .worker_config import WorkerConfig
from .handlers.registry import HandlerRegistry
import db.db as db
from shared.queue_client import send_command, build_authenticated_command, QueueUnavailableError, QueueTimeoutError, QueueAuthError

logger = logging.getLogger(__name__)

class Worker:
    def __init__(self, config: WorkerConfig, registry: HandlerRegistry):
        self.config = config
        self.registry = registry
        self._stop_event = threading.Event()
        self.consecutive_error_count = 0
        self.auth_token = None

    def register(self) -> None:
        self.auth_token = str(uuid.uuid4())
        try:
            db.register_worker(self.config.worker_id, self.auth_token)
            logger.info(f"Worker {self.config.worker_id} registered with DB")
        except Exception as e:
            raise RuntimeError(f"Failed to register worker with DB: {e}") from e

        try:
            response = send_command(f"REGISTER {self.config.worker_id} {self.auth_token}", self.config.queue_host, self.config.queue_port)
            if response == "OK":
                logger.info(f"Worker {self.config.worker_id} registered with queue server")
            elif response == "ERROR worker_id already registered":
                logger.warning("Queue server reported worker_id already registered (token known). Proceeding.")
            else:
                raise RuntimeError(f"Queue server registration returned error: {response}")
        except Exception as e:
            raise RuntimeError(f"Queue server registration failed: {e}") from e

    def start_heartbeat_thread(self) -> threading.Thread:
        thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        thread.start()
        return thread

    def _heartbeat_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                db.update_worker_heartbeat(self.config.worker_id)
            except Exception as e:
                logger.warning(f"Failed to update heartbeat: {e}")
            self._stop_event.wait(timeout=self.config.heartbeat_interval_seconds)

    def stop(self) -> None:
        self._stop_event.set()

    def run(self) -> None:
        self.register()
        heartbeat_thread = self.start_heartbeat_thread()
        logger.info(f"Worker {self.config.worker_id} starting main loop")
        
        while not self._stop_event.is_set():
            self._poll_and_execute()
            if self.consecutive_error_count >= self.config.max_consecutive_errors:
                logger.critical(f"Worker {self.config.worker_id} exceeded max consecutive errors, shutting down")
                self.stop()
                break
                
        heartbeat_thread.join(timeout=5)
        try:
            send_command(f"DEREGISTER {self.config.worker_id} {self.auth_token}", self.config.queue_host, self.config.queue_port)
        except Exception as e:
            logger.warning(f"Failed to deregister from queue server: {e}")
        logger.info(f"Worker {self.config.worker_id} shut down cleanly")

    def _poll_and_execute(self) -> None:
        try:
            self._do_poll_and_execute()
        except QueueAuthError:
            logger.warning("Auth token rejected by queue server — attempting re-registration")
            try:
                response = send_command(f"REGISTER {self.config.worker_id} {self.auth_token}", self.config.queue_host, self.config.queue_port)
                if response in ("OK", "ERROR worker_id already registered"):
                    logger.info("Re-registered successfully")
                else:
                    logger.error(f"Re-registration returned error: {response}")
                    self.consecutive_error_count += 1
            except Exception as e:
                logger.error(f"Re-registration failed: {e}")
                self.consecutive_error_count += 1
            return

    def _do_poll_and_execute(self) -> None:
        # Step 1: Dequeue from C++ queue server
        try:
            cmd = build_authenticated_command(["DEQUEUE", str(self.config.lease_duration_seconds)], self.auth_token)
            response = send_command(
                cmd,
                self.config.queue_host, 
                self.config.queue_port
            )
        except (QueueUnavailableError, QueueTimeoutError) as e:
            logger.error(f"Queue error: {e}")
            self.consecutive_error_count += 1
            time.sleep(self.config.poll_interval_seconds)
            return
        except QueueAuthError:
            raise
        except Exception as e:
            logger.error(f"unexpected queue error: {e}")
            self.consecutive_error_count += 1
            time.sleep(self.config.poll_interval_seconds)
            return

        if response == "EMPTY":
            time.sleep(self.config.poll_interval_seconds)
            self.consecutive_error_count = 0
            return
            
        if response.startswith("ERROR"):
            logger.warning(f"Queue responded with error: {response}")
            self.consecutive_error_count += 1
            time.sleep(self.config.poll_interval_seconds)
            return
            
        if not response.startswith("JOB "):
            logger.error(f"unexpected queue response: {response}")
            self.consecutive_error_count += 1
            return
            
        job_id = response[4:].strip()

        # Step 2: Fetch job from DB
        try:
            job = db.get_job(job_id)
        except Exception as e:
            logger.error(f"Failed to fetch job {job_id} from DB: {e}")
            self.consecutive_error_count += 1
            time.sleep(self.config.poll_interval_seconds)
            return

        if job is None:
            logger.warning(f"Dequeued job_id {job_id} not found in DB — possible DB inconsistency, sending ACK to remove from queue")
            try:
                cmd = build_authenticated_command(["ACK", job_id], self.auth_token)
                send_command(cmd, self.config.queue_host, self.config.queue_port)
            except QueueAuthError:
                raise
            except Exception:
                pass
            return
            
        if job.get("status") not in ("PENDING", "IN_PROGRESS"):
            logger.warning(f"Job {job_id} is in status {job.get('status')}, skipping and ACKing")
            try:
                cmd = build_authenticated_command(["ACK", job_id], self.auth_token)
                send_command(cmd, self.config.queue_host, self.config.queue_port)
            except QueueAuthError:
                raise
            except Exception:
                pass
            return
            
        if job.get("retry_count", 0) > job.get("max_retries", 0):
            logger.warning(f"Job {job_id} retry_count exceeded max_retries, sending ACK")
            try:
                cmd = build_authenticated_command(["ACK", job_id], self.auth_token)
                send_command(cmd, self.config.queue_host, self.config.queue_port)
            except QueueAuthError:
                raise
            except Exception:
                pass
            return

        # Step 3: Mark IN_PROGRESS
        lease_expires_at = (datetime.utcnow() + timedelta(seconds=self.config.lease_duration_seconds)).strftime("%Y-%m-%d %H:%M:%S")
        try:
            updated = db.update_job_status(
                job_id, 
                "IN_PROGRESS", 
                worker_id=self.config.worker_id, 
                lease_expires_at=lease_expires_at
            )
        except Exception as e:
            logger.error(f"Failed to update job status: {e}")
            self.consecutive_error_count += 1
            return

        if not updated:
            logger.warning(f"Job {job_id} could not be updated (likely disappeared), sending ACK")
            try:
                cmd = build_authenticated_command(["ACK", job_id], self.auth_token)
                send_command(cmd, self.config.queue_host, self.config.queue_port)
            except QueueAuthError:
                raise
            except Exception:
                pass
            return

        # Step 4: Dispatch to handler
        job_type = job.get("job_type")
        try:
            handler = self.registry.get(job_type)
        except KeyError:
            logger.error(f"No handler for job_type {job_type} — marking FAILED, not retrying")
            db.update_job_status(job_id, "FAILED", result=f"Unregistered job_type: {job_type}")
            try:
                cmd = build_authenticated_command(["ACK", job_id], self.auth_token)
                send_command(cmd, self.config.queue_host, self.config.queue_port)
            except QueueAuthError:
                raise
            except Exception:
                pass
            self.consecutive_error_count = 0
            return
            
        try:
            payload_dict = json.loads(job.get("payload", "{}"))
        except (TypeError, ValueError):
            db.update_job_status(job_id, "FAILED", result="Invalid payload JSON")
            try:
                cmd = build_authenticated_command(["ACK", job_id], self.auth_token)
                send_command(cmd, self.config.queue_host, self.config.queue_port)
            except QueueAuthError:
                raise
            except Exception:
                pass
            self.consecutive_error_count = 0
            return

        # Step 5: Execute handler
        try:
            result_dict = handler.execute(job_id, payload_dict)
        except Exception as e:
            result_dict = {"success": False, "result": None, "error": str(e)}

        # Step 6: Handle result
        if result_dict.get("success"):
            db.update_job_status(job_id, "COMPLETED", result=result_dict.get("result"))
            try:
                cmd = build_authenticated_command(["ACK", job_id], self.auth_token)
                send_command(cmd, self.config.queue_host, self.config.queue_port)
            except QueueAuthError:
                raise
            except Exception:
                pass
            logger.info(f"Job {job_id} completed successfully")
            self.consecutive_error_count = 0
        else:
            try:
                cmd = build_authenticated_command(["ACK", job_id], self.auth_token)
                send_command(cmd, self.config.queue_host, self.config.queue_port)
            except QueueAuthError:
                raise
            except Exception:
                pass
            db.update_job_status(job_id, "FAILED", result=result_dict.get("error"))
            logger.warning(f"Job {job_id} failed: {result_dict.get('error')}")
            self.consecutive_error_count = 0

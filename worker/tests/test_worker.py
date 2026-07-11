import json
import threading
import time
from unittest.mock import patch, call, MagicMock
import pytest

from worker.worker import Worker
from worker.worker_config import WorkerConfig
from worker.handlers.registry import HandlerRegistry
from worker.handlers.base_handler import JobHandler
from shared.queue_client import QueueUnavailableError

class DummySuccessHandler(JobHandler):
    @property
    def job_type(self) -> str:
        return "success_job"
    def execute(self, job_id: str, payload: dict) -> dict:
        return {"success": True, "result": "done", "error": None}

class DummyFailureHandler(JobHandler):
    @property
    def job_type(self) -> str:
        return "fail_job"
    def execute(self, job_id: str, payload: dict) -> dict:
        return {"success": False, "result": None, "error": "failed on purpose"}

class DummyExceptionHandler(JobHandler):
    @property
    def job_type(self) -> str:
        return "exc_job"
    def execute(self, job_id: str, payload: dict) -> dict:
        raise ValueError("exception on purpose")

@pytest.fixture
def config():
    return WorkerConfig(
        worker_id="test_worker_1",
        poll_interval_seconds=0.01,
        heartbeat_interval_seconds=1,
        lease_duration_seconds=10
    )

@pytest.fixture
def registry():
    reg = HandlerRegistry()
    reg.register(DummySuccessHandler())
    reg.register(DummyFailureHandler())
    reg.register(DummyExceptionHandler())
    return reg

@pytest.fixture
def worker(config, registry):
    return Worker(config, registry)

@patch("worker.worker.db")
@patch("worker.worker.send_command")
@patch("worker.worker.time.sleep")
def test_worker_registers_on_startup(mock_sleep, mock_send, mock_db, worker):
    mock_send.return_value = "EMPTY"
    
    # Run in a thread so we can stop it
    t = threading.Thread(target=worker.run)
    t.start()
    
    # Let it run briefly
    time.sleep(0.1)
    worker.stop()
    t.join(timeout=1.0)
    
    mock_db.register_worker.assert_called_with("test_worker_1", worker.auth_token)

@patch("worker.worker.db")
@patch("worker.worker.send_command")
@patch("worker.worker.time.sleep")
def test_empty_queue_causes_sleep(mock_sleep, mock_send, mock_db, worker):
    mock_send.return_value = "EMPTY"
    worker._poll_and_execute()
    mock_sleep.assert_called_once_with(worker.config.poll_interval_seconds)
    assert not mock_db.get_job.called

@patch("worker.worker.db")
@patch("worker.worker.send_command")
@patch("worker.worker.time.sleep")
def test_valid_job_success_flow(mock_sleep, mock_send, mock_db, worker):
    mock_send.return_value = "JOB job-123"
    
    mock_db.get_job.return_value = {
        "job_id": "job-123",
        "status": "PENDING",
        "job_type": "success_job",
        "payload": '{"foo": "bar"}'
    }
    mock_db.update_job_status.return_value = True

    worker._poll_and_execute()
    
    # Verify order of calls
    # send_command -> db.get_job -> db.update_job_status(IN_PROGRESS) -> handler -> db.update_job_status(COMPLETED) -> send_command(ACK)
    assert mock_send.call_args_list[0] == call(worker.config.queue_host, worker.config.queue_port, f"DEQUEUE {worker.config.lease_duration_seconds}")
    assert mock_db.get_job.call_args_list[0] == call("job-123")
    
    # IN_PROGRESS update
    in_prog_call = mock_db.update_job_status.call_args_list[0]
    assert in_prog_call.args == ("job-123", "IN_PROGRESS")
    assert in_prog_call.kwargs["worker_id"] == "test_worker_1"
    
    # COMPLETED update
    comp_call = mock_db.update_job_status.call_args_list[1]
    assert comp_call.args == ("job-123", "COMPLETED")
    assert comp_call.kwargs["result"] == "done"
    
    # ACK sent
    assert mock_send.call_args_list[1] == call(worker.config.queue_host, worker.config.queue_port, "ACK job-123")
    
    assert worker.consecutive_error_count == 0

@patch("worker.worker.db")
@patch("worker.worker.send_command")
@patch("worker.worker.time.sleep")
def test_handler_exception(mock_sleep, mock_send, mock_db, worker):
    mock_send.return_value = "JOB job-exc"
    
    mock_db.get_job.return_value = {
        "job_id": "job-exc",
        "status": "PENDING",
        "job_type": "exc_job",
        "payload": "{}"
    }
    mock_db.update_job_status.return_value = True

    worker._poll_and_execute()
    
    # Check failed update
    fail_call = mock_db.update_job_status.call_args_list[1]
    assert fail_call.args == ("job-exc", "FAILED")
    assert "exception on purpose" in fail_call.kwargs["result"]
    
    # REQUEUE sent
    assert mock_send.call_args_list[1] == call(worker.config.queue_host, worker.config.queue_port, "REQUEUE job-exc")

@patch("worker.worker.db")
@patch("worker.worker.send_command")
@patch("worker.worker.time.sleep")
def test_handler_failure(mock_sleep, mock_send, mock_db, worker):
    mock_send.return_value = "JOB job-fail"
    
    mock_db.get_job.return_value = {
        "job_id": "job-fail",
        "status": "PENDING",
        "job_type": "fail_job",
        "payload": "{}"
    }
    mock_db.update_job_status.return_value = True

    worker._poll_and_execute()
    
    fail_call = mock_db.update_job_status.call_args_list[1]
    assert fail_call.args == ("job-fail", "FAILED")
    assert fail_call.kwargs["result"] == "failed on purpose"
    
    # REQUEUE sent
    assert mock_send.call_args_list[1] == call(worker.config.queue_host, worker.config.queue_port, "REQUEUE job-fail")

@patch("worker.worker.db")
@patch("worker.worker.send_command")
@patch("worker.worker.time.sleep")
def test_job_not_found(mock_sleep, mock_send, mock_db, worker):
    mock_send.return_value = "JOB job-missing"
    mock_db.get_job.return_value = None
    
    worker._poll_and_execute()
    
    assert mock_send.call_args_list[1] == call(worker.config.queue_host, worker.config.queue_port, "ACK job-missing")
    assert not mock_db.update_job_status.called

@patch("worker.worker.db")
@patch("worker.worker.send_command")
@patch("worker.worker.time.sleep")
def test_job_already_completed(mock_sleep, mock_send, mock_db, worker):
    mock_send.return_value = "JOB job-done"
    mock_db.get_job.return_value = {
        "job_id": "job-done",
        "status": "COMPLETED",
        "job_type": "success_job",
        "payload": "{}"
    }
    
    worker._poll_and_execute()
    
    assert mock_send.call_args_list[1] == call(worker.config.queue_host, worker.config.queue_port, "ACK job-done")
    assert not mock_db.update_job_status.called

@patch("worker.worker.db")
@patch("worker.worker.send_command")
@patch("worker.worker.time.sleep")
def test_unregistered_job_type(mock_sleep, mock_send, mock_db, worker):
    mock_send.return_value = "JOB job-unknown"
    mock_db.get_job.return_value = {
        "job_id": "job-unknown",
        "status": "PENDING",
        "job_type": "unknown_job",
        "payload": "{}"
    }
    mock_db.update_job_status.return_value = True
    
    worker._poll_and_execute()
    
    fail_call = mock_db.update_job_status.call_args_list[1]
    assert fail_call.args == ("job-unknown", "FAILED")
    assert "Unregistered job_type: unknown_job" in fail_call.kwargs["result"]
    assert mock_send.call_args_list[1] == call(worker.config.queue_host, worker.config.queue_port, "ACK job-unknown")

@patch("worker.worker.db")
@patch("worker.worker.send_command")
@patch("worker.worker.time.sleep")
def test_consecutive_error_count(mock_sleep, mock_send, mock_db, worker):
    mock_send.side_effect = QueueUnavailableError("down")
    
    worker._poll_and_execute()
    assert worker.consecutive_error_count == 1
    
    worker._poll_and_execute()
    assert worker.consecutive_error_count == 2
    
    mock_send.side_effect = None
    mock_send.return_value = "EMPTY"
    worker._poll_and_execute()
    assert worker.consecutive_error_count == 0

@patch("worker.worker.db")
@patch("worker.worker.send_command")
@patch("worker.worker.time.sleep")
def test_worker_stop_event(mock_sleep, mock_send, mock_db, worker):
    mock_send.return_value = "EMPTY"
    
    t = threading.Thread(target=worker.run)
    t.start()
    
    # Must have called register
    time.sleep(0.1)
    worker.stop()
    t.join(timeout=2.0)
    assert not t.is_alive()

@patch("worker.worker.db")
@patch("worker.worker.send_command")
@patch("worker.worker.time.sleep")
def test_heartbeat_loop_tolerates_failure(mock_sleep, mock_send, mock_db, worker):
    # Adjust heartbeat interval for fast test
    worker.config.heartbeat_interval_seconds = 0.01
    worker.config.lease_duration_seconds = 1
    
    def side_effect(*args, **kwargs):
        if mock_db.update_worker_heartbeat.call_count == 2:
            worker.stop()
        if mock_db.update_worker_heartbeat.call_count == 1:
            raise Exception("db down")
        return True
        
    mock_db.update_worker_heartbeat.side_effect = side_effect
    
    # Run synchronously. It will loop and wait on _stop_event.
    # On the second iteration, the side effect will call worker.stop() 
    # which sets the event, and the loop will break.
    worker._heartbeat_loop()
    
    assert mock_db.update_worker_heartbeat.call_count >= 2

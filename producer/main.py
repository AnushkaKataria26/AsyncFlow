import os
import sys
import uuid
import json
import logging
from typing import Optional, Any, Dict

from fastapi import FastAPI, HTTPException, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

# Ensure we can import db and shared modules from the project root
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import db
from shared import queue_client

# Configuration
QUEUE_HOST = os.environ.get("QUEUE_HOST", "localhost")
QUEUE_PORT = int(os.environ.get("QUEUE_PORT", 9000))

# Producer acts as a queue client, needs auth token
PRODUCER_WORKER_ID = f"producer_{uuid.uuid4().hex}"
PRODUCER_TOKEN = str(uuid.uuid4())
is_registered = False

app = FastAPI(title="AsyncFlow Producer API")
logger = logging.getLogger(__name__)

# --- Models ---
class JobCreateRequest(BaseModel):
    job_type: str = Field(..., min_length=1, max_length=100)
    payload: Dict[str, Any]
    idempotency_key: Optional[str] = Field(None, max_length=255)
    max_retries: int = Field(3, ge=0, le=10)

# --- Exception Handlers ---
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"error": "Internal Server Error", "detail": "An unexpected error occurred."}
    )

# --- Endpoints ---
@app.post("/jobs", status_code=status.HTTP_201_CREATED)
def create_job(job_req: JobCreateRequest):
    job_id = str(uuid.uuid4())
    
    try:
        payload_str = json.dumps(job_req.payload)
    except (TypeError, ValueError):
        raise HTTPException(status_code=422, detail="payload is not valid JSON")

    # Insert into DB
    try:
        job_dict, is_duplicate = db.insert_job(
            job_id=job_id,
            job_type=job_req.job_type,
            payload=payload_str,
            idempotency_key=job_req.idempotency_key,
            max_retries=job_req.max_retries
        )
    except Exception as e:
        logger.error(f"DB insert failed: {e}")
        raise HTTPException(status_code=500, detail="Database error")

    if is_duplicate:
        # Existing job returned
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "job_id": job_dict["job_id"],
                "status": job_dict["status"],
                "duplicate": True
            }
        )
    
    queue_enqueue_failed = False
    # Enqueue to C++ server
    try:
        global is_registered
        if not is_registered:
            try:
                queue_client.send_command(f"REGISTER {PRODUCER_WORKER_ID} {PRODUCER_TOKEN}", QUEUE_HOST, QUEUE_PORT)
                is_registered = True
            except Exception as e:
                logger.error(f"Failed to register producer with queue: {e}")
                
        # We use the db generated job_id
        response = queue_client.send_command(f"ENQUEUE {job_id} {PRODUCER_TOKEN}", QUEUE_HOST, QUEUE_PORT)
        if response == "DUPLICATE":
            logger.warning(f"Queue returned DUPLICATE for newly created job {job_id}")
    except queue_client.QueueUnavailableError as e:
        logger.error(f"Queue unavailable, job {job_id} remains PENDING in DB: {e}")
        queue_enqueue_failed = True
    except queue_client.QueueTimeoutError as e:
        logger.error(f"Queue timeout, job {job_id} remains PENDING in DB: {e}")
        queue_enqueue_failed = True

    response_content = {
        "job_id": job_id,
        "status": "PENDING",
        "duplicate": False
    }
    
    if queue_enqueue_failed:
        response_content["queue_enqueue_failed"] = True
        return JSONResponse(
            status_code=status.HTTP_202_ACCEPTED,
            content=response_content
        )
        
    return response_content


@app.get("/jobs/{job_id}")
def get_job(job_id: str, verbose: bool = False):
    try:
        job = db.get_job(job_id)
    except Exception as e:
        logger.error(f"DB get failed: {e}")
        raise HTTPException(status_code=500, detail="Database error")
        
    if not job:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
        
    response = {
        "job_id": job["job_id"],
        "job_type": job["job_type"],
        "status": job["status"],
        "retry_count": job["retry_count"],
        "max_retries": job["max_retries"],
        "result": job["result"],
        "created_at": job["created_at"],
        "updated_at": job["updated_at"],
    }
    
    if verbose:
        response["lease_expires_at"] = job.get("lease_expires_at")
        response["worker_id"] = job.get("worker_id")
        response["payload"] = job.get("payload")
        
    return response

@app.get("/health")
def health_check():
    db_reachable = False
    try:
        with db.engine.connect() as conn:
            conn.execute(db.text("SELECT 1"))
        db_reachable = True
    except Exception:
        pass
        
    queue_reachable = False
    try:
        res = queue_client.send_command("PING", QUEUE_HOST, QUEUE_PORT, timeout_seconds=2.0)
        if res == "PONG":
            queue_reachable = True
    except Exception:
        pass
        
    return {
        "status": "ok",
        "db_reachable": db_reachable,
        "queue_reachable": queue_reachable
    }

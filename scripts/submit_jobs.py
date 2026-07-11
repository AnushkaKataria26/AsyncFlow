import argparse
import requests
import uuid
import random
from concurrent.futures import ThreadPoolExecutor, as_completed

def submit_job(host, port, job_type, payload):
    url = f"http://{host}:{port}/jobs"
    idempotency_key = str(uuid.uuid4())
    data = {
        "job_type": job_type,
        "payload": payload,
        "idempotency_key": idempotency_key
    }
    try:
        response = requests.post(url, json=data, timeout=5)
        if response.status_code == 201:
            return True, response.json().get("job_id")
        else:
            print(f"Error submitting job: HTTP {response.status_code} - {response.text}")
            return False, None
    except Exception as e:
        print(f"Connection error: {e}")
        return False, None

def main():
    parser = argparse.ArgumentParser(description="Submit a batch of jobs to Producer API")
    parser.add_argument("--count", type=int, default=50, help="Number of jobs to submit")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="Producer API host")
    parser.add_argument("--port", type=int, default=8000, help="Producer API port")
    args = parser.parse_args()
    
    jobs_to_submit = []
    
    # 40% send_email valid
    for _ in range(int(args.count * 0.4)):
        jobs_to_submit.append(("send_email", {"to": "user@example.com", "subject": "Test", "body": "Hello"}))
        
    # 10% send_email fail
    for _ in range(int(args.count * 0.1)):
        jobs_to_submit.append(("send_email", {"to": "test@fail.com", "subject": "Test", "body": "Fail"}))
        
    # 30% resize_image valid
    for _ in range(int(args.count * 0.3)):
        jobs_to_submit.append(("resize_image", {"image_path": "/tmp/img.png", "width": 800, "height": 600}))
        
    # 10% resize_image dim > 5000 (simulate non-exception failure)
    for _ in range(int(args.count * 0.1)):
        jobs_to_submit.append(("resize_image", {"image_path": "/tmp/img.png", "width": 6000, "height": 6000}))
        
    # 10% noop
    for _ in range(int(args.count * 0.1)):
        jobs_to_submit.append(("noop", {}))
        
    # Pad to exact count if rounding lost some
    while len(jobs_to_submit) < args.count:
        jobs_to_submit.append(("noop", {}))
        
    random.shuffle(jobs_to_submit)
    
    total_submitted = 0
    total_succeeded = 0
    total_failed = 0
    job_ids = []
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [
            executor.submit(submit_job, args.host, args.port, job_type, payload)
            for job_type, payload in jobs_to_submit
        ]
        
        for future in as_completed(futures):
            total_submitted += 1
            success, job_id = future.result()
            if success:
                total_succeeded += 1
                job_ids.append(job_id)
            else:
                total_failed += 1
                
    print(f"\nSummary:")
    print(f"Total Submitted: {total_submitted}")
    print(f"Total Succeeded (201): {total_succeeded}")
    print(f"Total Failed/Errored: {total_failed}")
    print(f"Job IDs: {job_ids}")

if __name__ == "__main__":
    main()

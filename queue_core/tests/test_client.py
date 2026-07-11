import socket
import threading
import time
import subprocess
import os
import sys
import unittest
import uuid

PORT = 9000

class QueueClient:
    def __init__(self, port=PORT):
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect(("127.0.0.1", self.port))

    def send_command(self, cmd):
        self.sock.sendall((cmd + "\n").encode('utf-8'))
        response = self.sock.recv(1024).decode('utf-8')
        return response.strip()

    def close(self):
        self.sock.close()

class TestQueueServer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Build the server before running tests
        build_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "build")
        os.makedirs(build_dir, exist_ok=True)
        subprocess.run(["cmake", ".."], cwd=build_dir, check=True)
        subprocess.run(["make"], cwd=build_dir, check=True)

        cls.server_proc = subprocess.Popen(
            [os.path.join(build_dir, "queue_server"), str(PORT)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        time.sleep(1) # wait for server to start

    @classmethod
    def tearDownClass(cls):
        cls.server_proc.terminate()
        cls.server_proc.wait()
        
        # Optionally print server logs for debugging
        out, err = cls.server_proc.communicate()
        print("\n--- Server Log ---")
        print(out)
        print("------------------")

    def setUp(self):
        self.client = QueueClient()

    def tearDown(self):
        self.client.close()

    def test_auth_commands(self):
        worker1 = "worker-1"
        token1 = "token-1"
        worker2 = "worker-2"
        token2 = "token-2"

        # REGISTER successful
        self.assertEqual(self.client.send_command(f"REGISTER {worker1} {token1}"), "OK")
        
        # REGISTER idempotent (same worker and token)
        self.assertEqual(self.client.send_command(f"REGISTER {worker1} {token1}"), "OK")
        
        # REGISTER error (same worker, different token)
        self.assertTrue(self.client.send_command(f"REGISTER {worker1} {token2}").startswith("ERROR worker_id already registered"))
        
        # DEREGISTER successful
        self.assertEqual(self.client.send_command(f"DEREGISTER {worker1} {token1}"), "OK")
        
        # DEREGISTER error (wrong token)
        self.assertEqual(self.client.send_command(f"REGISTER {worker1} {token1}"), "OK")
        self.assertTrue(self.client.send_command(f"DEREGISTER {worker1} {token2}").startswith("ERROR invalid token"))
        self.client.send_command(f"DEREGISTER {worker1} {token1}")

    def test_auth_errors(self):
        job = str(uuid.uuid4())
        # ENQUEUE without token (old format) => error missing token or auth error
        resp = self.client.send_command(f"ENQUEUE {job}")
        self.assertTrue(resp.startswith("ERROR") or resp == "AUTH_ERROR")

        # DEQUEUE without token
        resp = self.client.send_command("DEQUEUE 10")
        self.assertTrue(resp.startswith("ERROR") or resp == "AUTH_ERROR")

        # DEQUEUE with unregistered token
        self.assertEqual(self.client.send_command("DEQUEUE 10 invalid-token"), "AUTH_ERROR")
        
        # ENQUEUE with unregistered token
        self.assertEqual(self.client.send_command(f"ENQUEUE {job} invalid-token"), "AUTH_ERROR")
        
        # ACK with unregistered token
        self.assertEqual(self.client.send_command(f"ACK {job} invalid-token"), "AUTH_ERROR")
        
        # REQUEUE with unregistered token
        self.assertEqual(self.client.send_command(f"REQUEUE {job} invalid-token"), "AUTH_ERROR")

    def test_basic_commands(self):
        token = "test-token-1"
        worker = "test-worker-1"
        self.client.send_command(f"REGISTER {worker} {token}")

        # STATUS empty
        self.assertEqual(self.client.send_command("STATUS"), "PENDING 0 LEASED 0")
        
        # PING
        self.assertEqual(self.client.send_command("PING"), "PONG")
        
        # ENQUEUE
        job1 = str(uuid.uuid4())
        self.assertEqual(self.client.send_command(f"ENQUEUE {job1} {token}"), "OK")
        self.assertEqual(self.client.send_command("STATUS"), "PENDING 1 LEASED 0")
        
        # ENQUEUE DUPLICATE
        self.assertEqual(self.client.send_command(f"ENQUEUE {job1} {token}"), "DUPLICATE")
        
        # DEQUEUE
        res = self.client.send_command(f"DEQUEUE 10 {token}")
        self.assertEqual(res, f"JOB {job1}")
        self.assertEqual(self.client.send_command("STATUS"), "PENDING 0 LEASED 1")
        
        # DEQUEUE EMPTY
        self.assertEqual(self.client.send_command(f"DEQUEUE 10 {token}"), "EMPTY")
        
        # ACK
        self.assertEqual(self.client.send_command(f"ACK {job1} {token}"), "OK")
        self.assertEqual(self.client.send_command("STATUS"), "PENDING 0 LEASED 0")
        
        # ACK NOT FOUND
        self.assertEqual(self.client.send_command(f"ACK {job1} {token}"), "NOT_FOUND")

    def test_error_cases(self):
        # Invalid commands
        self.assertTrue(self.client.send_command("UNKNOWN").startswith("ERROR"))
        
        # Missing args
        self.assertTrue(self.client.send_command("ENQUEUE").startswith("ERROR"))
        self.assertTrue(self.client.send_command("DEQUEUE").startswith("ERROR"))
        
        # Whitespace in job id
        # 'ENQUEUE job id with spaces' parses as job_id='job', auth_token='id'. 
        # Since 'id' is not a valid token, it returns AUTH_ERROR.
        resp = self.client.send_command("ENQUEUE job id with spaces")
        self.assertTrue(resp.startswith("ERROR") or resp == "AUTH_ERROR")

        # Negative lease
        self.assertTrue(self.client.send_command("DEQUEUE -5 token").startswith("ERROR") or self.client.send_command("DEQUEUE -5 token") == "AUTH_ERROR")
        self.assertTrue(self.client.send_command("DEQUEUE 0 token").startswith("ERROR") or self.client.send_command("DEQUEUE 0 token") == "AUTH_ERROR")
        self.assertTrue(self.client.send_command("DEQUEUE abc token").startswith("ERROR") or self.client.send_command("DEQUEUE abc token") == "AUTH_ERROR")

        # Line too long
        long_line = "A" * 300
        self.assertTrue(self.client.send_command(long_line).startswith("ERROR line too long"))

    def test_lease_expiry_and_requeue(self):
        token = "test-token-2"
        worker = "test-worker-2"
        self.client.send_command(f"REGISTER {worker} {token}")

        job = str(uuid.uuid4())
        self.client.send_command(f"ENQUEUE {job} {token}")
        
        # Lease for 1 second
        self.assertEqual(self.client.send_command(f"DEQUEUE 1 {token}"), f"JOB {job}")
        self.assertEqual(self.client.send_command("STATUS"), "PENDING 0 LEASED 1")
        
        # Sleep to let lease expire
        time.sleep(2)
        
        # Sweep should occur on next DEQUEUE, and it should get the same job back
        self.assertEqual(self.client.send_command(f"DEQUEUE 10 {token}"), f"JOB {job}")
        
        # REQUEUE command
        self.assertEqual(self.client.send_command(f"REQUEUE {job} {token}"), "OK")
        self.assertEqual(self.client.send_command("STATUS"), "PENDING 1 LEASED 0")
        
        # Check it can be dequeued again
        self.assertEqual(self.client.send_command(f"DEQUEUE 10 {token}"), f"JOB {job}")
        self.client.send_command(f"ACK {job} {token}")

    def test_concurrent_dequeue(self):
        token = "test-token-3"
        worker = "test-worker-3"
        self.client.send_command(f"REGISTER {worker} {token}")

        jobs = [str(uuid.uuid4()) for _ in range(3)]
        for j in jobs:
            self.assertEqual(self.client.send_command(f"ENQUEUE {j} {token}"), "OK")
            
        results = []
        def concurrent_worker():
            # Dedicated client per thread
            cli = QueueClient()
            res = cli.send_command(f"DEQUEUE 10 {token}")
            results.append(res)
            cli.close()

        threads = [threading.Thread(target=concurrent_worker) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        job_responses = [r for r in results if r.startswith("JOB")]
        empty_responses = [r for r in results if r == "EMPTY"]

        self.assertEqual(len(job_responses), 3)
        self.assertEqual(len(empty_responses), 2)
        
        # Assert all 3 jobs are distinct
        job_ids = set([r.split()[1] for r in job_responses])
        self.assertEqual(len(job_ids), 3)
        self.assertEqual(job_ids, set(jobs))

        # Clean up
        for j in job_ids:
            self.client.send_command(f"ACK {j} {token}")

if __name__ == '__main__':
    unittest.main()

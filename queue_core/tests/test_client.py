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

    def test_basic_commands(self):
        # STATUS empty
        self.assertEqual(self.client.send_command("STATUS"), "PENDING 0 LEASED 0")
        
        # PING
        self.assertEqual(self.client.send_command("PING"), "PONG")
        
        # ENQUEUE
        job1 = str(uuid.uuid4())
        self.assertEqual(self.client.send_command(f"ENQUEUE {job1}"), "OK")
        self.assertEqual(self.client.send_command("STATUS"), "PENDING 1 LEASED 0")
        
        # ENQUEUE DUPLICATE
        self.assertEqual(self.client.send_command(f"ENQUEUE {job1}"), "DUPLICATE")
        
        # DEQUEUE
        res = self.client.send_command("DEQUEUE 10")
        self.assertEqual(res, f"JOB {job1}")
        self.assertEqual(self.client.send_command("STATUS"), "PENDING 0 LEASED 1")
        
        # DEQUEUE EMPTY
        self.assertEqual(self.client.send_command("DEQUEUE 10"), "EMPTY")
        
        # ACK
        self.assertEqual(self.client.send_command(f"ACK {job1}"), "OK")
        self.assertEqual(self.client.send_command("STATUS"), "PENDING 0 LEASED 0")
        
        # ACK NOT FOUND
        self.assertEqual(self.client.send_command(f"ACK {job1}"), "NOT_FOUND")

    def test_error_cases(self):
        # Invalid commands
        self.assertTrue(self.client.send_command("UNKNOWN").startswith("ERROR"))
        
        # Missing args
        self.assertTrue(self.client.send_command("ENQUEUE").startswith("ERROR"))
        self.assertTrue(self.client.send_command("DEQUEUE").startswith("ERROR"))
        
        # Whitespace in job id
        self.assertTrue(self.client.send_command("ENQUEUE job id with spaces").startswith("ERROR"))
        
        # Negative lease
        self.assertTrue(self.client.send_command("DEQUEUE -5").startswith("ERROR"))
        self.assertTrue(self.client.send_command("DEQUEUE 0").startswith("ERROR"))
        self.assertTrue(self.client.send_command("DEQUEUE abc").startswith("ERROR"))

        # Line too long
        long_line = "A" * 300
        self.assertTrue(self.client.send_command(long_line).startswith("ERROR line too long"))

    def test_lease_expiry_and_requeue(self):
        job = str(uuid.uuid4())
        self.client.send_command(f"ENQUEUE {job}")
        
        # Lease for 1 second
        self.assertEqual(self.client.send_command("DEQUEUE 1"), f"JOB {job}")
        self.assertEqual(self.client.send_command("STATUS"), "PENDING 0 LEASED 1")
        
        # Sleep to let lease expire
        time.sleep(2)
        
        # Sweep should occur on next DEQUEUE, and it should get the same job back
        self.assertEqual(self.client.send_command("DEQUEUE 10"), f"JOB {job}")
        
        # REQUEUE command
        self.assertEqual(self.client.send_command(f"REQUEUE {job}"), "OK")
        self.assertEqual(self.client.send_command("STATUS"), "PENDING 1 LEASED 0")
        
        # Check it can be dequeued again
        self.assertEqual(self.client.send_command("DEQUEUE 10"), f"JOB {job}")
        self.client.send_command(f"ACK {job}")

    def test_concurrent_dequeue(self):
        jobs = [str(uuid.uuid4()) for _ in range(3)]
        for j in jobs:
            self.assertEqual(self.client.send_command(f"ENQUEUE {j}"), "OK")
            
        results = []
        def worker():
            # Dedicated client per thread
            cli = QueueClient()
            res = cli.send_command("DEQUEUE 10")
            results.append(res)
            cli.close()

        threads = [threading.Thread(target=worker) for _ in range(5)]
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
            self.client.send_command(f"ACK {j}")

if __name__ == '__main__':
    unittest.main()

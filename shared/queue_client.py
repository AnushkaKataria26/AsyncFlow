import socket

class QueueUnavailableError(Exception):
    pass

class QueueTimeoutError(Exception):
    pass

def send_command(command: str, host: str, port: int, timeout_seconds: float = 5.0) -> str:
    """
    Connects to the C++ queue server, sends a command, and reads the response.
    """
    try:
        with socket.create_connection((host, port), timeout=timeout_seconds) as sock:
            sock.sendall((command + "\n").encode('utf-8'))
            
            response = b""
            while True:
                chunk = sock.recv(1024)
                if not chunk:
                    break
                response += chunk
                if b"\n" in response:
                    break
                    
            return response.decode('utf-8').strip()
    except socket.timeout:
        raise QueueTimeoutError(f"Connection to queue server at {host}:{port} timed out after {timeout_seconds}s")
    except ConnectionRefusedError:
        raise QueueUnavailableError(f"Connection refused by queue server at {host}:{port}")
    except socket.error as e:
        raise QueueUnavailableError(f"Queue server at {host}:{port} unavailable: {e}")

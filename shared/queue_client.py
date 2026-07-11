import socket

class QueueUnavailableError(Exception):
    pass

class QueueTimeoutError(Exception):
    pass

class QueueAuthError(Exception):
    pass

def build_authenticated_command(command_parts: list[str], auth_token: str) -> str:
    """
    Builds a command string with the auth token appended.
    """
    return " ".join(command_parts + [auth_token])

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
                    
            resp_str = response.decode('utf-8').strip()
            if resp_str == "AUTH_ERROR":
                raise QueueAuthError("Auth token rejected by queue server")
            return resp_str
    except socket.timeout:
        raise QueueTimeoutError(f"Connection to queue server at {host}:{port} timed out after {timeout_seconds}s")
    except ConnectionRefusedError:
        raise QueueUnavailableError(f"Connection refused by queue server at {host}:{port}")
    except socket.error as e:
        raise QueueUnavailableError(f"Queue server at {host}:{port} unavailable: {e}")

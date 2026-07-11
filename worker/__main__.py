import logging
import signal
import sys
from .worker_config import WorkerConfig
from .handlers.registry import HandlerRegistry
from .worker import Worker

def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    config = WorkerConfig()
    registry = HandlerRegistry.create_default()
    
    worker = Worker(config, registry)
    
    def shutdown_handler(signum, frame):
        logging.info(f"Received signal {signum}, shutting down worker...")
        worker.stop()
        
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)
    
    worker.run()

if __name__ == "__main__":
    main()

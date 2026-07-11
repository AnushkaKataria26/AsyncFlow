import sys
import logging
import signal
from scheduler.scheduler_config import SchedulerConfig
from scheduler.scheduler import Scheduler

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    
    config = SchedulerConfig.from_env()
    scheduler = Scheduler(config)
    
    def handle_sigint(signum, frame):
        logging.info("Received signal %d, stopping scheduler...", signum)
        scheduler.stop()

    signal.signal(signal.SIGINT, handle_sigint)
    signal.signal(signal.SIGTERM, handle_sigint)

    scheduler.run()

if __name__ == "__main__":
    main()

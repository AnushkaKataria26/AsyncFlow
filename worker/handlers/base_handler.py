from abc import ABC, abstractmethod
from typing import Optional

class JobHandler(ABC):
    @property
    @abstractmethod
    def job_type(self) -> str:
        pass

    @abstractmethod
    def execute(self, job_id: str, payload: dict) -> dict:
        pass

    def validate_payload(self, payload: dict) -> None:
        pass

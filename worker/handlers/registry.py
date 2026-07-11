from .base_handler import JobHandler
from .concrete_handlers import SendEmailHandler, ResizeImageHandler, GenerateReportHandler, NoOpHandler

class HandlerRegistry:
    def __init__(self):
        self._handlers = {}

    def register(self, handler: JobHandler) -> None:
        if handler.job_type in self._handlers:
            raise ValueError(f"Handler for job_type '{handler.job_type}' is already registered")
        self._handlers[handler.job_type] = handler

    def get(self, job_type: str) -> JobHandler:
        if job_type not in self._handlers:
            raise KeyError(f"No handler registered for job_type '{job_type}'")
        return self._handlers[job_type]

    def list_types(self) -> list[str]:
        return list(self._handlers.keys())

    @classmethod
    def create_default(cls) -> "HandlerRegistry":
        registry = cls()
        registry.register(SendEmailHandler())
        registry.register(ResizeImageHandler())
        registry.register(GenerateReportHandler())
        registry.register(NoOpHandler())
        return registry

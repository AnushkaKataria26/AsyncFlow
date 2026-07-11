import random
import time
from datetime import datetime
from .base_handler import JobHandler

class SendEmailHandler(JobHandler):
    @property
    def job_type(self) -> str:
        return "send_email"

    def validate_payload(self, payload: dict) -> None:
        if "to" not in payload or not isinstance(payload["to"], str):
            raise ValueError("Missing or invalid 'to'")
        if "@" not in payload["to"]:
            raise ValueError("'to' must contain '@'")
        if "subject" not in payload or not isinstance(payload["subject"], str) or not payload["subject"]:
            raise ValueError("Missing or invalid 'subject'")
        if "body" not in payload or not isinstance(payload["body"], str):
            raise ValueError("Missing or invalid 'body'")

    def execute(self, job_id: str, payload: dict) -> dict:
        try:
            if payload["to"].endswith("@fail.com"):
                raise RuntimeError("SMTP server rejected")
            time.sleep(random.uniform(0.5, 2.0))
            return {"success": True, "result": f"Email sent to {payload['to']}", "error": None}
        except Exception as e:
            return {"success": False, "result": None, "error": str(e)}

class ResizeImageHandler(JobHandler):
    @property
    def job_type(self) -> str:
        return "resize_image"

    def validate_payload(self, payload: dict) -> None:
        if "image_path" not in payload or not isinstance(payload["image_path"], str):
            raise ValueError("Missing or invalid 'image_path'")
        for key in ["width", "height"]:
            if key not in payload or type(payload[key]) is not int:
                raise ValueError(f"Missing or invalid '{key}'")
            if not (1 <= payload[key] <= 9999):
                raise ValueError(f"'{key}' must be between 1 and 9999")

    def execute(self, job_id: str, payload: dict) -> dict:
        try:
            if payload["width"] > 5000 or payload["height"] > 5000:
                return {"success": False, "result": None, "error": "Dimension exceeds processing limit"}
            time.sleep(random.uniform(1.0, 3.0))
            return {"success": True, "result": f"Resized {payload['image_path']} to {payload['width']}x{payload['height']}", "error": None}
        except Exception as e:
            return {"success": False, "result": None, "error": str(e)}

class GenerateReportHandler(JobHandler):
    @property
    def job_type(self) -> str:
        return "generate_report"

    def validate_payload(self, payload: dict) -> None:
        if "report_type" not in payload or payload["report_type"] not in ["daily", "weekly", "monthly"]:
            raise ValueError("Missing or invalid 'report_type'")
        if "date" not in payload or not isinstance(payload["date"], str):
            raise ValueError("Missing or invalid 'date'")
        try:
            datetime.strptime(payload["date"], "%Y-%m-%d")
        except ValueError:
            raise ValueError("Invalid date format, should be YYYY-MM-DD")

    def execute(self, job_id: str, payload: dict) -> dict:
        try:
            dt = datetime.strptime(payload["date"], "%Y-%m-%d")
            if payload["report_type"] == "weekly" and dt.weekday() >= 5:
                return {"success": False, "result": None, "error": "Weekly reports cannot be generated on weekends"}
            time.sleep(random.uniform(2.0, 4.0))
            return {"success": True, "result": f"Report {payload['report_type']} generated for {payload['date']}", "error": None}
        except Exception as e:
            return {"success": False, "result": None, "error": str(e)}

class NoOpHandler(JobHandler):
    @property
    def job_type(self) -> str:
        return "noop"

    def validate_payload(self, payload: dict) -> None:
        pass

    def execute(self, job_id: str, payload: dict) -> dict:
        try:
            if "delay_seconds" in payload:
                time.sleep(payload["delay_seconds"])
            return {"success": True, "result": "noop", "error": None}
        except Exception as e:
            return {"success": False, "result": None, "error": str(e)}

import pytest
from worker.handlers.concrete_handlers import (
    SendEmailHandler,
    ResizeImageHandler,
    GenerateReportHandler,
    NoOpHandler,
)
from worker.handlers.registry import HandlerRegistry

def test_send_email_handler_validate_payload():
    handler = SendEmailHandler()
    with pytest.raises(ValueError):
        handler.validate_payload({})
    with pytest.raises(ValueError):
        handler.validate_payload({"to": "test@test.com"})
    with pytest.raises(ValueError):
        handler.validate_payload({"to": "testtest.com", "subject": "test", "body": "test"})
    with pytest.raises(ValueError):
        handler.validate_payload({"to": "test@test.com", "subject": "", "body": "test"})
    handler.validate_payload({"to": "test@test.com", "subject": "test", "body": "test"})

def test_send_email_handler_execute():
    handler = SendEmailHandler()
    res = handler.execute("1", {"to": "test@test.com", "subject": "test", "body": "test"})
    assert res["success"] is True
    assert res["result"] == "Email sent to test@test.com"

    res = handler.execute("2", {"to": "test@fail.com", "subject": "test", "body": "test"})
    assert res["success"] is False
    assert res["error"] == "SMTP server rejected"

def test_resize_image_handler_validate_payload():
    handler = ResizeImageHandler()
    with pytest.raises(ValueError):
        handler.validate_payload({})
    with pytest.raises(ValueError):
        handler.validate_payload({"image_path": "test.png", "width": 100})
    with pytest.raises(ValueError):
        handler.validate_payload({"image_path": "test.png", "width": "100", "height": 100})
    with pytest.raises(ValueError):
        handler.validate_payload({"image_path": "test.png", "width": 10000, "height": 100})
    with pytest.raises(ValueError):
        handler.validate_payload({"image_path": "test.png", "width": 0, "height": 100})
    handler.validate_payload({"image_path": "test.png", "width": 100, "height": 100})

def test_resize_image_handler_execute():
    handler = ResizeImageHandler()
    res = handler.execute("1", {"image_path": "test.png", "width": 100, "height": 100})
    assert res["success"] is True
    assert res["result"] == "Resized test.png to 100x100"

    res = handler.execute("2", {"image_path": "test.png", "width": 5001, "height": 100})
    assert res["success"] is False
    assert res["error"] == "Dimension exceeds processing limit"

def test_generate_report_handler_validate_payload():
    handler = GenerateReportHandler()
    with pytest.raises(ValueError):
        handler.validate_payload({})
    with pytest.raises(ValueError):
        handler.validate_payload({"report_type": "yearly", "date": "2023-01-01"})
    with pytest.raises(ValueError):
        handler.validate_payload({"report_type": "daily", "date": "2023-13-01"})
    handler.validate_payload({"report_type": "daily", "date": "2023-01-01"})

def test_generate_report_handler_execute():
    handler = GenerateReportHandler()
    res = handler.execute("1", {"report_type": "daily", "date": "2023-01-01"})
    assert res["success"] is True
    assert res["result"] == "Report daily generated for 2023-01-01"

    # 2023-01-01 is a Sunday
    res = handler.execute("2", {"report_type": "weekly", "date": "2023-01-01"})
    assert res["success"] is False
    assert res["error"] == "Weekly reports cannot be generated on weekends"

def test_noop_handler():
    handler = NoOpHandler()
    handler.validate_payload({})
    res = handler.execute("1", {})
    assert res["success"] is True
    assert res["result"] == "noop"

def test_registry():
    registry = HandlerRegistry()
    handler = NoOpHandler()
    registry.register(handler)
    with pytest.raises(ValueError):
        registry.register(handler)

    assert registry.get("noop") is handler
    with pytest.raises(KeyError):
        registry.get("unknown")

def test_registry_create_default():
    registry = HandlerRegistry.create_default()
    types = registry.list_types()
    assert set(types) == {"send_email", "resize_image", "generate_report", "noop"}

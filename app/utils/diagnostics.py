from __future__ import annotations
import json, time, uuid, platform, traceback, sys, os
from contextlib import ContextDecorator
from typing import Any, Dict, List, Optional

def _now_ms() -> int:
    return int(time.time() * 1000)

class DiagnosticContext(ContextDecorator):
    """
    Lightweight, structured diagnostics with correlation IDs and step timing.
    Usage:
        with DiagnosticContext(file_name=name, file_size=len(data)) as diag:
            diag.step("read_excel_start")
            ...
            diag.warn("missing_qty", sheet="Sheet1")
    """
    def __init__(self, **root: Any) -> None:
        self.correlation_id = str(uuid.uuid4())
        self.started_at_ms = _now_ms()
        self.finished_at_ms: Optional[int] = None
        self.root: Dict[str, Any] = dict(root)
        self.events: List[Dict[str, Any]] = []
        self.warnings: List[Dict[str, Any]] = []
        self.error_info: Optional[Dict[str, Any]] = None

        # capture environment details once
        self.environment = {
            "python_version": sys.version.split()[0],
            "platform": platform.platform(),
            "app_env": os.environ.get("APP_ENV"),
            "debug": os.environ.get("DEBUG") in ("1", "true", "True"),
        }
        # optional libs (best-effort)
        try:
            import pandas as pd  # type: ignore
            self.environment["pandas_version"] = getattr(pd, "__version__", None)
        except Exception:
            pass

    def step(self, name: str, **kv: Any) -> None:
        self.events.append({"t_ms": _now_ms(), "step": name, **kv})

    def warn(self, code: str, message: Optional[str] = None, **kv: Any) -> None:
        self.warnings.append({"t_ms": _now_ms(), "code": code, "message": message, **kv})

    def error(self, code: str, exc: Optional[BaseException] = None, **kv: Any) -> None:
        info: Dict[str, Any] = {"code": code, **kv}
        if exc is not None:
            info["type"] = exc.__class__.__name__
            info["message"] = str(exc)
            if self.environment.get("debug"):
                info["stack"] = "".join(traceback.format_exception(exc))
        self.error_info = info

    def to_dict(self) -> Dict[str, Any]:
        dur = None if self.finished_at_ms is None else (self.finished_at_ms - self.started_at_ms)
        return {
            "correlation_id": self.correlation_id,
            "started_at_ms": self.started_at_ms,
            "finished_at_ms": self.finished_at_ms,
            "duration_ms": dur,
            "root": self.root,
            "events": self.events,
            "warnings": self.warnings,
            "error": self.error_info,
            "environment": self.environment,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2)

    def __enter__(self) -> "DiagnosticContext":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.finished_at_ms = _now_ms()
        # don't swallow exceptions; just enrich diagnostics if unhandled
        if exc is not None and self.error_info is None:
            self.error("unhandled_exception", exc)
        return False

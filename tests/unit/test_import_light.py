"""WE-T3: `import webhook_engine` needs only the base dependencies."""

from __future__ import annotations

import subprocess
import sys

_BLOCKED = ("redis", "fastapi", "motor", "sqlalchemy", "asyncpg", "uvicorn", "pydantic_settings")

_PROBE = f"""
import importlib.abc, sys

class Block(importlib.abc.MetaPathFinder):
    def find_spec(self, name, path, target=None):
        if name.split(".")[0] in {_BLOCKED!r}:
            raise ImportError("blocked: " + name)
        return None

sys.meta_path.insert(0, Block())
import webhook_engine
webhook_engine.WebhookEngine, webhook_engine.WebhookDispatcher, webhook_engine.ProfileRegistry
print("ok")
"""


def test_the_package_imports_without_any_extra() -> None:
    out = subprocess.run(
        [sys.executable, "-c", _PROBE], capture_output=True, text=True, check=False
    )
    assert out.stdout.strip() == "ok", out.stderr

import os
import sys
from pathlib import Path


def pytest_configure():
    # Ensure repo root is importable for `import app` in tests.
    repo_root = Path(__file__).resolve().parents[1]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

    # Prevent background threads from starting during import in tests.
    os.environ.setdefault("DISABLE_BACKGROUND_THREADS", "1")
    # Ensure we don't accidentally try to talk to production Postgres from tests.
    os.environ.pop("DATABASE_URL", None)
    # Prevent any accidental outbound webhook calls.
    os.environ.pop("DISCORD_WEBHOOK_URL", None)

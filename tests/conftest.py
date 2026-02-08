import os


def pytest_configure():
    # Prevent background threads from starting during import in tests.
    os.environ.setdefault("DISABLE_BACKGROUND_THREADS", "1")
    # Ensure we don't accidentally try to talk to production Postgres from tests.
    os.environ.pop("DATABASE_URL", None)
    # Prevent any accidental outbound webhook calls.
    os.environ.pop("DISCORD_WEBHOOK_URL", None)

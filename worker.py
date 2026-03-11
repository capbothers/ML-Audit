"""
Standalone scheduler worker process.

Run as a separate Render worker dyno so the scheduler lifecycle is
decoupled from web-process restarts.  The web dyno (app/main.py) no
longer starts APScheduler — this process owns it exclusively.

Usage:
    python worker.py
"""
import asyncio
import signal
import sys

from app.utils.logger import log


async def main() -> None:
    # ── Credentials ──────────────────────────────────────────────────
    from app.utils.credentials import bootstrap_credentials
    bootstrap_credentials()

    # ── Database ─────────────────────────────────────────────────────
    from app.models.base import init_db, SessionLocal
    init_db()
    log.info("Worker: database initialised")

    # Seed initial admin user (idempotent — safe to call from either process)
    from app.services import auth_service
    db = SessionLocal()
    try:
        auth_service.seed_initial_user(db)
    finally:
        db.close()

    # ── Scheduler ────────────────────────────────────────────────────
    from app.scheduler import start_scheduler, stop_scheduler

    start_scheduler()
    log.info("Worker: scheduler started — waiting for jobs")

    # Stay alive until SIGTERM / SIGINT
    stop_event = asyncio.Event()

    def _shutdown(sig, _frame):
        log.info(f"Worker: received {signal.Signals(sig).name}, shutting down")
        stop_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, _shutdown)

    await stop_event.wait()

    stop_scheduler()
    log.info("Worker: scheduler stopped — exiting")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)

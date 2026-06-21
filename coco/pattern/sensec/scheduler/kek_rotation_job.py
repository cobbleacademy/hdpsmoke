"""
APScheduler cron job for periodic KEK rotation.
Registered in main.py lifespan if KEK_ROTATION_ENABLED=true.
"""

from __future__ import annotations

import structlog
from apscheduler.schedulers.asyncio import AsyncIOScheduler

log = structlog.get_logger("kek_rotation_job")


def start_rotation_scheduler(
    cron_expr: str,
    kek_client,
    session_factory,
) -> AsyncIOScheduler:
    minute, hour, day, month, day_of_week = cron_expr.split()

    scheduler = AsyncIOScheduler()

    async def _job():
        from app.services.rotation_service import rotate_kek
        log.info("kek_rotation_job_triggered")
        try:
            result = await rotate_kek(
                kek_client=kek_client,
                session_factory=session_factory,
                triggered_by="scheduler",
            )
            log.info("kek_rotation_job_completed", records=result.records_queued)
        except Exception as exc:
            log.error("kek_rotation_job_failed", error=str(exc))

    scheduler.add_job(
        _job,
        trigger="cron",
        minute=minute,
        hour=hour,
        day=day,
        month=month,
        day_of_week=day_of_week,
        id="kek_rotation",
        replace_existing=True,
    )

    scheduler.start()
    return scheduler

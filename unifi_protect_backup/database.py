# noqa: D100

import time
from datetime import datetime

import aiosqlite
from sqlite3 import IntegrityError
from uiprotect.data.nvr import Event

STATUS_QUEUED = "queued"
STATUS_DOWNLOADING = "downloading"
STATUS_DOWNLOADED = "downloaded"
STATUS_UPLOADING = "uploading"
STATUS_UPLOADED = "uploaded"
STATUS_FAILED = "failed"
STATUS_IGNORED = "ignored"

RETRYABLE_STATUSES = (STATUS_FAILED,)
ACTIVE_STATUSES = (STATUS_QUEUED, STATUS_DOWNLOADING, STATUS_DOWNLOADED, STATUS_UPLOADING)
TERMINAL_STATUSES = (STATUS_UPLOADED, STATUS_IGNORED)


def _now() -> float:
    return time.time()


async def create_database(path: str):
    """Create sqlite database and creates the events, backups, and event status tables."""
    db = await aiosqlite.connect(path)
    await db.execute("CREATE TABLE events(id PRIMARY KEY, type, camera_id, start REAL, end REAL)")
    await db.execute(
        "CREATE TABLE backups(id REFERENCES events(id) ON DELETE CASCADE, remote, path, PRIMARY KEY (id, remote))"
    )
    await migrate_database(db)
    return db


async def migrate_database(db: aiosqlite.Connection) -> None:
    """Add event processing state to existing databases and recover abandoned claims."""
    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS event_processing(
            id PRIMARY KEY,
            status TEXT NOT NULL,
            updated_at REAL NOT NULL
        )
        """
    )
    await db.execute("CREATE INDEX IF NOT EXISTS idx_event_processing_status ON event_processing(status)")
    now = _now()
    await db.execute(
        """
        INSERT OR IGNORE INTO event_processing(id, status, updated_at)
        SELECT id, ?, ? FROM events
        """,
        (STATUS_UPLOADED, now),
    )
    await db.execute(
        """
        UPDATE event_processing
        SET status = ?, updated_at = ?
        WHERE id IN (SELECT id FROM events)
          AND status NOT IN (?, ?)
        """,
        (STATUS_UPLOADED, now, STATUS_UPLOADED, STATUS_IGNORED),
    )
    await db.execute(
        f"""
        UPDATE event_processing
        SET status = ?, updated_at = ?
        WHERE status IN ({",".join("?" for _ in ACTIVE_STATUSES)})
        """,
        (STATUS_FAILED, now, *ACTIVE_STATUSES),
    )
    await db.commit()


async def claim_event_for_processing(db: aiosqlite.Connection, event_id: str) -> bool:
    """Atomically claim an event before it is placed on the download queue."""
    now = _now()
    cursor = await db.execute(
        """
        INSERT OR IGNORE INTO event_processing(id, status, updated_at)
        SELECT ?, ?, ?
        WHERE NOT EXISTS (SELECT 1 FROM events WHERE id = ?)
        """,
        (event_id, STATUS_QUEUED, now, event_id),
    )
    if cursor.rowcount == 1:
        await db.commit()
        return True

    cursor = await db.execute(
        """
        UPDATE event_processing
        SET status = ?, updated_at = ?
        WHERE id = ?
          AND status = ?
          AND NOT EXISTS (SELECT 1 FROM events WHERE id = ?)
        """,
        (STATUS_QUEUED, now, event_id, STATUS_FAILED, event_id),
    )
    await db.commit()
    return cursor.rowcount == 1


async def active_or_completed_event_ids(db: aiosqlite.Connection) -> set[str]:
    """Return event IDs that should not be queued again."""
    async with db.execute("SELECT id FROM events") as cursor:
        event_ids = {row[0] async for row in cursor}

    async with db.execute(
        f"""
        SELECT id FROM event_processing
        WHERE status IN ({",".join("?" for _ in ACTIVE_STATUSES)})
        """,
        ACTIVE_STATUSES,
    ) as cursor:
        async for row in cursor:
            event_ids.add(row[0])

    return event_ids


async def mark_event_downloading(db: aiosqlite.Connection, event_id: str) -> bool:
    """Move a queued event into the downloading state."""
    cursor = await db.execute(
        """
        UPDATE event_processing
        SET status = ?, updated_at = ?
        WHERE id = ?
          AND status = ?
          AND NOT EXISTS (SELECT 1 FROM events WHERE id = ?)
        """,
        (STATUS_DOWNLOADING, _now(), event_id, STATUS_QUEUED, event_id),
    )
    await db.commit()
    return cursor.rowcount == 1


async def mark_event_downloaded(db: aiosqlite.Connection, event_id: str) -> None:
    """Mark an event as downloaded while its video bytes wait in the upload queue."""
    await db.execute(
        """
        UPDATE event_processing
        SET status = ?, updated_at = ?
        WHERE id = ?
          AND status = ?
        """,
        (STATUS_DOWNLOADED, _now(), event_id, STATUS_DOWNLOADING),
    )
    await db.commit()


async def claim_event_for_upload(db: aiosqlite.Connection, event_id: str) -> bool:
    """Atomically claim an event immediately before uploading it."""
    cursor = await db.execute(
        """
        UPDATE event_processing
        SET status = ?, updated_at = ?
        WHERE id = ?
          AND status = ?
          AND NOT EXISTS (SELECT 1 FROM events WHERE id = ?)
        """,
        (STATUS_UPLOADING, _now(), event_id, STATUS_DOWNLOADED, event_id),
    )
    await db.commit()
    return cursor.rowcount == 1


async def mark_event_failed(db: aiosqlite.Connection, event_id: str) -> None:
    """Make an event eligible for retry without marking it as backed up."""
    now = _now()
    await db.execute(
        """
        INSERT OR IGNORE INTO event_processing(id, status, updated_at)
        VALUES (?, ?, ?)
        """,
        (event_id, STATUS_FAILED, now),
    )
    await db.execute(
        """
        UPDATE event_processing
        SET status = ?, updated_at = ?
        WHERE id = ?
          AND status NOT IN (?, ?)
        """,
        (STATUS_FAILED, now, event_id, STATUS_UPLOADED, STATUS_IGNORED),
    )
    await db.commit()


async def mark_event_upload_failed(db: aiosqlite.Connection, event_id: str) -> None:
    """Move a failed upload attempt back to retryable state."""
    await db.execute(
        """
        UPDATE event_processing
        SET status = ?, updated_at = ?
        WHERE id = ?
          AND status = ?
        """,
        (STATUS_FAILED, _now(), event_id, STATUS_UPLOADING),
    )
    await db.commit()


async def mark_event_ignored(db: aiosqlite.Connection, event: Event) -> None:
    """Record an event as intentionally ignored so it is not retried."""
    assert isinstance(event.start, datetime)
    assert isinstance(event.end, datetime)
    try:
        await db.execute(
            """
            INSERT INTO events VALUES (?, ?, ?, ?, ?)
            """,
            (event.id, event.type.value, event.camera_id, event.start.timestamp(), event.end.timestamp()),
        )
    except IntegrityError:
        pass
    await db.execute(
        """
        INSERT INTO event_processing(id, status, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET status = excluded.status, updated_at = excluded.updated_at
        """,
        (event.id, STATUS_IGNORED, _now()),
    )
    await db.commit()


async def mark_event_uploaded(db: aiosqlite.Connection, event_id: str) -> None:
    """Record successful completion in the processing state table."""
    await db.execute(
        """
        INSERT INTO event_processing(id, status, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET status = excluded.status, updated_at = excluded.updated_at
        """,
        (event_id, STATUS_UPLOADED, _now()),
    )
    await db.commit()

import asyncio
import pathlib
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from unifi_protect_backup.database import (
    STATUS_DOWNLOADED,
    STATUS_DOWNLOADING,
    STATUS_FAILED,
    STATUS_QUEUED,
    STATUS_UPLOADING,
    claim_event_for_upload,
    claim_event_for_processing,
    create_database,
    mark_event_downloaded,
    mark_event_downloading,
    mark_event_failed,
    migrate_database,
)
from unifi_protect_backup.uploader import VideoUploader
from unifi_protect_backup.utils import VideoQueue


def _event(event_id: str = "fb213956-442f-4e1a-9284-7769eace438f"):
    end = datetime.now(timezone.utc) - timedelta(minutes=5)
    return SimpleNamespace(
        id=event_id,
        type=SimpleNamespace(value="motion"),
        camera_id="camera-1",
        start=end - timedelta(seconds=20),
        end=end,
        smart_detect_types=[],
    )


def test_processing_claim_suppresses_duplicates_and_allows_failed_retry(tmp_path):
    async def run_test():
        db = await create_database(str(tmp_path / "events.sqlite"))
        event = _event()

        assert await claim_event_for_processing(db, event.id) is True
        assert await claim_event_for_processing(db, event.id) is False

        assert await mark_event_downloading(db, event.id) is True
        assert await claim_event_for_processing(db, event.id) is False

        await mark_event_failed(db, event.id)
        assert await claim_event_for_processing(db, event.id) is True

        await db.close()

    asyncio.run(run_test())


def test_startup_migration_makes_abandoned_claims_retryable(tmp_path):
    async def run_test():
        db = await create_database(str(tmp_path / "events.sqlite"))
        active_statuses = [STATUS_QUEUED, STATUS_DOWNLOADING, STATUS_DOWNLOADED, STATUS_UPLOADING]
        for index, status in enumerate(active_statuses):
            await db.execute(
                """
                INSERT INTO event_processing(id, status, updated_at)
                VALUES (?, ?, ?)
                """,
                (f"event-{index}", status, 1.0),
            )
        await db.commit()

        await migrate_database(db)

        for index, _ in enumerate(active_statuses):
            event_id = f"event-{index}"
            async with db.execute("SELECT status FROM event_processing WHERE id = ?", (event_id,)) as cursor:
                row = await cursor.fetchone()
            assert row[0] == STATUS_FAILED
            assert await claim_event_for_processing(db, event_id) is True

        await db.close()

    asyncio.run(run_test())


def test_upload_claim_requires_downloaded_state(tmp_path):
    async def run_test():
        db = await create_database(str(tmp_path / "events.sqlite"))
        event = _event()

        assert await claim_event_for_processing(db, event.id) is True
        assert await mark_event_downloading(db, event.id) is True
        assert await claim_event_for_upload(db, event.id) is False

        await mark_event_downloaded(db, event.id)
        assert await claim_event_for_upload(db, event.id) is True
        assert await claim_event_for_upload(db, event.id) is False

        await db.close()

    asyncio.run(run_test())


def test_parallel_uploaders_skip_duplicate_event_ids_before_upload(tmp_path):
    async def run_test():
        db = await create_database(str(tmp_path / "events.sqlite"))
        upload_queue = VideoQueue(1024)
        event = _event()
        upload_started = asyncio.Event()
        upload_calls = 0

        class CountingUploader(VideoUploader):
            async def _generate_file_path(self, event):
                return pathlib.Path("b2:bucket/path.mp4")

            async def _upload_video(self, video, destination, rclone_args):
                nonlocal upload_calls
                upload_calls += 1
                upload_started.set()
                await asyncio.sleep(0.1)

        assert await claim_event_for_processing(db, event.id) is True
        assert await mark_event_downloading(db, event.id) is True
        await mark_event_downloaded(db, event.id)

        await upload_queue.put((event, b"first"))
        await upload_queue.put((event, b"second"))

        uploaders = [
            CountingUploader(None, upload_queue, "b2:bucket", "", "{event.id}.mp4", db, False),
            CountingUploader(None, upload_queue, "b2:bucket", "", "{event.id}.mp4", db, False),
        ]
        tasks = [asyncio.create_task(uploader.start()) for uploader in uploaders]

        await asyncio.wait_for(upload_started.wait(), timeout=1)
        await asyncio.sleep(0.2)

        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

        assert upload_calls == 1

        await db.close()

    asyncio.run(run_test())


def test_file_path_formats_event_times_in_nvr_timezone():
    async def run_test():
        class FakeProtect:
            pass

        connect_event = asyncio.Event()
        connect_event.set()
        protect = FakeProtect()
        protect.connect_event = connect_event
        protect.bootstrap = SimpleNamespace(
            nvr=SimpleNamespace(timezone=ZoneInfo("America/Chicago")),
            cameras={"camera-1": SimpleNamespace(name="Front Door")},
        )
        start = datetime(2026, 6, 25, 14, 43, 41, tzinfo=timezone.utc)
        event = SimpleNamespace(
            id="event-1",
            type=SimpleNamespace(value="motion"),
            camera_id="camera-1",
            start=start,
            end=start + timedelta(seconds=10),
            smart_detect_types=[],
        )
        uploader = VideoUploader(
            protect,
            VideoQueue(1024),
            "b2:bucket",
            "",
            "{event.start:%Y-%m-%dT%H-%M-%S}.mp4",
            None,
            False,
        )

        path = await uploader._generate_file_path(event)

        assert str(path).endswith("2026-06-25T09-43-41.mp4")
        assert event.start == start

    asyncio.run(run_test())

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from unifi_protect_backup.utils import CameraPriorityConfig, EventQueue, VideoQueue, parse_camera_priority_config


def _event(event_id: str, camera_id: str, camera_name: str | None = None):
    end = datetime.now(timezone.utc) - timedelta(minutes=5)
    event = SimpleNamespace(
        id=event_id,
        type=SimpleNamespace(value="motion"),
        camera_id=camera_id,
        start=end - timedelta(seconds=20),
        end=end,
        smart_detect_types=[],
    )
    if camera_name is not None:
        event.camera_name = camera_name
    return event


def _protect(camera_names: dict[str, str]):
    class FakeProtect:
        pass

    protect = FakeProtect()
    protect.connect_event = asyncio.Event()
    protect.connect_event.set()
    protect.bootstrap = SimpleNamespace(
        cameras={
            camera_id: SimpleNamespace(name=camera_name)
            for camera_id, camera_name in camera_names.items()
        }
    )
    return protect


def test_no_priority_config_preserves_fifo_behavior():
    async def run_test():
        queue = VideoQueue(1024)
        await queue.put((_event("event-1", "camera-1"), b"1"))
        await queue.put((_event("event-2", "camera-2"), b"2"))
        await queue.put((_event("event-3", "camera-3"), b"3"))

        assert (await queue.get())[0].id == "event-1"
        assert (await queue.get())[0].id == "event-2"
        assert (await queue.get())[0].id == "event-3"

    asyncio.run(run_test())


def test_download_queue_no_priority_config_preserves_fifo_behavior():
    async def run_test():
        queue = EventQueue()
        await queue.put(_event("event-1", "camera-1"))
        await queue.put(_event("event-2", "camera-2"))
        await queue.put(_event("event-3", "camera-3"))

        assert (await queue.get()).id == "event-1"
        assert (await queue.get()).id == "event-2"
        assert (await queue.get()).id == "event-3"

    asyncio.run(run_test())


def test_priority_camera_queued_after_normal_events_uploads_first():
    async def run_test():
        queue = VideoQueue(1024, priority_config=CameraPriorityConfig({"camera-3": 100}))
        await queue.put((_event("normal-1", "camera-1"), b"1"))
        await queue.put((_event("normal-2", "camera-2"), b"2"))
        await queue.put((_event("priority-1", "camera-3"), b"3"))

        assert (await queue.get())[0].id == "priority-1"
        assert (await queue.get())[0].id == "normal-1"
        assert (await queue.get())[0].id == "normal-2"

    asyncio.run(run_test())


def test_priority_camera_queued_after_normal_events_downloads_first():
    async def run_test():
        queue = EventQueue(priority_config=CameraPriorityConfig({"camera-3": 100}))
        await queue.put(_event("normal-1", "camera-1"))
        await queue.put(_event("normal-2", "camera-2"))
        await queue.put(_event("priority-1", "camera-3"))

        assert (await queue.get()).id == "priority-1"
        assert (await queue.get()).id == "normal-1"
        assert (await queue.get()).id == "normal-2"

    asyncio.run(run_test())


def test_same_priority_preserves_fifo_order():
    async def run_test():
        queue = VideoQueue(1024, priority_config=CameraPriorityConfig({"camera-1": 100}))
        await queue.put((_event("event-1", "camera-1"), b"1"))
        await queue.put((_event("event-2", "camera-1"), b"2"))

        assert (await queue.get())[0].id == "event-1"
        assert (await queue.get())[0].id == "event-2"

    asyncio.run(run_test())


def test_camera_id_priority_matching_works():
    async def run_test():
        queue = VideoQueue(1024, priority_config=CameraPriorityConfig({"priority-id": 100}))
        await queue.put((_event("normal-1", "camera-1"), b"1"))
        await queue.put((_event("priority-1", "priority-id"), b"2"))

        assert (await queue.get())[0].id == "priority-1"

    asyncio.run(run_test())


def test_camera_name_priority_matching_works():
    async def run_test():
        queue = VideoQueue(
            1024,
            protect=_protect({"camera-1": "Normal Camera", "camera-2": "Priority Camera"}),
            priority_config=CameraPriorityConfig({"Priority Camera": 100}),
        )
        await queue.put((_event("normal-1", "camera-1"), b"1"))
        await queue.put((_event("priority-1", "camera-2"), b"2"))

        assert (await queue.get())[0].id == "priority-1"

    asyncio.run(run_test())


def test_invalid_camera_priorities_are_ignored_with_warnings(caplog):
    caplog.set_level(logging.WARNING, logger="unifi_protect_backup.utils")

    config = parse_camera_priority_config(
        "Default Priority",
        "NoEquals,=10,BadNumber=abc,Explicit Priority=75",
    )

    assert config.priorities == {
        "Default Priority": 100,
        "Explicit Priority": 75,
    }
    assert "Ignoring invalid CAMERA_PRIORITIES entry 'NoEquals'" in caplog.text
    assert "Ignoring invalid CAMERA_PRIORITIES entry '=10'" in caplog.text
    assert "Ignoring invalid CAMERA_PRIORITIES entry 'BadNumber=abc'" in caplog.text


def test_starvation_prevention_allows_old_normal_priority_events_to_upload():
    async def run_test():
        queue = VideoQueue(
            1024,
            priority_config=CameraPriorityConfig({"priority-camera": 100}),
            priority_aging_seconds=1,
        )
        await queue.put((_event("old-normal", "normal-camera"), b"1"))
        await queue.put((_event("new-priority", "priority-camera"), b"2"))

        queue._queue[0].enqueued_at -= 101

        assert (await queue.get())[0].id == "old-normal"

    asyncio.run(run_test())


def test_download_queue_starvation_prevention_allows_old_normal_priority_events_to_download():
    async def run_test():
        queue = EventQueue(
            priority_config=CameraPriorityConfig({"priority-camera": 100}),
            priority_aging_seconds=1,
        )
        await queue.put(_event("old-normal", "normal-camera"))
        await queue.put(_event("new-priority", "priority-camera"))

        queue._queue[0].enqueued_at -= 101

        assert (await queue.get()).id == "old-normal"

    asyncio.run(run_test())

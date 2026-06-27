"""Utility functions used throughout the code, kept here to allow re use and/or minimize clutter elsewhere."""

import asyncio
from dataclasses import dataclass
import logging
import re
import time
from datetime import datetime
from typing import Optional, Set

from apprise import NotifyType
from async_lru import alru_cache
from uiprotect import ProtectApiClient
from uiprotect.data.nvr import Event
from uiprotect.data.types import EventType, SmartDetectObjectType, SmartDetectAudioType

from unifi_protect_backup import notifications

logger = logging.getLogger(__name__)

DEFAULT_PRIORITY_CAMERA_PRIORITY = 100
DEFAULT_PRIORITY_AGING_SECONDS = 60


def add_logging_level(levelName: str, levelNum: int, methodName: Optional[str] = None) -> None:
    """Comprehensively adds a new logging level to the `logging` module and the currently configured logging class.

    `levelName` becomes an attribute of the `logging` module with the value
    `levelNum`. `methodName` becomes a convenience method for both `logging`
    itself and the class returned by `logging.getLoggerClass()` (usually just
    `logging.Logger`).

    To avoid accidental clobbering of existing attributes, this method will
    raise an `AttributeError` if the level name is already an attribute of the
    `logging` module or if the method name is already present

    Credit: https://stackoverflow.com/a/35804945

    Args:
        levelName (str): The name of the new logging level (in all caps).
        levelNum (int): The priority value of the logging level, lower=more verbose.
        methodName (str): The name of the method used to log using this.
                          If `methodName` is not specified, `levelName.lower()` is used.

    Example:
    ::
        >>> add_logging_level('TRACE', logging.DEBUG - 5)
        >>> logging.getLogger(__name__).setLevel("TRACE")
        >>> logging.getLogger(__name__).trace('that worked')
        >>> logging.trace('so did this')
        >>> logging.TRACE
        5

    """
    if not methodName:
        methodName = levelName.lower()

    if hasattr(logging, levelName):
        raise AttributeError("{} already defined in logging module".format(levelName))
    if hasattr(logging, methodName):
        raise AttributeError("{} already defined in logging module".format(methodName))
    if hasattr(logging.getLoggerClass(), methodName):
        raise AttributeError("{} already defined in logger class".format(methodName))

    # This method was inspired by the answers to Stack Overflow post
    # http://stackoverflow.com/q/2183233/2988730, especially
    # http://stackoverflow.com/a/13638084/2988730
    def logForLevel(self, message, *args, **kwargs):
        if self.isEnabledFor(levelNum):
            self._log(levelNum, message, args, **kwargs)

    def logToRoot(message, *args, **kwargs):
        logging.log(levelNum, message, *args, **kwargs)

    def adapterLog(self, msg, *args, **kwargs):
        """Delegate an error call to the underlying logger."""
        self.log(levelNum, msg, *args, **kwargs)

    logging.addLevelName(levelNum, levelName)
    setattr(logging, levelName, levelNum)
    setattr(logging.getLoggerClass(), methodName, logForLevel)
    setattr(logging, methodName, logToRoot)
    setattr(logging.LoggerAdapter, methodName, adapterLog)


color_logging = False


def add_color_to_record_levelname(record):
    """Colorizes logging level names."""
    levelno = record.levelno
    if levelno >= logging.CRITICAL:
        color = "\x1b[31;1m"  # RED
    elif levelno >= logging.ERROR:
        color = "\x1b[31;1m"  # RED
    elif levelno >= logging.WARNING:
        color = "\x1b[33;1m"  # YELLOW
    elif levelno >= logging.INFO:
        color = "\x1b[32;1m"  # GREEN
    elif levelno >= logging.DEBUG:
        color = "\x1b[36;1m"  # CYAN
    elif levelno >= logging.EXTRA_DEBUG:
        color = "\x1b[35;1m"  # MAGENTA
    else:
        color = "\x1b[0m"

    return f"{color}{record.levelname}\x1b[0m"


class AppriseStreamHandler(logging.StreamHandler):
    """Logging handler that also sends logging output to configured Apprise notifiers."""

    def __init__(self, color_logging: bool, *args, **kwargs):
        """Init.

        Args:
            color_logging (bool): If true logging levels will be colorized
            *args (): Positional arguments to pass to StreamHandler
            **kwargs: Keyword arguments to pass to StreamHandler

        """
        super().__init__(*args, **kwargs)
        self.color_logging = color_logging

    def _emit_apprise(self, record):
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            return  # There is no running loop

        msg = self.format(record)
        logging_map = {
            logging.ERROR: NotifyType.FAILURE,
            logging.WARNING: NotifyType.WARNING,
            logging.INFO: NotifyType.INFO,
            logging.DEBUG: NotifyType.INFO,
            logging.EXTRA_DEBUG: NotifyType.INFO,
            logging.WEBSOCKET_DATA: NotifyType.INFO,
        }

        # Only try notifying if there are notification servers configured
        # and the asyncio loop isn't closed (aka we are quitting)
        if notifications.notifier.servers and not loop.is_closed():
            notify = notifications.notifier.async_notify(
                body=msg,
                title=record.levelname,
                notify_type=logging_map[record.levelno],
                tag=[record.levelname],
            )
            if loop.is_running():
                asyncio.create_task(notify)
            else:
                loop.run_until_complete(notify)

    def _emit_stream(self, record):
        record.levelname = f"{record.levelname:^11s}"  # Pad level name to max width
        if self.color_logging:
            record.levelname = add_color_to_record_levelname(record)

        msg = self.format(record)
        stream = self.stream
        # issue 35046: merged two stream.writes into one.
        stream.write(msg + self.terminator)
        self.flush()

    def emit(self, record):
        """Emit log to stdout and apprise."""
        try:
            self._emit_apprise(record)
        except RecursionError:  # See issue 36272
            raise
        except Exception:
            self.handleError(record)

        try:
            self._emit_stream(record)
        except RecursionError:  # See issue 36272
            raise
        except Exception:
            self.handleError(record)


def create_logging_handler(format, color_logging):
    """Construct apprise logging handler for the given format."""
    date_format = "%Y-%m-%d %H:%M:%S"
    style = "{"

    sh = AppriseStreamHandler(color_logging)
    formatter = logging.Formatter(format, date_format, style)
    sh.setFormatter(formatter)
    return sh


def setup_logging(verbosity: int, color_logging: bool = False) -> None:
    """Configure loggers to provided the desired level of verbosity.

    Verbosity 0: Only log info messages created by `unifi-protect-backup`, and all warnings
    verbosity 1: Only log info & debug messages created by `unifi-protect-backup`, and all warnings
    verbosity 2: Log info & debug messages created by `unifi-protect-backup`, command output, and
                 all warnings
    Verbosity 3: Log debug messages created by `unifi-protect-backup`, command output, all info
                 messages, and all warnings
    Verbosity 4: Log debug messages created by `unifi-protect-backup` command output, all info
                 messages, all warnings, and websocket data
    Verbosity 5: Log websocket data, command output, all debug messages, all info messages and all
                 warnings

    Args:
        verbosity (int): The desired level of verbosity
        color_logging (bool): If colors should be used in the log (default=False)

    """
    add_logging_level(
        "EXTRA_DEBUG",
        logging.DEBUG - 1,
    )
    add_logging_level(
        "WEBSOCKET_DATA",
        logging.DEBUG - 2,
    )

    format = "{asctime} [{levelname:^11s}] {name:<46} :  {message}"
    sh = create_logging_handler(format, color_logging)

    logger = logging.getLogger("unifi_protect_backup")
    logger.addHandler(sh)
    logger.propagate = False

    if verbosity == 0:
        logging.basicConfig(level=logging.WARN, handlers=[sh])
        logger.setLevel(logging.INFO)
    elif verbosity == 1:
        logging.basicConfig(level=logging.WARN, handlers=[sh])
        logger.setLevel(logging.DEBUG)
    elif verbosity == 2:
        logging.basicConfig(level=logging.WARN, handlers=[sh])
        logger.setLevel(logging.EXTRA_DEBUG)  # type: ignore
    elif verbosity == 3:
        logging.basicConfig(level=logging.INFO, handlers=[sh])
        logger.setLevel(logging.EXTRA_DEBUG)  # type: ignore
    elif verbosity == 4:
        logging.basicConfig(level=logging.INFO, handlers=[sh])
        logger.setLevel(logging.WEBSOCKET_DATA)  # type: ignore
    elif verbosity >= 5:
        logging.basicConfig(level=logging.DEBUG, handlers=[sh])
        logger.setLevel(logging.WEBSOCKET_DATA)  # type: ignore


_initialized_loggers = []


def setup_event_logger(logger, color_logging):
    """Set up a logger that also displays the event ID currently being processed."""
    global _initialized_loggers
    if logger not in _initialized_loggers:
        format = "{asctime} [{levelname:^11s}] {name:<46} :{event}  {message}"
        sh = create_logging_handler(format, color_logging)
        logger.addHandler(sh)
        logger.propagate = False
        _initialized_loggers.append(logger)


_suffixes = ["B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB"]


# Regex patterns for known UniFi Protect event ID formats.
# The suffix (e.g. camera ID appended by the websocket) is optional.
_UUID_RE = re.compile(r"^([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})(?:-.+)?$", re.IGNORECASE)
_HEX_ID_RE = re.compile(r"^([0-9a-f]{24})(?:-.+)?$", re.IGNORECASE)


def normalize_event_id(event_id: str) -> str:
    """Normalize a UniFi Protect event ID to its canonical form.

    UniFi Protect may send event IDs in several formats:
      - Standard UUID: ``f9f5a34b-867d-4001-9b42-c3429c1785df``
      - Old hex ID: ``69be9ae203c9f503e4357080``
      - UUID with appended camera ID: ``<uuid>-<camera_id>``
      - Hex ID with appended suffix: ``<hex_id>-<suffix>``

    This function extracts just the event ID portion, stripping any appended
    camera/suffix data so that IDs are consistent between the websocket and API.
    """
    m = _UUID_RE.match(event_id) or _HEX_ID_RE.match(event_id)
    if m:
        return m.group(1)
    # Unknown format — return unchanged
    return event_id


def human_readable_size(num: float):
    """Turn a number into a human readable number with ISO/IEC 80000 binary prefixes.

    Based on: https://stackoverflow.com/a/1094933

    Args:
        num (int): The number to be converted into human readable format

    """
    for unit in _suffixes:
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}"
        num /= 1024.0
    raise ValueError("`num` too large, ran out of prefixes")


def human_readable_to_float(num: str):
    """Turn a human readable ISO/IEC 80000 suffix value to its full float value."""
    pattern = r"([\d.]+)(" + "|".join(_suffixes) + ")"
    result = re.match(pattern, num)
    if result is None:
        raise ValueError(f"Value '{num}' is not a valid ISO/IEC 80000 binary value")

    value = float(result[1])
    suffix = result[2]
    multiplier = 1024 ** _suffixes.index(suffix)
    return value * multiplier


# Cached so that actions like uploads can continue when the connection to the api is lost
# No max size, and a 6 hour ttl
@alru_cache(None, ttl=60 * 60 * 6)
async def get_camera_name(protect: ProtectApiClient, id: str):
    """Return the name for the camera with the given ID.

    If the camera ID is not know, it tries refreshing the cached data
    """
    # Wait for unifi protect to be connected
    await protect.connect_event.wait()  # type: ignore

    try:
        return protect.bootstrap.cameras[id].name
    except KeyError:
        # Refresh cameras
        logger.debug(f"Unknown camera id: '{id}', checking API")

        await protect.update()

        try:
            name = protect.bootstrap.cameras[id].name
        except KeyError:
            logger.debug(f"Unknown camera id: '{id}'")
            raise

        logger.debug(f"Found camera - {id}: {name}")
        return name


class SubprocessException(Exception):
    """Class to capture: stdout, stderr, and return code of Subprocess errors."""

    def __init__(self, stdout, stderr, returncode):
        """Exception class for when rclone does not exit with `0`.

        Args:
          stdout (str): What rclone output to stdout
          stderr (str): What rclone output to stderr
          returncode (str): The return code of the rclone process

        """
        super().__init__()
        self.stdout: str = stdout
        self.stderr: str = stderr
        self.returncode: int = returncode

    def __str__(self):
        """Turn exception into a human readable form."""
        return f"Return Code: {self.returncode}\nStdout:\n{self.stdout}\nStderr:\n{self.stderr}"


async def run_command(cmd: str, data=None):
    """Run the given command returning the exit code, stdout and stderr."""
    proc = await asyncio.create_subprocess_shell(
        cmd,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate(data)
    stdout = stdout.decode()
    stdout_indented = "\t" + stdout.replace("\n", "\n\t").strip()
    stderr = stderr.decode()
    stderr_indented = "\t" + stderr.replace("\n", "\n\t").strip()

    if proc.returncode != 0:
        logger.error(f"Failed to run: '{cmd}")
        logger.error(f"stdout:\n{stdout_indented}")
        logger.error(f"stderr:\n{stderr_indented}")
    else:
        logger.extra_debug(f"stdout:\n{stdout_indented}")  # type: ignore
        logger.extra_debug(f"stderr:\n{stderr_indented}")  # type: ignore

    return proc.returncode, stdout, stderr


@dataclass
class CameraPriorityConfig:
    """Priority settings for upload queue camera ordering."""

    priorities: dict[str, int]


@dataclass
class _UploadQueueItem:
    """Internal upload queue entry with priority metadata."""

    event: Event
    video: bytes
    base_priority: int
    sequence: int
    enqueued_at: float
    camera_name: str | None


@dataclass
class _EventQueueItem:
    """Internal download queue entry with priority metadata."""

    event: Event
    base_priority: int
    sequence: int
    enqueued_at: float
    camera_name: str | None


def parse_csv_list(value) -> list[str]:
    """Parse a comma-separated environment value into trimmed entries."""
    if value is None:
        return []
    if isinstance(value, (tuple, list)):
        values = value
    else:
        values = str(value).split(",")
    return [item.strip() for item in values if item.strip()]


def parse_camera_priority_config(priority_cameras="", camera_priorities="") -> CameraPriorityConfig:
    """Parse PRIORITY_CAMERAS and CAMERA_PRIORITIES into a camera priority map."""
    priorities = {
        camera: DEFAULT_PRIORITY_CAMERA_PRIORITY
        for camera in parse_csv_list(priority_cameras)
    }

    for entry in parse_csv_list(camera_priorities):
        if "=" not in entry:
            logger.warning(f"Ignoring invalid CAMERA_PRIORITIES entry '{entry}': expected camera=priority")
            continue

        camera, priority = entry.split("=", 1)
        camera = camera.strip()
        priority = priority.strip()
        if not camera:
            logger.warning(f"Ignoring invalid CAMERA_PRIORITIES entry '{entry}': camera is empty")
            continue
        try:
            priorities[camera] = int(priority)
        except ValueError:
            logger.warning(f"Ignoring invalid CAMERA_PRIORITIES entry '{entry}': priority must be an integer")

    return CameraPriorityConfig(priorities)


class EventQueue(asyncio.Queue):
    """A priority-aware queue for events waiting to download."""

    def __init__(
        self,
        maxsize=0,
        *,
        protect: ProtectApiClient | None = None,
        priority_config: CameraPriorityConfig | None = None,
        priority_aging_seconds: int = DEFAULT_PRIORITY_AGING_SECONDS,
    ):
        """Init."""
        super().__init__(maxsize=maxsize)
        self._protect = protect
        self._priority_config = priority_config or CameraPriorityConfig({})
        self._priority_aging_seconds = priority_aging_seconds
        self._sequence = 0

    def _init(self, maxsize):
        self._queue = []

    def _get(self):
        index = self._best_item_index()
        data = self._queue.pop(index)

        normal_events_ahead = 0
        if data.base_priority > 0:
            normal_events_ahead = sum(1 for item in self._queue[:index] if item.base_priority == 0)
        if normal_events_ahead:
            logger.debug(
                f"Priority queue selected high-priority event before {normal_events_ahead} normal-priority events"
            )

        return data.event

    def _put(self, item: _EventQueueItem):
        self._queue.append(item)

    def _best_item_index(self) -> int:
        now = time.monotonic()
        best_index = 0
        best_key = self._priority_key(self._queue[0], now)
        for index, item in enumerate(self._queue[1:], start=1):
            key = self._priority_key(item, now)
            if key > best_key:
                best_index = index
                best_key = key
        return best_index

    def _priority_key(self, item: _EventQueueItem, now: float) -> tuple[float, int]:
        age_priority = 0
        if self._priority_aging_seconds > 0:
            age_priority = (now - item.enqueued_at) / self._priority_aging_seconds
        return item.base_priority + age_priority, -item.sequence

    async def put(self, item: Event):
        """Put an event into the download queue."""
        queue_item = await self._queue_item(item)

        while self.full():
            putter = self._get_loop().create_future()  # type: ignore
            self._putters.append(putter)  # type: ignore
            try:
                await putter
            except:  # noqa: E722
                putter.cancel()
                try:
                    self._putters.remove(putter)  # type: ignore
                except ValueError:
                    pass
                if not self.full() and not putter.cancelled():
                    self._wakeup_next(self._putters)  # type: ignore
                raise
        return self._put_queue_item_nowait(queue_item)

    def put_nowait(self, item: Event):
        """Put an event into the download queue without blocking."""
        queue_item = self._queue_item_nowait(item)
        if self.full():
            raise asyncio.QueueFull
        self._put_queue_item_nowait(queue_item)

    def _put_queue_item_nowait(self, queue_item: _EventQueueItem):
        self._put(queue_item)
        self._unfinished_tasks += 1  # type: ignore
        self._finished.clear()  # type: ignore
        self._wakeup_next(self._getters)  # type: ignore

    async def _queue_item(self, event: Event) -> _EventQueueItem:
        camera_name = await self._get_camera_name(event)
        return self._build_queue_item(event, camera_name)

    def _queue_item_nowait(self, event: Event) -> _EventQueueItem:
        camera_name = getattr(event, "camera_name", None)
        return self._build_queue_item(event, camera_name)

    def _build_queue_item(self, event: Event, camera_name: str | None) -> _EventQueueItem:
        priority = self._camera_priority(event, camera_name)
        setattr(event, "_download_queue_priority", priority)
        setattr(event, "_download_queue_camera_name", camera_name)
        queue_item = _EventQueueItem(
            event=event,
            base_priority=priority,
            sequence=self._sequence,
            enqueued_at=time.monotonic(),
            camera_name=camera_name,
        )
        self._sequence += 1
        logger.debug(
            f'Queued download event {event.id} camera="{camera_name or event.camera_id}" priority={priority}'
        )
        return queue_item

    def _camera_priority(self, event: Event, camera_name: str | None) -> int:
        camera_id_priority = self._priority_config.priorities.get(event.camera_id, 0)
        camera_name_priority = self._priority_config.priorities.get(camera_name, 0) if camera_name else 0
        return max(camera_id_priority, camera_name_priority)

    async def _get_camera_name(self, event: Event) -> str | None:
        if not self._priority_config.priorities or self._protect is None:
            return getattr(event, "camera_name", None)
        try:
            return await get_camera_name(self._protect, event.camera_id)
        except Exception as e:
            logger.warning(f"Unable to resolve camera name for priority matching: {event.camera_id}", exc_info=e)
            return None


class VideoQueue(asyncio.Queue):
    """A byte-limited upload queue that can prioritize selected cameras."""

    def __init__(
        self,
        maxsize=0,
        *,
        protect: ProtectApiClient | None = None,
        priority_config: CameraPriorityConfig | None = None,
        priority_aging_seconds: int = DEFAULT_PRIORITY_AGING_SECONDS,
    ):
        """Init."""
        super().__init__(maxsize=maxsize)
        self._bytes_sum = 0
        self._protect = protect
        self._priority_config = priority_config or CameraPriorityConfig({})
        self._priority_aging_seconds = priority_aging_seconds
        self._sequence = 0

    def _init(self, maxsize):
        self._queue = []

    def qsize(self):
        """Get number of bytes in the queue."""
        return self._bytes_sum

    def qsize_files(self):
        """Get number of files in the queue."""
        return len(self._queue)

    def _get(self):
        index = self._best_item_index()
        data = self._queue.pop(index)
        self._bytes_sum -= len(data.video)

        normal_events_ahead = 0
        if data.base_priority > 0:
            normal_events_ahead = sum(1 for item in self._queue[:index] if item.base_priority == 0)
        if normal_events_ahead:
            logger.debug(
                f"Priority queue selected high-priority event before {normal_events_ahead} normal-priority events"
            )

        return data.event, data.video

    def _put(self, item: _UploadQueueItem):
        self._queue.append(item)
        self._bytes_sum += len(item.video)

    def _best_item_index(self) -> int:
        now = time.monotonic()
        best_index = 0
        best_key = self._priority_key(self._queue[0], now)
        for index, item in enumerate(self._queue[1:], start=1):
            key = self._priority_key(item, now)
            if key > best_key:
                best_index = index
                best_key = key
        return best_index

    def _priority_key(self, item: _UploadQueueItem, now: float) -> tuple[float, int]:
        age_priority = 0
        if self._priority_aging_seconds > 0:
            age_priority = (now - item.enqueued_at) / self._priority_aging_seconds
        return item.base_priority + age_priority, -item.sequence

    def _item_size(self, item: tuple[Event, bytes] | _UploadQueueItem):
        if isinstance(item, _UploadQueueItem):
            return len(item.video)
        return len(item[1])

    def full(self, item: tuple[Event, bytes] | _UploadQueueItem | None = None):
        """Return True if there are maxsize bytes in the queue.

        optionally if `item` is provided, it will return False if there is enough space to
        fit it, otherwise it will return True

        Note: if the Queue was initialized with maxsize=0 (the default),
        then full() is never True.
        """
        if self._maxsize <= 0:  # type: ignore
            return False
        else:
            if item is None:
                return self.qsize() >= self._maxsize  # type: ignore
            else:
                return self.qsize() + self._item_size(item) >= self._maxsize  # type: ignore

    async def put(self, item: tuple[Event, bytes]):
        """Put an item into the queue.

        Put an item into the queue. If the queue is full, wait until a free
        slot is available before adding item.
        """
        if self._maxsize > 0 and len(item[1]) > self._maxsize:  # type: ignore
            raise ValueError(
                f"Item is larger ({human_readable_size(len(item[1]))}) "
                f"than the size of the buffer ({human_readable_size(self._maxsize)})"  # type: ignore
            )

        queue_item = await self._queue_item(item)

        while self.full(queue_item):
            putter = self._get_loop().create_future()  # type: ignore
            self._putters.append(putter)  # type: ignore
            try:
                await putter
            except:  # noqa: E722
                putter.cancel()  # Just in case putter is not done yet.
                try:
                    # Clean self._putters from canceled putters.
                    self._putters.remove(putter)  # type: ignore
                except ValueError:
                    # The putter could be removed from self._putters by a
                    # previous get_nowait call.
                    pass
                if not self.full(queue_item) and not putter.cancelled():
                    # We were woken up by get_nowait(), but can't take
                    # the call.  Wake up the next in line.
                    self._wakeup_next(self._putters)  # type: ignore
                raise
        return self._put_queue_item_nowait(queue_item)

    def put_nowait(self, item: tuple[Event, bytes]):
        """Put an item into the queue without blocking.

        If no free slot is immediately available, raise QueueFull.
        """
        queue_item = self._queue_item_nowait(item)
        if self.full(queue_item):
            raise asyncio.QueueFull
        self._put_queue_item_nowait(queue_item)

    def _put_queue_item_nowait(self, queue_item: _UploadQueueItem):
        self._put(queue_item)
        self._unfinished_tasks += 1  # type: ignore
        self._finished.clear()  # type: ignore
        self._wakeup_next(self._getters)  # type: ignore

    async def _queue_item(self, item: tuple[Event, bytes]) -> _UploadQueueItem:
        event, video = item
        camera_name = await self._get_camera_name(event)
        return self._build_queue_item(event, video, camera_name)

    def _queue_item_nowait(self, item: tuple[Event, bytes]) -> _UploadQueueItem:
        event, video = item
        camera_name = getattr(event, "camera_name", None)
        return self._build_queue_item(event, video, camera_name)

    def _build_queue_item(self, event: Event, video: bytes, camera_name: str | None) -> _UploadQueueItem:
        priority = self._camera_priority(event, camera_name)
        setattr(event, "_upload_queue_priority", priority)
        setattr(event, "_upload_queue_camera_name", camera_name)
        queue_item = _UploadQueueItem(
            event=event,
            video=video,
            base_priority=priority,
            sequence=self._sequence,
            enqueued_at=time.monotonic(),
            camera_name=camera_name,
        )
        self._sequence += 1
        logger.debug(
            f'Queued upload event {event.id} camera="{camera_name or event.camera_id}" priority={priority}'
        )
        return queue_item

    def _camera_priority(self, event: Event, camera_name: str | None) -> int:
        camera_id_priority = self._priority_config.priorities.get(event.camera_id, 0)
        camera_name_priority = self._priority_config.priorities.get(camera_name, 0) if camera_name else 0
        return max(camera_id_priority, camera_name_priority)

    async def _get_camera_name(self, event: Event) -> str | None:
        if not self._priority_config.priorities or self._protect is None:
            return getattr(event, "camera_name", None)
        try:
            return await get_camera_name(self._protect, event.camera_id)
        except Exception as e:
            logger.warning(f"Unable to resolve camera name for priority matching: {event.camera_id}", exc_info=e)
            return None


async def wait_until(dt):
    """Sleep until the specified datetime."""
    now = datetime.now()
    await asyncio.sleep((dt - now).total_seconds())


EVENT_TYPES_MAP = {
    EventType.MOTION: {"motion"},
    EventType.RING: {"ring"},
    EventType.SMART_DETECT_LINE: {"line"},
    EventType.FINGERPRINT_IDENTIFIED: {"fingerprint"},
    EventType.NFC_CARD_SCANNED: {"nfc"},
    EventType.SMART_DETECT: {t for t in SmartDetectObjectType.values() if t not in SmartDetectAudioType.values()},
    EventType.SMART_AUDIO_DETECT: {f"{t}" for t in SmartDetectAudioType.values()},
}


def wanted_event_type(event, wanted_detection_types: Set[str], cameras: Set[str], ignore_cameras: Set[str]):
    """Return True if this event is one we want."""
    if event.start is None or event.end is None:
        return False  # This event is still on-going

    if event.camera_id in ignore_cameras:
        return False

    if cameras and event.camera_id not in cameras:
        return False

    if event.type not in EVENT_TYPES_MAP:
        return False

    if event.type in [EventType.SMART_DETECT, EventType.SMART_AUDIO_DETECT]:
        detection_types = set(event.smart_detect_types)
    else:
        detection_types = EVENT_TYPES_MAP[event.type]
    if not detection_types & wanted_detection_types:  # No intersection
        return False

    return True

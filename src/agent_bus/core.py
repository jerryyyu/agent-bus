from __future__ import annotations

import errno
import fcntl
import hashlib
import json
import math
import os
import re
import stat
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


SCHEMA = "agent-bus/message"
VERSION = 1
KINDS = frozenset(
    ("ask-ready", "ask-withdrawn", "receipt-sealed", "run-started",
     "run-ended", "verdict", "blocker", "ruling", "task-ready",
     "result-ready", "status", "error", "fyi", "ack")
)
OPTIONAL = frozenset(("head", "verdict", "ledger", "note"))
REQUIRED = frozenset(("schema", "version", "sequence", "ts", "from", "to", "kind", "ref"))
# These names are rejected explicitly even though unknown fields are rejected too.
FORBIDDEN = frozenset(("authority", "grant", "execute"))
SAFE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")


class BusError(Exception):
    """A user-facing, safe failure (never executes message content)."""


@dataclass(frozen=True)
class Message:
    schema: str
    version: int
    sequence: int
    ts: str
    from_peer: str
    to_peer: str
    kind: str
    ref: str
    head: str | None = None
    verdict: str | None = None
    ledger: str | None = None
    note: str | None = None
    message_sha256: str = ""

    def body(self) -> dict[str, Any]:
        data: dict[str, Any] = {
            "schema": self.schema, "version": self.version,
            "sequence": self.sequence, "ts": self.ts,
            "from": self.from_peer, "to": self.to_peer,
            "kind": self.kind, "ref": self.ref,
        }
        for key in ("head", "verdict", "ledger", "note"):
            value = getattr(self, key)
            if value is not None:
                data[key] = value
        return data

    def as_dict(self) -> dict[str, Any]:
        data = self.body()
        data["message_sha256"] = self.message_sha256
        return data


def _canonical(data: dict[str, Any]) -> bytes:
    return json.dumps(data, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode("ascii")


def _hash_body(data: dict[str, Any]) -> str:
    return hashlib.sha256(_canonical(data)).hexdigest()


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def project_id(project: str | os.PathLike[str]) -> str:
    """Return the stable ID for a resolved project directory."""
    try:
        resolved = Path(project).expanduser().resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise BusError(f"project cannot be resolved: {project}: {exc}") from exc
    if not resolved.is_dir():
        raise BusError(f"project is not a directory: {resolved}")
    return hashlib.sha256(os.fsencode(str(resolved))).hexdigest()


def _check_name(value: str, label: str) -> str:
    if not isinstance(value, str) or not SAFE_NAME.fullmatch(value):
        raise BusError(f"invalid {label}")
    return value


def _lstat(path: Path, label: str, *, directory: bool | None = None) -> os.stat_result:
    try:
        info = path.lstat()
    except FileNotFoundError as exc:
        raise BusError(f"missing {label}: {path}") from exc
    if stat.S_ISLNK(info.st_mode):
        raise BusError(f"refusing symlink {label}: {path}")
    if directory is True and not stat.S_ISDIR(info.st_mode):
        raise BusError(f"{label} is not a directory: {path}")
    if directory is False and not stat.S_ISREG(info.st_mode):
        raise BusError(f"{label} is not a regular file: {path}")
    if info.st_uid != os.geteuid():
        raise BusError(f"{label} is not owned by this user: {path}")
    if info.st_mode & 0o077:
        raise BusError(f"{label} is writable/readable by group or other: {path}")
    return info


def _ensure_file(path: Path, label: str) -> None:
    if path.exists() or path.is_symlink():
        _lstat(path, label, directory=False)
        return
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        fd = os.open(path, flags, 0o600)
    except FileExistsError:
        _lstat(path, label, directory=False)
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    _lstat(path, label, directory=False)


def _safe_open_log(path: Path) -> int:
    flags = os.O_RDWR | os.O_APPEND | os.O_CREAT
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        fd = os.open(path, flags, 0o600)
    except OSError as exc:
        if exc.errno in (errno.ELOOP, errno.EMLINK):
            raise BusError(f"refusing symlink log: {path}") from exc
        raise
    info = os.fstat(fd)
    if info.st_uid != os.geteuid() or info.st_mode & 0o077 or not stat.S_ISREG(info.st_mode):
        os.close(fd)
        raise BusError(f"unsafe log file: {path}")
    return fd


def _validate_message(data: Any, *, expected_from: str | None = None,
                      expected_to: str | None = None, previous: int | None = None) -> Message:
    if not isinstance(data, dict):
        raise BusError("message is not a JSON object")
    keys = set(data)
    if keys & FORBIDDEN:
        raise BusError("forbidden authority/grant/execute field")
    allowed = REQUIRED | OPTIONAL | {"message_sha256"}
    unknown = keys - allowed
    if unknown:
        raise BusError(f"unexpected message fields: {', '.join(sorted(unknown))}")
    missing = (REQUIRED | {"message_sha256"}) - keys
    if missing:
        raise BusError(f"message missing fields: {', '.join(sorted(missing))}")
    if data["schema"] != SCHEMA or data["version"] != VERSION:
        raise BusError("unsupported message schema/version")
    sequence = data["sequence"]
    if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence <= 0:
        raise BusError("invalid message sequence")
    if previous is not None and sequence != previous + 1:
        raise BusError("non-contiguous message sequence")
    from_peer, to_peer = data["from"], data["to"]
    if not isinstance(from_peer, str) or not SAFE_NAME.fullmatch(from_peer):
        raise BusError("invalid sender")
    if not isinstance(to_peer, str) or not SAFE_NAME.fullmatch(to_peer):
        raise BusError("invalid message peers")
    if from_peer == to_peer:
        raise BusError("sender and recipient must differ")
    if expected_from is not None and from_peer != expected_from:
        raise BusError("message is in the wrong direction")
    if expected_to is not None and to_peer != expected_to:
        raise BusError("message is in the wrong direction")
    if data["kind"] not in KINDS:
        raise BusError("invalid message kind")
    if not isinstance(data["ref"], str) or not data["ref"] or len(data["ref"]) > 8192:
        raise BusError("invalid message ref")
    ts = data["ts"]
    if not isinstance(ts, str) or not ts.endswith("Z"):
        raise BusError("timestamp must be UTC")
    try:
        parsed = datetime.fromisoformat(ts[:-1] + "+00:00")
    except ValueError as exc:
        raise BusError("invalid timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise BusError("timestamp must be UTC")
    for key in OPTIONAL:
        if key in data and (not isinstance(data[key], str) or len(data[key]) > 8192):
            raise BusError(f"invalid message {key}")
    digest = data["message_sha256"]
    if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
        raise BusError("invalid message hash")
    body = {key: data[key] for key in data if key != "message_sha256"}
    if _hash_body(body) != digest:
        raise BusError("message hash verification failed")
    return Message(
        schema=data["schema"], version=data["version"], sequence=sequence,
        ts=ts, from_peer=from_peer, to_peer=to_peer, kind=data["kind"], ref=data["ref"],
        head=data.get("head"), verdict=data.get("verdict"), ledger=data.get("ledger"),
        note=data.get("note"), message_sha256=digest,
    )


def _line_records(raw: bytes, start: int = 0) -> Iterable[tuple[int, bytes]]:
    if start < 0 or start > len(raw):
        raise BusError("cursor is outside log")
    if start and (start > len(raw) or raw[start - 1:start] != b"\n"):
        raise BusError("cursor does not point to a line boundary")
    position = start
    while position < len(raw):
        end = raw.find(b"\n", position)
        if end < 0:
            raise BusError(f"malformed trailing line at byte {position}")
        yield end + 1, raw[position:end]
        position = end + 1


class Bus:
    """Filesystem-backed, append-only signaling for one resolved project."""

    def __init__(self, project: str | os.PathLike[str], state_root: str | os.PathLike[str] | None = None):
        try:
            self.project = Path(project).expanduser().resolve(strict=True)
        except (OSError, RuntimeError) as exc:
            raise BusError(f"project cannot be resolved: {project}: {exc}") from exc
        if not self.project.is_dir():
            raise BusError(f"project is not a directory: {self.project}")
        self.project_id = hashlib.sha256(os.fsencode(str(self.project))).hexdigest()
        root = Path(state_root).expanduser() if state_root is not None else Path.home() / ".local" / "state" / "agent-bus"
        self.state_root = root
        self.state_dir = root / "projects" / self.project_id

    @property
    def initialized(self) -> bool:
        return self.state_dir.exists() and not self.state_dir.is_symlink()

    def init(self) -> None:
        if self.state_root.exists() or self.state_root.is_symlink():
            _lstat(self.state_root, "state root", directory=True)
        else:
            self.state_root.mkdir(parents=True, mode=0o700)
        _lstat(self.state_root, "state root", directory=True)
        projects = self.state_root / "projects"
        if projects.exists() or projects.is_symlink():
            _lstat(projects, "projects directory", directory=True)
        else:
            projects.mkdir(mode=0o700)
        _lstat(projects, "projects directory", directory=True)
        if self.state_dir.exists() or self.state_dir.is_symlink():
            _lstat(self.state_dir, "project state directory", directory=True)
        else:
            self.state_dir.mkdir(mode=0o700)
        _lstat(self.state_dir, "project state directory", directory=True)
        inboxes = self.state_dir / "inboxes"
        if inboxes.exists() or inboxes.is_symlink():
            _lstat(inboxes, "inbox directory", directory=True)
        else:
            inboxes.mkdir(mode=0o700)
        _lstat(inboxes, "inbox directory", directory=True)
        cursors = self.state_dir / "cursors"
        if cursors.exists() or cursors.is_symlink():
            _lstat(cursors, "cursor directory", directory=True)
        else:
            cursors.mkdir(mode=0o700)
        _lstat(cursors, "cursor directory", directory=True)
    def _require_init(self) -> None:
        if not self.initialized:
            raise BusError("project is not initialized; run init first")
        _lstat(self.state_root, "state root", directory=True)
        _lstat(self.state_root / "projects", "projects directory", directory=True)
        _lstat(self.state_dir, "project state directory", directory=True)
        _lstat(self.state_dir / "inboxes", "inbox directory", directory=True)
        _lstat(self.state_dir / "cursors", "cursor directory", directory=True)

    @staticmethod
    def _direction(from_peer: str, to_peer: str) -> str:
        _check_name(from_peer, "sender")
        _check_name(to_peer, "recipient")
        if from_peer == to_peer:
            raise BusError("sender and recipient must differ")
        return f"{to_peer}.jsonl"

    def send(self, from_peer: str, to_peer: str, kind: str, ref: str, **optional: str) -> Message:
        self._require_init()
        from_peer = _check_name(from_peer, "sender")
        to_peer = _check_name(to_peer, "recipient")
        if from_peer == to_peer:
            raise BusError("sender and recipient must differ")
        if not isinstance(kind, str) or kind not in KINDS:
            raise BusError("invalid message kind")
        if not isinstance(ref, str) or not ref or len(ref) > 8192:
            raise BusError("invalid message ref")
        unexpected = set(optional) - OPTIONAL
        if unexpected:
            raise BusError(f"unexpected message fields: {', '.join(sorted(unexpected))}")
        path = self.state_dir / "inboxes" / self._direction(from_peer, to_peer)
        _ensure_file(path, "inbox log")
        fd = _safe_open_log(path)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            raw = os.pread(fd, os.fstat(fd).st_size, 0)
            previous = 0
            try:
                records = _line_records(raw, 0)
                for _end, line in records:
                    if not line.strip():
                        raise BusError("malformed blank line")
                    candidate = json.loads(line.decode("ascii"))
                    previous = _validate_message(
                        candidate, expected_to=to_peer,
                        previous=previous).sequence
            except (UnicodeDecodeError, json.JSONDecodeError, BusError) as exc:
                raise BusError(
                    f"existing inbox is malformed; refusing append: {exc}") from exc
            data: dict[str, Any] = {
                "schema": SCHEMA, "version": VERSION,
                "sequence": previous + 1,
                "ts": _utc_now(), "from": from_peer, "to": to_peer,
                "kind": kind, "ref": ref,
            }
            for key in OPTIONAL:
                if key in optional:
                    if not isinstance(optional[key], str) or len(optional[key]) > 8192:
                        raise BusError(f"invalid message {key}")
                    data[key] = optional[key]
            data["message_sha256"] = _hash_body(data)
            # Validate the complete envelope before the one append.  A bad API
            # argument must never poison an otherwise valid recipient log.
            message = _validate_message(
                data, expected_from=from_peer, expected_to=to_peer,
                previous=previous)
            line = _canonical(data) + b"\n"
            written = os.write(fd, line)
            if written != len(line):
                raise BusError("short append; message was not safely written")
            os.fsync(fd)
            return message
        finally:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)

    def _cursor_path(self, to_peer: str, consumer: str) -> Path:
        _check_name(to_peer, "recipient")
        _check_name(consumer, "consumer")
        # Length prefixes make the two safe-name components unambiguous even
        # when either contains dots (for example ``a``/``b.jsonl.c`` versus
        # ``a.jsonl.b``/``c``).
        name = f"{len(to_peer)}-{to_peer}.{len(consumer)}-{consumer}.cursor"
        return self.state_dir / "cursors" / name

    def _read_cursor(self, path: Path) -> int:
        if not path.exists() and not path.is_symlink():
            return 0
        _lstat(path, "cursor", directory=False)
        try:
            data = json.loads(path.read_text(encoding="ascii"))
            offset = data["offset"]
            if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
                raise ValueError
            return offset
        except (OSError, UnicodeDecodeError, json.JSONDecodeError, KeyError, ValueError) as exc:
            raise BusError(f"invalid cursor: {path}") from exc

    def _write_cursor(self, path: Path, offset: int) -> None:
        _lstat(path.parent, "cursor directory", directory=True)
        payload = _canonical({"offset": offset}) + b"\n"
        fd, name = tempfile.mkstemp(prefix=".cursor.", dir=path.parent)
        temp = Path(name)
        try:
            os.fchmod(fd, 0o600)
            if os.write(fd, payload) != len(payload):
                raise BusError("short cursor write")
            os.fsync(fd)
            os.close(fd)
            os.replace(temp, path)
            dfd = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(dfd)
            finally:
                os.close(dfd)
            _lstat(path, "cursor", directory=False)
        finally:
            try:
                os.close(fd)
            except OSError:
                pass
            try:
                temp.unlink()
            except FileNotFoundError:
                pass

    def inbox(self, to_peer: str, consumer: str, *, peek: bool = False) -> tuple[list[Message], list[str]]:
        self._require_init()
        to_peer = _check_name(to_peer, "recipient")
        consumer = _check_name(consumer, "consumer")
        direction = f"{to_peer}.jsonl"
        log = self.state_dir / "inboxes" / direction
        if not log.exists() and not log.is_symlink():
            _ensure_file(log, "inbox log")
        cursor = self._cursor_path(to_peer, consumer)
        fd = _safe_open_log(log)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            raw = os.pread(fd, os.fstat(fd).st_size, 0)
            offset = self._read_cursor(cursor)
            messages: list[Message] = []
            issues: list[str] = []
            next_offset = offset
            previous = 0
            # Validate sequence continuity from the beginning, while only returning new records.
            try:
                records = _line_records(raw, 0)
                for end, line in records:
                    if not line.strip():
                        issues.append(f"malformed blank line at byte {end - 1}")
                        break
                    try:
                        data = json.loads(line.decode("ascii"))
                        msg = _validate_message(data, expected_to=to_peer, previous=previous)
                        previous = msg.sequence
                    except (UnicodeDecodeError, json.JSONDecodeError, BusError) as exc:
                        issues.append(f"malformed message at byte {end - len(line) - 1}: {exc}")
                        break
                    if end > offset:
                        messages.append(msg)
                        next_offset = end
                    elif end == offset:
                        next_offset = end
            except BusError as exc:
                issues.append(str(exc))
            if offset > len(raw):
                issues.append("cursor is beyond log end")
            elif offset and offset <= len(raw) and raw[offset - 1:offset] != b"\n":
                issues.append("cursor does not point to a line boundary")
            if not peek and not issues and next_offset != offset:
                self._write_cursor(cursor, next_offset)
            return messages, issues
        finally:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)

    def status(self) -> dict[str, Any]:
        self._require_init()
        files = {}
        total = 0
        for path in sorted((self.state_dir / "inboxes").glob("*.jsonl")):
            name = path.name
            info = _lstat(path, name, directory=False)
            files[name] = {"bytes": info.st_size}
            total += info.st_size
        return {"project": str(self.project), "project_id": self.project_id,
                "state_dir": str(self.state_dir), "files": files,
                "total_bytes": total, "warning": "state exceeds 1 MiB; manual export/rotation only" if total > 1024 * 1024 else None}

    def doctor(self) -> list[str]:
        self._require_init()
        findings: list[str] = []
        status = self.status()
        if status["warning"]:
            findings.append(status["warning"])
        for path in sorted((self.state_dir / "inboxes").glob("*.jsonl")):
            name = path.name
            fd = _safe_open_log(path)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX)
                raw = os.pread(fd, os.fstat(fd).st_size, 0)
                try:
                    previous = 0
                    for end, line in _line_records(raw, 0):
                        try:
                            record = json.loads(line.decode("ascii"))
                            recipient = path.stem
                            previous = _validate_message(record, expected_to=recipient, previous=previous).sequence
                        except (UnicodeDecodeError, json.JSONDecodeError, BusError) as exc:
                            findings.append(f"{name}: malformed message at byte {end - len(line) - 1}: {exc}")
                            break
                except BusError as exc:
                    findings.append(f"{name}: {exc}")
            finally:
                fcntl.flock(fd, fcntl.LOCK_UN)
                os.close(fd)
        for path in sorted((self.state_dir / "cursors").iterdir()):
            try:
                if path.name.startswith(".cursor."):
                    continue
                self._read_cursor(path)
            except BusError as exc:
                findings.append(f"{path.name}: {exc}")
        if not findings:
            findings.append("ok")
        return findings

    def watch(self, to_peer: str, consumer: str, *, timeout: float | None = None,
              poll_interval: float = 0.1) -> tuple[list[Message], list[str]]:
        if (timeout is not None
                and (not isinstance(timeout, (int, float))
                     or isinstance(timeout, bool)
                     or not math.isfinite(timeout) or timeout < 0)):
            raise BusError("timeout must be finite and non-negative")
        if (not isinstance(poll_interval, (int, float))
                or isinstance(poll_interval, bool)
                or not math.isfinite(poll_interval) or poll_interval <= 0):
            raise BusError("poll interval must be finite and positive")
        started = time.monotonic()
        all_messages: list[Message] = []
        all_issues: list[str] = []
        while True:
            messages, issues = self.inbox(to_peer, consumer)
            all_messages.extend(messages)
            all_issues.extend(issues)
            if messages or issues:
                return all_messages, all_issues
            if timeout is not None and time.monotonic() - started >= timeout:
                return all_messages, all_issues
            remaining = None if timeout is None else max(0.0, timeout - (time.monotonic() - started))
            time.sleep(poll_interval if remaining is None else min(poll_interval, remaining))

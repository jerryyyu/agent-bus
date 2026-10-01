from __future__ import annotations

import base64
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
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


SCHEMA = "agent-bus/message"
# Version 2 is the pre-0.4.0 envelope. Version 3 is written only when a
# message carries a coordination kind or field introduced in 0.4.0, so an
# unchanged sender keeps producing byte-identical version 2 lines.
VERSION = 2
COORDINATION_VERSION = 3
SUPPORTED_VERSIONS = frozenset((1, VERSION, COORDINATION_VERSION))
LEGACY_KINDS = frozenset(
    ("ask-ready", "ask-withdrawn", "receipt-sealed", "run-started",
     "run-ended", "verdict", "blocker", "ruling", "task-ready",
     "result-ready", "status", "error", "fyi", "ack")
)
COORDINATION_KINDS = frozenset(("head-moved", "host", "decision"))
KINDS = LEGACY_KINDS | COORDINATION_KINDS
CHATTER_KINDS = frozenset(("ack", "status", "fyi"))
ACTIONABLE_KINDS = frozenset(
    ("ask-ready", "receipt-sealed", "run-started", "run-ended", "verdict",
     "blocker", "ruling", "task-ready", "result-ready", "error",
     "head-moved", "host", "decision")
)
HOST_ACTIONS = frozenset(
    ("armed", "started", "preempted", "resumed", "released", "nominated"))
# Host transitions that hand something to the recipient. The others describe
# the sender's own state and collapse silently into the latest host line.
HOST_ACTIONABLE_ACTIONS = frozenset(("preempted", "released", "nominated"))
VERDICTS = frozenset(("PASS", "HOLD"))
LEGACY_TEXT = frozenset(("head", "verdict", "ledger", "note"))
COORDINATION_TEXT = frozenset(
    ("prev_head", "reason", "host", "action", "lock", "rearm", "receipt",
     "depends_on"))
COORDINATION_INTEGERS = frozenset(("pid", "pgid", "count", "rc"))
COORDINATION_FIELDS = COORDINATION_TEXT | COORDINATION_INTEGERS
OPTIONAL_TEXT = LEGACY_TEXT | COORDINATION_TEXT
OPTIONAL_LINKS = frozenset(("reply_to", "supersedes"))
OPTIONAL_INTEGERS = frozenset(("seen_peer_sequence", "peer_sequence_at_send"))
OPTIONAL = (OPTIONAL_TEXT | OPTIONAL_LINKS | OPTIONAL_INTEGERS
            | COORDINATION_INTEGERS)
REQUIRED = frozenset(("schema", "version", "sequence", "ts", "from", "to", "kind", "ref"))
# These names are rejected explicitly even though unknown fields are rejected too.
FORBIDDEN = frozenset(("authority", "grant", "execute"))
SAFE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
MESSAGE_LINK = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}:[1-9][0-9]{0,18}$")
FULL_HEAD = re.compile(r"^[0-9a-f]{40}$")
NOTE_SOFT_LIMIT = 280
STRICT_ENV = "AGENT_BUS_STRICT"
# Readers at or above this version accept version 3 lines. A reader records
# the marker when it reads an inbox so senders can refuse to append a line an
# older reader would report as malformed.
READER_VERSION = "0.4.0"
_GITHUB_THREAD = re.compile(
    r"^((https?://[^\s#?]+?)/(?:pull|issues)/([0-9]+))", re.IGNORECASE)
_PR_NUMBER = re.compile(r"^#?([1-9][0-9]{0,9})$")


def strict_default() -> bool:
    """Whether ``AGENT_BUS_STRICT`` asks for refusals instead of warnings."""
    value = os.environ.get(STRICT_ENV, "").strip().lower()
    return value not in ("", "0", "false", "no", "off")


def thread_key(ref: str) -> str:
    """Collapse a ref to the thread it belongs to.

    GitHub PR and issue URLs collapse to the PR/issue URL without comment
    anchors, so a verdict posted as a comment URL threads with the ask that
    pointed at the PR. Anything else keeps its text without a fragment.
    """
    match = _GITHUB_THREAD.match(ref)
    if match:
        return match.group(1).lower()
    return ref.split("#", 1)[0]


def repo_of(ref: str) -> str | None:
    """The repository URL a GitHub PR/issue ref belongs to, lowercased."""
    match = _GITHUB_THREAD.match(ref)
    return match.group(2).lower() if match else None


def same_thread(query: str, ref: str, *, repo: str | None = None) -> bool:
    """Whether ``ref`` belongs to the thread named by ``query``.

    ``query`` may be a ref (threaded like ``thread_key``) or a bare PR/issue
    number such as ``680`` or ``#680``. A bare number is scoped to ``repo``
    when one is given (a dependency resolves inside the asking ref's own
    repository); without ``repo`` it matches that number in any repository,
    which is what an interactive ``log --ref 680`` wants.
    """
    number = _PR_NUMBER.match(query.strip())
    if number:
        match = _GITHUB_THREAD.match(ref)
        if not match or match.group(3) != number.group(1):
            return False
        return repo is None or match.group(2).lower() == repo
    return thread_key(query) == thread_key(ref)


BODY_OPTIONAL_ORDER = (
    "head", "verdict", "ledger", "note", "reply_to", "supersedes",
    "seen_peer_sequence", "peer_sequence_at_send", "prev_head", "reason",
    "host", "action", "lock", "rearm", "receipt", "depends_on", "pid",
    "pgid", "count", "rc")


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
    reply_to: str | None = None
    supersedes: str | None = None
    seen_peer_sequence: int | None = None
    peer_sequence_at_send: int | None = None
    # Coordination fields (0.4.0, schema version 3).
    prev_head: str | None = None
    reason: str | None = None
    host: str | None = None
    action: str | None = None
    lock: str | None = None
    rearm: str | None = None
    receipt: str | None = None
    depends_on: str | None = None
    pid: int | None = None
    pgid: int | None = None
    count: int | None = None
    rc: int | None = None
    message_sha256: str = ""
    duplicate_suppressed: bool = False
    # Send-time advisories (never stored; strict mode turns them into errors).
    warnings: tuple[str, ...] = ()
    # Derived by thread/actionable views: a later head-moved or re-ask reported
    # this head for the same thread, so a verdict no longer binds.
    stale_head: str | None = None

    def body(self) -> dict[str, Any]:
        data: dict[str, Any] = {
            "schema": self.schema, "version": self.version,
            "sequence": self.sequence, "ts": self.ts,
            "from": self.from_peer, "to": self.to_peer,
            "kind": self.kind, "ref": self.ref,
        }
        for key in BODY_OPTIONAL_ORDER:
            value = getattr(self, key)
            if value is not None:
                data[key] = value
        return data

    def as_dict(self) -> dict[str, Any]:
        data = self.body()
        data["message_sha256"] = self.message_sha256
        return data

    @property
    def stale_premise(self) -> bool:
        """Whether the sender admitted unread peer messages at send time."""
        return (self.seen_peer_sequence is not None
                and self.peer_sequence_at_send is not None
                and self.seen_peer_sequence < self.peer_sequence_at_send)


@dataclass(frozen=True)
class InboxBatch:
    """Opaque acknowledgement token and human-readable range metadata."""

    token: str
    recipient: str
    consumer: str
    start_sequence: int
    end_sequence: int
    message_count: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "token": self.token,
            "recipient": self.recipient,
            "consumer": self.consumer,
            "start_sequence": self.start_sequence,
            "end_sequence": self.end_sequence,
            "message_count": self.message_count,
        }


@dataclass(frozen=True)
class ActionableItem:
    """One conservative unresolved item derived from pending raw messages."""

    kind: str
    ref: str
    head: str | None
    sender: str
    newest_sequence: int
    collapsed_transition_count: int
    sequence_anchors: tuple[str, ...]
    # A verdict whose thread moved to another head after it was sent.
    stale_head: str | None = None
    # An ask whose dependency has no current PASS verdict.
    blocked_on: str | None = None
    # An ask whose thread moved after it was sent (``head`` is the new one).
    head_moved_from: str | None = None
    # Kind-specific fields worth showing without opening the raw message.
    detail: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "ref": self.ref,
            "head": self.head,
            "sender": self.sender,
            "newest_sequence": self.newest_sequence,
            "collapsed_transition_count": self.collapsed_transition_count,
            "sequence_anchors": list(self.sequence_anchors),
            "stale_head": self.stale_head,
            "blocked_on": self.blocked_on,
            "head_moved_from": self.head_moved_from,
            "detail": dict(self.detail),
        }


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
    if (data["schema"] != SCHEMA
            or data["version"] not in SUPPORTED_VERSIONS):
        raise BusError("unsupported message schema/version")
    if data["version"] == 1 and keys & OPTIONAL_LINKS:
        raise BusError("message links require schema version 2")
    coordination = bool(keys & COORDINATION_FIELDS
                        or data["kind"] in COORDINATION_KINDS)
    if coordination and data["version"] < COORDINATION_VERSION:
        raise BusError("coordination fields require schema version 3")
    if data["version"] >= COORDINATION_VERSION and not coordination:
        raise BusError("schema version 3 without coordination fields")
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
    for key in OPTIONAL_TEXT:
        if key in data and (not isinstance(data[key], str) or len(data[key]) > 8192):
            raise BusError(f"invalid message {key}")
    for key in OPTIONAL_LINKS:
        if key in data and (not isinstance(data[key], str)
                            or not MESSAGE_LINK.fullmatch(data[key])):
            raise BusError(f"invalid message {key}")
    integer_keys = keys & OPTIONAL_INTEGERS
    if integer_keys and data["version"] == 1:
        raise BusError("causal metadata requires schema version 2")
    if integer_keys and integer_keys != OPTIONAL_INTEGERS:
        raise BusError("causal metadata must include seen and available sequences")
    for key in OPTIONAL_INTEGERS:
        if key in data and (isinstance(data[key], bool)
                            or not isinstance(data[key], int)
                            or data[key] < 0):
            raise BusError(f"invalid message {key}")
    if (integer_keys
            and data["seen_peer_sequence"] > data["peer_sequence_at_send"]):
        raise BusError("seen peer sequence exceeds available peer sequence")
    for key in COORDINATION_INTEGERS:
        if key in data and (isinstance(data[key], bool)
                            or not isinstance(data[key], int)):
            raise BusError(f"invalid message {key}")
        if key in data and key != "rc" and data[key] < 0:
            raise BusError(f"invalid message {key}")
    _check_kind_shape(data)
    digest = data["message_sha256"]
    if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
        raise BusError("invalid message hash")
    body = {key: data[key] for key in data if key != "message_sha256"}
    if _hash_body(body) != digest:
        raise BusError("message hash verification failed")
    return Message(
        schema=data["schema"], version=data["version"], sequence=sequence,
        ts=ts, from_peer=from_peer, to_peer=to_peer, kind=data["kind"], ref=data["ref"],
        reply_to=data.get("reply_to"), supersedes=data.get("supersedes"),
        seen_peer_sequence=data.get("seen_peer_sequence"),
        peer_sequence_at_send=data.get("peer_sequence_at_send"),
        message_sha256=digest,
        **{key: data[key] for key in (LEGACY_TEXT | COORDINATION_FIELDS)
           if key in data},
    )


def _check_kind_shape(data: dict[str, Any]) -> None:
    """Hard requirements of the 0.4.0 kinds.

    These kinds are new, so no existing sender is broken by refusing a
    malformed one outright; the advisory checks for legacy kinds live in
    ``_advisories`` and only refuse under strict mode.
    """
    kind = data["kind"]
    if kind == "head-moved":
        for key in ("head", "prev_head"):
            value = data.get(key)
            if not isinstance(value, str) or not FULL_HEAD.fullmatch(value):
                raise BusError(f"head-moved requires a full 40-hex {key}")
        if data["head"] == data["prev_head"]:
            raise BusError("head-moved head equals prev_head")
    elif kind == "host":
        if not isinstance(data.get("host"), str) or not data["host"]:
            raise BusError("host requires a host name")
        if data.get("action") not in HOST_ACTIONS:
            raise BusError("host action must be one of "
                           + ", ".join(sorted(HOST_ACTIONS)))
    elif kind == "decision":
        # The bus is not an authorisation surface; a decision must point at
        # the place a human said it.
        if not re.match(r"^https?://", data["ref"], re.IGNORECASE):
            raise BusError(
                "decision requires --ref pointing at the comment URL where the human decided")
    dependency = data.get("depends_on")
    if (dependency is not None and _PR_NUMBER.match(dependency.strip())
            and repo_of(data["ref"]) is None):
        raise BusError(
            "a numeric --depends-on resolves inside the ref's own repository; "
            "give a full PR URL when --ref is not a GitHub PR or issue URL")
    if "action" in data and kind != "host" and data["action"] not in HOST_ACTIONS:
        raise BusError("invalid message action")


def _advisories(data: dict[str, Any]) -> list[str]:
    """Soft contract checks: warnings by default, refusals under strict."""
    found: list[str] = []
    kind = data["kind"]
    head = data.get("head")
    if head is not None and not FULL_HEAD.fullmatch(head):
        found.append(f"--head should be a full 40-hex commit sha, got {head!r}")
    if kind == "verdict":
        if data.get("verdict") not in VERDICTS:
            found.append("verdict should carry --verdict PASS|HOLD")
        if head is None:
            found.append("verdict should carry --head (the reviewed commit)")
    elif kind == "ask-ready" and head is None:
        found.append("ask-ready should carry --head (the commit to review)")
    elif kind == "decision" and "#" not in data["ref"]:
        found.append("decision ref should be a comment URL (with a # anchor)")
    note = data.get("note")
    if note is not None and len(note) > NOTE_SOFT_LIMIT:
        found.append(
            f"note is {len(note)} chars; keep it under {NOTE_SOFT_LIMIT} and put "
            "the evidence at the ref")
    reason = data.get("reason")
    if reason is not None and len(reason) > NOTE_SOFT_LIMIT:
        found.append(f"reason is {len(reason)} chars; keep it short")
    if "ledger" in data:
        found.append("--ledger is deprecated and ignored by readers; point --ref at the ledger instead")
    return found


def _dedupe_key(data: dict[str, Any]) -> bytes:
    """Canonical semantic identity for optional send-side suppression."""
    ignored = frozenset(("sequence", "ts", "message_sha256",
                         "peer_sequence_at_send"))
    return _canonical({key: value for key, value in data.items()
                       if key not in ignored})


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


def _validated_index(raw: bytes, *, to_peer: str) -> list[tuple[int, Message]]:
    """Validate a complete recipient log and retain exact record boundaries."""
    result: list[tuple[int, Message]] = []
    previous = 0
    try:
        records = _line_records(raw, 0)
        for end, line in records:
            if not line.strip():
                raise BusError(f"malformed blank line at byte {end - 1}")
            try:
                data = json.loads(line.decode("ascii"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise BusError(
                    f"malformed message at byte {end - len(line) - 1}: {exc}") from exc
            try:
                message = _validate_message(
                    data, expected_to=to_peer, previous=previous)
            except BusError as exc:
                raise BusError(
                    f"malformed message at byte {end - len(line) - 1}: {exc}") from exc
            previous = message.sequence
            result.append((end, message))
    except BusError:
        raise
    return result


def _scan_pending(raw: bytes, *, to_peer: str, offset: int) -> tuple[
        list[tuple[int, Message]], list[Message], list[str], int]:
    """Validate a recipient log and return its valid pending prefix."""
    index: list[tuple[int, Message]] = []
    messages: list[Message] = []
    issues: list[str] = []
    next_offset = offset
    previous = 0
    try:
        for end, line in _line_records(raw, 0):
            if not line.strip():
                issues.append(f"malformed blank line at byte {end - 1}")
                break
            try:
                data = json.loads(line.decode("ascii"))
                message = _validate_message(
                    data, expected_to=to_peer, previous=previous)
            except (UnicodeDecodeError, json.JSONDecodeError, BusError) as exc:
                issues.append(
                    f"malformed message at byte {end - len(line) - 1}: {exc}")
                break
            previous = message.sequence
            index.append((end, message))
            if end > offset:
                messages.append(message)
                next_offset = end
            elif end == offset:
                next_offset = end
    except BusError as exc:
        issues.append(str(exc))
    if offset > len(raw):
        issues.append("cursor is beyond log end")
    elif offset and raw[offset - 1:offset] != b"\n":
        issues.append("cursor does not point to a line boundary")
    return index, messages, issues, next_offset


_BATCH_PREFIX = "agent-bus-batch-v1."
_BATCH_FIELDS = frozenset((
    "schema", "recipient", "consumer", "start_offset", "start_sequence",
    "end_offset", "end_sequence", "device", "inode", "digest"))


def _batch_digest(payload: dict[str, Any], message_hashes: list[str]) -> str:
    bound = {
        key: payload[key] for key in payload
        if key not in ("schema", "digest")
    }
    bound["message_sha256s"] = message_hashes
    return hashlib.sha256(_canonical(bound)).hexdigest()


def _encode_batch_token(payload: dict[str, Any]) -> str:
    encoded = base64.urlsafe_b64encode(_canonical(payload)).decode("ascii")
    return _BATCH_PREFIX + encoded.rstrip("=")


def _decode_batch_token(token: str) -> dict[str, Any]:
    if (not isinstance(token, str) or not token.startswith(_BATCH_PREFIX)
            or len(token) > 2048):
        raise BusError("invalid batch token")
    encoded = token[len(_BATCH_PREFIX):]
    try:
        padding = "=" * (-len(encoded) % 4)
        raw = base64.b64decode(
            encoded + padding, altchars=b"-_", validate=True)
        payload = json.loads(raw.decode("ascii"))
    except (ValueError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise BusError("invalid batch token") from exc
    if not isinstance(payload, dict) or set(payload) != _BATCH_FIELDS:
        raise BusError("invalid batch token fields")
    if payload["schema"] != "agent-bus/inbox-batch-v1":
        raise BusError("unsupported batch token")
    for key in ("recipient", "consumer"):
        _check_name(payload[key], f"batch {key}")
    for key in ("start_offset", "start_sequence", "end_offset",
                "end_sequence", "device", "inode"):
        value = payload[key]
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise BusError("invalid batch token range")
    if (payload["end_offset"] <= payload["start_offset"]
            or payload["end_sequence"] <= payload["start_sequence"]):
        raise BusError("invalid batch token range")
    if (not isinstance(payload["digest"], str)
            or not re.fullmatch(r"[0-9a-f]{64}", payload["digest"])):
        raise BusError("invalid batch token digest")
    return payload


def _chronological(message: Message) -> tuple[str, str, int, str, str]:
    return (message.ts, message.to_peer, message.sequence,
            message.from_peer, message.message_sha256)


def _current_head(thread: list[Message]) -> str | None:
    """The head a thread is at now: the latest ask-ready or head-moved head."""
    current = None
    for message in thread:
        if message.kind in ("ask-ready", "head-moved") and message.head is not None:
            current = message.head
    return current


def _verdict_stale_head(verdict: Message, thread: list[Message]) -> str | None:
    """The head that makes ``verdict`` stale, if any.

    A verdict that names a head is stale whenever the thread's current head
    (the latest ask-ready or head-moved head) differs from it, regardless of
    message order: a PASS at A that arrives after the thread moved to B does
    not become current. A verdict without a head cannot be compared, so it
    is stale only once a head-moved follows it; a later re-ask alone is a
    nudge, not a rebase.
    """
    current = _current_head(thread)
    if verdict.head is not None:
        return current if current is not None and current != verdict.head else None
    after = False
    moved = None
    for message in thread:
        if message is verdict:
            after = True
        elif after and message.kind == "head-moved":
            moved = message.head
    return moved


def annotate_stale(messages: list[Message]) -> list[Message]:
    """Mark verdicts whose thread is not at the head they reviewed."""
    ordered = sorted(messages, key=_chronological)
    by_key: dict[str, list[Message]] = {}
    for message in ordered:
        by_key.setdefault(thread_key(message.ref), []).append(message)
    stale: dict[str, str | None] = {}
    for thread in by_key.values():
        for message in thread:
            if message.kind == "verdict":
                stale[message.message_sha256] = _verdict_stale_head(
                    message, thread)
    return [replace(message, stale_head=stale[message.message_sha256])
            if (message.message_sha256 in stale
                and stale[message.message_sha256] != message.stale_head)
            else message
            for message in ordered]


def _dependency_satisfied(dependency: str, repo: str | None,
                          annotated: list[Message]) -> bool:
    """Whether the latest verdict on the dependency is a current PASS.

    ``annotated`` is the project context after ``annotate_stale``; a bare
    PR number resolves inside ``repo``, the asking ref's own repository.
    """
    verdicts = [message for message in annotated
                if message.kind == "verdict"
                and same_thread(dependency, message.ref, repo=repo)]
    if not verdicts:
        return False
    latest = max(verdicts, key=_chronological)
    return latest.verdict == "PASS" and latest.stale_head is None


def _actionable_items(messages: list[Message],
                      context: list[Message] | None = None
                      ) -> list[ActionableItem]:
    """Reduce one recipient's pending messages without optimistic closure.

    ``context`` is the whole project log; when omitted only the pending
    messages themselves are consulted for head moves and dependencies.
    """
    if not messages:
        return []
    if context is None:
        context = list(messages)
    nodes = {f"{message.to_peer}:{message.sequence}": message
             for message in messages}
    superseded: set[str] = set()
    closed: set[str] = set()
    local_closers: set[str] = set()

    # Explicit replacements hide only the exact message they replace. A reply
    # to an older chain member must not close a newer successor. Chatter is
    # deliberately incapable of replacing or closing an ask.
    for anchor, message in nodes.items():
        if (message.supersedes in nodes
                and message.kind not in CHATTER_KINDS):
            superseded.add(message.supersedes)

    # Link only the named exact close transitions. A generic reply and an ack
    # are delivery context, not proof that the underlying work is resolved.
    for anchor, message in nodes.items():
        if (message.kind == "ask-withdrawn"
                and (message.reply_to in nodes
                     or message.supersedes in nodes)):
            target = (message.reply_to if message.reply_to in nodes
                      else message.supersedes)
            assert target is not None
            closed.add(target)
            local_closers.add(anchor)
        elif message.kind == "verdict" and message.reply_to in nodes:
            closed.add(message.reply_to)
            local_closers.add(anchor)
    run_starts: dict[str, list[str]] = {}
    for anchor, message in nodes.items():
        if message.kind == "run-started":
            run_starts.setdefault(message.ref, []).append(anchor)
        elif message.kind == "run-ended":
            eligible = [candidate for candidate in run_starts.get(
                message.ref, []) if nodes[candidate].sequence < message.sequence]
            if eligible:
                closed.add(eligible[-1])
                local_closers.add(anchor)

    # A head move re-heads the open ask it follows instead of standing alone;
    # a host line is the latest word about that host, and only a handoff
    # (preempted/released/nominated) asks the recipient to do anything.
    moved_into: dict[str, str] = {}
    open_asks = [anchor for anchor, message in nodes.items()
                 if message.kind == "ask-ready"
                 and anchor not in superseded and anchor not in closed]
    for anchor, message in nodes.items():
        if message.kind != "head-moved":
            continue
        key = thread_key(message.ref)
        earlier = [ask for ask in open_asks
                   if thread_key(nodes[ask].ref) == key
                   and nodes[ask].sequence < message.sequence]
        if earlier:
            moved_into[anchor] = earlier[-1]
    latest_host: dict[str, str] = {}
    for anchor, message in nodes.items():
        if message.kind == "host" and message.host is not None:
            previous = latest_host.get(message.host)
            if previous is None or nodes[previous].sequence < message.sequence:
                latest_host[message.host] = anchor
    hidden_host = {anchor for anchor, message in nodes.items()
                   if message.kind == "host"
                   and (latest_host.get(message.host) != anchor
                        or message.action not in HOST_ACTIONABLE_ACTIONS)}

    annotated = annotate_stale(context)
    stale_by_hash = {message.message_sha256: message.stale_head
                     for message in annotated if message.kind == "verdict"}
    by_key: dict[str, list[Message]] = {}
    for message in context:
        by_key.setdefault(thread_key(message.ref), []).append(message)

    independent: list[ActionableItem] = []
    blocked: list[ActionableItem] = []
    for anchor, message in nodes.items():
        if (anchor in superseded or anchor in closed or anchor in local_closers
                or anchor in moved_into or anchor in hidden_host
                or message.kind not in ACTIONABLE_KINDS):
            continue
        ancestry = [anchor]
        prior = message.supersedes
        seen = {anchor}
        while prior in nodes and prior not in seen:
            ancestry.append(prior)
            seen.add(prior)
            prior = nodes[prior].supersedes
        ancestry.extend(mover for mover, target in moved_into.items()
                        if target == anchor)
        ancestry.sort(key=lambda item: nodes[item].sequence)
        head = message.head
        head_moved_from = None
        if message.kind == "ask-ready":
            for later in by_key.get(thread_key(message.ref), []):
                if (later.kind == "head-moved"
                        and _chronological(later) > _chronological(message)
                        and later.head != head):
                    head_moved_from = head_moved_from or head
                    head = later.head
        blocked_on = None
        if (message.kind in ("ask-ready", "task-ready")
                and message.depends_on is not None
                and not _dependency_satisfied(
                    message.depends_on, repo_of(message.ref), annotated)):
            blocked_on = message.depends_on
        detail = {key: getattr(message, key)
                  for key in ("verdict", "depends_on", "host", "action",
                              "count", "rc", "receipt", "prev_head", "reason")
                  if getattr(message, key) is not None}
        item = ActionableItem(
            kind=message.kind, ref=message.ref, head=head,
            sender=message.from_peer, newest_sequence=max(
                nodes[item].sequence for item in ancestry),
            collapsed_transition_count=len(ancestry) - 1,
            sequence_anchors=tuple(ancestry),
            stale_head=stale_by_hash.get(message.message_sha256),
            blocked_on=blocked_on, head_moved_from=head_moved_from,
            detail=detail)
        (blocked if blocked_on is not None else independent).append(item)
    independent.sort(key=lambda item: item.newest_sequence)
    blocked.sort(key=lambda item: item.newest_sequence)
    return independent + blocked


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

    def _recipient_index(self, to_peer: str) -> list[tuple[int, Message]]:
        """Read and validate one recipient log under its own lock."""
        path = self.state_dir / "inboxes" / f"{to_peer}.jsonl"
        if not path.exists() and not path.is_symlink():
            _ensure_file(path, "inbox log")
        fd = _safe_open_log(path)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            raw = os.pread(fd, os.fstat(fd).st_size, 0)
            return _validated_index(raw, to_peer=to_peer)
        finally:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)

    def _peer_marker(self, peer: str) -> Path:
        return self.state_dir / "peers" / f"{len(peer)}-{peer}.reader"

    def _record_reader(self, to_peer: str) -> None:
        """Remember that ``to_peer`` was consumed by a 0.4.0+ reader.

        Only a cursor-advancing read or an acknowledgement counts: those are
        made by the automation that actually consumes the inbox. A peek or
        actionable view is diagnostic and may come from a human while an
        older automation still reads the same inbox. Senders consult the
        marker before appending a version 3 line, which a 0.3.0 reader
        reports as malformed and then blocks on until it is upgraded.
        """
        peers = self.state_dir / "peers"
        if not peers.exists() and not peers.is_symlink():
            try:
                peers.mkdir(mode=0o700)
            except FileExistsError:
                pass
        _lstat(peers, "peer directory", directory=True)
        marker = self._peer_marker(to_peer)
        payload = _canonical({"reader_version": READER_VERSION}) + b"\n"
        if marker.exists() and not marker.is_symlink():
            _lstat(marker, "peer marker", directory=False)
            if marker.read_bytes() == payload:
                return
        self._write_atomic(marker, payload, "peer marker")

    def peer_reader_version(self, peer: str) -> str | None:
        marker = self._peer_marker(peer)
        if not marker.exists() and not marker.is_symlink():
            return None
        _lstat(marker, "peer marker", directory=False)
        try:
            data = json.loads(marker.read_text(encoding="ascii"))
            version = data["reader_version"]
            if not isinstance(version, str):
                raise ValueError
            return version
        except (OSError, UnicodeDecodeError, json.JSONDecodeError, KeyError,
                TypeError, ValueError) as exc:
            raise BusError(f"invalid peer marker: {marker}") from exc

    def send(self, from_peer: str, to_peer: str, kind: str, ref: str, *,
             once: bool = False, dedupe_window: int = 20,
             strict: bool | None = None, assume_peer_upgraded: bool = False,
             **optional: Any) -> Message:
        self._require_init()
        if strict is None:
            strict = strict_default()
        from_peer = _check_name(from_peer, "sender")
        to_peer = _check_name(to_peer, "recipient")
        if from_peer == to_peer:
            raise BusError("sender and recipient must differ")
        if not isinstance(kind, str) or kind not in KINDS:
            raise BusError("invalid message kind")
        if not isinstance(ref, str) or not ref or len(ref) > 8192:
            raise BusError("invalid message ref")
        if not isinstance(once, bool):
            raise BusError("once must be a boolean")
        if (isinstance(dedupe_window, bool)
                or not isinstance(dedupe_window, int)
                or dedupe_window <= 0):
            raise BusError("dedupe window must be a positive integer")
        unexpected = set(optional) - OPTIONAL
        if unexpected:
            raise BusError(f"unexpected message fields: {', '.join(sorted(unexpected))}")
        if "peer_sequence_at_send" in optional:
            raise BusError("peer_sequence_at_send is derived by the bus")
        if "seen_peer_sequence" in optional:
            seen = optional["seen_peer_sequence"]
            if (isinstance(seen, bool) or not isinstance(seen, int) or seen < 0):
                raise BusError("invalid message seen_peer_sequence")
            peer_index = self._recipient_index(from_peer)
            peer_available = peer_index[-1][1].sequence if peer_index else 0
            if seen > peer_available:
                raise BusError("seen peer sequence is not present in sender inbox")
            optional["peer_sequence_at_send"] = peer_available
        path = self.state_dir / "inboxes" / self._direction(from_peer, to_peer)
        _ensure_file(path, "inbox log")
        fd = _safe_open_log(path)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            raw = os.pread(fd, os.fstat(fd).st_size, 0)
            try:
                index = _validated_index(raw, to_peer=to_peer)
            except (UnicodeDecodeError, json.JSONDecodeError, BusError) as exc:
                raise BusError(
                    f"existing inbox is malformed; refusing append: {exc}") from exc
            previous = index[-1][1].sequence if index else 0
            coordination = bool(set(optional) & COORDINATION_FIELDS
                                or kind in COORDINATION_KINDS)
            data: dict[str, Any] = {
                "schema": SCHEMA,
                "version": COORDINATION_VERSION if coordination else VERSION,
                "sequence": previous + 1,
                "ts": _utc_now(), "from": from_peer, "to": to_peer,
                "kind": kind, "ref": ref,
            }
            for key in OPTIONAL_TEXT:
                if key in optional:
                    if not isinstance(optional[key], str) or len(optional[key]) > 8192:
                        raise BusError(f"invalid message {key}")
                    data[key] = optional[key]
            for key in OPTIONAL_LINKS:
                if key in optional:
                    if (not isinstance(optional[key], str)
                            or not MESSAGE_LINK.fullmatch(optional[key])):
                        raise BusError(f"invalid message {key}")
                    data[key] = optional[key]
            for key in OPTIONAL_INTEGERS | COORDINATION_INTEGERS:
                if key in optional:
                    data[key] = optional[key]
            _check_kind_shape(data)
            warnings = _advisories(data)
            if strict and warnings:
                raise BusError("strict: " + "; ".join(warnings))
            if coordination and not assume_peer_upgraded:
                reader = self.peer_reader_version(to_peer)
                if reader is None:
                    raise BusError(
                        f"{to_peer} has not read this project with agent-bus >= "
                        f"{READER_VERSION}; an older reader would report this "
                        "coordination message as malformed and block its inbox. "
                        "Upgrade the recipient first, or pass "
                        "--assume-peer-upgraded")
            if once:
                identity = _dedupe_key(data)
                for _end, candidate in reversed(index[-dedupe_window:]):
                    if _dedupe_key(candidate.body()) == identity:
                        return replace(candidate, duplicate_suppressed=True,
                                       warnings=tuple(warnings))
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
            return replace(message, warnings=tuple(warnings))
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
        self._write_atomic(path, _canonical({"offset": offset}) + b"\n",
                           "cursor")

    def _write_atomic(self, path: Path, payload: bytes, label: str) -> None:
        fd, name = tempfile.mkstemp(prefix=".cursor.", dir=path.parent)
        temp = Path(name)
        try:
            os.fchmod(fd, 0o600)
            if os.write(fd, payload) != len(payload):
                raise BusError(f"short {label} write")
            os.fsync(fd)
            os.close(fd)
            os.replace(temp, path)
            dfd = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(dfd)
            finally:
                os.close(dfd)
            _lstat(path, label, directory=False)
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
            _index, messages, issues, next_offset = _scan_pending(
                raw, to_peer=to_peer, offset=offset)
            if not peek and not issues and next_offset != offset:
                self._write_cursor(cursor, next_offset)
            if not peek:
                self._record_reader(to_peer)
            return messages, issues
        finally:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)

    def peek_batch(self, to_peer: str, consumer: str) -> tuple[
            list[Message], list[str], InboxBatch | None]:
        """Peek pending messages and bind the exact range to a stateless token."""
        self._require_init()
        to_peer = _check_name(to_peer, "recipient")
        consumer = _check_name(consumer, "consumer")
        log = self.state_dir / "inboxes" / f"{to_peer}.jsonl"
        if not log.exists() and not log.is_symlink():
            _ensure_file(log, "inbox log")
        cursor = self._cursor_path(to_peer, consumer)
        fd = _safe_open_log(log)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            info = os.fstat(fd)
            raw = os.pread(fd, info.st_size, 0)
            offset = self._read_cursor(cursor)
            index, messages, issues, next_offset = _scan_pending(
                raw, to_peer=to_peer, offset=offset)
            if issues or not messages:
                return messages, issues, None
            by_end = {end: message for end, message in index}
            if offset and offset not in by_end:
                return messages, ["cursor does not identify a complete message"], None
            start_message = by_end.get(offset)
            payload: dict[str, Any] = {
                "schema": "agent-bus/inbox-batch-v1",
                "recipient": to_peer,
                "consumer": consumer,
                "start_offset": offset,
                "start_sequence": start_message.sequence if start_message else 0,
                "end_offset": next_offset,
                "end_sequence": messages[-1].sequence,
                "device": info.st_dev,
                "inode": info.st_ino,
            }
            payload["digest"] = _batch_digest(
                payload, [message.message_sha256 for message in messages])
            token = _encode_batch_token(payload)
            return messages, issues, InboxBatch(
                token=token, recipient=to_peer, consumer=consumer,
                start_sequence=payload["start_sequence"],
                end_sequence=payload["end_sequence"],
                message_count=len(messages))
        finally:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)

    def actionable_inbox(self, to_peer: str, consumer: str) -> tuple[
            list[ActionableItem], list[str], InboxBatch | None]:
        """Return a compact, cursor-neutral view of unresolved pending items.

        Items come only from this recipient's pending batch. The whole
        project log (every direction, read without cursors) is consulted
        for context: PASS verdicts that satisfy a dependency and head moves
        that make a verdict stale usually live in the other inbox.
        """
        messages, issues, batch = self.peek_batch(to_peer, consumer)
        context = self._project_context() if messages else []
        return _actionable_items(messages, context), issues, batch

    def _project_context(self) -> list[Message]:
        """Every readable message in every direction, oldest first."""
        messages: list[Message] = []
        for path in sorted((self.state_dir / "inboxes").glob("*.jsonl")):
            try:
                index = self._recipient_index(path.stem)
            except BusError:
                # A malformed peer log is reported by doctor and by that
                # recipient's own reads; context stays best-effort.
                continue
            messages.extend(message for _end, message in index)
        messages.sort(key=_chronological)
        return messages

    def thread(self, query: str) -> tuple[list[Message], list[str]]:
        """Return the merged log restricted to one thread, verdicts annotated.

        ``query`` is a ref or a bare PR/issue number. Verdicts whose thread
        moved to another head afterwards carry ``stale_head``.
        """
        messages, issues = self.log()
        selected = [message for message in messages
                    if same_thread(query, message.ref)]
        return annotate_stale(selected), issues

    def ack(self, to_peer: str, consumer: str, *,
            through_sequence: int) -> dict[str, int]:
        """Advance one consumer through exactly an already-observed sequence.

        This is the commit half of a safe ``peek -> process -> ack`` adapter.
        Messages appended after the peek remain pending because acknowledgement
        targets an exact sequence instead of draining the live tail.
        """
        self._require_init()
        to_peer = _check_name(to_peer, "recipient")
        consumer = _check_name(consumer, "consumer")
        if (isinstance(through_sequence, bool)
                or not isinstance(through_sequence, int)
                or through_sequence <= 0):
            raise BusError("ack sequence must be a positive integer")
        log = self.state_dir / "inboxes" / f"{to_peer}.jsonl"
        if not log.exists() and not log.is_symlink():
            _ensure_file(log, "inbox log")
        cursor = self._cursor_path(to_peer, consumer)
        fd = _safe_open_log(log)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            raw = os.pread(fd, os.fstat(fd).st_size, 0)
            index = _validated_index(raw, to_peer=to_peer)
            offset = self._read_cursor(cursor)
            if offset > len(raw) or (offset and raw[offset - 1:offset] != b"\n"):
                raise BusError("cursor is outside log or not on a line boundary")
            by_end = {end: message.sequence for end, message in index}
            if offset and offset not in by_end:
                raise BusError("cursor does not identify a complete message")
            current_sequence = by_end.get(offset, 0)
            if through_sequence < current_sequence:
                raise BusError("ack sequence is behind cursor")
            targets = [(end, message) for end, message in index
                       if message.sequence == through_sequence]
            if len(targets) != 1:
                raise BusError("ack sequence is not present in inbox")
            target_offset = targets[0][0]
            if through_sequence > current_sequence:
                self._write_cursor(cursor, target_offset)
            self._record_reader(to_peer)
            return {"acked_sequence": through_sequence,
                    "offset": target_offset,
                    "pending_count": len(index) - through_sequence}
        finally:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)

    def ack_batch(self, to_peer: str, consumer: str, *,
                  token: str) -> dict[str, int]:
        """Verify and acknowledge only the exact range represented by token."""
        self._require_init()
        to_peer = _check_name(to_peer, "recipient")
        consumer = _check_name(consumer, "consumer")
        payload = _decode_batch_token(token)
        if (payload["recipient"] != to_peer
                or payload["consumer"] != consumer):
            raise BusError("batch token recipient or consumer mismatch")
        log = self.state_dir / "inboxes" / f"{to_peer}.jsonl"
        if not log.exists() and not log.is_symlink():
            raise BusError("batch inbox log is missing")
        cursor = self._cursor_path(to_peer, consumer)
        fd = _safe_open_log(log)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            info = os.fstat(fd)
            if (info.st_dev != payload["device"]
                    or info.st_ino != payload["inode"]):
                raise BusError("batch inbox log was rotated")
            raw = os.pread(fd, info.st_size, 0)
            index = _validated_index(raw, to_peer=to_peer)
            offset = self._read_cursor(cursor)
            if offset != payload["start_offset"]:
                raise BusError("batch cursor changed after peek")
            by_end = {end: message for end, message in index}
            start_message = by_end.get(offset)
            start_sequence = start_message.sequence if start_message else 0
            if start_sequence != payload["start_sequence"]:
                raise BusError("batch start sequence drift")
            if payload["end_offset"] not in by_end:
                raise BusError("batch end is missing or rewritten")
            selected = [
                message for end, message in index
                if offset < end <= payload["end_offset"]]
            if (not selected
                    or selected[-1].sequence != payload["end_sequence"]
                    or selected[0].sequence != start_sequence + 1):
                raise BusError("batch sequence range drift")
            recomputed = _batch_digest(
                payload, [message.message_sha256 for message in selected])
            if recomputed != payload["digest"]:
                raise BusError("batch token verification failed")
            self._write_cursor(cursor, payload["end_offset"])
            self._record_reader(to_peer)
            return {
                "acked_sequence": payload["end_sequence"],
                "offset": payload["end_offset"],
                "pending_count": len(index) - payload["end_sequence"],
            }
        finally:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)

    def consumer_status(self, to_peer: str, consumer: str) -> dict[str, Any]:
        """Report exact, read-only lag for one recipient/consumer cursor."""
        self._require_init()
        to_peer = _check_name(to_peer, "recipient")
        consumer = _check_name(consumer, "consumer")
        log = self.state_dir / "inboxes" / f"{to_peer}.jsonl"
        if not log.exists() and not log.is_symlink():
            _ensure_file(log, "inbox log")
        cursor = self._cursor_path(to_peer, consumer)
        fd = _safe_open_log(log)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            raw = os.pread(fd, os.fstat(fd).st_size, 0)
            index = _validated_index(raw, to_peer=to_peer)
            offset = self._read_cursor(cursor)
            if offset > len(raw) or (offset and raw[offset - 1:offset] != b"\n"):
                raise BusError("cursor is outside log or not on a line boundary")
            by_end = {end: message for end, message in index}
            if offset and offset not in by_end:
                raise BusError("cursor does not identify a complete message")
            acked = by_end.get(offset)
            pending = [message for end, message in index if end > offset]
            return {
                "recipient": to_peer, "consumer": consumer,
                "cursor_offset": offset, "log_bytes": len(raw),
                "acked_sequence": acked.sequence if acked else 0,
                "last_sequence": index[-1][1].sequence if index else 0,
                "pending_count": len(pending),
                "first_pending_sequence": pending[0].sequence if pending else None,
                "last_message_ts": index[-1][1].ts if index else None,
                "last_acked_ts": acked.ts if acked else None,
            }
        finally:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)

    def log(self) -> tuple[list[Message], list[str]]:
        """Return one merged, cursor-free chronological project view."""
        self._require_init()
        messages: list[Message] = []
        issues: list[str] = []
        for path in sorted((self.state_dir / "inboxes").glob("*.jsonl")):
            recipient = path.stem
            try:
                index = self._recipient_index(recipient)
            except BusError as exc:
                issues.append(f"{path.name}: {exc}")
                continue
            messages.extend(message for _end, message in index)
        messages.sort(key=lambda message: (
            message.ts, message.to_peer, message.sequence,
            message.from_peer, message.message_sha256))
        return messages, issues

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
              poll_interval: float = 1.0) -> tuple[list[Message], list[str]]:
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

    def watch_actionable(self, to_peer: str, consumer: str, *,
                         timeout: float | None = None,
                         poll_interval: float = 1.0) -> tuple[
                             list[ActionableItem], list[str],
                             InboxBatch | None, bool]:
        """Wait for the actionable set to change without advancing a cursor.

        Callers should inspect ``actionable_inbox`` once before entering this
        passive watch. The baseline is the current set, so chatter appended
        after startup does not wake the consumer.
        """
        if (timeout is not None
                and (not isinstance(timeout, (int, float))
                     or isinstance(timeout, bool)
                     or not math.isfinite(timeout) or timeout < 0)):
            raise BusError("timeout must be finite and non-negative")
        if (not isinstance(poll_interval, (int, float))
                or isinstance(poll_interval, bool)
                or not math.isfinite(poll_interval) or poll_interval <= 0):
            raise BusError("poll interval must be finite and positive")

        def signature(items: list[ActionableItem], issues: list[str]) -> bytes:
            return _canonical({
                "items": [item.as_dict() for item in items],
                "issues": issues,
            })

        started = time.monotonic()
        items, issues, batch = self.actionable_inbox(to_peer, consumer)
        baseline = signature(items, issues)
        if issues:
            return items, issues, batch, True
        while True:
            if timeout is not None and time.monotonic() - started >= timeout:
                return [], [], None, False
            remaining = None if timeout is None else max(
                0.0, timeout - (time.monotonic() - started))
            time.sleep(poll_interval if remaining is None else min(
                poll_interval, remaining))
            items, issues, batch = self.actionable_inbox(to_peer, consumer)
            current = signature(items, issues)
            if current != baseline:
                return items, issues, batch, True

"""``agent-bus wait``: block until one watched source has something new.

Every source is probed in its own child process with a hard timeout, so a
stalled ``ssh`` or ``gh`` can neither outlive the wait deadline nor delay the
other sources. A source that cannot be read is *unavailable*: that is never an
event, and its last good observation is kept until it can be read again.

The bus source peeks the caller's consumer cursor (it never acknowledges), so
anything still pending fires, including messages that arrived between turns.
The other sources compare against an observation: the first successful read
of this run, or the one saved in ``--state`` by an earlier run or by
``--arm``. Probe argv is built only from command-line arguments, never from
bus text or probe output.
"""
from __future__ import annotations

import json
import math
import os
import re
import shlex
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import agent_bus

from .core import BusError

STATE_SCHEMA = "agent-bus/wait-state-v1"
# Exit status of the file probe's shell when the path does not exist.
MISSING_RC = 44
MAX_EVENTS_PER_SOURCE = 20
DETAIL_LIMIT = 400
SSH_HOST = re.compile(r"[A-Za-z0-9_.%+@][A-Za-z0-9_.%+@-]*")
GH_REPO = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+")
FILE_PROBE = 'test -e "$1" || exit 44; exec cat -- "$1"'
SSH_OPTIONS = ("-o", "BatchMode=yes", "-o", "ConnectTimeout=10",
               "-o", "ServerAliveInterval=10", "-o", "ServerAliveCountMax=2")
GH_ITEM_JQ = '.[] | "\\(.id) \\(.user.login // "-") \\(.html_url // "-")"'
CI_GREEN = frozenset(("SUCCESS", "NEUTRAL", "SKIPPED"))
CI_PENDING_STATES = frozenset(("PENDING", "EXPECTED", ""))
CI_RED_STATES = frozenset(("FAILURE", "ERROR"))

EXIT_EVENT, EXIT_TIMEOUT, EXIT_ERROR = 0, 1, 2


class Unavailable(Exception):
    """This probe could not be read; not an event."""


@dataclass
class Event:
    source: str
    kind: str
    detail: str
    data: dict[str, Any] = field(default_factory=dict)


def _clip(text: str, limit: int = DETAIL_LIMIT) -> str:
    text = " ".join(text.split())
    return text if len(text) <= limit else text[:limit - 3] + "..."


class Source:
    key = ""
    label = ""
    stateful = True

    def argv(self) -> list[str]:
        raise NotImplementedError

    def env(self) -> dict[str, str] | None:
        return None

    def observe(self, rc: int, out: bytes) -> Any:
        raise NotImplementedError

    def compare(self, prior: Any, reading: Any) -> tuple[list[Event], Any]:
        """Return the new events and the observation to keep.

        ``prior`` is None when there is no observation yet; the reading then
        becomes the baseline and reports nothing.
        """
        raise NotImplementedError


class BusSource(Source):
    stateful = False

    def __init__(self, project: str, state_root: str | None, to_peer: str,
                 consumer: str, actionable: bool):
        self.project, self.state_root = project, state_root
        self.to_peer, self.consumer = to_peer, consumer
        self.actionable = actionable
        self.key = f"bus:{to_peer}:{consumer}"
        self.label = "bus"

    def argv(self) -> list[str]:
        argv = [sys.executable, "-m", "agent_bus", "inbox",
                "--project", self.project]
        if self.state_root is not None:
            argv += ["--state-root", self.state_root]
        argv += ["--to", self.to_peer, "--consumer", self.consumer, "--json"]
        argv += ["--actionable"] if self.actionable else ["--peek", "--batch"]
        return argv

    def env(self) -> dict[str, str]:
        # The child must import this same package, even from a source tree.
        package_root = str(Path(agent_bus.__file__).resolve().parents[1])
        env = dict(os.environ)
        env["PYTHONPATH"] = os.pathsep.join(
            [package_root] + ([env["PYTHONPATH"]] if env.get("PYTHONPATH") else []))
        return env

    def observe(self, rc: int, out: bytes) -> dict[str, Any]:
        try:
            payload = json.loads(out.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise Unavailable(f"inbox output unreadable (exit {rc})") from exc
        if not isinstance(payload, dict):
            raise Unavailable("inbox output is not an object")
        issues = payload.get("issues")
        if rc != 0 or issues:
            raise Unavailable(f"inbox exit {rc}: {issues}")
        items = payload.get("actionable" if self.actionable else "messages")
        if not isinstance(items, list):
            raise Unavailable("inbox output has no message list")
        return payload

    def compare(self, prior: Any, reading: dict[str, Any]) -> tuple[list[Event], Any]:
        batch = reading.get("batch")
        events = []
        if self.actionable:
            for item in reading["actionable"]:
                detail = (f"{item.get('sender')}->{self.to_peer} {item.get('kind')} "
                          f"{item.get('ref')} newest={self.to_peer}:{item.get('newest_sequence')}")
                if item.get("head"):
                    detail += f" head={item['head']}"
                events.append(Event("bus", "actionable", _clip(detail), item))
        else:
            for message in reading["messages"]:
                detail = (f"{message.get('to')}:{message.get('sequence')} "
                          f"{message.get('from')}->{message.get('to')} "
                          f"{message.get('kind')} {message.get('ref')}")
                for name in ("verdict", "head", "note"):
                    if message.get(name) is not None:
                        detail += f" {name}={message[name]}"
                events.append(Event("bus", "message", _clip(detail), message))
        if events and isinstance(batch, dict) and batch.get("token"):
            events.append(Event("bus", "batch", f"token={batch['token']}", batch))
        return events, None


class GhItemsSource(Source):
    """New issue comments, reviews or review comments: ids above the max seen."""

    def __init__(self, gh: str, repo: str, number: int, path: str, kind: str,
                 label: str):
        self.gh, self.repo, self.number = gh, repo, number
        self.path, self.kind, self.label = path, kind, label
        self.key = f"gh:{repo}:{label}:{kind}"

    def argv(self) -> list[str]:
        return [self.gh, "api", "--paginate",
                f"repos/{self.repo}/{self.path}?per_page=100", "--jq", GH_ITEM_JQ]

    def observe(self, rc: int, out: bytes) -> list[tuple[int, str]]:
        # A nonzero exit can follow a partial page set: nothing from it counts.
        if rc != 0:
            raise Unavailable(f"gh exit {rc}")
        items = []
        for line in out.decode("utf-8", "replace").splitlines():
            if not line.strip():
                continue
            head, _, rest = line.partition(" ")
            if not head.isdigit():
                raise Unavailable("gh output line has no numeric id")
            items.append((int(head), rest))
        return items

    def compare(self, prior: Any, reading: list[tuple[int, str]]) -> tuple[list[Event], Any]:
        top = max((item_id for item_id, _ in reading), default=0)
        if prior is None:
            return [], {"max_id": top}
        seen = int(prior.get("max_id", 0))
        fresh = sorted(item for item in reading if item[0] > seen)
        events = [Event(self.label, self.kind, _clip(f"{item_id} {rest}"),
                        {"id": item_id}) for item_id, rest in fresh]
        return events, {"max_id": max(seen, top)}


def _ci_summary(rollup: Any) -> str:
    if not rollup:
        return "none"
    pending = failed = False
    for check in rollup:
        if not isinstance(check, dict):
            raise Unavailable("statusCheckRollup entry is not an object")
        if "status" in check:
            if str(check.get("status") or "").upper() != "COMPLETED":
                pending = True
            elif str(check.get("conclusion") or "").upper() not in CI_GREEN:
                failed = True
        else:
            state = str(check.get("state") or "").upper()
            if state in CI_PENDING_STATES:
                pending = True
            elif state in CI_RED_STATES:
                failed = True
    return "failure" if failed else "pending" if pending else "success"


class GhPrViewSource(Source):
    """The head sha, and optionally the PR state and the head's checks."""

    def __init__(self, gh: str, repo: str, number: int, state: bool, checks: bool):
        self.gh, self.repo, self.number = gh, repo, number
        self.watch_state, self.watch_checks = state, checks
        self.label = f"pr#{number}"
        self.key = f"gh:{repo}:{self.label}:view"

    def argv(self) -> list[str]:
        fields = ["headRefOid"]
        if self.watch_state:
            fields.append("state")
        if self.watch_checks:
            fields.append("statusCheckRollup")
        return [self.gh, "pr", "view", str(self.number), "-R", self.repo,
                "--json", ",".join(fields)]

    def observe(self, rc: int, out: bytes) -> dict[str, str]:
        if rc != 0:
            raise Unavailable(f"gh exit {rc}")
        try:
            data = json.loads(out.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise Unavailable("gh pr view output unreadable") from exc
        if not isinstance(data, dict) or not isinstance(data.get("headRefOid"), str) \
                or not data["headRefOid"]:
            raise Unavailable("gh pr view output has no head")
        reading = {"head": data["headRefOid"]}
        if self.watch_state:
            if not isinstance(data.get("state"), str):
                raise Unavailable("gh pr view output has no state")
            reading["state"] = data["state"]
        if self.watch_checks:
            reading["ci"] = _ci_summary(data.get("statusCheckRollup"))
        return reading

    def compare(self, prior: Any, reading: dict[str, str]) -> tuple[list[Event], Any]:
        if prior is None:
            return [], reading
        events = []
        if prior.get("head") != reading["head"]:
            events.append(Event(self.label, "head",
                                f"{prior.get('head')}->{reading['head']}", reading))
        if self.watch_state and prior.get("state") not in (None, reading["state"]):
            events.append(Event(self.label, "state",
                                f"{prior['state']}->{reading['state']}", reading))
        ci = reading.get("ci")
        if (self.watch_checks and ci in ("success", "failure")
                and (prior.get("ci") != ci or prior.get("head") != reading["head"])):
            events.append(Event(self.label, "checks", f"{ci} head={reading['head']}",
                                reading))
        return events, reading


class FileSource(Source):
    """New complete lines (matching REGEX) in a local or remote file.

    The observation is the matching-line count and the last matching line.
    A missing path is no records when nothing was observed yet, and otherwise
    unavailable. A file that no longer extends the observed lines was
    replaced: its matching lines after the last observed one (or all of
    them) are new.
    """

    def __init__(self, path: str, regex: re.Pattern[str] | None, spec: str,
                 ssh: str | None = None, host: str | None = None):
        self.path, self.regex, self.ssh, self.host = path, regex, ssh, host
        self.label = f"ssh:{spec}" if host else f"file:{spec}"
        self.key = self.label

    def argv(self) -> list[str]:
        if self.host is None:
            return ["/bin/sh", "-c", FILE_PROBE, "sh", self.path]
        quoted = shlex.quote(self.path)
        remote = f"test -e {quoted} || exit {MISSING_RC}; exec cat -- {quoted}"
        return [self.ssh, *SSH_OPTIONS, "--", self.host, remote]

    def observe(self, rc: int, out: bytes) -> list[str] | None:
        if rc == MISSING_RC:
            return None
        if rc != 0:
            raise Unavailable(f"read exit {rc}")
        # An unterminated last line may still be being written.
        lines = out.decode("utf-8", "replace").split("\n")[:-1]
        if self.regex is None:
            return lines
        return [line for line in lines if self.regex.search(line)]

    def compare(self, prior: Any, reading: list[str] | None) -> tuple[list[Event], Any]:
        if reading is None:
            return [], prior if prior is not None else {"count": 0, "last": None}
        observation = {"count": len(reading), "last": reading[-1] if reading else None}
        if prior is None:
            return [], observation
        count, last = int(prior.get("count", 0)), prior.get("last")
        if len(reading) >= count and (count == 0 or reading[count - 1] == last):
            fresh = reading[count:]
        elif last is not None and last in reading:
            fresh = reading[len(reading) - reading[::-1].index(last):]
        else:
            fresh = reading
        return [Event(self.label, "line", _clip(line)) for line in fresh], observation


def _split_spec(spec: str, label: str) -> tuple[str, str | None]:
    path, sep, regex = spec.partition(":")
    if not path:
        raise BusError(f"{label}: empty path in {spec!r}")
    if "\n" in path or "\0" in path:
        raise BusError(f"{label}: path has a newline or NUL")
    return path, (regex if sep and regex else None)


def _compile(regex: str | None, label: str) -> re.Pattern[str] | None:
    if regex is None:
        return None
    try:
        return re.compile(regex)
    except re.error as exc:
        raise BusError(f"{label}: invalid regex {regex!r}: {exc}") from exc


def _tool(name: str, env_name: str) -> str:
    found = shutil.which(os.environ.get(env_name) or name)
    if found is None:
        raise BusError(f"{name} not found on PATH")
    return found


def build_sources(args: Any) -> list[Source]:
    sources: list[Source] = []
    if not args.no_bus:
        sources.append(BusSource(args.project, args.state_root, args.to_peer,
                                 args.consumer, args.actionable))
    if (args.gh_pr or args.gh_issue) and not args.gh_repo:
        raise BusError("--gh-pr and --gh-issue need --gh-repo OWNER/REPO")
    if (args.gh_state or args.gh_checks) and not args.gh_pr:
        raise BusError("--gh-state and --gh-checks need --gh-pr")
    if args.gh_repo is not None and not GH_REPO.fullmatch(args.gh_repo):
        raise BusError(f"--gh-repo must be OWNER/REPO: {args.gh_repo!r}")
    gh = _tool("gh", "AGENT_BUS_GH") if (args.gh_pr or args.gh_issue) else ""
    for number in args.gh_pr or []:
        if number <= 0:
            raise BusError("--gh-pr takes a positive number")
        label = f"pr#{number}"
        sources.append(GhPrViewSource(gh, args.gh_repo, number, args.gh_state,
                                      args.gh_checks))
        sources.append(GhItemsSource(gh, args.gh_repo, number,
                                     f"issues/{number}/comments", "comment", label))
        sources.append(GhItemsSource(gh, args.gh_repo, number,
                                     f"pulls/{number}/reviews", "review", label))
        sources.append(GhItemsSource(gh, args.gh_repo, number,
                                     f"pulls/{number}/comments", "review-comment", label))
    for number in args.gh_issue or []:
        if number <= 0:
            raise BusError("--gh-issue takes a positive number")
        sources.append(GhItemsSource(gh, args.gh_repo, number,
                                     f"issues/{number}/comments", "comment",
                                     f"issue#{number}"))
    for spec in args.file or []:
        path, regex = _split_spec(spec, "--file")
        sources.append(FileSource(path, _compile(regex, "--file"), spec))
    ssh = _tool("ssh", "AGENT_BUS_SSH") if args.ssh else ""
    for spec in args.ssh or []:
        host, sep, rest = spec.partition(":")
        if not sep or not SSH_HOST.fullmatch(host):
            raise BusError(f"--ssh must be HOST:PATH[:REGEX] with a plain host: {spec!r}")
        path, regex = _split_spec(rest, "--ssh")
        sources.append(FileSource(path, _compile(regex, "--ssh"), spec, ssh, host))
    if len({source.key for source in sources}) != len(sources):
        raise BusError("a source is listed twice")
    if not sources:
        raise BusError("nothing to wait on")
    return sources


def load_state(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise BusError(f"invalid wait state {path}: {exc}") from exc
    if (not isinstance(data, dict) or data.get("schema") != STATE_SCHEMA
            or not isinstance(data.get("sources"), dict)):
        raise BusError(f"invalid wait state {path}: wrong schema")
    return data["sources"]


def save_state(path: Path, sources: dict[str, Any]) -> None:
    payload = json.dumps({"schema": STATE_SCHEMA, "sources": sources},
                         sort_keys=True, indent=1) + "\n"
    fd, name = tempfile.mkstemp(prefix=".wait-state.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(name, path)
    finally:
        try:
            os.unlink(name)
        except FileNotFoundError:
            pass


@dataclass
class _Running:
    source: Source
    proc: subprocess.Popen[bytes]
    out: Any
    started: float
    kill_at: float


def _kill(run: _Running) -> None:
    # Each probe leads its own process group; this kills only that group.
    try:
        os.killpg(run.proc.pid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        pass
    run.proc.wait()


def poll(sources: list[Source], observations: dict[str, Any], *, timeout: float,
         interval: float, probe_timeout: float, arm: bool = False) -> list[Event]:
    """Probe until an event, the deadline, or (with ``arm``) one read of each.

    Updates ``observations`` in place and returns the events found.
    """
    start = time.monotonic()
    deadline = start + timeout
    next_at = {source.key: start for source in sources}
    probed: set[str] = set()
    running: dict[str, _Running] = {}
    warned: dict[str, str] = {}
    events: list[Event] = []
    try:
        while True:
            now = time.monotonic()
            if now >= deadline:
                break
            for source in sources:
                if (source.key in running or now < next_at[source.key]
                        or (arm and source.key in probed)):
                    continue
                out = tempfile.TemporaryFile()
                try:
                    proc = subprocess.Popen(
                        source.argv(), stdin=subprocess.DEVNULL, stdout=out,
                        stderr=subprocess.DEVNULL, env=source.env(),
                        start_new_session=True)
                except OSError as exc:
                    out.close()
                    probed.add(source.key)
                    next_at[source.key] = now + interval
                    _warn(warned, source, f"cannot start probe: {exc}")
                    continue
                running[source.key] = _Running(
                    source, proc, out, now,
                    now + min(probe_timeout, deadline - now))
            for key, run in list(running.items()):
                rc = run.proc.poll()
                timed_out = False
                if rc is None:
                    if time.monotonic() < run.kill_at:
                        continue
                    _kill(run)
                    timed_out = True
                del running[key]
                probed.add(key)
                next_at[key] = run.started + interval
                run.out.seek(0)
                data = run.out.read()
                run.out.close()
                try:
                    if timed_out:
                        raise Unavailable("probe timed out")
                    reading = run.source.observe(run.proc.returncode, data)
                    found, observation = run.source.compare(
                        observations.get(key), reading)
                except Unavailable as exc:
                    _warn(warned, run.source, str(exc))
                    continue
                warned.pop(key, None)
                if run.source.stateful:
                    observations[key] = observation
                events.extend(found)
            if events or (arm and not running and len(probed) == len(sources)):
                break
            time.sleep(max(0.0, min(0.1, deadline - time.monotonic())))
    finally:
        for run in running.values():
            _kill(run)
            run.out.close()
    return events


def _warn(warned: dict[str, str], source: Source, reason: str) -> None:
    if warned.get(source.key) != reason:
        warned[source.key] = reason
        print(f"agent-bus: wait: {source.key} unavailable: {reason}", file=sys.stderr)


def _number(value: float, label: str, minimum: float) -> float:
    if isinstance(value, bool) or not math.isfinite(value) or value < minimum:
        raise BusError(f"{label} must be finite and at least {minimum:g}")
    return value


def run_wait(args: Any) -> int:
    timeout = _number(args.timeout, "--timeout", 0)
    interval = _number(args.interval, "--interval", 1)
    probe_timeout = _number(args.probe_timeout, "--probe-timeout", 1)
    sources = build_sources(args)
    if args.arm:
        if args.state is None:
            raise BusError("--arm needs --state")
        sources = [source for source in sources if source.stateful]
        if not sources:
            raise BusError("--arm needs a --gh-*, --file or --ssh source")
    state_path = Path(args.state).expanduser() if args.state else None
    saved = load_state(state_path)
    observations = {} if args.arm else {
        source.key: saved[source.key] for source in sources
        if source.stateful and source.key in saved}
    events = poll(sources, observations, timeout=timeout, interval=interval,
                  probe_timeout=probe_timeout, arm=args.arm)
    rc = EXIT_EVENT
    if state_path is not None:
        merged = dict(saved)
        merged.update(observations)
        try:
            save_state(state_path, merged)
        except OSError as exc:
            print(f"agent-bus: wait: state not saved, events will repeat: {exc}",
                  file=sys.stderr)
            rc = EXIT_ERROR
    if args.arm:
        for source in sources:
            word = "ARMED" if source.key in observations else "UNAVAILABLE"
            _emit(args.json, {"armed": word == "ARMED", "source": source.key},
                  f"{word} {source.key}")
        return rc if all(source.key in observations for source in sources) else EXIT_ERROR
    if not events:
        _emit(args.json, {"timeout": timeout}, f"TIMEOUT after {timeout:g}s")
        return EXIT_TIMEOUT if rc == EXIT_EVENT else rc
    by_source: dict[str, int] = {}
    for event in events:
        by_source[event.source] = by_source.get(event.source, 0) + 1
        if by_source[event.source] > MAX_EVENTS_PER_SOURCE:
            continue
        _emit(args.json, {"event": {"source": event.source, "kind": event.kind,
                                    "detail": event.detail, "data": event.data}},
              f"EVENT {event.source} {event.kind} {event.detail}")
    for source, count in by_source.items():
        if count > MAX_EVENTS_PER_SOURCE:
            _emit(args.json, {"truncated": {"source": source, "count": count}},
                  f"EVENT {source} truncated {count - MAX_EVENTS_PER_SOURCE} more")
    return rc


def _emit(as_json: bool, payload: dict[str, Any], text: str) -> None:
    if as_json:
        print(json.dumps({"status": "NON_AUTHORITATIVE", **payload},
                         ensure_ascii=True, sort_keys=True, separators=(",", ":")),
              flush=True)
    else:
        print(f"NON_AUTHORITATIVE {text}", flush=True)

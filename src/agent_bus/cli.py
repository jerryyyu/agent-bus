from __future__ import annotations

import argparse
import json
import math
import sys
import time
from typing import Any

from .core import ActionableItem, Bus, BusError, InboxBatch, Message


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="agent-bus",
        description="Local, untrusted pointer signaling between agent harnesses")
    sub = parser.add_subparsers(dest="command", required=True)

    def project_args(command: argparse.ArgumentParser) -> None:
        command.add_argument("--project", required=True, help="project directory (Git is not required)")
        command.add_argument("--state-root", help="managed state root (defaults outside project repositories)")

    init = sub.add_parser("init", help="initialize private state for a project")
    project_args(init)

    send = sub.add_parser("send", help="append one pointer message")
    project_args(send)
    send.add_argument("--from", dest="from_peer", required=True, help="sender agent ID")
    send.add_argument("--to", dest="to_peer", required=True, help="recipient agent ID")
    send.add_argument("--kind", required=True)
    send.add_argument("--ref", required=True)
    for name in ("head", "verdict", "ledger", "note", "reply-to",
                 "supersedes"):
        send.add_argument(f"--{name}")
    send.add_argument("--seen-peer-sequence", type=int)
    send.add_argument("--once", action="store_true",
                      help="suppress a semantic duplicate in the recent window")
    send.add_argument("--dedupe-window", type=int, default=20)
    send.add_argument("--json", action="store_true")

    for name in ("inbox", "watch"):
        command = sub.add_parser(name, help=f"{name} untrusted pointer messages")
        project_args(command)
        command.add_argument("--to", dest="to_peer", required=True, help="recipient agent ID")
        command.add_argument("--consumer", required=True)
        command.add_argument("--json", action="store_true")
        if name == "inbox":
            command.add_argument("--peek", action="store_true", help="read without advancing this consumer cursor")
            command.add_argument(
                "--batch", action="store_true",
                help="return one peek envelope with a batch acknowledgement token")
            command.add_argument(
                "--actionable", action="store_true",
                help="show a compact unresolved view without advancing the cursor")
        else:
            command.add_argument("--timeout", type=float, help="maximum seconds to observe")
            command.add_argument(
                "--actionable", action="store_true",
                help="emit only when the cursor-neutral actionable set changes")

    status = sub.add_parser("status", help="show state size and project ID")
    project_args(status)
    status.add_argument("--to", dest="to_peer",
                        help="recipient for consumer-lag detail")
    status.add_argument("--consumer",
                        help="consumer for consumer-lag detail")
    status.add_argument("--json", action="store_true")
    ack = sub.add_parser(
        "ack", help="advance one consumer through an exact processed sequence")
    project_args(ack)
    ack.add_argument("--to", dest="to_peer", required=True,
                     help="recipient agent ID")
    ack.add_argument("--consumer", required=True)
    ack_target = ack.add_mutually_exclusive_group(required=True)
    ack_target.add_argument("--through", type=int,
                            help="last sequence successfully processed")
    ack_target.add_argument("--batch",
                            help="token returned by inbox --peek --batch")
    ack.add_argument("--json", action="store_true")
    log = sub.add_parser(
        "log", help="merge all directions without advancing cursors")
    project_args(log)
    log.add_argument("--follow", action="store_true")
    log.add_argument("--timeout", type=float,
                     help="bounded follow duration in seconds")
    log.add_argument("--poll-interval", type=float, default=1.0)
    log.add_argument("--json", action="store_true")
    doctor = sub.add_parser("doctor", help="check private state and logs")
    project_args(doctor)
    doctor.add_argument("--json", action="store_true")
    return parser


def _render_message(message: Message, as_json: bool, *, compact: bool = False,
                    flush: bool = False) -> None:
    # Bus text is always a pointer and explicitly non-authoritative.
    annotations: dict[str, Any] = {}
    if message.stale_premise:
        annotations["stale_premise"] = {
            "seen_peer_sequence": message.seen_peer_sequence,
            "peer_sequence_at_send": message.peer_sequence_at_send,
        }
    if message.duplicate_suppressed:
        annotations["duplicate_suppressed"] = True
    if as_json:
        payload: dict[str, Any] = {
            "status": "NON_AUTHORITATIVE", "message": message.as_dict()}
        if annotations:
            payload["annotations"] = annotations
        print(json.dumps(payload, ensure_ascii=True, sort_keys=True,
                         separators=(",", ":")), flush=flush)
    elif compact:
        timestamp = message.ts[11:19]
        fields = [
            f"{timestamp} {message.from_peer}->{message.to_peer}",
            message.kind, message.ref]
        for key in ("verdict", "ledger", "head", "reply_to", "supersedes"):
            value = getattr(message, key)
            if value is not None:
                fields.append(f"{key}={value}")
        if message.stale_premise:
            fields.append(
                f"STALE(seen={message.seen_peer_sequence},available={message.peer_sequence_at_send})")
        print("NON_AUTHORITATIVE " + " ".join(fields), flush=flush)
    else:
        payload = {"message": message.as_dict()}
        if annotations:
            payload["annotations"] = annotations
        print("NON_AUTHORITATIVE " + json.dumps(
            payload, ensure_ascii=True, sort_keys=True,
            separators=(",", ":")), flush=flush)


def _render_batch(messages: list[Message], issues: list[str],
                  batch: InboxBatch | None, *, as_json: bool) -> None:
    payload: dict[str, Any] = {
        "status": "NON_AUTHORITATIVE",
        "messages": [message.as_dict() for message in messages],
        "issues": issues,
        "batch": batch.as_dict() if batch is not None else None,
    }
    rendered = json.dumps(
        payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    print(rendered if as_json else "NON_AUTHORITATIVE " + rendered)


def _render_actionable(items: list[ActionableItem], issues: list[str],
                       batch: InboxBatch | None, *, as_json: bool,
                       changed: bool | None = None) -> None:
    payload: dict[str, Any] = {
        "status": "NON_AUTHORITATIVE",
        "actionable": [item.as_dict() for item in items],
        "issues": issues,
        "batch": batch.as_dict() if batch is not None else None,
    }
    if changed is not None:
        payload["changed"] = changed
    rendered = json.dumps(
        payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    print(rendered if as_json else "NON_AUTHORITATIVE " + rendered)


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        bus = Bus(args.project, args.state_root)
        if args.command == "init":
            bus.init()
            print(f"initialized {bus.project_id} at {bus.state_dir}")
            return 0
        if args.command == "send":
            optional = {
                key: getattr(args, key)
                for key in ("head", "verdict", "ledger", "note",
                            "reply_to", "supersedes", "seen_peer_sequence")
                if getattr(args, key) is not None}
            message = bus.send(
                args.from_peer, args.to_peer, args.kind, args.ref,
                once=args.once, dedupe_window=args.dedupe_window, **optional)
            _render_message(message, args.json)
            return 0
        if args.command in ("inbox", "watch"):
            if args.command == "inbox":
                if args.actionable:
                    if args.peek:
                        raise BusError("--actionable is already cursor-neutral; omit --peek")
                    items, issues, batch = bus.actionable_inbox(
                        args.to_peer, args.consumer)
                    _render_actionable(
                        items, issues, batch, as_json=args.json)
                    return 2 if issues else 0
                if args.batch:
                    if not args.peek:
                        raise BusError("--batch requires --peek")
                    messages, issues, batch = bus.peek_batch(
                        args.to_peer, args.consumer)
                    _render_batch(
                        messages, issues, batch, as_json=args.json)
                    return 2 if issues else 0
                messages, issues = bus.inbox(
                    args.to_peer, args.consumer, peek=args.peek)
            else:
                if args.actionable:
                    items, issues, batch, changed = bus.watch_actionable(
                        args.to_peer, args.consumer, timeout=args.timeout)
                    if changed:
                        _render_actionable(
                            items, issues, batch, as_json=args.json,
                            changed=True)
                    return 2 if issues else 0
                messages, issues = bus.watch(
                    args.to_peer, args.consumer, timeout=args.timeout)
            for message in messages:
                _render_message(message, args.json)
            for issue in issues:
                if args.json:
                    print(json.dumps({"status": "NON_AUTHORITATIVE", "issue": issue}, ensure_ascii=True, sort_keys=True, separators=(",", ":")))
                else:
                    print(f"NON_AUTHORITATIVE issue: {issue}", file=sys.stderr)
            return 2 if issues else 0
        if args.command == "status":
            result = bus.status()
            if (args.to_peer is None) != (args.consumer is None):
                raise BusError("status requires --to and --consumer together")
            if args.to_peer is not None:
                result["consumer"] = bus.consumer_status(
                    args.to_peer, args.consumer)
            if args.json:
                print(json.dumps(result, ensure_ascii=True, sort_keys=True, separators=(",", ":")))
            else:
                print(f"project_id={result['project_id']} state_dir={result['state_dir']} total_bytes={result['total_bytes']}")
                if result["warning"]:
                    print(f"warning: {result['warning']}", file=sys.stderr)
                if "consumer" in result:
                    consumer = result["consumer"]
                    print(
                        f"consumer={consumer['consumer']} recipient={consumer['recipient']} "
                        f"acked={consumer['acked_sequence']} last={consumer['last_sequence']} "
                        f"pending={consumer['pending_count']}")
            return 0
        if args.command == "ack":
            if args.batch is not None:
                result = bus.ack_batch(
                    args.to_peer, args.consumer, token=args.batch)
            else:
                result = bus.ack(
                    args.to_peer, args.consumer,
                    through_sequence=args.through)
            if args.json:
                print(json.dumps(
                    {"status": "NON_AUTHORITATIVE", "ack": result},
                    ensure_ascii=True, sort_keys=True, separators=(",", ":")))
            else:
                print("NON_AUTHORITATIVE " + json.dumps(
                    result, ensure_ascii=True, sort_keys=True,
                    separators=(",", ":")))
            return 0
        if args.command == "log":
            if (args.timeout is not None
                    and (isinstance(args.timeout, bool)
                         or not math.isfinite(args.timeout)
                         or args.timeout < 0)):
                raise BusError("timeout must be finite and non-negative")
            if (isinstance(args.poll_interval, bool)
                    or not math.isfinite(args.poll_interval)
                    or args.poll_interval <= 0):
                raise BusError("poll interval must be finite and positive")
            started = time.monotonic()
            messages, issues = bus.log()
            seen = {message.message_sha256 for message in messages}
            for message in messages:
                _render_message(
                    message, args.json, compact=not args.json,
                    flush=args.follow)
            while args.follow and not issues:
                elapsed = time.monotonic() - started
                if args.timeout is not None and elapsed >= args.timeout:
                    break
                remaining = (None if args.timeout is None else
                             max(0.0, args.timeout - elapsed))
                time.sleep(args.poll_interval if remaining is None else
                           min(args.poll_interval, remaining))
                current, issues = bus.log()
                for message in current:
                    if message.message_sha256 not in seen:
                        seen.add(message.message_sha256)
                        _render_message(
                            message, args.json, compact=not args.json,
                            flush=True)
            for issue in issues:
                print(f"NON_AUTHORITATIVE issue: {issue}", file=sys.stderr)
            return 2 if issues else 0
        if args.command == "doctor":
            findings = bus.doctor()
            if args.json:
                print(json.dumps({"status": "NON_AUTHORITATIVE", "findings": findings}, ensure_ascii=True, sort_keys=True, separators=(",", ":")))
            else:
                for finding in findings:
                    print(finding)
            return 0 if findings == ["ok"] else 1
        raise BusError("unknown command")
    except (BusError, OSError) as exc:
        print(f"agent-bus: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

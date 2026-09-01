from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from .core import Bus, BusError


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
    for name in ("head", "verdict", "ledger", "note"):
        send.add_argument(f"--{name}")
    send.add_argument("--json", action="store_true")

    for name in ("inbox", "watch"):
        command = sub.add_parser(name, help=f"{name} untrusted pointer messages")
        project_args(command)
        command.add_argument("--to", dest="to_peer", required=True, help="recipient agent ID")
        command.add_argument("--consumer", required=True)
        command.add_argument("--json", action="store_true")
        if name == "inbox":
            command.add_argument("--peek", action="store_true", help="read without advancing this consumer cursor")
        else:
            command.add_argument("--timeout", type=float, help="maximum seconds to observe")

    status = sub.add_parser("status", help="show state size and project ID")
    project_args(status)
    status.add_argument("--json", action="store_true")
    doctor = sub.add_parser("doctor", help="check private state and logs")
    project_args(doctor)
    doctor.add_argument("--json", action="store_true")
    return parser


def _render_message(data: dict[str, Any], as_json: bool) -> None:
    # Bus text is always a pointer and explicitly non-authoritative.
    if as_json:
        print(json.dumps({"status": "NON_AUTHORITATIVE", "message": data}, ensure_ascii=True, sort_keys=True, separators=(",", ":")))
    else:
        print("NON_AUTHORITATIVE " + json.dumps(data, ensure_ascii=True, sort_keys=True, separators=(",", ":")))


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        bus = Bus(args.project, args.state_root)
        if args.command == "init":
            bus.init()
            print(f"initialized {bus.project_id} at {bus.state_dir}")
            return 0
        if args.command == "send":
            optional = {key: getattr(args, key) for key in ("head", "verdict", "ledger", "note") if getattr(args, key) is not None}
            message = bus.send(args.from_peer, args.to_peer, args.kind, args.ref, **optional)
            _render_message(message.as_dict(), args.json)
            return 0
        if args.command in ("inbox", "watch"):
            if args.command == "inbox":
                messages, issues = bus.inbox(args.to_peer, args.consumer, peek=args.peek)
            else:
                messages, issues = bus.watch(args.to_peer, args.consumer, timeout=args.timeout)
            for message in messages:
                _render_message(message.as_dict(), args.json)
            for issue in issues:
                if args.json:
                    print(json.dumps({"status": "NON_AUTHORITATIVE", "issue": issue}, ensure_ascii=True, sort_keys=True, separators=(",", ":")))
                else:
                    print(f"NON_AUTHORITATIVE issue: {issue}", file=sys.stderr)
            return 2 if issues else 0
        if args.command == "status":
            result = bus.status()
            if args.json:
                print(json.dumps(result, ensure_ascii=True, sort_keys=True, separators=(",", ":")))
            else:
                print(f"project_id={result['project_id']} state_dir={result['state_dir']} total_bytes={result['total_bytes']}")
                if result["warning"]:
                    print(f"warning: {result['warning']}", file=sys.stderr)
            return 0
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

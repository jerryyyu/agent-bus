from __future__ import annotations

import json
import io
import multiprocessing
import os
import sys
import tempfile
import threading
import time
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from agent_bus import Bus, BusError, project_id
from agent_bus.cli import main as cli_main


def _send_one(args: tuple[str, str, str]) -> int:
    state, project, ref = args
    return Bus(project, state).send("codex", "claude", "fyi", ref).sequence


class BusTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.state = root / "state"
        self.project = root / "project"
        self.other = root / "other"
        self.project.mkdir()
        self.other.mkdir()
        self.bus = Bus(self.project, self.state)
        self.bus.init()

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_project_isolation_and_stable_resolved_id(self) -> None:
        alias = self.project.parent / "alias"
        alias.symlink_to(self.project, target_is_directory=True)
        self.assertEqual(project_id(self.project), project_id(alias))
        other = Bus(self.other, self.state)
        other.init()
        self.assertNotEqual(self.bus.project_id, other.project_id)
        self.bus.send("codex", "claude", "fyi", "only-project")
        self.assertEqual(other.inbox("claude", "reader"), ([], []))
        self.assertEqual(len(self.bus.inbox("claude", "reader")[0]), 1)
        # A third agent has its own recipient log and independent cursor namespace.
        self.bus.send("codex", "muse", "fyi", "third-agent")
        self.bus.send("qwen-local", "muse", "task-ready", "fourth-agent")
        messages, issues = self.bus.inbox("muse", "muse-monitor")
        self.assertFalse(issues)
        self.assertEqual(
            [(message.from_peer, message.ref) for message in messages],
            [("codex", "third-agent"), ("qwen-local", "fourth-agent")])
        self.assertEqual(self.bus.inbox("muse", "muse-monitor"), ([], []))

    def test_concurrent_process_sends_are_ordered_and_intact(self) -> None:
        count = 24
        ctx = multiprocessing.get_context("fork")
        with ctx.Pool(6) as pool:
            sequences = pool.map(_send_one, [(str(self.state), str(self.project), f"r-{i}") for i in range(count)])
        self.assertEqual(sorted(sequences), list(range(1, count + 1)))
        messages, issues = self.bus.inbox("claude", "parallel")
        self.assertFalse(issues)
        self.assertEqual([m.sequence for m in messages], list(range(1, count + 1)))
        self.assertEqual({m.ref for m in messages}, {f"r-{i}" for i in range(count)})

    def test_cursor_peek_idempotence_and_separate_consumers(self) -> None:
        self.bus.send("codex", "claude", "fyi", "x")
        first, _ = self.bus.inbox("claude", "a", peek=True)
        again, _ = self.bus.inbox("claude", "a", peek=True)
        self.assertEqual(first, again)
        consumed, _ = self.bus.inbox("claude", "a")
        self.assertEqual(len(consumed), 1)
        self.assertEqual(self.bus.inbox("claude", "a"), ([], []))
        other, _ = self.bus.inbox("claude", "b")
        self.assertEqual(len(other), 1)

    def test_malformed_trailing_line_reported_without_cursor_advance(self) -> None:
        self.bus.send("codex", "claude", "fyi", "good")
        log = self.bus.state_dir / "inboxes" / "claude.jsonl"
        with log.open("ab") as stream:
            stream.write(b'{"sequence":999')
        messages, issues = self.bus.inbox("claude", "reader")
        self.assertEqual(len(messages), 1)
        self.assertTrue(issues)
        self.assertFalse(self.bus._cursor_path("claude", "reader").exists())
        self.assertTrue(self.bus.inbox("claude", "reader", peek=True)[1])

    def test_cursor_keys_do_not_collide_for_dotted_safe_names(self) -> None:
        first_recipient, first_consumer = "a", "b.jsonl.c"
        second_recipient, second_consumer = "a.jsonl.b", "c"
        self.assertNotEqual(
            self.bus._cursor_path(first_recipient, first_consumer),
            self.bus._cursor_path(second_recipient, second_consumer))
        self.bus.send("source", first_recipient, "fyi", "first")
        self.bus.send("source", second_recipient, "fyi", "second")
        self.assertEqual(
            [message.ref for message in self.bus.inbox(
                first_recipient, first_consumer)[0]], ["first"])
        self.assertEqual(
            [message.ref for message in self.bus.inbox(
                second_recipient, second_consumer)[0]], ["second"])

    def test_refuses_symlink_and_permissive_log(self) -> None:
        self.bus.send("codex", "claude", "fyi", "existing")
        log = self.bus.state_dir / "inboxes" / "claude.jsonl"
        saved = log.read_bytes()
        log.unlink()
        target = self.state / "target"
        target.write_bytes(saved)
        log.symlink_to(target)
        with self.assertRaises(BusError):
            self.bus.send("codex", "claude", "fyi", "no")
        log.unlink()
        log.write_bytes(saved)
        log.chmod(0o644)
        with self.assertRaises(BusError):
            self.bus.send("codex", "claude", "fyi", "no")

    def test_hash_tamper_and_forbidden_fields(self) -> None:
        self.bus.send("codex", "claude", "fyi", "x")
        log = self.bus.state_dir / "inboxes" / "claude.jsonl"
        raw = log.read_bytes()
        log.write_bytes(raw.replace(b'"x"', b'"y"', 1))
        messages, issues = self.bus.inbox("claude", "tamper")
        self.assertEqual(messages, [])
        self.assertTrue(any("hash" in issue for issue in issues))
        log.write_bytes(raw)
        external = json.loads(raw)
        external["execute"] = "echo unsafe"
        with log.open("ab") as stream:
            stream.write(json.dumps(external).encode("ascii") + b"\n")
        _, issues = self.bus.inbox("claude", "forbidden")
        self.assertTrue(any("forbidden" in issue for issue in issues))

    def test_invalid_new_message_and_malformed_existing_log_never_append(self) -> None:
        self.bus.send("codex", "claude", "fyi", "valid")
        log = self.bus.state_dir / "inboxes" / "claude.jsonl"
        before = log.read_bytes()
        with self.assertRaisesRegex(BusError, "invalid message ref"):
            self.bus.send("codex", "claude", "fyi", "x" * 8193)
        with self.assertRaisesRegex(BusError, "invalid message note"):
            self.bus.send("codex", "claude", "fyi", "valid", note="x" * 8193)
        with self.assertRaisesRegex(BusError, "unexpected message fields"):
            self.bus.send("codex", "claude", "fyi", "valid", execute="unsafe")
        self.assertEqual(log.read_bytes(), before)

        log.write_bytes(before + b'{"torn":')
        malformed = log.read_bytes()
        with self.assertRaisesRegex(BusError, "existing inbox is malformed"):
            self.bus.send("qwen", "claude", "result-ready", "unreachable")
        self.assertEqual(log.read_bytes(), malformed)

    def test_sequence_gap_is_detected_by_reader_sender_and_doctor(self) -> None:
        self.bus.send("codex", "claude", "fyi", "one")
        self.bus.send("qwen", "claude", "fyi", "two")
        log = self.bus.state_dir / "inboxes" / "claude.jsonl"
        lines = log.read_bytes().splitlines(keepends=True)
        log.write_bytes(lines[1])
        messages, issues = self.bus.inbox("claude", "gap")
        self.assertEqual(messages, [])
        self.assertTrue(any("non-contiguous" in issue for issue in issues))
        with self.assertRaisesRegex(BusError, "non-contiguous"):
            self.bus.send("terra", "claude", "fyi", "three")
        self.assertTrue(any("non-contiguous" in finding
                            for finding in self.bus.doctor()))

    def test_watch_observes_append_and_timeout_is_bounded(self) -> None:
        def append() -> None:
            time.sleep(0.08)
            self.bus.send("codex", "claude", "fyi", "watched")

        thread = threading.Thread(target=append)
        thread.start()
        started = time.monotonic()
        messages, issues = self.bus.watch("claude", "watcher", timeout=1)
        elapsed = time.monotonic() - started
        thread.join()
        self.assertFalse(issues)
        self.assertEqual([m.ref for m in messages], ["watched"])
        self.assertLess(elapsed, 1)
        started = time.monotonic()
        self.assertEqual(self.bus.watch("quiet-target", "quiet", timeout=0.12), ([], []))
        self.assertLess(time.monotonic() - started, 0.5)
        for timeout in (float("nan"), float("inf"), -1, True):
            with self.assertRaisesRegex(BusError, "finite and non-negative"):
                self.bus.watch("quiet-target", "quiet", timeout=timeout)
        for interval in (0, float("nan"), float("inf"), True):
            with self.assertRaisesRegex(BusError, "finite and positive"):
                self.bus.watch(
                    "quiet-target", "quiet", timeout=0,
                    poll_interval=interval)

    def test_cli_round_trip_is_generic_and_explicitly_nonauthoritative(self) -> None:
        state = self.state.parent / "cli-state"

        def invoke(args: list[str]) -> tuple[int, str, str]:
            stdout, stderr = io.StringIO(), io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                code = cli_main(args)
            return code, stdout.getvalue(), stderr.getvalue()

        common = ["--project", str(self.project), "--state-root", str(state)]
        self.assertEqual(invoke(["init", *common])[0], 0)
        code, output, error = invoke([
            "send", *common, "--from", "deepseek-local", "--to", "muse",
            "--kind", "task-ready", "--ref", "artifact://plan", "--json"])
        self.assertEqual((code, error), (0, ""))
        rendered = json.loads(output)
        self.assertEqual(rendered["status"], "NON_AUTHORITATIVE")
        self.assertEqual(rendered["message"]["from"], "deepseek-local")
        code, output, error = invoke([
            "inbox", *common, "--to", "muse", "--consumer", "harness",
            "--json"])
        self.assertEqual((code, error), (0, ""))
        rendered = json.loads(output)
        self.assertEqual(rendered["status"], "NON_AUTHORITATIVE")
        self.assertEqual(rendered["message"]["ref"], "artifact://plan")
        self.assertEqual(invoke(["doctor", *common])[1].strip(), "ok")


if __name__ == "__main__":
    unittest.main()

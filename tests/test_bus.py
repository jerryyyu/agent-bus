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
from agent_bus.core import SCHEMA, _canonical, _hash_body


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

    def test_exact_ack_leaves_messages_appended_after_peek_pending(self) -> None:
        for ref in ("one", "two", "three"):
            self.bus.send("codex", "claude", "fyi", ref)
        peeked, issues = self.bus.inbox("claude", "adapter", peek=True)
        self.assertFalse(issues)
        self.assertEqual([message.sequence for message in peeked], [1, 2, 3])
        self.bus.send("codex", "claude", "fyi", "after-peek")
        ack = self.bus.ack("claude", "adapter", through_sequence=3)
        self.assertEqual(ack["acked_sequence"], 3)
        self.assertEqual(ack["pending_count"], 1)
        pending, issues = self.bus.inbox("claude", "adapter", peek=True)
        self.assertFalse(issues)
        self.assertEqual([(message.sequence, message.ref) for message in pending],
                         [(4, "after-peek")])
        self.assertEqual(
            self.bus.ack("claude", "adapter", through_sequence=3), ack)
        with self.assertRaisesRegex(BusError, "behind cursor"):
            self.bus.ack("claude", "adapter", through_sequence=2)
        with self.assertRaisesRegex(BusError, "not present"):
            self.bus.ack("claude", "adapter", through_sequence=5)

    def test_batch_ack_is_bound_to_peek_and_leaves_later_messages_pending(self) -> None:
        for ref in ("one", "two", "three"):
            self.bus.send("codex", "claude", "fyi", ref)
        messages, issues, batch = self.bus.peek_batch("claude", "batch-reader")
        self.assertFalse(issues)
        self.assertIsNotNone(batch)
        assert batch is not None
        self.assertEqual([message.sequence for message in messages], [1, 2, 3])
        self.assertEqual((batch.start_sequence, batch.end_sequence,
                          batch.message_count), (0, 3, 3))
        # A later append is not part of the token and must remain pending.
        self.bus.send("codex", "claude", "fyi", "after-peek")
        result = self.bus.ack_batch(
            "claude", "batch-reader", token=batch.token)
        self.assertEqual(result["acked_sequence"], 3)
        self.assertEqual(result["pending_count"], 1)
        pending, issues = self.bus.inbox(
            "claude", "batch-reader", peek=True)
        self.assertFalse(issues)
        self.assertEqual([(message.sequence, message.ref)
                          for message in pending], [(4, "after-peek")])
        with self.assertRaisesRegex(BusError, "cursor changed"):
            self.bus.ack_batch("claude", "batch-reader", token=batch.token)
        with self.assertRaisesRegex(BusError, "recipient or consumer mismatch"):
            self.bus.ack_batch("claude", "other-reader", token=batch.token)

    def test_batch_ack_refuses_token_tamper_and_log_rotation(self) -> None:
        self.bus.send("codex", "claude", "ask-ready", "review")
        _messages, _issues, batch = self.bus.peek_batch("claude", "rotated")
        assert batch is not None
        changed = batch.token[:-1] + ("A" if batch.token[-1] != "A" else "B")
        with self.assertRaisesRegex(BusError, "batch token"):
            self.bus.ack_batch("claude", "rotated", token=changed)

        log = self.bus.state_dir / "inboxes" / "claude.jsonl"
        replacement = log.with_suffix(".replacement")
        replacement.write_bytes(log.read_bytes())
        replacement.chmod(0o600)
        os.replace(replacement, log)
        with self.assertRaisesRegex(BusError, "rotated"):
            self.bus.ack_batch("claude", "rotated", token=batch.token)

    def test_actionable_view_is_conservative_compact_and_cursor_neutral(self) -> None:
        self.bus.send("codex", "claude", "ask-ready", "ack-must-not-close")
        self.bus.send(
            "terra", "claude", "ack", "ack-must-not-close",
            reply_to="claude:1")
        self.bus.send("codex", "claude", "ask-ready", "old-request")
        self.bus.send(
            "codex", "claude", "ask-ready", "new-request",
            head="abc123", supersedes="claude:3")
        self.bus.send("codex", "claude", "ask-ready", "withdraw-me")
        self.bus.send(
            "codex", "claude", "ask-withdrawn", "withdraw-me",
            reply_to="claude:5")
        self.bus.send("codex", "claude", "ask-ready", "answer-me")
        self.bus.send(
            "terra", "claude", "verdict", "answer-me",
            verdict="PASS", reply_to="claude:7")
        self.bus.send("codex", "claude", "run-started", "run/one")
        self.bus.send("codex", "claude", "run-ended", "run/one")
        self.bus.send("codex", "claude", "status", "routine")
        self.bus.send("luna", "claude", "task-ready", "still-open")

        first, issues, batch = self.bus.actionable_inbox(
            "claude", "action-reader")
        second, second_issues, second_batch = self.bus.actionable_inbox(
            "claude", "action-reader")
        self.assertEqual((first, issues, batch),
                         (second, second_issues, second_batch))
        self.assertFalse(issues)
        self.assertIsNotNone(batch)
        self.assertFalse(
            self.bus._cursor_path("claude", "action-reader").exists())
        self.assertEqual(
            [(item.kind, item.ref, item.head, item.newest_sequence,
              item.collapsed_transition_count, item.sequence_anchors)
             for item in first],
            [
                ("ask-ready", "ack-must-not-close", None, 1, 0,
                 ("claude:1",)),
                ("ask-ready", "new-request", "abc123", 4, 1,
                 ("claude:3", "claude:4")),
                ("task-ready", "still-open", None, 12, 0,
                 ("claude:12",)),
            ])

    def test_actionable_reports_malformed_input_without_batch_or_cursor(self) -> None:
        self.bus.send("codex", "claude", "ask-ready", "good")
        log = self.bus.state_dir / "inboxes" / "claude.jsonl"
        with log.open("ab") as stream:
            stream.write(b'{"sequence":999')
        items, issues, batch = self.bus.actionable_inbox(
            "claude", "malformed-action")
        self.assertEqual([item.ref for item in items], ["good"])
        self.assertTrue(issues)
        self.assertIsNone(batch)
        self.assertFalse(
            self.bus._cursor_path("claude", "malformed-action").exists())

    def test_actionable_retains_unmatched_incoming_terminal_signal(self) -> None:
        self.bus.send("codex", "claude", "run-started", "run/earlier")
        self.bus.ack("claude", "terminal-reader", through_sequence=1)
        self.bus.send("codex", "claude", "run-ended", "run/earlier")
        items, issues, batch = self.bus.actionable_inbox(
            "claude", "terminal-reader")
        self.assertFalse(issues)
        self.assertIsNotNone(batch)
        self.assertEqual(
            [(item.kind, item.ref, item.sequence_anchors) for item in items],
            [("run-ended", "run/earlier", ("claude:2",))])

    def test_consumer_status_reports_exact_lag_without_advancing(self) -> None:
        self.bus.send("codex", "claude", "fyi", "one")
        self.bus.send("codex", "claude", "fyi", "two")
        before = self.bus.consumer_status("claude", "status-reader")
        self.assertEqual(before["acked_sequence"], 0)
        self.assertEqual(before["pending_count"], 2)
        self.assertEqual(before["first_pending_sequence"], 1)
        self.bus.ack("claude", "status-reader", through_sequence=1)
        after = self.bus.consumer_status("claude", "status-reader")
        self.assertEqual(after["acked_sequence"], 1)
        self.assertEqual(after["last_sequence"], 2)
        self.assertEqual(after["pending_count"], 1)
        self.assertEqual(after["first_pending_sequence"], 2)

    def test_v1_log_reopens_and_v2_links_are_hash_bound(self) -> None:
        body = {
            "schema": SCHEMA, "version": 1, "sequence": 1,
            "ts": "2026-09-01T00:00:00.000000Z", "from": "codex",
            "to": "claude", "kind": "ask-ready", "ref": "legacy",
        }
        body["message_sha256"] = _hash_body(body)
        log = self.bus.state_dir / "inboxes" / "claude.jsonl"
        log.write_bytes(_canonical(body) + b"\n")
        log.chmod(0o600)
        current = self.bus.send(
            "claude", "claude-review", "verdict", "review/current",
            reply_to="claude:1", supersedes="codex:9")
        # The same recipient log may transition from legacy v1 to v2.
        appended = self.bus.send(
            "terra", "claude", "ruling", "current",
            reply_to="claude:1", supersedes="claude:1")
        messages, issues = self.bus.inbox("claude", "mixed-reader")
        self.assertFalse(issues)
        self.assertEqual([message.version for message in messages], [1, 2])
        self.assertEqual(messages[1].reply_to, "claude:1")
        self.assertEqual(messages[1].supersedes, "claude:1")
        self.assertEqual(current.version, 2)
        with self.assertRaisesRegex(BusError, "invalid message reply_to"):
            self.bus.send("codex", "claude", "fyi", "bad",
                          reply_to="not-a-link")

    def test_send_once_suppresses_only_a_recent_semantic_duplicate(self) -> None:
        first = self.bus.send(
            "codex", "claude", "status", "run/one", once=True,
            note="still-running")
        duplicate = self.bus.send(
            "codex", "claude", "status", "run/one", once=True,
            note="still-running")
        self.assertEqual(duplicate.sequence, first.sequence)
        self.assertTrue(duplicate.duplicate_suppressed)
        messages, issues = self.bus.inbox("claude", "dedupe", peek=True)
        self.assertFalse(issues)
        self.assertEqual(len(messages), 1)
        intentional = self.bus.send(
            "codex", "claude", "status", "run/one",
            note="still-running")
        self.assertEqual(intentional.sequence, 2)
        self.assertFalse(intentional.duplicate_suppressed)
        changed = self.bus.send(
            "codex", "claude", "status", "run/one", once=True,
            note="finished")
        self.assertEqual(changed.sequence, 3)

    def test_causal_metadata_surfaces_stale_premise_at_send_time(self) -> None:
        self.bus.send("claude", "codex", "ruling", "new-ruling")
        stale = self.bus.send(
            "codex", "claude", "ask-withdrawn", "old-premise",
            seen_peer_sequence=0)
        self.assertEqual(stale.seen_peer_sequence, 0)
        self.assertEqual(stale.peer_sequence_at_send, 1)
        self.assertTrue(stale.stale_premise)
        current = self.bus.send(
            "codex", "claude", "ack", "new-ruling",
            seen_peer_sequence=1, reply_to="claude:1")
        self.assertFalse(current.stale_premise)
        with self.assertRaisesRegex(BusError, "not present"):
            self.bus.send(
                "codex", "claude", "fyi", "future",
                seen_peer_sequence=2)
        reopened, issues = self.bus.inbox("claude", "causal", peek=True)
        self.assertFalse(issues)
        self.assertTrue(reopened[0].stale_premise)
        self.assertFalse(reopened[1].stale_premise)

    def test_merged_log_is_chronological_and_cursor_free(self) -> None:
        self.bus.send("codex", "claude", "ask-ready", "first")
        time.sleep(0.001)
        self.bus.send("claude", "codex", "verdict", "second")
        messages, issues = self.bus.log()
        self.assertFalse(issues)
        self.assertEqual([message.ref for message in messages],
                         ["first", "second"])
        self.assertFalse(any((self.bus.state_dir / "cursors").iterdir()))

    def test_cli_log_follow_observes_append_without_cursor(self) -> None:
        def append() -> None:
            time.sleep(0.04)
            self.bus.send("luna", "codex", "result-ready", "late-result")

        thread = threading.Thread(target=append)
        thread.start()
        stdout, stderr = io.StringIO(), io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            code = cli_main([
                "log", "--project", str(self.project),
                "--state-root", str(self.state), "--follow",
                "--timeout", "0.12", "--poll-interval", "0.01"])
        thread.join()
        self.assertEqual((code, stderr.getvalue()), (0, ""))
        self.assertIn("luna->codex result-ready late-result", stdout.getvalue())
        self.assertFalse(any((self.bus.state_dir / "cursors").iterdir()))

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
        messages, issues = self.bus.watch(
            "claude", "watcher", timeout=1, poll_interval=0.02)
        elapsed = time.monotonic() - started
        thread.join()
        self.assertFalse(issues)
        self.assertEqual([m.ref for m in messages], ["watched"])
        self.assertLess(elapsed, 1)
        started = time.monotonic()
        self.assertEqual(self.bus.watch(
            "quiet-target", "quiet", timeout=0.12,
            poll_interval=0.02), ([], []))
        self.assertLess(time.monotonic() - started, 0.5)
        for timeout in (float("nan"), float("inf"), -1, True):
            with self.assertRaisesRegex(BusError, "finite and non-negative"):
                self.bus.watch("quiet-target", "quiet", timeout=timeout)
        for interval in (0, float("nan"), float("inf"), True):
            with self.assertRaisesRegex(BusError, "finite and positive"):
                self.bus.watch(
                    "quiet-target", "quiet", timeout=0,
                    poll_interval=interval)

    def test_actionable_watch_ignores_chatter_and_is_cursor_neutral(self) -> None:
        def append() -> None:
            time.sleep(0.04)
            self.bus.send("codex", "claude", "status", "routine")
            time.sleep(0.04)
            self.bus.send("codex", "claude", "ask-ready", "needs-review")

        thread = threading.Thread(target=append)
        thread.start()
        items, issues, batch, changed = self.bus.watch_actionable(
            "claude", "action-watch", timeout=1, poll_interval=0.01)
        thread.join()
        self.assertTrue(changed)
        self.assertFalse(issues)
        self.assertEqual([item.ref for item in items], ["needs-review"])
        self.assertIsNotNone(batch)
        self.assertFalse(
            self.bus._cursor_path("claude", "action-watch").exists())

        # A new watcher baselines the existing ask. More chatter does not
        # produce a change or wake event.
        def chatter() -> None:
            time.sleep(0.03)
            self.bus.send(
                "terra", "claude", "ack", "needs-review",
                reply_to="claude:2")

        thread = threading.Thread(target=chatter)
        thread.start()
        items, issues, batch, changed = self.bus.watch_actionable(
            "claude", "action-watch", timeout=0.12, poll_interval=0.01)
        thread.join()
        self.assertEqual((items, issues, batch, changed), ([], [], None, False))

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
        code, output, error = invoke([
            "status", *common, "--to", "muse", "--consumer", "harness",
            "--json"])
        self.assertEqual((code, error), (0, ""))
        self.assertEqual(json.loads(output)["consumer"]["pending_count"], 0)
        code, output, error = invoke([
            "send", *common, "--from", "codex", "--to", "muse",
            "--kind", "fyi", "--ref", "artifact://followup",
            "--reply-to", "muse:1", "--supersedes", "muse:1", "--json"])
        self.assertEqual((code, error), (0, ""))
        self.assertEqual(json.loads(output)["message"]["reply_to"], "muse:1")
        code, output, error = invoke([
            "send", *common, "--from", "codex", "--to", "muse",
            "--kind", "fyi", "--ref", "artifact://followup",
            "--reply-to", "muse:1", "--supersedes", "muse:1",
            "--once", "--json"])
        self.assertEqual((code, error), (0, ""))
        self.assertTrue(json.loads(output)["annotations"][
            "duplicate_suppressed"])
        code, output, error = invoke([
            "log", *common, "--json"])
        self.assertEqual((code, error), (0, ""))
        self.assertEqual(len(output.splitlines()), 2)
        code, output, error = invoke([
            "ack", *common, "--to", "muse", "--consumer", "harness",
            "--through", "2", "--json"])
        self.assertEqual((code, error), (0, ""))
        self.assertEqual(json.loads(output)["ack"]["acked_sequence"], 2)
        self.assertEqual(invoke(["doctor", *common])[1].strip(), "ok")

    def test_cli_batch_and_actionable_round_trip(self) -> None:
        state = self.state.parent / "cli-batch-state"

        def invoke(args: list[str]) -> tuple[int, str, str]:
            stdout, stderr = io.StringIO(), io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                code = cli_main(args)
            return code, stdout.getvalue(), stderr.getvalue()

        common = ["--project", str(self.project), "--state-root", str(state)]
        self.assertEqual(invoke(["init", *common])[0], 0)
        self.assertEqual(invoke([
            "send", *common, "--from", "codex", "--to", "claude",
            "--kind", "ask-ready", "--ref", "review/one"])[0], 0)
        code, output, error = invoke([
            "inbox", *common, "--to", "claude", "--consumer", "model",
            "--actionable", "--json"])
        self.assertEqual((code, error), (0, ""))
        actionable = json.loads(output)
        self.assertEqual(actionable["status"], "NON_AUTHORITATIVE")
        self.assertEqual(actionable["actionable"][0]["ref"], "review/one")
        token = actionable["batch"]["token"]
        code, output, error = invoke([
            "ack", *common, "--to", "claude", "--consumer", "model",
            "--batch", token, "--json"])
        self.assertEqual((code, error), (0, ""))
        self.assertEqual(json.loads(output)["ack"]["acked_sequence"], 1)

        self.assertEqual(invoke([
            "send", *common, "--from", "codex", "--to", "claude",
            "--kind", "fyi", "--ref", "raw/two"])[0], 0)
        code, output, error = invoke([
            "inbox", *common, "--to", "claude", "--consumer", "model",
            "--peek", "--batch", "--json"])
        self.assertEqual((code, error), (0, ""))
        envelope = json.loads(output)
        self.assertEqual(envelope["messages"][0]["ref"], "raw/two")
        self.assertEqual(envelope["batch"]["start_sequence"], 1)
        self.assertEqual(envelope["batch"]["end_sequence"], 2)


if __name__ == "__main__":
    unittest.main()

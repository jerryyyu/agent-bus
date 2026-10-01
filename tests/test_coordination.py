"""Coordination fields introduced in 0.4.0 (schema version 3)."""

from __future__ import annotations

import io
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from agent_bus import Bus, BusError
from agent_bus.cli import main as cli_main
from agent_bus.core import (COORDINATION_VERSION, LEGACY_TEXT, OPTIONAL_LINKS,
                            OPTIONAL_INTEGERS, REQUIRED, VERSION,
                            annotate_stale, same_thread, thread_key)

A = "a" * 40
B = "b" * 40
C = "c" * 40
PR = "https://github.com/owner/repo/pull/680"
OTHER_REPO_DEP = "https://github.com/owner/other/pull/670"
PR_COMMENT = PR + "#issuecomment-123"
DEP = "https://github.com/owner/repo/pull/670"
DEP_COMMENT = DEP + "#issuecomment-9"


def invoke(args: list[str]) -> tuple[int, str, str]:
    stdout, stderr = io.StringIO(), io.StringIO()
    with redirect_stdout(stdout), redirect_stderr(stderr):
        code = cli_main(args)
    return code, stdout.getvalue(), stderr.getvalue()


class CoordinationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.state = root / "state"
        self.project = root / "project"
        self.project.mkdir()
        self.bus = Bus(self.project, self.state)
        self.bus.init()
        self.common = ["--project", str(self.project),
                       "--state-root", str(self.state)]
        self.saved_env = os.environ.pop("AGENT_BUS_STRICT", None)
        # Both agents have consumed their inboxes with this version, which
        # is what lets a sender append version 3 lines to them.
        for peer in ("claude", "codex"):
            self.bus.inbox(peer, "setup")

    def tearDown(self) -> None:
        if self.saved_env is not None:
            os.environ["AGENT_BUS_STRICT"] = self.saved_env
        self.temp.cleanup()

    # -- threads and keys -------------------------------------------------

    def test_thread_key_collapses_comment_anchors_and_numbers(self) -> None:
        self.assertEqual(thread_key(PR_COMMENT), PR)
        self.assertEqual(thread_key("RL_PLAN.md tip 1234#frag"),
                         "RL_PLAN.md tip 1234")
        self.assertTrue(same_thread("680", PR_COMMENT))
        self.assertTrue(same_thread("#680", PR))
        self.assertFalse(same_thread("68", PR))
        self.assertTrue(same_thread(PR_COMMENT, PR))
        self.assertFalse(same_thread(DEP, PR))

    # -- round trips ------------------------------------------------------

    def test_head_moved_round_trips_and_threads(self) -> None:
        code, output, error = invoke([
            "send", *self.common, "--from", "codex", "--to", "claude",
            "--kind", "head-moved", "--ref", PR, "--head", B,
            "--prev-head", A, "--reason", "rebased after ledger freeze",
            "--json"])
        self.assertEqual((code, error), (0, ""))
        rendered = json.loads(output)["message"]
        self.assertEqual(rendered["version"], COORDINATION_VERSION)
        self.assertEqual((rendered["head"], rendered["prev_head"],
                          rendered["reason"]),
                         (B, A, "rebased after ledger freeze"))
        messages, issues = self.bus.inbox("claude", "reader", peek=True)
        self.assertFalse(issues)
        self.assertEqual((messages[0].kind, messages[0].prev_head), ("head-moved", A))
        code, output, error = invoke(["log", *self.common, "--ref", "680"])
        self.assertEqual((code, error), (0, ""))
        self.assertIn(f"head-moved {PR} head={B} prev_head={A} "
                      "reason=rebased after ledger freeze", output)
        with self.assertRaisesRegex(BusError, "40-hex prev_head"):
            self.bus.send("codex", "claude", "head-moved", PR, head=B)
        with self.assertRaisesRegex(BusError, "head equals prev_head"):
            self.bus.send("codex", "claude", "head-moved", PR, head=B,
                          prev_head=B)

    def test_host_round_trips_as_one_line_with_validated_action(self) -> None:
        code, output, error = invoke([
            "send", *self.common, "--from", "claude", "--to", "codex",
            "--kind", "host", "--ref", "fl-pilot/lane-v35c", "--host", "perf",
            "--action", "released", "--pid", "4242", "--pgid", "4242",
            "--lock", "/tmp/perf.lock", "--count", "5",
            "--rearm", "nohup ./rearm.sh", "--json"])
        self.assertEqual((code, error), (0, ""))
        rendered = json.loads(output)["message"]
        self.assertEqual(rendered["version"], COORDINATION_VERSION)
        self.assertEqual((rendered["host"], rendered["action"], rendered["pid"],
                          rendered["pgid"], rendered["lock"], rendered["count"],
                          rendered["rearm"]),
                         ("perf", "released", 4242, 4242, "/tmp/perf.lock", 5,
                          "nohup ./rearm.sh"))
        code, output, error = invoke(["log", *self.common])
        self.assertEqual((code, error), (0, ""))
        lines = [line for line in output.splitlines() if "host" in line]
        self.assertEqual(len(lines), 1)
        self.assertIn("host=perf action=released pid=4242 pgid=4242 count=5 "
                      "lock=/tmp/perf.lock rearm=nohup ./rearm.sh", lines[0])
        with self.assertRaisesRegex(BusError, "host action must be one of"):
            self.bus.send("claude", "codex", "host", "x", host="perf",
                          action="exploded")
        with self.assertRaisesRegex(BusError, "requires a host name"):
            self.bus.send("claude", "codex", "host", "x", action="armed")
        with self.assertRaisesRegex(BusError, "invalid message pid"):
            self.bus.send("claude", "codex", "host", "x", host="perf",
                          action="armed", pid=-1)
        self.assertEqual(invoke([
            "send", *self.common, "--from", "claude", "--to", "codex",
            "--kind", "host", "--ref", "x", "--host", "perf",
            "--action", "started", "--json"])[0], 0)

    def test_rc_and_receipt_round_trip_on_run_ended(self) -> None:
        code, output, error = invoke([
            "send", *self.common, "--from", "codex", "--to", "claude",
            "--kind", "run-ended", "--ref", "runs/v35c", "--rc", "1",
            "--receipt", "runs/v35c/receipt.json", "--json"])
        self.assertEqual((code, error), (0, ""))
        rendered = json.loads(output)["message"]
        self.assertEqual((rendered["rc"], rendered["receipt"]),
                         (1, "runs/v35c/receipt.json"))
        items, issues, _batch = self.bus.actionable_inbox("claude", "r")
        self.assertFalse(issues)
        self.assertEqual(items[0].detail,
                         {"rc": 1, "receipt": "runs/v35c/receipt.json"})
        code, output, _error = invoke(["log", *self.common])
        self.assertIn("run-ended runs/v35c rc=1 receipt=runs/v35c/receipt.json",
                      output)
        negative = self.bus.send("codex", "claude", "result-ready", "r",
                                 rc=-9)
        self.assertEqual(negative.rc, -9)

    def test_decision_requires_a_comment_url_even_without_strict(self) -> None:
        with self.assertRaisesRegex(BusError, "decision requires --ref"):
            self.bus.send("claude", "codex", "decision", "Jerry said yes")
        with self.assertRaisesRegex(BusError, "decision requires --ref"):
            self.bus.send("claude", "codex", "decision",
                          "HANDOFF_ACTIVE.md tip abc")
        anchored = self.bus.send(
            "claude", "codex", "decision", PR_COMMENT,
            note="Jerry: merge after the fresh-seed read")
        self.assertEqual((anchored.version, anchored.warnings),
                         (COORDINATION_VERSION, ()))
        unanchored = self.bus.send("claude", "codex", "decision", PR)
        self.assertTrue(any("comment URL" in w for w in unanchored.warnings))
        with self.assertRaisesRegex(BusError, "strict: decision ref"):
            self.bus.send("claude", "codex", "decision", PR, strict=True)
        items, issues, _batch = self.bus.actionable_inbox("codex", "r")
        self.assertFalse(issues)
        self.assertEqual([item.kind for item in items],
                         ["decision", "decision"])

    def test_depends_on_round_trips(self) -> None:
        code, output, error = invoke([
            "send", *self.common, "--from", "codex", "--to", "claude",
            "--kind", "ask-ready", "--ref", PR, "--head", A,
            "--depends-on", "670", "--no-auto-head", "--json"])
        self.assertEqual((code, error), (0, ""))
        self.assertEqual(json.loads(output)["message"]["depends_on"], "670")
        messages, _issues = self.bus.inbox("claude", "r", peek=True)
        self.assertEqual(messages[0].depends_on, "670")
        code, output, _error = invoke(["log", *self.common, "--ref", PR])
        self.assertIn(f"ask-ready {PR} head={A} depends_on=670", output)

    # -- staleness --------------------------------------------------------

    def test_head_moved_makes_an_earlier_verdict_stale_across_inboxes(self) -> None:
        self.bus.send("codex", "claude", "ask-ready", PR, head=A)
        verdict = self.bus.send(
            "claude", "codex", "verdict", PR_COMMENT, verdict="PASS", head=A,
            reply_to="claude:1")
        items, issues, _batch = self.bus.actionable_inbox("codex", "r")
        self.assertFalse(issues)
        self.assertEqual([(item.kind, item.stale_head) for item in items],
                         [("verdict", None)])
        self.bus.send("codex", "claude", "head-moved", PR, head=B,
                      prev_head=A, reason="rebase")
        items, issues, _batch = self.bus.actionable_inbox("codex", "r")
        self.assertFalse(issues)
        self.assertEqual([(item.kind, item.head, item.stale_head)
                          for item in items], [("verdict", A, B)])
        thread, issues = self.bus.thread(PR)
        self.assertFalse(issues)
        self.assertEqual(
            [(m.kind, m.stale_head) for m in thread],
            [("ask-ready", None), ("verdict", B), ("head-moved", None)])
        code, output, error = invoke(["log", *self.common, "--ref", PR])
        self.assertEqual((code, error), (0, ""))
        self.assertIn(f"verdict {PR_COMMENT} verdict=PASS head={A} "
                      f"reply_to=claude:1 STALE(head-moved-to={B})", output)
        code, output, _error = invoke(["log", *self.common, "--ref", PR, "--json"])
        stale = [json.loads(line) for line in output.splitlines()][1]
        self.assertEqual(stale["annotations"]["stale_verdict"],
                         {"head_moved_to": B})
        # A verdict at the moved-to head is current again.
        self.bus.send("claude", "codex", "verdict", PR_COMMENT, verdict="PASS",
                      head=B)
        thread, _issues = self.bus.thread(PR)
        self.assertEqual([m.stale_head for m in thread if m.kind == "verdict"],
                         [B, None])
        self.assertEqual(verdict.stale_head, None)

    def test_unheaded_verdict_is_stale_after_head_moved_but_not_after_a_nudge(self) -> None:
        self.bus.send("claude", "codex", "verdict", PR, verdict="HOLD")
        self.bus.send("codex", "claude", "ask-ready", PR, head=A)
        self.bus.send("codex", "claude", "ask-ready", PR, head=A)
        thread, _issues = self.bus.thread(PR)
        self.assertEqual(thread[0].stale_head, None)
        self.bus.send("codex", "claude", "head-moved", PR, head=B, prev_head=A)
        thread, _issues = self.bus.thread(PR)
        self.assertEqual(thread[0].stale_head, B)

    def test_re_ask_at_a_different_head_also_stales_a_verdict(self) -> None:
        self.bus.send("claude", "codex", "verdict", PR, verdict="PASS", head=A)
        self.bus.send("codex", "claude", "ask-ready", PR, head=C)
        items, _issues, _batch = self.bus.actionable_inbox("codex", "r")
        self.assertEqual(items[0].stale_head, C)

    def test_head_moved_reheads_the_open_ask_in_the_actionable_view(self) -> None:
        self.bus.send("codex", "claude", "ask-ready", PR, head=A)
        self.bus.send("codex", "claude", "head-moved", PR, head=B, prev_head=A)
        items, issues, _batch = self.bus.actionable_inbox("claude", "r")
        self.assertFalse(issues)
        self.assertEqual(
            [(item.kind, item.head, item.head_moved_from, item.newest_sequence,
              item.collapsed_transition_count, item.sequence_anchors)
             for item in items],
            [("ask-ready", B, A, 2, 1, ("claude:1", "claude:2"))])
        # Without an open ask the move stands on its own as actionable.
        self.bus.send("codex", "claude", "head-moved", DEP, head=B, prev_head=A)
        items, _issues, _batch = self.bus.actionable_inbox("claude", "r")
        self.assertEqual([item.kind for item in items],
                         ["ask-ready", "head-moved"])
        self.assertEqual(items[1].detail["prev_head"], A)

    # -- dependencies -----------------------------------------------------

    def test_blocked_asks_follow_independent_ones_until_a_current_pass(self) -> None:
        self.bus.send("codex", "claude", "ask-ready", PR, head=A,
                      depends_on="670")
        self.bus.send("codex", "claude", "ask-ready", DEP, head=C)
        items, issues, _batch = self.bus.actionable_inbox("claude", "r")
        self.assertFalse(issues)
        self.assertEqual([(item.ref, item.blocked_on) for item in items],
                         [(DEP, None), (PR, "670")])
        code, output, error = invoke([
            "inbox", *self.common, "--to", "claude", "--consumer", "r",
            "--actionable", "--json"])
        self.assertEqual((code, error), (0, ""))
        rendered = json.loads(output)["actionable"]
        self.assertEqual([item["blocked_on"] for item in rendered],
                         [None, "670"])
        # A HOLD on the dependency keeps the ask blocked; a PASS in the
        # other inbox releases it; a head move after the PASS blocks again.
        self.bus.send("claude", "codex", "verdict", DEP_COMMENT, verdict="HOLD",
                      head=C)
        items, _issues, _batch = self.bus.actionable_inbox("claude", "r")
        self.assertEqual(items[1].blocked_on, "670")
        self.bus.send("claude", "codex", "verdict", DEP_COMMENT, verdict="PASS",
                      head=C)
        items, _issues, _batch = self.bus.actionable_inbox("claude", "r")
        self.assertEqual([(item.ref, item.blocked_on) for item in items],
                         [(PR, None), (DEP, None)])
        self.bus.send("codex", "claude", "head-moved", DEP, head=B, prev_head=C)
        items, _issues, _batch = self.bus.actionable_inbox("claude", "r")
        self.assertEqual([(item.ref, item.blocked_on) for item in items],
                         [(DEP, None), (PR, "670")])

    def test_late_pass_at_the_old_head_stays_stale_and_blocks_dependents(self) -> None:
        self.bus.send("codex", "claude", "ask-ready", DEP, head=A)
        self.bus.send("codex", "claude", "ask-ready", PR, head=C,
                      depends_on="670")
        self.bus.send("codex", "claude", "head-moved", DEP, head=B, prev_head=A)
        # A delayed PASS naming the old head arrives after the move.
        self.bus.send("claude", "codex", "verdict", DEP_COMMENT, verdict="PASS",
                      head=A)
        thread, _issues = self.bus.thread(DEP)
        self.assertEqual([m.stale_head for m in thread if m.kind == "verdict"],
                         [B])
        items, _issues, _batch = self.bus.actionable_inbox("claude", "r")
        self.assertEqual([(item.ref, item.head, item.blocked_on) for item in items],
                         [(DEP, B, None), (PR, C, "670")])
        items, _issues, _batch = self.bus.actionable_inbox("codex", "r")
        self.assertEqual([(item.kind, item.stale_head) for item in items],
                         [("verdict", B)])
        # A PASS at the current head is current and releases the dependent.
        self.bus.send("claude", "codex", "verdict", DEP_COMMENT, verdict="PASS",
                      head=B)
        thread, _issues = self.bus.thread(DEP)
        self.assertEqual([m.stale_head for m in thread if m.kind == "verdict"],
                         [B, None])
        items, _issues, _batch = self.bus.actionable_inbox("claude", "r")
        self.assertEqual([(item.ref, item.blocked_on) for item in items],
                         [(PR, None), (DEP, None)])
        # A verdict naming a head the thread was never asked at is not current.
        self.bus.send("claude", "codex", "verdict", PR_COMMENT, verdict="PASS",
                      head=A)
        thread, _issues = self.bus.thread(PR)
        self.assertEqual([m.stale_head for m in thread if m.kind == "verdict"],
                         [C])

    def test_numeric_dependency_resolves_in_the_asking_refs_repository(self) -> None:
        self.bus.send("codex", "claude", "ask-ready", PR, head=A,
                      depends_on="670")
        # A PASS on #670 of another repository does not count.
        self.bus.send("claude", "codex", "verdict",
                      OTHER_REPO_DEP + "#issuecomment-1", verdict="PASS", head=C)
        items, _issues, _batch = self.bus.actionable_inbox("claude", "r")
        self.assertEqual(items[0].blocked_on, "670")
        self.bus.send("claude", "codex", "verdict", DEP_COMMENT, verdict="PASS",
                      head=C)
        items, _issues, _batch = self.bus.actionable_inbox("claude", "r")
        self.assertEqual(items[0].blocked_on, None)
        # A full URL dependency is matched exactly, across repositories.
        self.bus.send("codex", "claude", "ask-ready", "runs/x", head=A,
                      depends_on=OTHER_REPO_DEP)
        items, _issues, _batch = self.bus.actionable_inbox("claude", "r")
        self.assertEqual([item.blocked_on for item in items], [None, None])
        # A bare number has no repository to resolve in when the ref is not
        # a GitHub PR or issue URL.
        with self.assertRaisesRegex(BusError, "numeric --depends-on"):
            self.bus.send("codex", "claude", "ask-ready", "runs/x", head=A,
                          depends_on="670")
        self.assertTrue(same_thread("670", OTHER_REPO_DEP))
        self.assertFalse(same_thread("670", OTHER_REPO_DEP,
                                     repo="https://github.com/owner/repo"))

    def test_dependency_by_url_matches_comment_verdicts(self) -> None:
        self.bus.send("codex", "claude", "ask-ready", PR, head=A,
                      depends_on=DEP)
        self.bus.send("claude", "codex", "verdict", DEP_COMMENT, verdict="PASS",
                      head=C)
        items, _issues, _batch = self.bus.actionable_inbox("claude", "r")
        self.assertEqual(items[0].blocked_on, None)

    # -- hosts ------------------------------------------------------------

    def test_host_lines_collapse_to_the_latest_and_only_handoffs_act(self) -> None:
        self.bus.send("claude", "codex", "host", "lane", host="perf",
                      action="armed")
        self.bus.send("claude", "codex", "host", "lane", host="perf",
                      action="started", pid=7)
        items, _issues, _batch = self.bus.actionable_inbox("codex", "r")
        self.assertEqual(items, [])
        self.bus.send("claude", "codex", "host", "lane", host="perf",
                      action="released", rearm="./rearm.sh")
        self.bus.send("claude", "codex", "host", "lane", host="cloud",
                      action="nominated", count=4)
        items, _issues, _batch = self.bus.actionable_inbox("codex", "r")
        self.assertEqual(
            [(item.detail["host"], item.detail["action"]) for item in items],
            [("perf", "released"), ("cloud", "nominated")])
        self.bus.send("claude", "codex", "host", "lane", host="perf",
                      action="resumed")
        items, _issues, _batch = self.bus.actionable_inbox("codex", "r")
        self.assertEqual([item.detail["host"] for item in items], ["cloud"])

    # -- strict mode and advisories --------------------------------------

    def test_old_style_verdict_still_sends_with_a_warning(self) -> None:
        message = self.bus.send("claude", "codex", "verdict", PR_COMMENT)
        self.assertEqual(message.version, VERSION)
        self.assertEqual(
            message.warnings,
            ("verdict should carry --verdict PASS|HOLD",
             "verdict should carry --head (the reviewed commit)"))
        # Nothing a 0.3.0 reader does not know is in the stored line.
        legacy = REQUIRED | LEGACY_TEXT | OPTIONAL_LINKS | OPTIONAL_INTEGERS
        self.assertTrue(set(message.body()) <= legacy)
        code, output, error = invoke([
            "send", *self.common, "--from", "claude", "--to", "codex",
            "--kind", "verdict", "--ref", PR_COMMENT, "--no-auto-head",
            "--json"])
        self.assertEqual(code, 0)
        self.assertIn("agent-bus: warning: verdict should carry --verdict",
                      error)
        self.assertIn("warning: verdict should carry --head", error)
        rendered = json.loads(output)
        self.assertEqual(rendered["message"]["version"], VERSION)
        self.assertEqual(len(rendered["annotations"]["warnings"]), 2)

    def test_strict_refuses_what_default_only_warns_about(self) -> None:
        with self.assertRaisesRegex(BusError, "strict: .*--head"):
            self.bus.send("claude", "codex", "verdict", PR_COMMENT,
                          verdict="PASS", strict=True)
        with self.assertRaisesRegex(BusError, "strict: .*PASS\\|HOLD"):
            self.bus.send("claude", "codex", "verdict", PR_COMMENT,
                          verdict="MAYBE", head=A, strict=True)
        with self.assertRaisesRegex(BusError, "strict: .*40-hex"):
            self.bus.send("claude", "codex", "verdict", PR_COMMENT,
                          verdict="PASS", head="abc123", strict=True)
        with self.assertRaisesRegex(BusError, "strict: ask-ready should carry --head"):
            self.bus.send("codex", "claude", "ask-ready", PR, strict=True)
        clean = self.bus.send("claude", "codex", "verdict", PR_COMMENT,
                              verdict="PASS", head=A, strict=True)
        self.assertEqual(clean.warnings, ())
        code, _output, error = invoke([
            "send", *self.common, "--from", "claude", "--to", "codex",
            "--kind", "verdict", "--ref", PR_COMMENT, "--no-auto-head",
            "--strict"])
        self.assertEqual(code, 2)
        self.assertIn("agent-bus: strict:", error)
        os.environ["AGENT_BUS_STRICT"] = "1"
        try:
            code, _output, error = invoke([
                "send", *self.common, "--from", "claude", "--to", "codex",
                "--kind", "verdict", "--ref", PR_COMMENT, "--no-auto-head"])
            self.assertEqual(code, 2)
            with self.assertRaisesRegex(BusError, "strict"):
                self.bus.send("codex", "claude", "ask-ready", PR)
            os.environ["AGENT_BUS_STRICT"] = "0"
            self.assertEqual(
                self.bus.send("codex", "claude", "ask-ready", PR).sequence, 1)
        finally:
            del os.environ["AGENT_BUS_STRICT"]

    def test_note_length_and_ledger_are_advisories(self) -> None:
        short = self.bus.send("codex", "claude", "fyi", "x", note="n" * 280)
        self.assertEqual(short.warnings, ())
        long = self.bus.send("codex", "claude", "fyi", "x", note="n" * 281)
        self.assertEqual(long.warnings,
                         ("note is 281 chars; keep it under 280 and put the "
                          "evidence at the ref",))
        self.assertEqual(long.note, "n" * 281)
        with self.assertRaisesRegex(BusError, "strict: note is 281"):
            self.bus.send("codex", "claude", "fyi", "x", note="n" * 281,
                          strict=True)
        ledger = self.bus.send("codex", "claude", "fyi", "x", ledger="abc")
        self.assertEqual(ledger.ledger, "abc")
        self.assertTrue(any("deprecated" in w for w in ledger.warnings))
        code, _output, error = invoke([
            "send", *self.common, "--from", "codex", "--to", "claude",
            "--kind", "fyi", "--ref", "x", "--ledger", "abc"])
        self.assertEqual(code, 0)
        self.assertIn("--ledger is deprecated", error)

    # -- auto head --------------------------------------------------------

    @unittest.skipIf(shutil.which("git") is None, "git is not installed")
    def test_head_auto_fills_from_the_current_checkout(self) -> None:
        repo = Path(self.temp.name) / "repo"
        repo.mkdir()
        env = {**os.environ, "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@x",
               "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@x",
               "HOME": self.temp.name}
        for command in (["git", "init", "-q"],
                        ["git", "commit", "-q", "--allow-empty", "-m", "one"]):
            subprocess.run(command, cwd=repo, env=env, check=True)
        head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=repo, env=env,
                              check=True, capture_output=True,
                              text=True).stdout.strip()
        previous = os.getcwd()
        os.chdir(repo)
        try:
            code, output, error = invoke([
                "send", *self.common, "--from", "codex", "--to", "claude",
                "--kind", "ask-ready", "--ref", PR, "--json"])
            self.assertEqual(code, 0)
            self.assertEqual(json.loads(output)["message"]["head"], head)
            self.assertIn(f"--head auto-filled from git rev-parse HEAD in "
                          f"{os.getcwd()}: {head}", error)
            self.assertNotIn("warning", error)
            code, output, error = invoke([
                "send", *self.common, "--from", "codex", "--to", "claude",
                "--kind", "ask-ready", "--ref", PR, "--no-auto-head",
                "--json"])
            self.assertEqual(code, 0)
            self.assertNotIn("head", json.loads(output)["message"])
            self.assertIn("warning: ask-ready should carry --head", error)
            code, output, _error = invoke([
                "send", *self.common, "--from", "codex", "--to", "claude",
                "--kind", "fyi", "--ref", PR, "--json"])
            self.assertNotIn("head", json.loads(output)["message"])
            # An inherited GIT_DIR pointing at another repository must not
            # select that repository's head.
            other = Path(self.temp.name) / "other"
            other.mkdir()
            for command in (["git", "init", "-q"],
                            ["git", "commit", "-q", "--allow-empty", "-m", "x"]):
                subprocess.run(command, cwd=other, env=env, check=True)
            other_head = subprocess.run(
                ["git", "rev-parse", "HEAD"], cwd=other, env=env, check=True,
                capture_output=True, text=True).stdout.strip()
            self.assertNotEqual(other_head, head)
            os.environ["GIT_DIR"] = str(other / ".git")
            os.environ["GIT_WORK_TREE"] = str(other)
            try:
                code, output, _error = invoke([
                    "send", *self.common, "--from", "codex", "--to", "claude",
                    "--kind", "ask-ready", "--ref", PR, "--json"])
                self.assertEqual(code, 0)
                self.assertEqual(json.loads(output)["message"]["head"], head)
            finally:
                del os.environ["GIT_DIR"]
                del os.environ["GIT_WORK_TREE"]
        finally:
            os.chdir(previous)
        os.chdir(self.temp.name)
        try:
            code, output, error = invoke([
                "send", *self.common, "--from", "codex", "--to", "claude",
                "--kind", "ask-ready", "--ref", PR, "--json"])
            self.assertEqual(code, 0)
            self.assertNotIn("head", json.loads(output)["message"])
            self.assertNotIn("auto-filled", error)
        finally:
            os.chdir(previous)

    # -- compatibility gate -----------------------------------------------

    def test_coordination_lines_need_a_recipient_seen_on_this_version(self) -> None:
        fresh = Bus(self.project, self.state.parent / "fresh")
        fresh.init()
        plain = fresh.send("codex", "claude", "ask-ready", PR, head=A)
        self.assertEqual(plain.version, VERSION)
        with self.assertRaisesRegex(BusError, "has not read this project"):
            fresh.send("codex", "claude", "ask-ready", PR, head=A,
                       depends_on="670")
        forced = fresh.send("codex", "claude", "ask-ready", PR, head=A,
                            depends_on="670", assume_peer_upgraded=True)
        self.assertEqual(forced.version, COORDINATION_VERSION)
        # Diagnostic reads never mark: a human may peek while an older
        # automation is still the one consuming the inbox.
        fresh.inbox("claude", "claude-session", peek=True)
        fresh.peek_batch("claude", "claude-session")
        fresh.actionable_inbox("claude", "claude-session")
        self.assertEqual(fresh.peer_reader_version("claude"), None)
        with self.assertRaisesRegex(BusError, "has not read this project"):
            fresh.send("codex", "claude", "head-moved", PR, head=B,
                       prev_head=A)
        # A cursor-advancing read is made by the consumer itself.
        messages, issues = fresh.inbox("claude", "claude-session")
        self.assertFalse(issues)
        self.assertEqual([m.version for m in messages], [2, 3])
        self.assertEqual(fresh.peer_reader_version("claude"), "0.4.0")
        self.assertEqual(fresh.peer_reader_version("codex"), None)
        allowed = fresh.send("codex", "claude", "head-moved", PR, head=B,
                             prev_head=A)
        self.assertEqual(allowed.version, COORDINATION_VERSION)
        # So is an acknowledgement of a peeked batch.
        fresh.send("claude", "codex", "fyi", "x")
        _messages, _issues, batch = fresh.peek_batch("codex", "codex-session")
        self.assertEqual(fresh.peer_reader_version("codex"), None)
        assert batch is not None
        fresh.ack_batch("codex", "codex-session", token=batch.token)
        self.assertEqual(fresh.peer_reader_version("codex"), "0.4.0")
        fresh.send("claude", "codex", "fyi", "y")
        fresh.ack("codex", "codex-session", through_sequence=2)
        self.assertEqual(fresh.doctor(), ["ok"])

    def test_log_ref_filters_and_follows_one_thread(self) -> None:
        self.bus.send("codex", "claude", "ask-ready", PR, head=A)
        self.bus.send("codex", "claude", "ask-ready", DEP, head=C)
        self.bus.send("claude", "codex", "verdict", PR_COMMENT, verdict="PASS",
                      head=A)
        code, output, error = invoke(["log", *self.common, "--ref", PR])
        self.assertEqual((code, error), (0, ""))
        lines = output.splitlines()
        self.assertEqual(len(lines), 2)
        self.assertNotIn(DEP, output)
        code, output, _error = invoke([
            "log", *self.common, "--ref", "670", "--follow", "--timeout",
            "0.02", "--poll-interval", "0.01"])
        self.assertEqual(code, 0)
        self.assertEqual(len(output.splitlines()), 1)

    def test_annotate_stale_is_idempotent(self) -> None:
        self.bus.send("claude", "codex", "verdict", PR, verdict="PASS", head=A)
        self.bus.send("codex", "claude", "head-moved", PR, head=B, prev_head=A)
        messages, _issues = self.bus.log()
        once = annotate_stale(messages)
        self.assertEqual(annotate_stale(once), once)


if __name__ == "__main__":
    unittest.main()

"""agent-bus wait, driven as a real CLI process with gh and ssh mocks on PATH."""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

SRC = Path(__file__).parents[1] / "src"
sys.path.insert(0, str(SRC))

from agent_bus import Bus  # noqa: E402

MOCK_GH = r'''
import os, sys, time
from pathlib import Path
root = Path(os.environ["MOCK_DIR"])
with open(root / "calls.log", "a") as log:
    log.write("gh " + " ".join(sys.argv[1:]) + "\n")
args = sys.argv[1:]
if args[:2] == ["pr", "view"]:
    name = "pr_view_" + args[2]
elif args[:2] == ["api", "--paginate"]:
    name = args[2].split("?")[0].replace("/", "_")
else:
    sys.exit(9)
out, rc, delay = root / (name + ".out"), root / (name + ".rc"), root / (name + ".sleep")
if delay.exists():
    time.sleep(float(delay.read_text()))
if out.exists():
    sys.stdout.write(out.read_text())
sys.exit(int(rc.read_text()) if rc.exists() else (0 if out.exists() else 1))
'''

MOCK_SSH = r'''
import os, sys, time
from pathlib import Path
root = Path(os.environ["MOCK_DIR"])
args, rest, host = sys.argv[1:], [], None
i = 0
while i < len(args):
    if args[i] == "-o":
        i += 2
        continue
    if args[i] == "--" and host is None:
        i += 1
        continue
    if host is None:
        host = args[i]
    else:
        rest.append(args[i])
    i += 1
with open(root / "calls.log", "a") as log:
    log.write("ssh " + repr(sys.argv[1:]) + "\n")
if host == "stall":
    (root / "stall.pid").write_text(str(os.getpid()))
    time.sleep(120)
    sys.exit(0)
os.execv("/bin/sh", ["/bin/sh", "-c", " ".join(rest)])
'''


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


class WaitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.state = self.root / "state"
        self.project = self.root / "project"
        self.project.mkdir()
        self.bus = Bus(self.project, self.state)
        self.bus.init()
        self.mock = self.root / "mock"
        self.mock.mkdir()
        bindir = self.root / "bin"
        bindir.mkdir()
        for name, body in (("gh", MOCK_GH), ("ssh", MOCK_SSH)):
            path = bindir / name
            path.write_text(f"#!{sys.executable}\n{body}")
            path.chmod(0o755)
        self.env = {**os.environ, "PATH": f"{bindir}{os.pathsep}{os.environ['PATH']}",
                    "PYTHONPATH": str(SRC), "MOCK_DIR": str(self.mock)}
        self.env.pop("AGENT_BUS_GH", None)
        self.env.pop("AGENT_BUS_SSH", None)

    def tearDown(self) -> None:
        pid_file = self.mock / "stall.pid"
        if pid_file.exists():
            pid = int(pid_file.read_text())
            if _alive(pid):
                os.kill(pid, 9)
        self.temp.cleanup()

    def wait(self, *args: str, bus: bool = True, steps=(), timeout: float = 20):
        argv = [sys.executable, "-m", "agent_bus", "wait", "--project", str(self.project),
                "--state-root", str(self.state), "--interval", "1"]
        argv += ["--to", "codex", "--consumer", "codex-session"] if bus else ["--no-bus"]
        argv += list(args)
        if "--timeout" not in args:
            argv += ["--timeout", str(timeout)]

        def script() -> None:
            for delay, action in steps:
                time.sleep(delay)
                action()

        started = time.monotonic()
        proc = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                text=True, env=self.env)
        worker = threading.Thread(target=script)
        worker.start()
        try:
            out, err = proc.communicate(timeout=60)
        finally:
            worker.join()
        return proc.returncode, out, err, time.monotonic() - started

    def gh(self, name: str, out: str | None = None, rc: int | None = None) -> None:
        for suffix, value in ((".out", out), (".rc", rc)):
            path = self.mock / (name + suffix)
            if value is None:
                path.unlink(missing_ok=True)
            else:
                path.write_text(str(value))

    @staticmethod
    def append(path: Path, text: str):
        def go() -> None:
            with path.open("a") as handle:
                handle.write(text)
        return go

    # bus

    def test_first_message_into_an_empty_inbox_fires(self) -> None:
        send = lambda: self.bus.send("claude", "codex", "verdict", "https://x/pull/1#c",
                                     verdict="PASS", head="a" * 40)
        rc, out, err, elapsed = self.wait(steps=[(1.5, send)])
        self.assertEqual(rc, 0, err)
        self.assertIn("EVENT bus message codex:1 claude->codex verdict", out)
        self.assertIn("EVENT bus batch token=", out)
        self.assertLess(elapsed, 10)

    def test_messages_to_another_recipient_do_not_fire(self) -> None:
        send = lambda: self.bus.send("codex", "claude", "ask-ready", "claude->codex decoy")
        rc, out, _, _ = self.wait("--timeout", "4", steps=[(1, send)])
        self.assertEqual(rc, 1)
        self.assertNotIn("EVENT", out)
        self.assertIn("TIMEOUT", out)

    def test_a_message_from_between_turns_fires_until_acknowledged(self) -> None:
        self.bus.send("claude", "codex", "fyi", "early")
        rc, out, _, elapsed = self.wait()
        self.assertEqual(rc, 0)
        self.assertIn("fyi early", out)
        self.assertLess(elapsed, 5)
        self.bus.ack("codex", "codex-session", through_sequence=1)
        rc, out, _, _ = self.wait("--timeout", "3")
        self.assertEqual(rc, 1)
        self.assertEqual(self.bus.consumer_status("codex", "codex-session")["acked_sequence"], 1)

    # files

    def test_a_removed_file_and_a_recreated_unrelated_one_do_not_fire(self) -> None:
        status = self.root / "lane.status"
        status.write_text("lane DONE old\n")
        recreate = lambda: status.write_text("lane START again\n")
        rc, out, err, elapsed = self.wait(
            "--file", f"{status}:DONE|ABORT", bus=False,
            steps=[(1.5, status.unlink), (2.5, recreate),
                   (2.5, self.append(status, "lane DONE new\n"))])
        self.assertEqual(rc, 0, err)
        self.assertIn("lane DONE new", out)
        self.assertNotIn("DONE old", out)
        self.assertNotIn("START", out)
        self.assertGreaterEqual(elapsed, 6.5)

    def test_a_missing_file_fires_on_its_first_matching_line_only(self) -> None:
        status = self.root / "later.status"
        create = lambda: status.write_text("START\n")
        rc, out, _, elapsed = self.wait(
            "--file", f"{status}:DONE", bus=False,
            steps=[(1.5, create), (2.5, self.append(status, "DONE\n"))])
        self.assertEqual(rc, 0)
        self.assertIn("EVENT file:", out)
        self.assertNotIn("START", out)
        self.assertGreaterEqual(elapsed, 4)

    def test_transient_read_loss_is_not_an_event(self) -> None:
        status = self.root / "lane.status"
        status.write_text("lane DONE old\n")
        rc, out, err, elapsed = self.wait(
            "--file", f"{status}:DONE", bus=False,
            steps=[(1.5, lambda: status.chmod(0)), (2.5, lambda: status.chmod(0o644)),
                   (2.5, self.append(status, "lane DONE new\n"))])
        self.assertEqual(rc, 0, err)
        self.assertIn("lane DONE new", out)
        self.assertNotIn("DONE old", out)
        self.assertGreaterEqual(elapsed, 6.5)
        self.assertIn("unavailable", err)

    def test_an_unterminated_line_waits_for_its_newline(self) -> None:
        status = self.root / "lane.status"
        status.write_text("")
        rc, out, _, elapsed = self.wait(
            "--file", f"{status}:DONE", bus=False,
            steps=[(1.5, self.append(status, "DONE par")),
                   (2.5, self.append(status, "tial\n"))])
        self.assertEqual(rc, 0)
        self.assertIn("DONE partial", out)
        self.assertGreaterEqual(elapsed, 4)

    # gh

    def test_failed_or_partial_gh_pagination_is_not_an_event(self) -> None:
        page = "".join(f"{i} codex https://x/c{i}\n" for i in range(1, 151))
        name = "repos_o_r_issues_7_comments"
        self.gh(name, page)
        self.gh("pr_view_7", json.dumps({"headRefOid": "a" * 40}))
        for other in ("repos_o_r_pulls_7_reviews", "repos_o_r_pulls_7_comments"):
            self.gh(other, "")

        def partial() -> None:
            # A failed run's output never counts, even an id above the max.
            self.gh(name, "".join(page.splitlines(True)[:100]) + "151 x https://x/c151\n", 1)
            self.gh("pr_view_7", "", 1)

        def restore() -> None:
            self.gh(name, page, None)
            self.gh("pr_view_7", json.dumps({"headRefOid": "a" * 40}), None)

        rc, out, err, _ = self.wait("--gh-repo", "o/r", "--gh-pr", "7", "--timeout", "6",
                                    bus=False, steps=[(1.5, partial), (2.5, restore)])
        self.assertEqual(rc, 1, out)
        self.assertNotIn("EVENT", out)
        self.assertIn("gh exit 1", err)

    def test_gh_new_ids_head_state_and_checks_fire(self) -> None:
        base = {"headRefOid": "a" * 40, "state": "OPEN",
                "statusCheckRollup": [{"status": "IN_PROGRESS", "conclusion": ""}]}
        self.gh("pr_view_7", json.dumps(base))
        self.gh("repos_o_r_issues_7_comments", "5 codex https://x/c5\n")
        self.gh("repos_o_r_pulls_7_reviews", "")
        self.gh("repos_o_r_pulls_7_comments", "")
        flags = ("--gh-repo", "o/r", "--gh-pr", "7", "--gh-state", "--gh-checks")
        cases = [
            (lambda: self.gh("repos_o_r_pulls_7_reviews", "9 codex https://x/r9\n"),
             "EVENT pr#7 review 9 codex https://x/r9"),
            (lambda: self.gh("pr_view_7", json.dumps({**base, "headRefOid": "b" * 40})),
             f"EVENT pr#7 head {'a' * 40}->{'b' * 40}"),
            (lambda: self.gh("pr_view_7", json.dumps({**base, "headRefOid": "b" * 40,
                                                      "state": "MERGED"})),
             "EVENT pr#7 state OPEN->MERGED"),
            (lambda: self.gh("pr_view_7", json.dumps({**base, "headRefOid": "b" * 40,
                                                      "state": "MERGED",
                                                      "statusCheckRollup": [
                                                          {"status": "COMPLETED",
                                                           "conclusion": "SUCCESS"}]})),
             "EVENT pr#7 checks success"),
        ]
        state = str(self.root / "wait.json")
        for change, expected in cases:
            rc, out, _, _ = self.wait(*flags, "--state", state, "--timeout", "3", bus=False)
            self.assertEqual(rc, 1, out)
            change()
            rc, out, err, _ = self.wait(*flags, "--state", state, bus=False)
            self.assertEqual(rc, 0, err)
            self.assertIn(expected, out)

    # ssh and bounds

    def test_a_stalled_probe_does_not_delay_a_healthy_source(self) -> None:
        status = self.root / "lane.status"
        status.write_text("")
        rc, out, err, elapsed = self.wait(
            "--ssh", "stall:/x/lane.status:DONE", "--file", f"{status}:DONE",
            bus=False, steps=[(1.5, self.append(status, "DONE here\n"))])
        self.assertEqual(rc, 0, err)
        self.assertIn("EVENT file:", out)
        self.assertLess(elapsed, 5)
        time.sleep(0.2)
        self.assertFalse(_alive(int((self.mock / "stall.pid").read_text())))

    def test_the_timeout_bounds_a_stalled_baseline(self) -> None:
        rc, out, _, elapsed = self.wait("--ssh", "stall:/x/lane.status", "--timeout", "2",
                                        bus=False)
        self.assertEqual(rc, 1)
        self.assertIn("TIMEOUT", out)
        self.assertLess(elapsed, 5)
        time.sleep(0.2)
        self.assertFalse(_alive(int((self.mock / "stall.pid").read_text())))

    def test_an_apostrophe_in_an_ssh_path_and_regex(self) -> None:
        folder = self.root / "it's here; $(touch pwned)"
        folder.mkdir()
        status = folder / "lane.status"
        status.write_text("")
        rc, out, err, _ = self.wait(
            "--ssh", f"box:{status}:it's DONE", bus=False,
            steps=[(1.5, self.append(status, "it's DONE now\n"))])
        self.assertEqual(rc, 0, err)
        self.assertIn("it's DONE now", out)
        self.assertFalse((Path.cwd() / "pwned").exists())
        self.assertFalse((self.root / "pwned").exists())

    def test_unsafe_ssh_hosts_and_bad_arguments_refuse_before_any_probe(self) -> None:
        for args in (["--ssh", "-oProxyCommand=x:/p"], ["--ssh", "h st:/p"],
                     ["--ssh", "host"], ["--file", "/p:("], ["--gh-pr", "7"],
                     ["--interval", "0"], ["--timeout", "x"], ["--gh-state"]):
            rc, _, _, _ = self.wait(*args, bus=False)
            self.assertEqual(rc, 2, args)
        rc, _, err, _ = self.wait("--file", "/nonexistent", "--timeout", "2")
        self.assertEqual(rc, 1, err)
        self.assertFalse((self.mock / "calls.log").exists())

    # across turns

    def test_a_state_file_keeps_an_event_that_arrives_between_runs(self) -> None:
        status = self.root / "lane.status"
        status.write_text("lane DONE old\n")
        state = str(self.root / "wait.json")
        rc, out, _, _ = self.wait("--file", f"{status}:DONE", "--state", state,
                                  "--timeout", "2", bus=False)
        self.assertEqual(rc, 1)
        status.write_text("lane DONE old\nlane DONE between\n")
        rc, out, _, elapsed = self.wait("--file", f"{status}:DONE", "--state", state,
                                        bus=False)
        self.assertEqual(rc, 0)
        self.assertIn("lane DONE between", out)
        self.assertNotIn("DONE old", out)
        self.assertLess(elapsed, 5)

    def test_arm_then_wait_reports_what_changed_after_the_arm(self) -> None:
        self.gh("repos_o_r_issues_3_comments", "4 claude https://x/c4\n")
        state = str(self.root / "wait.json")
        rc, out, _, _ = self.wait("--gh-repo", "o/r", "--gh-issue", "3", "--state", state,
                                  "--arm", bus=False)
        self.assertEqual(rc, 0, out)
        self.assertIn("ARMED gh:o/r:issue#3:comment", out)
        self.gh("repos_o_r_issues_3_comments", "4 claude https://x/c4\n6 claude https://x/c6\n")
        rc, out, _, elapsed = self.wait("--gh-repo", "o/r", "--gh-issue", "3",
                                        "--state", state, bus=False)
        self.assertEqual(rc, 0)
        self.assertIn("EVENT issue#3 comment 6 claude https://x/c6", out)
        self.assertNotIn("c4", out)
        self.assertLess(elapsed, 5)


if __name__ == "__main__":
    unittest.main()

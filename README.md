# agent-bus

`agent-bus` is a small Python 3.11+ command-line signaling layer for local
agent harnesses. Runtime dependencies are Python's standard library only. It
passes short, structured *pointers* between agents; it does not carry evidence,
grant permission, invoke models, or make model API traffic local.

## Safety and architecture

For a resolved project root, the stable project ID is
`sha256(resolved-project-root)`. State is kept outside the project by default:
`~/.local/state/agent-bus/projects/<project-id>/`. Use `--state-root` for an
isolated test or harness state directory. The project need not be a Git repo.

Each recipient has an append-only `inboxes/<recipient>.jsonl` log. A message
has a schema/version, sequence, UTC timestamp, sender, recipient, fixed small
kind vocabulary, a `ref` pointer, optional metadata, and a SHA-256 over its
canonical ASCII JSON body. Appends use one O_APPEND write while holding an
`fcntl` lock, followed by `fsync`. Managed directories are 0700 and files
0600; pre-existing symlinks at managed leaves, foreign ownership, and
group/other permissions are refused.
The vocabulary covers generic task/result/status/error signals as well as
review and run lifecycle signals; message payloads remain pointers in every
case.

Every consumer has an atomic, fsynced cursor. `--peek` leaves it unchanged.
Malformed, torn, tampered, or external lines are reported as
`NON_AUTHORITATIVE` and do not advance the cursor past the problem. No bus
line is evidence or authority, and no message text is executed. The CLI and
watcher have no subprocess, socket, network, tmux, callback, or model-invocation
surface.

Agent IDs are extensible safe names (Claude and Codex are the first examples;
other local harnesses can use IDs such as `muse`, `luna`, or `terra`). Future
adapters may translate a harness event into `agent-bus send`; adapters remain
outside this package and do not change the untrusted-pointer rule.

The bus is not an operating-system security boundary between processes owned
by the same Unix user. Such a process can already rewrite the bus files,
impersonate any sender, or race pathname checks. Run mutually hostile agents
under separate OS accounts or a real sandbox. The permission and symlink
checks protect normal cooperative operation and catch stale/misconfigured
state; authority must never depend on them or on message identity.

## Commands

For an isolated local installation with `uv`:

```sh
uv tool install --editable /path/to/agent-bus
```

Then initialize any Git or non-Git project:

```sh
agent-bus init --project /path/to/project
agent-bus send --project /path/to/project --from codex --to claude \
  --kind ask-ready --ref 'canonical review pointer'
agent-bus inbox --project /path/to/project --to claude --consumer monitor
agent-bus watch --project /path/to/project --to claude --consumer monitor --timeout 60
agent-bus status --project /path/to/project
agent-bus doctor --project /path/to/project
```

For example, an author can point Claude at a PR comment or sealed artifact,
and Claude can point Codex at a verdict or blocker. In a Shengji workflow those
references may be `PR#180 comment ...`, `HANDOFF_REVIEW.md`, or a sealed output
path; the canonical comment, handoff, or artifact must still be independently
verified. A bus pointer cannot authorize a run.

`watch` only observes files and state. It prints messages when appended and
returns after the timeout; it never wakes another process or sends keystrokes.
Codex cannot be woken after an ended turn without external product support,
although a Claude monitor can run `watch` while its turn remains active.

State rotation is manual and per project: export what is needed, then rotate
or remove only `<state-root>/projects/<project-id>/` under an operator's
change procedure. It is never automatic and never an authority action.
`status` and `doctor` warn when logs exceed 1 MiB.

## Rollback and uninstall

Stop any host harness using the bus, retain/export pointers if desired, then
remove the installed package (`uv tool uninstall agent-bus` for the example
above).
To delete runtime state, verify the exact project ID from `status`, then remove
only `<state-root>/projects/<project-id>/`; with the default root this is
`~/.local/state/agent-bus/projects/<project-id>/`. Never delete an entire
custom state root unless you deliberately intend to remove every project it
contains. This state is recoverable only from your export or backups.

When the state root is outside the project (the default), the tool does not
intentionally modify project files, Git metadata, remotes, PRs, or canonical
handoff records during cooperative operation. Choosing an in-project
`--state-root` intentionally places managed state there. As noted above,
another process with the same OS identity is already able to mutate those
files and is outside this tool's security boundary.

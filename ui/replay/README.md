# DistAlgo Replay

Step through a **recorded** run of a DistAlgo program in the browser,
forwards and backwards, with the same page as DistAlgo Live: a network view
and a time-space (timeline) view, an explanation of every message and state
change with the code that caused it highlighted, line breakpoints, and
conditions ("pause when") written as DistAlgo expressions over the whole
system.  Nothing is simulated: the run is a normal DistAlgo run (processes
or threads, real messages), recorded, then replayed.

## Use

Record a run, then replay it:

    python -m da -T --logdir mytrace ping.da          # -T = --record-trace
    python -m da.replay mytrace                      # opens http://localhost:8642

or put `config(visualize='replay')` in `main()` (it turns on recording and
serves the replay when the run ends).  `config(visualize=True)` still makes
the static viz page; every recorded run prints the `da.replay` command that
replays it.

Options: `python -m da.replay LOGDIR [--port 8642] [--no-browser]`.

## What is recorded

With trace recording on, every process writes `<process>.events` in the log
directory (next to the `.trace`, `.snd` and `.state` files), one JSON line
per event, written as it happens (so a run that is stopped can still be
replayed):

| event | when | data |
|---|---|---|
| `run` | the process starts running | class, parent, source file, state |
| `setup` | its `setup()` ran (in the new process) | arguments, state |
| `new`, `setupcall`, `startcall` | it called `new`, `setup`, `start` | the processes, where in the program |
| `send` | `send(...)` | destinations, payload, Lamport clock, where, cause |
| `recv` | a message was taken off its queue (at a label) | sender, sender's clock, payload |
| `handler`, `handled` | a `receive` handler ran | handler, bindings, the message, state after |
| `await` | it blocked at an `await` | where (with local variables), state |
| `timeout` | an `await`'s timeout expired | label |
| `output` | `output(...)` | text, where |
| `end` | `run()` returned, exited or crashed | status, error, state (`main()`'s variables) |

"Where" is the program's own stack (file, line, function), "cause" is the
handler and message being handled, or what resumed `run()` (a message or a
timeout).  Values are encoded keeping tuples, sets, dicts and process ids
(`da.common.encode_value`), so conditions can be evaluated on them.

## How a run becomes steps

A step is one activation of one process: it starts running, handles one
message taken off its queue, or has an `await` time out.  Everything it does
until its next step (sends, handlers, state changes, output) belongs to
that step.  Steps are ordered by causality (a message is received after it
is sent; a process runs after it is started) and otherwise by recorded
wall-clock time, so the order is a possible order of the real run.  Each
send is paired with its receive by (sender, receiver, sender's Lamport
clock), or by order on the channel without clocks.

Line breakpoints stop at the next step that reaches the line through a
recorded event: a send, output, `await`, `new`/`setup`/`start`, or the
`receive` handler defined on it.  Line-by-line execution is not recorded.

## Where the code comes from

* `da/sim.py`, `da/common.py`: the recording, built on viz-states' per-process
  `StateQueue` (generalized to also write the event log).
* `da/replay.py`: reading the logs, ordering, explanations, conditions, and
  the HTTP server.  The value formatting, the code-around-a-line index and
  condition compiling are taken from DistAlgo Live (`livesim.py`), the server
  and its API from DistAlgo Live's `server.py`.
* `ui/replay/`: DistAlgo Live's page (`app.js`, `style.css`, `index.html`),
  with editing and running removed and loading a trace added; `d3.min.js` is
  the viz branch's `ui/js/d3.min.js`.

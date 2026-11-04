# DistAlgo Replay

Step through a **recorded** run of a DistAlgo program in the browser,
forwards and backwards, with the same page as DistAlgo Live: a network view
and a time-space (timeline) view, an explanation of every message and state
change with the code that caused it highlighted, line breakpoints, and
conditions ("pause when") written as DistAlgo expressions over the whole
system.  Nothing is simulated: the run is a normal DistAlgo run (processes
or threads, real messages), recorded, then replayed.

## Use

Like viz, a recorded run is replayed from **one data file**,
`<program>.replay.json`: the event logs of all its processes and the
program's source, so the file is all the replay needs (it can be moved or
sent to someone; the `.da` file and the log directory are not needed).

    python -m da ping.da              # with config(visualize=True) in main()
    python -m da.replay ping.replay.json            # opens http://localhost:8642

With `config(visualize=True)` the run writes `ping.replay.json` (next to
viz's `ping.html`) once all its processes have ended; with
`config(visualize='replay')` it writes it and serves the replay right away
(no viz page).  A run recorded only with `-T` (`--record-trace`) can be
replayed from its log directory, or merged into a data file first:

    python -m da -T --logdir mytrace ping.da
    python -m da.replay mytrace                     # merged when loaded
    python -m da.replay mytrace --save ping.replay.json

Options: `python -m da.replay DATA [--port 8642] [--no-browser]`.

The data file is JSON: `{"format": "distalgo-replay", "version": 1,
"program": <main .da file>, "sources": {file: text}, "processes": {process
id: [event, ...]}, ...}`, the events as described below.

## What is recorded

With trace recording on, every process writes `<process>.events` in the log
directory (next to the `.trace`, `.snd` and `.state` files), one JSON line
per event, written as it happens (so a run that is stopped can still be
replayed); they are merged into the data file at the end, as viz merges the
`.trace` and `.state` files into its page:

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

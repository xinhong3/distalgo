"""Replay a recorded DistAlgo run in the browser.

    python -m da.replay DATA [--port 8642] [--no-browser]
    python -m da.replay LOGDIR --save DATA        # only write the data file

A recorded run is replayed from one data file, `<program>.replay.json`
(like viz's one `<program>.html`): all processes' event logs plus the
program's source, so the file is all the replay needs.  A run with
`config(visualize=...)` writes it when the run ends; for a run recorded with
`-T`/`--record-trace` (and `--logdir LOGDIR`), give LOGDIR instead of DATA
(its logs are merged when loaded) or write the file with `--save`.

While recording, every process writes an event log, `<process>.events`,
next to its `.trace`/`.snd`/`.state` files (see `DistProcess._record` in
sim.py).  This module turns those logs into a sequence of steps and serves a
page that steps through them forwards and backwards, in a network view and a
time-space (timeline) view, explains each message and state change with the
code that caused it, and pauses when a condition (a DistAlgo expression over
the whole system) becomes true.

A step is one activation of one process: a process starts running, handles
one delivered message, or has an `await` time out; everything it does until
its next step (sends, handlers, state changes, output) belongs to that step.
Steps are put in an order that respects causality (a message is received
after it was sent, a process starts after it was started) and otherwise
follows the recorded wall-clock times.
"""

import argparse
import ast
import collections
import contextlib
import glob
import heapq
import io
import itertools
import json
import os
import re
import sys
import traceback
import types
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer

import da
from da.compiler import ui as daui
from da.compiler import utils as dautils
from da.common import JsonLoader, ProcessId, TraceKind, fmt

UI_ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'ui')   # da/ui, next to the viz page
STATIC = os.path.join(UI_ROOT, 'replay')
MAIN = 'main'


@contextlib.contextmanager
def _quiet_compiler(buf=None):
    """Sends the compiler's messages to `buf` (its printers bind sys.stderr
    as a default argument, so redirecting stderr is not enough; from
    DistAlgo Live)."""
    buf = buf or io.StringIO()
    old = daui.stderr
    daui.stderr = buf
    printers = [f for f in vars(dautils).values()
                if callable(f) and getattr(f, '__defaults__', None)
                and f.__defaults__[-1] is sys.__stderr__]
    saved = [f.__defaults__ for f in printers]
    for f in printers:
        f.__defaults__ = f.__defaults__[:-1] + (buf,)
    try:
        with contextlib.redirect_stderr(buf), contextlib.redirect_stdout(buf):
            yield
    finally:
        daui.stderr = old
        for f, d in zip(printers, saved):
            f.__defaults__ = d


def _da_compile(conv, src, fn, buf=None):
    """`conv` (daui.dastr_to_pyast or dastr_to_pycode) of `src`, quietly."""
    with _quiet_compiler(buf):
        return conv(src, fn, daui.parse_compiler_args([]))


# --------------------------------------------------------------------------
# The program's source: blocks for the UI, and the code around each line
# --------------------------------------------------------------------------

_HEADER_KINDS = {ast.FunctionDef: 'def', ast.For: 'for', ast.While: 'while',
                 ast.If: 'if', ast.With: 'with', ast.Try: 'try'}

class SourceMap:
    """The program files a trace refers to, split into named blocks."""

    def __init__(self, files, sources):
        """`sources`: {file: source text} (from the data file); a file not
        in it is read from disk."""
        self.files = {}           # file -> {'lines', 'headers'}
        self.blocks = []          # [{'name', 'code', 'kind', 'file', 'start', 'last'}]
        self.text = {}            # file -> source text
        for fn in files:
            src = sources.get(fn)
            if src is None:
                src = _read_source(fn)
            if src is None:
                continue
            src = src.replace('\r\n', '\n').replace('\t', '    ')
            self.text[fn] = src
            lines = src.split('\n')
            if lines and lines[-1] == '':
                lines.pop()
            self.files[fn] = {'lines': lines, 'headers': self._headers(src, fn)}
            base = os.path.splitext(os.path.basename(fn))[0]
            for name, first, last in self._split_blocks(lines):
                code = '\n'.join(lines[first - 1:last])
                kind = MAIN if name == MAIN else (
                    'process' if re.search(r'^\s*class\s+\w+\s*\([^)]*process', code, re.M) else 'code')
                self.blocks.append({'name': '%s.%s' % (base, name) if len(files) > 1 else name,
                                    'code': code, 'kind': kind, 'file': fn, 'start': first, 'last': last})

    @staticmethod
    def _split_blocks(lines):
        """[(name, first line, last line)] (1-based, inclusive): code before the
        first class ('setup'), one block per top-level class, and the rest
        ('main').  Unlike DistAlgo Live's split, no line is dropped, so a line
        of the file maps to a block by an offset."""
        matches = [(i + 1, m.group(1)) for i, m in
                   enumerate(re.match(r'^class\s+(\w+)', l) for l in lines) if m]
        n = len(lines)
        if not matches:
            return [(MAIN, 1, max(n, 1))]
        firsts, names = map(list, zip(*matches))
        # the last class ends at the next unindented statement
        end = next((j for j in range(firsts[-1], n)
                    if lines[j][:1].strip() and lines[j][0] != '#'), n)
        lasts = [f - 1 for f in firsts[1:]] + [end]
        blocks = []
        if any(l.strip() for l in lines[:firsts[0] - 1]):
            blocks.append(('setup', 1, firsts[0] - 1))
        else:
            firsts[0] = 1
        blocks += zip(names, firsts, lasts)
        if end < n:
            blocks.append((MAIN, end + 1, n))
        return blocks

    @staticmethod
    def _headers(src, fn):
        """Headers from the compiled program, where an await is the loop the
        compiler generates for it; from the plain source if that fails."""

        def _index_headers(module):
            """line -> tuple of enclosing (header line, kind), outermost first
            (from DistAlgo Live)."""
            index = {}

            def visit(stmts, chain):
                for s in stmts:
                    index.setdefault(s.lineno, chain)
                    k = _HEADER_KINDS.get(type(s))
                    if k or isinstance(s, ast.ClassDef):
                        inner = chain + ((s.lineno, k),) if k else chain
                        for b in [s.body, getattr(s, 'orelse', [])] + \
                                [h.body for h in getattr(s, 'handlers', [])] + \
                                [getattr(s, 'finalbody', [])]:
                            visit(b, inner)
            visit(module.body, ())
            return index

        try:
            pyast = _da_compile(daui.dastr_to_pyast, src, fn)
            if pyast is not None:
                return _index_headers(pyast)
        except Exception:
            pass
        try:
            return _index_headers(ast.parse(src, fn))
        except SyntaxError:
            return {}

    def loc(self, fn, line):
        """{'block', 'line' (in the block), 'text'} of a line of a file."""
        if fn in self.files and line is not None:
            lines = self.files[fn]['lines']
            for b in self.blocks:
                if b['file'] == fn and b['start'] <= line <= b['last']:
                    return {'block': b['name'], 'line': line - b['start'] + 1,
                            'text': lines[line - 1].strip() if 1 <= line <= len(lines) else ''}
        return {'block': None, 'line': line, 'text': ''}

    def entry_chain(self, fn, line):
        """Enclosing block entries of a line, innermost first; the loops the
        compiler generates for awaits are shown as 'await'."""
        info = self.files.get(fn)
        if info is None:
            return []
        out = []
        for hline, kind in reversed(info['headers'].get(line, ())):
            d = self.loc(fn, hline)
            text = d['text']
            if kind == 'while' and 'await' in text and not text.startswith('while'):
                kind = 'await'
            if kind == 'if' and text.startswith('elif'):
                kind = 'elif'
            d['kind'] = kind
            out.append(d)
        return out

    def await_loc(self, fn, line):
        """The await a blocking label belongs to (the compiler gives the
        label call the line of a nearby statement)."""
        for e in self.entry_chain(fn, line):
            if e['kind'] == 'await':
                return e
        return self.loc(fn, line)


# --------------------------------------------------------------------------
# The data file: one recorded run in one file
# --------------------------------------------------------------------------

DATA_FORMAT = 'distalgo-replay'

def _read_source(fn):
    try:
        with open(fn) as f:
            return f.read()
    except (OSError, UnicodeDecodeError):
        return None

def _source_files(processes):
    """The program's source files that the events refer to."""
    events = [e for evs in processes.values() for e in evs]
    files = {fr['file'] for e in events for fr in e.get('frames') or ()}
    files |= {(e.get('cause') or {}).get('file') for e in events}
    files |= {e.get('file') for e in events}
    return {f for f in files if f}

def load(path):
    """One recorded run as one data dict, from a data file (see `save`) or a
    log directory: every process' event log (the `<process>.events` files)
    and the source of the program files they refer to (the way
    viz.trace_to_clocks_and_state collects a run's traces for its page)."""
    if not os.path.isdir(path):
        with open(path) as f:
            data = json.load(f)
        if not isinstance(data, dict) or data.get('format') != DATA_FORMAT:
            raise ValueError('%s is not a DistAlgo replay data file' % path)
        return data
    files = sorted(glob.glob(os.path.join(path, '*.events')))
    if not files:
        raise ValueError(
            'no event logs (*.events) in %s: record a run with -T '
            '(--record-trace) using this version of DistAlgo' % path)
    processes = {}
    for fn in files:
        evs = []
        with open(fn) as f:
            loader = JsonLoader(f)
            while True:
                try:
                    evs.append(loader.load())
                except EOFError:
                    break
                except ValueError:
                    pass           # a line cut off when the run stopped
        if evs:
            processes[evs[0]['pid']] = evs
    sources = {}
    for fn in _source_files(processes):
        text = _read_source(fn)
        if text is not None:
            sources[fn] = text
    return {'format': DATA_FORMAT, 'version': 1,
            'sources': sources,
            'processes': processes}

def save(data, path):
    with open(path, 'w') as f:
        json.dump(data, f, separators=(',', ':'))
    return path


# Representation of a recorded process, with events ordered
class Proc:
    def __init__(self, events, decode):
        self.events = events = sorted(events, key=lambda e: e.get('seq', 0))
        for e in events:            # kinds as TraceKind, also as dict keys
            e['kind'] = TraceKind(e['kind'])
        start = next(e for e in events if e['kind'] in (TraceKind.Run, TraceKind.Setup))
        self.pid = decode(start['id'])   # its ProcessId, as the program's values hold it
        self.is_main = bool(start.get('main'))

    @property
    def cls(self):
        return MAIN if self.is_main else self.pid.pcls.__name__


class Message:
    """A message: its send and, once paired, its receive."""

    def __init__(self, key, src, dst, sclk, value):
        self.key, self.src, self.dst, self.sclk, self.value = key, src, dst, sclk, value
        self.send_seg = self.recv_seg = None      # index of the segment
        self.send_step = self.recv_step = None


class Trace:
    """All event logs of one run (a data file, see `load`), ordered into
    steps."""

    def __init__(self, data):
        self.decode = JsonLoader().decode
        # recorded id, as the events name a process (pid._filename_form_()) -> Proc
        self.procs = {id: Proc(evs, self.decode)
                      for id, evs in data['processes'].items() if evs}
        if not self.procs:
            raise ValueError('the recorded run has no events')
        self.steps = []           # [{'id', 'kind', 'events'}] in causal order
        self.times = []           # step -> recorded time since the first step
        self.messages = set()     # set of msgs, one per send and destination
        self.recv_msg = {}        # (pid, seq) of a recv -> Message
        self.sent_msgs = collections.defaultdict(list)  # (pid, seq) of a send -> [Message]
        segs = self._segment()
        self._pair(segs)
        self._order(segs)
        procs = self.procs.values()
        self.main_file = next((p.events[0].get('file') for p in procs if p.is_main), None)
        files = sorted(_source_files(data['processes']),
                       key=lambda f: (f != self.main_file, f))
        self.src = SourceMap(files, data.get('sources') or {})
        self.classes = sorted({p.cls for p in procs if not p.is_main})

    def pid(self, id):
        """The ProcessId of a recorded id (the id itself if not recorded)."""
        p = self.procs.get(id)
        return p.pid if p else id

    def name(self, id):
        """The name shown for a process: DistAlgo's short form, e.g. Pong:60402, or main."""
        p = self.procs.get(id)
        return id if p is None else MAIN if p.is_main else p.pid._short_form_()[1:-1]

    # ---- segments: activations of a process (its setup(), run, a delivered
    # message, a timeout), each with what the process did until the next one
    def _segment(self):
        """[{'id', 'kind', 'events'}], each process' segments in its own order."""
        segs = []
        for id, p in self.procs.items():
            for i, e in enumerate(p.events):
                if e['kind'] in (TraceKind.Run, TraceKind.Recv, TraceKind.Timeout):
                    segs.append({'id': id, 'kind': e['kind'], 'events': [e]})
                elif i == 0:        # the events before run: setup()
                    segs.append({'id': id, 'kind': TraceKind.Setup, 'events': [e]})
                else:
                    segs[-1]['events'].append(e)
        return segs

    @staticmethod
    def _seg_events(segs, *kinds):
        """(segment index, event) of every event of one of `kinds`."""
        return [(i, e) for i, seg in enumerate(segs) for e in seg['events'] if e['kind'] in kinds]

    def _pair(self, segs):
        """Pairs every send with its receive, from each process' own events:
        a receive gets the first unpaired send on its channel with the same
        sender's clock (so in order on the channel, without clocks)."""
        chans = collections.defaultdict(list)    # (src, dst) -> [Message] in send order

        def new(src, dst, e):
            chan = chans[(src, dst)]
            m = Message('%s>%s#%d' % (self.name(src), self.name(dst), len(chan) + 1),
                        src, dst, e.get('sclk'), self.decode(e.get('payload')))
            chan.append(m)
            self.messages.add(m)
            return m

        for i, e in self._seg_events(segs, TraceKind.Send):
            src = segs[i]['id']
            for dst in e['to']:
                m = new(src, dst, e)
                m.send_seg = i
                self.sent_msgs[(src, e['seq'])].append(m)
        for i, e in self._seg_events(segs, TraceKind.Recv):
            dst = segs[i]['id']
            m = next((m for m in chans[(e['src'], dst)]
                      if m.recv_seg is None and m.sclk == e.get('sclk')), None)
            if m is None:          # sender not recorded: make one up
                m = new(e['src'], dst, e)
            m.recv_seg = i
            self.recv_msg[(dst, e['seq'])] = m

    def _order(self, segs):
        """Topological order of the segments into steps: a segment follows the
        process' previous one and what triggered it in another process
        (setup() -> setup, start() -> run, send -> receive); otherwise by
        recorded time."""
        # segment -> segments it follows: the process' previous one,
        deps = {i: {i - 1} if i > 0 and segs[i - 1]['id'] == seg['id'] else set()
                for i, seg in enumerate(segs)}
        # the setup() or start() call that triggers it,
        triggers = {TraceKind.SetupCall: TraceKind.Setup, TraceKind.StartCall: TraceKind.Run}
        at = {(segs[i]['id'], e['kind']): i for i, e in self._seg_events(segs, *triggers.values())}
        for i, e in self._seg_events(segs, *triggers):
            for c in e['procs']:
                j = at.get((c, triggers[e['kind']]))
                if j is not None:
                    deps[j].add(i)
        # and the send of a message, for its receive
        for m in self.messages:
            if None not in (m.send_seg, m.recv_seg) and m.send_seg != m.recv_seg:
                deps[m.recv_seg].add(m.send_seg)
        users = collections.defaultdict(set)
        for s, ds in deps.items():
            for d in ds:
                users[d].add(s)
        indeg = {s: len(ds) for s, ds in deps.items()}

        def prio(i):
            return (segs[i]['events'][0].get('t', 0), self.name(segs[i]['id']), i)
        heap = [(prio(s), s) for s in deps if indeg[s] == 0]
        heapq.heapify(heap)
        order = []
        while heap:
            _, s = heapq.heappop(heap)
            order.append(s)
            for u in users[s]:
                indeg[u] -= 1
                if indeg[u] == 0:
                    heapq.heappush(heap, (prio(u), u))
        if len(order) < len(deps):       # inconsistent trace: keep the rest
            done = set(order)
            order += sorted((s for s in deps if s not in done), key=prio)
        self.steps += [segs[i] for i in order]
        ts = [st['events'][0].get('t', 0) for st in self.steps]
        t0 = min(ts, default=0)
        # monotone step times (the order can put a later-recorded step first)
        self.times += [t - t0 for t in itertools.accumulate(ts, max)]
        step = {i: k for k, i in enumerate(order)}
        for m in self.messages:
            m.send_step, m.recv_step = step.get(m.send_seg), step.get(m.recv_seg)


# --------------------------------------------------------------------------
# What a step looks like to the UI (the same shapes as DistAlgo Live)
# --------------------------------------------------------------------------

class ProcView:
    """What a condition sees of process `p` in state `s`: its fields plus id,
    cls, status, clock, received and sent (as in DistAlgo Live)."""

    def __init__(self, p, s, decode):
        self.__dict__.update({f: decode(v) for f, v in s['state'].items()})
        self.id = p.pid
        self.cls = p.cls
        self.status = s['status']
        self.clock = s['clock']
        self.received = list(s['received'])
        self.sent = list(s['sent'])

    def __hash__(self):
        return hash(self.id)

    def __eq__(self, other):
        return isinstance(other, ProcView) and other.id == self.id

    def __repr__(self):
        return repr(self.id)


class Timeline:
    """The run at each step, independent of a cursor: the system's state,
    the messages in flight, and the history (events, state changes, output)
    with the code that caused it."""

    def __init__(self, trace):
        self.trace = trace
        self._at = None          # (step, procs, in flight) of the last at()
        # history of the whole run, each item tagged with its step; the
        # snapshot sends the items up to the cursor
        self.events = []         # sends and receives
        self.changes = []        # field changes, with the code that caused them
        self.logs = []           # output and lifecycle messages
        self.step_lines = []     # step -> {(file, line)} it reaches, for breakpoints
        self._build_history()

    # ---- history (events, changes, logs), computed once ---------------
    def _build_history(self):
        T = self.trace
        procs, net = {}, {}

        def log(text, kind='system'):         # of the step being added
            self.logs.append({'step': k, 'pid': name, 'kind': kind,
                              'text': text})

        for k, st in enumerate(T.steps):
            pid = st['id']
            name = T.name(pid)
            # the fields changed since the process' last event: by the
            # handler that just ran ('handled'), or else by run()
            for e, fields, old in self._apply(procs, net, k):
                why = self._why(pid, e)
                self.changes += [{'step': k, 'pid': name, 'field': f,
                                  'old': fmt(T.decode(old[f])) if f in old else None,
                                  'new': fmt(T.decode(e['state'][f])), 'why': why}
                                 for f in fields]
            lines = set()
            for e in st['events']:
                kind = e['kind']
                for fr in (e.get('frames') or ())[:1]:
                    lines.add((fr['file'], fr['line']))
                c = e.get('cause') or {}
                if kind == TraceKind.Handled and c.get('file'):
                    lines.add((c['file'], c['line']))
                if kind == TraceKind.Setup:
                    log('ran setup()')
                elif kind == TraceKind.Run:
                    log('starts running')
                elif kind == TraceKind.Recv:
                    m = T.recv_msg.get((pid, e['seq']))
                    if m is not None:
                        self.events.append({
                            'kind': 'recv', 'step': k, 'src': T.name(m.src),
                            'dst': name, 'key': m.key, 'payload': fmt(m.value)})
                        log('delivered %s from <%s>' % (fmt(m.value), T.name(m.src)))
                elif kind == TraceKind.Timeout:
                    log('timeout fired')
                elif kind == TraceKind.Send:
                    why = self._why(pid, e)
                    for m in T.sent_msgs[(pid, e['seq'])]:
                        self.events.append({
                            'kind': 'send', 'step': k, 'src': name,
                            'dst': T.name(m.dst), 'key': m.key,
                            'payload': fmt(m.value), 'why': why})
                elif kind == TraceKind.Output:
                    log(e.get('text', ''), 'print')
                elif kind == TraceKind.New:
                    for c in e.get('children', []):
                        log('created %r' % (T.pid(c),))
                elif kind == TraceKind.End:
                    status = e.get('status')
                    log({'done': 'run() finished', 'exited': 'exited',
                         'crashed': 'crashed: %s' % e.get('error')}.get(status, status),
                        'error' if status == 'crashed' else 'system')
            self.step_lines.append(lines)

    # ---- explanations ------------------------------------------------
    def frame_loc(self, fr, main):
        d = self.trace.src.loc(fr['file'], fr['line'])
        func = fr['func']
        d['func'] = MAIN if main and func == 'run' else (
            'receive' if re.match(r'^_\w+_handler_\d+$', func) else func)
        return d

    def _await_before(self, pid, seq):
        """Where `pid` was waiting just before its event `seq`."""
        for e in reversed(self.trace.procs[pid].events):
            if e['seq'] >= seq:
                continue
            if e['kind'] == TraceKind.Await and e.get('frames'):
                fr = e['frames'][0]
                return self.trace.src.await_loc(fr['file'], fr['line'])
            if e['kind'] in (TraceKind.Run, TraceKind.Setup):
                break
        return None

    def _why(self, pid, e):
        """Where event `e` of `pid` happened (line, enclosing entries, stack)
        and its cause (the handler and its message, or what resumed run())."""
        T = self.trace
        frames = e.get('frames')
        cause = e.get('cause') or {}
        main = T.procs[pid].is_main
        stack = [self.frame_loc(fr, main) for fr in frames or ()]
        line, entries = None, []
        if frames:
            fr = frames[0]
            line = self.trace.src.loc(fr['file'], fr['line'])
            entries = self.trace.src.entry_chain(fr['file'], fr['line'])
            w = next((d for d in entries if d['kind'] == 'await'), None)
            if e['kind'] in (TraceKind.Await, TraceKind.Handler) and w:
                # at an await's label: the label call has a nearby line
                entries = entries[entries.index(w):]
                line = {k: v for k, v in w.items() if k != 'kind'}
                stack[0].update(line=w['line'], text=w['text'])
        elif cause.get('type') == 'handler' and cause.get('file'):
            line = self.trace.src.loc(cause['file'], cause['line'])

        why = {'type': 'run'}
        after = cause.get('after') or {}
        if cause.get('type') == 'handler':
            m = T.recv_msg.get((pid, cause.get('recv')))
            why = {'type': 'handler',
                   'payload': fmt(m.value) if m else '?',
                   'src': T.name(m.src) if m else '?',
                   'msg': m.key if m else None,
                   'deliveredStep': m.recv_step if m else None,
                   'bindings': {k: fmt(T.decode(v))
                                for k, v in (cause.get('bindings') or {}).items()}}
        elif after.get('type') == 'message':
            m = T.recv_msg.get((pid, after.get('recv')))
            if m is not None:
                why['after'] = {'type': 'message', 'msg': m.key,
                                'src': T.name(m.src),
                                'payload': fmt(m.value),
                                'deliveredStep': m.recv_step,
                                'await': self._await_before(pid, after['recv'])}
        elif after.get('type') == 'timeout':
            why['after'] = {'type': 'timeout',
                            'await': self._await_before(pid, e['seq'])}
        return {'line': line or {'block': None, 'line': None, 'text': ''},
                'entries': entries, 'stack': stack, 'cause': why}

    # ---- the system at a step ----------------------------------------
    def _proc(self, procs, pid):
        """The state of `pid` in `procs`, made when first needed; None for a
        process that was not recorded."""
        if pid in self.trace.procs and pid not in procs:
            procs[pid] = {'status': 'created', 'state': {}, 'frames': [],
                          'waiting': None, 'clock': None, 'sent': [],
                          'received': [], 'error': None}
        return procs.get(pid)

    def _apply(self, procs, net, k):
        """Applies step k to `procs` ({pid: state}) and `net` ({key: message
        in flight}); returns the field changes [(event, fields, old state)]."""
        T = self.trace
        st = T.steps[k]
        pid = st['id']
        s = self._proc(procs, pid)
        if st['kind'] != TraceKind.Setup:
            s['status'] = 'running'
        elif s['status'] == 'created':      # else start() came first: still ready
            s['status'] = 'setup'
        s['waiting'] = None
        changes = []
        for e in st['events']:
            kind = e['kind']
            s['clock'] = e.get('clk', s['clock'])
            if e.get('state'):
                old = s['state']
                fields = [f for f, v in e['state'].items()
                          if f not in old or old[f] != v]
                if fields:
                    changes.append((e, fields, old))
                s['state'] = dict(old, **e['state'])
            if e.get('frames'):
                s['frames'] = e['frames']     # where it is
            if e.get('locals') is not None:
                s['main_locals'] = e['locals']   # main()'s variables
            if kind == TraceKind.New:
                for c in e.get('children', []):
                    self._proc(procs, c)
            elif kind == TraceKind.StartCall:
                for c in e.get('procs', []):
                    cs = self._proc(procs, c)
                    if cs is not None and cs['status'] in ('created', 'setup'):
                        cs['status'] = 'ready'
            elif kind == TraceKind.Send:
                for m in T.sent_msgs[(pid, e['seq'])]:
                    s['sent'].append((m.value, T.pid(m.dst)))
                    if m.recv_step is None or m.recv_step > k:
                        net[m.key] = m
            elif kind == TraceKind.Recv:
                m = T.recv_msg.get((pid, e['seq']))
                if m is not None:
                    s['received'].append((m.value, T.pid(m.src)))
                    net.pop(m.key, None)
            elif kind == TraceKind.Await:
                s['status'] = 'blocked'
                s['frames'] = e.get('frames') or []
                if e.get('frames'):
                    fr = e['frames'][0]
                    w = self.trace.src.await_loc(fr['file'], fr['line'])
                    w['timeout'] = e.get('timeout')
                    s['waiting'] = w
            elif kind == TraceKind.End:
                s['status'] = e.get('status') or 'done'
                s['error'] = e.get('error')
                s['frames'] = []
        return changes

    def at(self, k):
        """({pid: process state}, [messages in flight]) after steps 0..k,
        updated from the last call's (so do not keep them)."""
        k = min(k, len(self.trace.steps) - 1)
        if self._at is None or self._at[0] > k:
            self._at = (-1, {}, {})
        j, procs, net = self._at
        for i in range(j + 1, k + 1):
            self._apply(procs, net, i)
        self._at = (k, procs, net)
        return procs, list(net.values())

    def scope(self, k):
        """The names a condition sees at step k."""
        T = self.trace
        procs, net = self.at(k)
        views = {T.procs[id].pid: ProcView(T.procs[id], s, T.decode)
                 for id, s in procs.items() if not T.procs[id].is_main}

        def viewify(v):
            if isinstance(v, ProcessId):
                return views.get(v, v)
            if isinstance(v, (set, frozenset, list, tuple)):
                return type(v)(viewify(x) for x in v)
            if isinstance(v, dict):
                return {viewify(a): viewify(b) for a, b in v.items()}
            return v

        # process fields too, so that x.p == pong and q.total_pings work
        for view in views.values():
            view.__dict__.update({f: viewify(x) for f, x in view.__dict__.items() if f != 'id'})

        scope = {}
        for id, s in procs.items():
            if T.procs[id].is_main:
                # main()'s variables, as of its latest recorded event
                for name, v in (s.get('main_locals') or {}).items():
                    scope[name] = viewify(T.decode(v))
        allv = list(views.values())
        scope.update({'procs': allv,
                      'network': [viewify((T.pid(m.src), T.pid(m.dst), m.value))
                                  for m in net],
                      'step': k})
        for cls in T.classes:
            scope[cls] = [v for v in allv if v.cls == cls]
        return scope

    def time(self, k):
        times = self.trace.times
        return times[max(0, min(k, len(times) - 1))] if times else 0.0

    def action(self, k):
        """The step k as a DistAlgo Live scheduler action."""
        if k is None or not (0 <= k < len(self.trace.steps)):
            return None
        st = self.trace.steps[k]
        if st['kind'] == TraceKind.Recv:
            m = self.trace.recv_msg.get((st['id'], st['events'][0]['seq']))
            return {'type': 'deliver', 'msg': m.key if m else None}
        return {'type': st['kind'], 'pid': self.trace.name(st['id'])}


# --------------------------------------------------------------------------
# Conditions
# --------------------------------------------------------------------------

class Conditions:
    """Conditions over the whole system: DistAlgo expressions, evaluated in
    the program's module-level namespace and a scope of the system."""

    def __init__(self, trace):
        self.items = []
        self.namespace = {}
        fn = trace.main_file
        src = trace.src.text.get(fn) if fn else None
        if src is None:
            return
        # if it fails part way, conditions can use what was defined until then
        ns = self.namespace = {'__name__': '__dareplay__', '__file__': fn}
        sys.path.insert(0, os.path.dirname(fn))    # for the program's imports
        try:     # main() is not run
            code = _da_compile(daui.dastr_to_pycode, src, fn)
            if code is not None:
                with _quiet_compiler():
                    exec(code, ns)
        except Exception:
            pass
        finally:
            sys.path.remove(os.path.dirname(fn))

    def set(self, conds):
        self.items = [{'id': i, 'expr': c.get('expr', ''),
                       'enabled': c.get('enabled', True),
                       'fn': None, 'error': None}
                      for i, c in enumerate(conds or [])]
        for c in self.items:
            if not c['expr'].strip():
                continue
            try:
                c['fn'] = compile_query(c['expr'])
            except CompileError as e:
                c['error'] = str(e)

    def evaluate(self, scope):
        """[(id, value, error)]; `scope()` gives the names they see."""
        names = None
        res = []
        for c in self.items:
            if c['fn'] is None:
                res.append((c['id'], None, c['error']))
                continue
            if names is None:
                names = scope()
            # the namespace has no builtins if the program could not be run
            g = dict(self.namespace, da=da, __builtins__=__builtins__)
            g.update(names)
            try:
                fn = types.FunctionType(c['fn'].__code__, g, '_query_')
                res.append((c['id'], bool(fn()), None))
            except NameError as e:
                res.append((c['id'], None, '%s. You can use: %s' % (
                    e, ', '.join(sorted(names)))))
            except Exception as e:
                res.append((c['id'], None, '%s: %s' % (type(e).__name__, e)))
        return res

    @staticmethod
    def example(scope):
        """An example condition, for the input's placeholder:
        some(p in <a variable of main() holding processes>,
        has= p.<field> == <its value now>), else over a process class."""
        groups = []
        for name, v in scope.items():
            if name in ('procs', 'network', 'step'):
                continue
            items = [v] if isinstance(v, ProcView) else (
                list(v) if isinstance(v, (list, tuple, set, frozenset))
                else [])
            views = [x for x in items if isinstance(x, ProcView)]
            if views:
                groups.append((name[:1].isupper(), name, views))
        groups.sort(key=lambda g: g[0])      # main()'s variables first
        for _, name, views in groups:
            for f, val in views[0].__dict__.items():
                if isinstance(val, (bool, int, str)) and \
                        f not in ('id', 'cls', 'status', 'clock'):
                    var = name[0].lower() if name[0].isalpha() else 'p'
                    if var == name:
                        var = 'x'
                    return 'some(%s in %s, has= %s.%s == %r)' % (
                        var, name, var, f, val)
        if groups:
            return 'some(p in %s, has= p.status == %r)' % (
                groups[0][1], groups[0][2][0].status)
        return 'some(p in procs, has= len(p.received) > 0)'


class CompileError(Exception):
    pass


def compile_query(expr):
    """A condition becomes `def _query_(): return (<expr>)`, compiled by
    DistAlgo, so some/each/setof/... work (from DistAlgo Live)."""
    expr = expr.strip()
    if not expr:
        raise CompileError('empty condition')
    if '\n' in expr:
        raise CompileError('a condition must be a single expression')
    try:
        ast.parse(expr, mode='eval')
    except SyntaxError as e:
        raise CompileError('SyntaxError: %s' % e.msg)
    src = 'def _query_():\n    return (%s)\n' % expr
    err = io.StringIO()
    try:
        pyast = _da_compile(daui.dastr_to_pyast, src, '<condition>', err)
    except SyntaxError as e:
        raise CompileError('SyntaxError: %s' % e.msg)
    except Exception as e:
        raise CompileError('%s: %s' % (type(e).__name__, e))
    if pyast is None:
        msg = err.getvalue().strip().splitlines()
        raise CompileError(re.sub(r'<condition>:\d+:(\d+:)?\s*', '',
                                  msg[0] if msg else 'cannot compile'))
    pyast.body = [s for s in pyast.body
                  if not (isinstance(s, ast.Import) and
                          any(a.name == 'da' for a in s.names))]
    ast.fix_missing_locations(pyast)
    ns = {'da': da, '__name__': '__dareplay_query__'}
    exec(compile(pyast, '<condition>', 'exec'), ns)
    return ns['_query_']


# --------------------------------------------------------------------------
# A cursor into the run
# --------------------------------------------------------------------------

class ReplaySession:
    """A recorded run and a cursor into its steps.  It answers the same
    requests as DistAlgo Live's Session, so the page can be shared."""

    def __init__(self, path):
        self.path = os.path.abspath(path)
        self.trace = Trace(load(path))
        self.timeline = Timeline(self.trace)
        self.conds = Conditions(self.trace)
        self.cursor = 0
        self.last_stop = None
        self.breaklines = set()           # (file, line)
        self.now = self.timeline.time(0)

    def snapshot(self, since=None):
        T, tl, k = self.trace, self.timeline, self.cursor
        states, net = tl.at(k)
        procs = []
        for pid, s in states.items():
            p = T.procs[pid]
            frames = [tl.frame_loc(fr, p.is_main)
                      for fr in reversed(s['frames'])]   # outermost first
            w = s['waiting']
            if frames and w:
                # a blocked process is at its await (as its card shows), not
                # the nearby line the compiler gives the blocking label call
                frames[-1].update(block=w['block'], line=w['line'],
                                  text=w['text'])
            procs.append({
                'id': T.name(pid), 'cls': p.cls, 'isMain': p.is_main,
                'status': s['status'], 'waitingAt': w, 'clock': s['clock'],
                'state': {f: fmt(T.decode(v)) for f, v in s['state'].items()},
                'frames': frames, 'sent': len(s['sent']),
                'received': len(s['received']), 'error': s['error']})
        network = [{'key': m.key, 'src': T.name(m.src), 'dst': T.name(m.dst),
                    'payload': fmt(m.value), 'sentStep': m.send_step,
                    'sentAt': round(tl.time(m.send_step), 6),
                    'arrives': None if m.recv_step is None
                    else round(tl.time(m.recv_step), 6),
                    'recvStep': m.recv_step} for m in net]
        scope = tl.scope(k)
        snap = {
            'step': k, 'time': round(self.now, 6),
            'processes': procs, 'network': network,
            # the history up to step k
            'events': [x for x in tl.events if x['step'] <= k],
            'changes': [x for x in tl.changes if x['step'] <= k],
            'logs': [x for x in tl.logs if x['step'] <= k],
            'next': tl.action(k + 1),
            'lastAction': tl.action(k),
            'lastStop': self.last_stop,
            'conditions': [
                {'id': c['id'], 'expr': c['expr'], 'enabled': c['enabled'],
                 'value': v, 'error': err}
                for c, (_, v, err) in zip(self.conds.items,
                                          self.conds.evaluate(lambda: scope))],
            'conditionNames': [n for n in scope if re.match(r'^[A-Za-z_]\w*$', n)],
            'conditionExample': Conditions.example(scope),
        }
        if since:
            # The history lists only grow with the step, so the page needs
            # only what it does not have yet; after a step back, only how
            # many items to keep ('trim').
            snap['delta'] = True
            snap['trim'] = {}
            for h in ('events', 'logs', 'changes'):
                have = max(0, int(since.get(h, 0)))
                if have <= len(snap[h]):
                    snap[h] = snap[h][have:]
                else:
                    snap['trim'][h] = len(snap[h])
                    snap[h] = []
        return snap

    # ---- moving through the steps ---------------------------------------
    def goto(self, k):
        self.cursor = max(0, min(int(k), len(self.trace.steps) - 1))
        self.now = self.timeline.time(self.cursor)

    def forward(self):
        if self.cursor + 1 >= len(self.trace.steps):
            return False
        self.goto(self.cursor + 1)
        return True

    def _hit_line(self, n):
        """The breakpoint line the step at the cursor reaches, the n-th step
        of a run."""
        hits = self.timeline.step_lines[self.cursor] & self.breaklines
        if not hits:
            return None
        fn, line = min(hits)
        return {'reason': 'line', 'steps': n, 'at': self.trace.src.loc(fn, line),
                'pid': self.trace.name(self.trace.steps[self.cursor]['id'])}

    def _values(self):
        return {cid: v for cid, v, _ in self.conds.evaluate(
            lambda: self.timeline.scope(self.cursor))}

    def _stop(self, n, before):
        """The stop the step at the cursor, the n-th of a run, reaches, if
        any; `before`: the conditions' values before it, updated."""
        hit = self._hit_line(n)
        if hit:
            return hit
        now = self._values()
        ids = [cid for cid, v in now.items() if v and not before.get(cid) and
               self.conds.items[cid]['enabled']]
        before.update(now)
        return {'reason': 'condition', 'ids': ids, 'steps': n} if ids else None

    def run_until(self, max_steps=100000):
        """Step until a step reaches a breakpoint line, a condition becomes
        true, or the run ends."""
        before = self._values()
        for n in range(max_steps):
            if not self.forward():
                self.last_stop = {'reason': 'quiet', 'steps': n}
                return
            self.last_stop = self._stop(n + 1, before)
            if self.last_stop:
                return
        self.last_stop = {'reason': 'limit', 'steps': max_steps}

    def advance(self, dt):
        """Real-time play: move the clock by dt (recorded seconds) and take
        the steps that happened by then."""
        self.last_stop = None
        target = self.now + max(0.0, float(dt))
        before = self._values()
        n = 0
        while self.cursor + 1 < len(self.trace.steps) and \
                self.timeline.time(self.cursor + 1) <= target:
            self.goto(self.cursor + 1)
            n += 1
            self.last_stop = self._stop(n, before)
            if self.last_stop:
                return
        self.now = target
        if self.cursor + 1 >= len(self.trace.steps):
            self.last_stop = {'reason': 'quiet', 'steps': n}


# --------------------------------------------------------------------------
# HTTP server (from DistAlgo Live's server.py)
# --------------------------------------------------------------------------

SESSION = None


def api(path, body):
    s = SESSION
    if path in ('/api/load', '/api/breakpoints'):
        # breakpoints {block name: [line in the block, ...]} as (file, line)
        start = {b['name']: (b['file'], b['start']) for b in s.trace.src.blocks}
        s.breaklines = {(start[name][0], start[name][1] + int(ln) - 1)
                        for name, lines in (body.get('breakpoints') or {}).items()
                        if name in start for ln in lines}
        s.conds.set(body.get('conditions', []))
        if path == '/api/load':
            s.last_stop = None
            T = s.trace
            return dict(s.snapshot(), info={
                'logdir': s.path, 'program': T.main_file,
                'steps': len(T.steps), 'processes': len(T.procs),
                'messages': len(T.messages), 'classes': T.classes,
                'blocks': [{'name': b['name'], 'code': b['code'],
                            'kind': b['kind']} for b in s.trace.src.blocks]})
    elif path == '/api/step':
        s.last_stop = None
        if s.forward():
            s.last_stop = s._hit_line(1)
    elif path == '/api/back':
        s.last_stop = None
        if s.cursor:
            s.goto(s.cursor - 1)
    elif path == '/api/goto':
        s.last_stop = None
        s.goto(body.get('step', 0))
    elif path == '/api/run_until':
        s.run_until(int(body.get('maxSteps', 100000)))
    elif path == '/api/advance':
        s.advance(float(body.get('dt', 0)))
    else:
        return None
    return s.snapshot(body.get('since'))


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass

    def _send(self, code, data, ctype):
        self.send_response(code)
        self.send_header('Content-Type', ctype)
        self.send_header('Content-Length', str(len(data)))
        self.send_header('Cache-Control', 'no-store')
        self.end_headers()
        self.wfile.write(data)

    def _json(self, obj, code=200):
        self._send(code, json.dumps(obj, default=str).encode('utf-8'),
                   'application/json')

    def do_GET(self):
        path = self.path.split('?')[0]
        if path == '/':
            path = '/index.html'
        if path == '/d3.min.js':          # shared with the viz page
            full = os.path.join(UI_ROOT, 'js', 'd3.min.js')
        else:
            full = os.path.normpath(os.path.join(STATIC, path.lstrip('/')))
            if not full.startswith(STATIC + os.sep):
                return self._send(404, b'not found', 'text/plain')
        if not os.path.isfile(full):
            return self._send(404, b'not found', 'text/plain')
        ctype = {'.html': 'text/html; charset=utf-8',
                 '.js': 'application/javascript; charset=utf-8',
                 '.css': 'text/css; charset=utf-8'}.get(
                     os.path.splitext(full)[1], 'application/octet-stream')
        with open(full, 'rb') as f:
            self._send(200, f.read(), ctype)

    def do_POST(self):
        n = int(self.headers.get('Content-Length') or 0)
        try:
            body = json.loads(self.rfile.read(n) or b'{}')
        except ValueError:
            return self._json({'error': 'bad JSON'}, 400)
        try:
            res = api(self.path.split('?')[0], body)
        except Exception as e:
            traceback.print_exc()
            return self._json({'error': 'internal error: %s: %s' %
                               (type(e).__name__, e)}, 500)
        if res is None:
            return self._json({'error': 'unknown endpoint'}, 404)
        self._json(res)


def serve(path, port=8642, open_browser=True):
    """Serves the replay of a recorded run (a data file or a log
    directory) until Ctrl-C."""
    global SESSION
    SESSION = ReplaySession(path)
    server = HTTPServer(('127.0.0.1', port), Handler)
    url = 'http://localhost:%d/' % port
    T = SESSION.trace
    print('DistAlgo Replay: %d steps, %d processes, %d messages from %s' %
          (len(T.steps), len(T.procs), len(T.messages),
           os.path.abspath(path)))
    print('Open %s   (Ctrl-C to stop)' % url)
    if open_browser:
        try:
            webbrowser.open(url)
        except Exception:
            pass
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print()
    finally:
        server.server_close()


def main(argv=None):
    ap = argparse.ArgumentParser(
        prog='python -m da.replay',
        description='Replay a recorded DistAlgo run in the browser.')
    ap.add_argument('data', help='the data file of a recorded run '
                    '(<program>.replay.json), or the directory with its '
                    'traces (the --logdir of a run with -T/--record-trace)')
    ap.add_argument('--save', metavar='FILE', help='only merge the run into '
                    'one data file FILE (from a log directory), do not serve')
    ap.add_argument('--port', type=int, default=8642)
    ap.add_argument('--no-browser', action='store_true')
    args = ap.parse_args(argv)
    try:
        if args.save:
            data = load(args.data)
            save(data, args.save)
            print('Wrote %s (%d processes)' % (args.save,
                                                len(data['processes'])))
            return
        serve(args.data, args.port, not args.no_browser)
    except (ValueError, OSError) as e:
        sys.exit(str(e))


if __name__ == '__main__':
    main()

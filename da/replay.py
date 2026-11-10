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

HERE = os.path.dirname(os.path.abspath(__file__))
UI_ROOT = os.path.join(HERE, 'ui')          # da/ui, next to the viz page
STATIC = os.path.join(UI_ROOT, 'replay')
STEP_KINDS = ('run', 'recv', 'timeout')
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
# Recorded values
# --------------------------------------------------------------------------

class ReplayPid:
    """A recorded process id: compares and hashes like the original one."""

    __slots__ = ('id', 'key', 'cls', 'short')

    def __init__(self, id, key, cls, short):
        self.id, self.key, self.cls, self.short = id, key, cls, short

    def __repr__(self):
        return '<%s>' % self.key

    def __eq__(self, other):
        return isinstance(other, ReplayPid) and other.id == self.id

    def __lt__(self, other):
        if not isinstance(other, ReplayPid):
            return NotImplemented
        return self.key < other.key

    def __hash__(self):
        return hash(self.id)


class ReplayObj(types.SimpleNamespace):
    """A recorded object that has fields."""

    def __init__(self, typename, fields):
        super().__init__(**fields)
        self.__dict__['_typename'] = typename

    def __repr__(self):
        return '%s(%s)' % (self._typename, ', '.join(
            '%s=%s' % (k, fmt(v)) for k, v in self.__dict__.items()
            if not k.startswith('_')))

    __hash__ = object.__hash__


class ReplayRepr(str):
    """A recorded value that was only kept as its repr()."""

    def __repr__(self):
        return str.__str__(self)


def decode_value(v, pids):
    """The inverse of `da.common.encode_value`; `pids` maps recorded process
    ids to `ReplayPid`s."""
    def items(xs):
        return [decode_value(x, pids) for x in xs
                if not (isinstance(x, dict) and '$more' in x)]

    if isinstance(v, list):
        return items(v)
    if not isinstance(v, dict):
        return v
    if '$pid' in v:
        p = pids.get(v['$pid'])
        if p is None:
            p = ReplayPid(v['$pid'], v.get('short', v['$pid']).strip('<>'),
                          v.get('cls', '?'), v.get('short', v['$pid']))
            pids[v['$pid']] = p
        return p
    if '$tuple' in v:
        return tuple(items(v['$tuple']))
    if '$set' in v:
        xs = items(v['$set'])
        try:
            return set(xs)
        except TypeError:
            return xs
    if '$dict' in v:
        out = {}
        for k, x in items(v['$dict']):
            try:
                out[k] = x
            except TypeError:
                out[ReplayRepr(fmt(k))] = x
        return out
    if '$obj' in v:
        return ReplayObj(v['$obj'], {k: decode_value(x, pids)
                                     for k, x in v.get('fields', {}).items()})
    if '$repr' in v:
        return ReplayRepr(v['$repr'])
    return v


def _pretty(v, depth=0):
    """repr() that is readable and stable: sets are sorted, process ids are
    short (from DistAlgo Live)."""
    if depth > 6:
        return '…'
    if isinstance(v, (str, bytes, int, float, bool, type(None), ReplayPid,
                      ReplayObj)):
        return repr(v)
    if isinstance(v, tuple):
        inner = ', '.join(_pretty(x, depth + 1) for x in v)
        return '(%s,)' % inner if len(v) == 1 else '(%s)' % inner
    if isinstance(v, list):
        return '[%s]' % ', '.join(_pretty(x, depth + 1) for x in v)
    if isinstance(v, (set, frozenset)):
        if not v:
            return 'set()'
        return '{%s}' % ', '.join(sorted(_pretty(x, depth + 1) for x in v))
    if isinstance(v, dict):
        return '{%s}' % ', '.join(
            '%s: %s' % (_pretty(k, depth + 1), _pretty(x, depth + 1))
            for k, x in v.items())
    return repr(v)


def fmt(v, limit=200):
    try:
        s = _pretty(v)
    except Exception as e:
        s = '<unprintable %s>' % type(e).__name__
    return s if len(s) <= limit else s[:limit - 1] + '…'


# --------------------------------------------------------------------------
# The program's source: blocks for the UI, and the code around each line
# --------------------------------------------------------------------------

_HEADER_KINDS = {ast.FunctionDef: 'def', ast.For: 'for', ast.While: 'while',
                 ast.If: 'if', ast.With: 'with', ast.Try: 'try'}
_CLASS_RE = re.compile(r'^class\s+(\w+)')
_HANDLER_RE = re.compile(r'^_\w+_handler_\d+$')


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


def split_blocks(lines):
    """[(name, first line, last line)] (1-based, inclusive): code before the
    first class ('setup'), one block per top-level class, and the rest
    ('main').  Unlike DistAlgo Live's split, no line is dropped, so a line
    of the file maps to a block by an offset."""
    matches = [(i + 1, m.group(1))
               for i, m in enumerate(map(_CLASS_RE.match, lines)) if m]
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


class SourceMap:
    """The program files a trace refers to, split into named blocks."""

    def __init__(self, files, sources=None):
        """`sources`: {file: source text} (from the data file); a file not
        in it is read from disk."""
        self.files = {}           # file -> {'lines', 'blocks', 'headers'}
        self.blocks = []          # [{'name', 'code', 'kind', 'file', 'start'}]
        self.text = {}            # file -> source text
        sources = sources or {}
        multi = len(files) > 1
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
            info = {'lines': lines, 'blocks': [], 'headers': {}}
            base = os.path.splitext(os.path.basename(fn))[0]
            for name, first, last in split_blocks(lines):
                bname = '%s.%s' % (base, name) if multi else name
                code = '\n'.join(lines[first - 1:last])
                kind = MAIN if name == MAIN else (
                    'process' if re.search(r'^\s*class\s+\w+\s*\([^)]*process',
                                           code, re.M) else 'code')
                info['blocks'].append((bname, first, last))
                self.blocks.append({'name': bname, 'code': code, 'kind': kind,
                                    'file': fn, 'start': first})
            info['headers'] = self._headers(src, fn)
            self.files[fn] = info

    @staticmethod
    def _headers(src, fn):
        """Headers from the compiled program, where an await is the loop the
        compiler generates for it; from the plain source if that fails."""
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
        info = self.files.get(fn)
        if info is None or line is None:
            return {'block': None, 'line': line, 'text': ''}
        for name, first, last in info['blocks']:
            if first <= line <= last:
                text = info['lines'][line - 1].strip() \
                    if 1 <= line <= len(info['lines']) else ''
                return {'block': name, 'line': line - first + 1,
                        'text': text}
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
            if kind == 'while' and 'await' in text and \
                    not text.startswith('while'):
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

    def file_line(self, block, line):
        """(file, line in file) for a line of a block."""
        for b in self.blocks:
            if b['name'] == block:
                return b['file'], b['start'] + line - 1
        return None, None


# --------------------------------------------------------------------------
# The data file: one recorded run in one file
# --------------------------------------------------------------------------

DATA_FORMAT = 'distalgo-replay'
DATA_VERSION = 1


def _read_source(fn):
    try:
        with open(fn) as f:
            return f.read()
    except (OSError, UnicodeDecodeError):
        return None


def _source_files(processes):
    """The program's source files that the events refer to."""
    files = set()
    for evs in processes.values():
        for e in evs:
            for fr in e.get('frames') or ():
                files.add(fr['file'])
            c = e.get('cause') or {}
            if c.get('file'):
                files.add(c['file'])
            if e.get('file'):
                files.add(e['file'])
    return {f for f in files if f}


def collect(logdir):
    """One recorded run as one data dict: every process' event log (the
    `<process>.events` files in `logdir`) and the source of the program
    files they refer to (the way viz.trace_to_clocks_and_state collects a
    run's traces for its page)."""
    files = sorted(glob.glob(os.path.join(logdir, '*.events')))
    if not files:
        raise ValueError(
            'no event logs (*.events) in %s: record a run with -T '
            '(--record-trace) using this version of DistAlgo' % logdir)
    processes = {}
    for fn in files:
        evs = []
        with open(fn) as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        evs.append(json.loads(line))
                    except ValueError:
                        pass           # a line cut off when the run stopped
        if evs:
            processes[evs[0]['pid']] = evs
    program = next((e.get('file') for evs in processes.values() for e in evs
                    if e['kind'] == 'run' and e.get('main')), None)
    sources = {}
    for fn in _source_files(processes):
        text = _read_source(fn)
        if text is not None:
            sources[fn] = text
    return {'format': DATA_FORMAT, 'version': DATA_VERSION,
            'program': program,
            'logdir': os.path.abspath(logdir),
            'recorded': min((evs[0].get('t', 0) for evs in processes.values()),
                            default=None),
            'sources': sources,
            'processes': processes}


def save(data, path):
    with open(path, 'w') as f:
        json.dump(data, f, separators=(',', ':'))
    return path


def load(path):
    """The data of a recorded run, from a data file or a log directory."""
    if os.path.isdir(path):
        data = collect(path)
    else:
        with open(path) as f:
            data = json.load(f)
        if not isinstance(data, dict) or data.get('format') != DATA_FORMAT:
            raise ValueError('%s is not a DistAlgo replay data file' % path)
    data['path'] = os.path.abspath(path)
    return data


# --------------------------------------------------------------------------
# The recorded run
# --------------------------------------------------------------------------

class Proc:
    def __init__(self, id, events):
        self.id = id
        self.events = events
        self.cls = next((e['cls'] for e in events if e.get('cls') and
                         e['kind'] in ('run', 'setup')), id.split('-')[0])
        self.is_main = any(e['kind'] == 'run' and e.get('main') for e in events)
        self.file = next((e.get('file') for e in events if e.get('file') and
                          e['kind'] in ('run', 'setup')), None)
        self.key = None
        self.pid = None           # ReplayPid
        self.segments = []        # [[event, ...]] each one a step
        self.pre = []             # events before the first step (setup)
        self.t0 = events[0].get('t', 0)


class Message:
    __slots__ = ('key', 'src', 'dst', 'sclk', 'value', 'send_ev', 'send_step',
                 'recv_ev', 'recv_step')

    def __init__(self):
        for f in self.__slots__:
            setattr(self, f, None)


class Trace:
    """All event logs of one run (a data file, see `collect`), ordered into
    steps."""

    def __init__(self, data):
        self.data = data
        self.procs = {}
        for pid, evs in data['processes'].items():
            if evs:
                evs = sorted(evs, key=lambda e: e.get('seq', 0))
                self.procs[pid] = Proc(pid, evs)
        if not self.procs:
            raise ValueError('the recorded run has no events')
        self._name_processes()
        self._segment()
        self._order()
        self._messages()
        main = data.get('program') or next(
            (p.file for p in self.procs.values() if p.is_main), None)
        files = sorted(_source_files(data['processes']),
                       key=lambda f: (f != main, f))
        self.src = SourceMap(files, data.get('sources'))
        self.main_file = main
        self.classes = sorted({p.cls for p in self.procs.values()
                               if not p.is_main})

    def decode(self, v):
        return decode_value(v, self.pids)

    # ---- naming: Cls:1, Cls:2, ... in creation order; main() is 'main'
    def _name_processes(self):
        order = {}
        for p in sorted(self.procs.values(), key=lambda p: p.t0):
            for e in p.events:
                if e['kind'] == 'new':
                    for c in e.get('children', []):
                        order.setdefault(c, (e['t'], len(order)))
        counts = collections.Counter()
        self.pids = {}
        for p in sorted(self.procs.values(),
                        key=lambda p: order.get(p.id, (p.t0, 1 << 30))):
            if p.is_main:
                p.key = p.cls = MAIN
            else:
                counts[p.cls] += 1
                p.key = '%s:%d' % (p.cls, counts[p.cls])
            # DistAlgo's short form: <Cls:last 5 hex digits of the uid>
            name, _, uid = p.id.partition('-')
            short = '<%s:%s>' % (name, uid.split('.')[0][-5:])
            p.pid = ReplayPid(p.id, p.key, p.cls, short)
            self.pids[p.id] = p.pid

    def key(self, pid):
        p = self.procs.get(pid)
        return p.key if p else str(pid)

    # ---- steps: one per run / recv / timeout
    def _segment(self):
        for p in self.procs.values():
            cur = None
            for e in p.events:
                if e['kind'] in STEP_KINDS:
                    cur = [e]
                    p.segments.append(cur)
                elif cur is None:
                    p.pre.append(e)
                else:
                    cur.append(e)

    def _order(self):
        """Topological order of all segments: each process' own order,
        send before receive, start() before the started process runs;
        otherwise by recorded time."""
        sends = collections.defaultdict(list)    # (src, dst, sclk) -> segment
        starts = {}                              # child -> segment
        for p in self.procs.values():
            for i, seg in enumerate(p.segments):
                for e in seg:
                    if e['kind'] == 'send':
                        for d in e.get('to', []):
                            sends[(p.id, d, e.get('sclk'))].append((p.id, i))
                    elif e['kind'] == 'startcall':
                        for c in e.get('procs', []):
                            starts.setdefault(c, (p.id, i))
        deps = {}             # (pid, segment index) -> segments it follows
        for p in self.procs.values():
            for i, seg in enumerate(p.segments):
                me = (p.id, i)
                deps[me] = {(p.id, i - 1)} if i > 0 else set()
                head = seg[0]
                if head['kind'] == 'recv':
                    cands = sends.get((head['src'], p.id, head.get('sclk')))
                    if cands:
                        deps[me].add(cands[0])
                if head['kind'] == 'run' and p.id in starts:
                    deps[me].add(starts[p.id])
                deps[me].discard(me)
        users = collections.defaultdict(set)
        for s, ds in deps.items():
            for d in ds:
                users[d].add(s)
        indeg = {s: len(ds) for s, ds in deps.items()}

        def prio(s):
            p = self.procs[s[0]]
            return (p.segments[s[1]][0].get('t', 0), p.key, s[1])
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
        self.steps = [{'pid': s[0], 'seg': s[1],
                       'events': self.procs[s[0]].segments[s[1]]}
                      for s in order]
        ts = [st['events'][0].get('t', 0) for st in self.steps]
        t0 = min(ts, default=0)
        # monotone step times (the order can put a later-recorded step first)
        self.times = [t - t0 for t in itertools.accumulate(ts, max)]

    def _messages(self):
        """Pair every send with its receive."""
        self.messages = []
        chan_count = collections.Counter()
        unmatched = collections.defaultdict(collections.deque)  # (src, dst)
        by_clock = {}

        def new(src, dst, e):
            m = Message()
            chan = (self.key(src), self.key(dst))
            chan_count[chan] += 1
            m.key = '%s>%s#%d' % (chan + (chan_count[chan],))
            m.src, m.dst, m.sclk = src, dst, e.get('sclk')
            m.value = self.decode(e.get('payload'))
            self.messages.append(m)
            return m

        for k, st in enumerate(self.steps):
            for e in st['events']:
                if e['kind'] != 'send':
                    continue
                for d in e.get('to', []):
                    m = new(st['pid'], d, e)
                    m.send_ev, m.send_step = e, k
                    unmatched[(m.src, d)].append(m)
                    if isinstance(m.sclk, int):
                        by_clock.setdefault((m.src, d, m.sclk), m)
        self.recv_msg = {}              # (pid, seq) of a recv -> Message
        for k, st in enumerate(self.steps):
            for e in st['events']:
                if e['kind'] != 'recv':
                    continue
                m = None
                if isinstance(e.get('sclk'), int):
                    m = by_clock.get((e['src'], st['pid'], e['sclk']))
                    if m is not None and m.recv_ev is not None:
                        m = None
                if m is None:
                    q = unmatched[(e['src'], st['pid'])]
                    while q and q[0].recv_ev is not None:
                        q.popleft()
                    m = q[0] if q else None
                if m is None:          # sender not recorded: make one up
                    m = new(e['src'], st['pid'], e)
                m.recv_ev, m.recv_step = e, k
                self.recv_msg[(st['pid'], e['seq'])] = m
        self.sent_msgs = collections.defaultdict(list)  # (pid, seq) of a send
        self.recv_index = {}               # (dst, src, sclk) -> Message
        for m in self.messages:
            if m.send_ev is not None:
                self.sent_msgs[(m.src, m.send_ev['seq'])].append(m)
            if m.recv_ev is not None:
                self.recv_index.setdefault((m.dst, m.src, m.sclk), m)


# --------------------------------------------------------------------------
# What a step looks like to the UI (the same shapes as DistAlgo Live)
# --------------------------------------------------------------------------

class ProcView:
    """What a condition sees of one process: its fields plus id, cls,
    status, clock, received, sent and inbox (as in DistAlgo Live)."""

    def __init__(self, st):
        self.__dict__.update(st['values'])
        self.id = st['pid']
        self.cls = st['cls']
        self.status = st['status']
        self.clock = st['clock']
        self.received = list(st['received'])
        self.sent = list(st['sent'])
        self.inbox = []

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
        self.src = trace.src
        self._at = None          # (step, procs, in flight) of the last at()
        self._build_history()

    # ---- explanations ------------------------------------------------
    def frame_loc(self, fr, main):
        d = self.src.loc(fr['file'], fr['line'])
        func = fr['func']
        d['func'] = MAIN if main and func == 'run' else (
            'receive' if _HANDLER_RE.match(func) else func)
        return d

    def _await_before(self, pid, seq):
        """Where `pid` was waiting just before its event `seq`."""
        for e in reversed(self.trace.procs[pid].events):
            if e['seq'] >= seq:
                continue
            if e['kind'] == 'await' and e.get('frames'):
                fr = e['frames'][0]
                return self.src.await_loc(fr['file'], fr['line'])
            if e['kind'] in ('run', 'setup'):
                break
        return None

    def _msg_for(self, pid, msg):
        """The Message a recorded {'src', 'sclk'} that `pid` received is."""
        if not msg:
            return None
        return self.trace.recv_index.get((pid, msg.get('src'),
                                          msg.get('sclk')))

    def _cause(self, pid, cause, seq):
        T = self.trace
        if not cause:
            return {'type': 'run'}
        if cause.get('type') == 'handler':
            m = self._msg_for(pid, cause.get('msg'))
            return {'type': 'handler',
                    'payload': fmt(m.value) if m else '?',
                    'src': T.key(m.src) if m else '?',
                    'msg': m.key if m else None,
                    'deliveredStep': m.recv_step if m else None,
                    'bindings': {k: fmt(T.decode(v))
                                 for k, v in (cause.get('bindings') or {}).items()}}
        after = cause.get('after') or {}
        out = {'type': 'run'}
        if after.get('type') == 'message':
            m = self._msg_for(pid, after)
            if m is not None:
                out['after'] = {'type': 'message', 'msg': m.key,
                                'src': T.key(m.src),
                                'payload': fmt(m.value),
                                'deliveredStep': m.recv_step,
                                'await': self._await_before(pid, m.recv_ev['seq'])}
        elif after.get('type') == 'timeout':
            out['after'] = {'type': 'timeout',
                            'await': self._await_before(pid, seq)}
        return out

    def _why(self, pid, e):
        frames = e.get('frames')
        cause = e.get('cause')
        main = self.trace.procs[pid].is_main
        stack = [self.frame_loc(fr, main) for fr in frames or ()]
        line, entries = None, []
        if frames:
            fr = frames[0]
            line = self.src.loc(fr['file'], fr['line'])
            entries = self.src.entry_chain(fr['file'], fr['line'])
            w = next((d for d in entries if d['kind'] == 'await'), None)
            if e['kind'] in ('await', 'handler') and w:
                # at an await's label: the label call has a nearby line
                entries = entries[entries.index(w):]
                line = {k: v for k, v in w.items() if k != 'kind'}
                stack[0].update(line=w['line'], text=w['text'])
        elif cause and cause.get('type') == 'handler' and cause.get('file'):
            line = self.src.loc(cause['file'], cause['line'])
        return {'line': line or {'block': None, 'line': None, 'text': ''},
                'entries': entries, 'stack': stack,
                'cause': self._cause(pid, cause, e['seq'])}

    # ---- history (events, changes, logs), computed once ---------------
    def _build_history(self):
        T = self.trace
        self.events, self.changes, self.logs = [], [], []
        self.step_lines = []                  # step -> set of (file, line)
        procs, net = {}, {}

        def log(k, pid, text, kind='system'):
            self.logs.append({'step': k, 'pid': pid, 'kind': kind,
                              'text': text})

        for k, st in enumerate(T.steps):
            pid = st['pid']
            p = T.procs[pid]
            # the fields changed since the process' last event: by the
            # handler that just ran ('handled'), or else by run()
            for e, fields, old in self._apply(procs, net, k):
                why = self._why(pid, e)
                for f in fields:
                    self.changes.append({
                        'step': k, 'pid': p.key, 'field': f,
                        'old': fmt(T.decode(old[f])) if f in old else None,
                        'new': fmt(T.decode(e['state'][f])), 'why': why})
            lines = set()
            for e in st['events']:
                kind = e['kind']
                for fr in (e.get('frames') or ())[:1]:
                    lines.add((fr['file'], fr['line']))
                c = e.get('cause') or {}
                if kind == 'handled' and c.get('file'):
                    lines.add((c['file'], c['line']))
                if kind == 'run':
                    log(k, p.key, 'starts running')
                elif kind == 'recv':
                    m = T.recv_msg.get((pid, e['seq']))
                    if m is not None:
                        self.events.append({
                            'kind': 'recv', 'step': k, 'src': T.key(m.src),
                            'dst': p.key, 'key': m.key, 'payload': fmt(m.value)})
                        log(k, p.key, 'delivered %s from <%s>'
                            % (fmt(m.value), T.key(m.src)))
                elif kind == 'timeout':
                    log(k, p.key, 'timeout fired')
                elif kind == 'send':
                    why = self._why(pid, e)
                    for m in T.sent_msgs[(pid, e['seq'])]:
                        self.events.append({
                            'kind': 'send', 'step': k, 'src': p.key,
                            'dst': T.key(m.dst), 'key': m.key,
                            'payload': fmt(m.value), 'why': why})
                elif kind == 'output':
                    log(k, p.key, e.get('text', ''), 'print')
                elif kind == 'new':
                    for c in e.get('children', []):
                        log(k, p.key, 'created %r' % T.pids.get(c, c))
                elif kind == 'end':
                    status = e.get('status')
                    text = {'done': 'run() finished', 'exited': 'exited',
                            'crashed': 'crashed: %s' % e.get('error')}.get(
                                status, status)
                    log(k, p.key, text,
                        'error' if status == 'crashed' else 'system')
            self.step_lines.append(lines)

    # ---- the system at a step ----------------------------------------
    def _setup_state(self, pid):
        """A process' fields after its setup() (before its first step)."""
        state = {}
        for e in self.trace.procs[pid].pre:
            state.update(e.get('state') or {})
        return state

    def _proc(self, procs, pid):
        """The state of `pid` in `procs`, made when first needed; None for a
        process that was not recorded."""
        p = self.trace.procs.get(pid)
        if p is not None and pid not in procs:
            procs[pid] = {'pid': p.pid, 'key': p.key, 'cls': p.cls,
                          'main': p.is_main, 'status': 'created',
                          'state': {}, 'values': {}, 'frames': [],
                          'waiting': None, 'clock': None, 'sent': [],
                          'received': [], 'error': None}
        return procs.get(pid)

    def _setup(self, s, pid):
        s['state'] = self._setup_state(pid)
        s['values'] = {f: self.trace.decode(v) for f, v in s['state'].items()}

    def _apply(self, procs, net, k):
        """Applies step k to `procs` ({pid: state}) and `net` ({key: message
        in flight}); returns the field changes [(event, fields, old state)]."""
        T = self.trace
        st = T.steps[k]
        pid = st['pid']
        s = self._proc(procs, pid)
        if s['status'] in ('created', 'setup', 'ready'):
            self._setup(s, pid)
        s['status'] = 'running'
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
                s['values'] = dict(s['values'], **{
                    f: T.decode(e['state'][f]) for f in fields})
            if e.get('frames'):
                s['frames'] = e['frames']     # where it is
            if e.get('locals') is not None:
                s['main_locals'] = e['locals']   # main()'s variables
            if kind == 'new':
                for c in e.get('children', []):
                    self._proc(procs, c)
            elif kind == 'setupcall':
                for c in e.get('procs', []):
                    cs = self._proc(procs, c)
                    if cs is not None:
                        self._setup(cs, c)
                        if cs['status'] == 'created':
                            cs['status'] = 'setup'
            elif kind == 'startcall':
                for c in e.get('procs', []):
                    cs = self._proc(procs, c)
                    if cs is not None and cs['status'] in ('created', 'setup'):
                        cs['status'] = 'ready'
            elif kind == 'send':
                for m in T.sent_msgs[(pid, e['seq'])]:
                    s['sent'].append((m.value, T.pids.get(m.dst, m.dst)))
                    if m.recv_step is None or m.recv_step > k:
                        net[m.key] = m
            elif kind == 'recv':
                m = T.recv_msg.get((pid, e['seq']))
                if m is not None:
                    s['received'].append((m.value, T.pids.get(m.src, m.src)))
                    net.pop(m.key, None)
            elif kind == 'await':
                s['status'] = 'blocked'
                s['frames'] = e.get('frames') or []
                if e.get('frames'):
                    fr = e['frames'][0]
                    w = self.src.await_loc(fr['file'], fr['line'])
                    w['timeout'] = e.get('timeout')
                    s['waiting'] = w
            elif kind == 'end':
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
        views = {s['pid']: ProcView(s) for s in procs.values()
                 if not s['main']}

        def viewify(v, depth=0):
            if isinstance(v, ReplayPid):
                return views.get(v, v)
            if depth < 2 and isinstance(v, (set, frozenset, list, tuple)):
                items = [viewify(x, depth + 1) for x in v]
                return items if not isinstance(v, tuple) else tuple(items)
            return v

        scope = {}
        for s in procs.values():
            if s['main']:
                # main()'s variables, as of its latest recorded event
                for name, v in (s.get('main_locals') or {}).items():
                    scope[name] = viewify(T.decode(v))
        allv = list(views.values())
        scope.update({'procs': allv,
                      'network': [(views.get(T.pids.get(m.src), m.src),
                                   views.get(T.pids.get(m.dst), m.dst),
                                   m.value) for m in net],
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
        head = st['events'][0]
        if head['kind'] == 'recv':
            m = self.trace.recv_msg.get((st['pid'], head['seq']))
            return {'type': 'deliver', 'msg': m.key if m else None}
        key = self.trace.procs[st['pid']].key
        return {'type': 'timeout' if head['kind'] == 'timeout' else 'run',
                'pid': key}


def _upto(items, k):
    """The items of steps up to k (`items` is sorted by step)."""
    n = 0
    while n < len(items) and items[n]['step'] <= k:
        n += 1
    return items[:n]


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
        try:     # main() is not run
            code = _da_compile(daui.dastr_to_pycode, src, fn)
            ns = {'__name__': '__dareplay__', '__file__': fn}
            if code is not None:
                with _quiet_compiler():
                    exec(code, ns)
            self.namespace = ns
        except Exception:
            pass

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
            g = dict(self.namespace, da=da)
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
    def names(scope):
        return [n for n in scope if re.match(r'^[A-Za-z_]\w*$', n)]

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

    HISTORY = ('events', 'logs', 'changes')

    def __init__(self, path):
        self.trace = Trace(load(path))
        self.src = self.trace.src
        self.timeline = Timeline(self.trace)
        self.conds = Conditions(self.trace)
        self.cursor = 0
        self.gen = 1
        self.last_stop = None
        self.breaklines = set()           # (file, line)
        self.now = self.timeline.time(0)

    def snapshot(self, since=None):
        T, tl, k = self.trace, self.timeline, self.cursor
        states, net = tl.at(k)
        procs = []
        for pid, s in states.items():
            frames = [tl.frame_loc(fr, s['main'])
                      for fr in reversed(s['frames'])]   # outermost first
            w = s['waiting']
            if frames and w:
                # a blocked process is at its await (as its card shows), not
                # the nearby line the compiler gives the blocking label call
                frames[-1].update(block=w['block'], line=w['line'],
                                  text=w['text'])
            procs.append({
                'id': s['key'], 'cls': s['cls'], 'realId': pid,
                'status': s['status'], 'isMain': s['main'],
                'waitingAt': w, 'pausedAt': None, 'clock': s['clock'],
                'state': {f: fmt(v) for f, v in s['values'].items()},
                'frames': frames, 'queue': [],
                'sent': len(s['sent']), 'received': len(s['received']),
                'error': s['error']})
        network = [{'key': m.key, 'src': T.key(m.src), 'dst': T.key(m.dst),
                    'payload': fmt(m.value), 'sentStep': m.send_step,
                    'sentAt': round(tl.time(m.send_step), 6),
                    'arrives': None if m.recv_step is None
                    else round(tl.time(m.recv_step), 6),
                    'recvStep': m.recv_step} for m in net]
        scope = tl.scope(k)
        snap = {
            'running': True, 'gen': self.gen, 'delta': False,
            'step': k, 'time': round(self.now, 6),
            'processes': procs, 'network': network,
            'events': _upto(tl.events, k),
            'changes': _upto(tl.changes, k),
            'logs': _upto(tl.logs, k),
            'next': tl.action(k + 1),
            'classes': T.classes,
            'timers': [],
            'cursor': k,
            'redo': max(0, len(T.steps) - 1 - k),
            'lastAction': tl.action(k),
            'paused': None,
            'lastStop': self.last_stop,
            'conditions': [
                {'id': c['id'], 'expr': c['expr'], 'enabled': c['enabled'],
                 'value': v, 'error': err}
                for c, (_, v, err) in zip(self.conds.items,
                                          self.conds.evaluate(lambda: scope))],
            'conditionNames': Conditions.names(scope),
            'conditionExample': Conditions.example(scope),
        }
        if since and since.get('gen') == self.gen:
            # The history lists only grow with the step, so the page needs
            # only what it does not have yet; after a step back, only how
            # many items to keep ('trim').
            snap['delta'] = True
            snap['trim'] = {}
            for h in self.HISTORY:
                have = max(0, int(since.get(h, 0)))
                if have <= len(snap[h]):
                    snap[h] = snap[h][have:]
                else:
                    snap['trim'][h] = len(snap[h])
                    snap[h] = []
        return snap

    def info(self):
        T = self.trace
        return {'logdir': T.data.get('path'),
                'program': T.main_file,
                'steps': len(T.steps),
                'processes': len(T.procs),
                'messages': len(T.messages),
                'blocks': [{'name': b['name'], 'code': b['code'],
                            'kind': b['kind']} for b in self.src.blocks]}

    # ---- moving through the steps ---------------------------------------
    def goto(self, k):
        self.cursor = max(0, min(int(k), len(self.trace.steps) - 1))
        self.now = self.timeline.time(self.cursor)

    def forward(self):
        if self.cursor + 1 >= len(self.trace.steps):
            return False
        self.goto(self.cursor + 1)
        return True

    def back(self):
        if self.cursor == 0:
            return False
        self.goto(self.cursor - 1)
        return True

    def _hit_line(self, n):
        """The breakpoint line the step at the cursor reaches, the n-th step
        of a run."""
        hits = self.timeline.step_lines[self.cursor] & self.breaklines
        if not hits:
            return None
        fn, line = min(hits)
        return {'reason': 'line', 'steps': n, 'at': self.src.loc(fn, line),
                'pid': self.trace.procs[self.trace.steps[self.cursor]['pid']].key}

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

    def set_breaklines(self, blocks_lines):
        """blocks_lines: {block name: [line, ...]} (1-based, per block)."""
        out = set()
        for name, lines in (blocks_lines or {}).items():
            for ln in lines:
                fn, line = self.src.file_line(name, int(ln))
                if fn:
                    out.add((fn, line))
        self.breaklines = out


# --------------------------------------------------------------------------
# HTTP server (from DistAlgo Live's server.py)
# --------------------------------------------------------------------------

SESSION = None


def api(path, body):
    s = SESSION
    if path in ('/api/load', '/api/breakpoints'):
        s.set_breaklines(body.get('breakpoints', {}))
        s.conds.set(body.get('conditions', []))
        if path == '/api/load':
            s.last_stop = None
            return dict(s.snapshot(), info=s.info())
    elif path == '/api/step':
        s.last_stop = None
        if s.forward():
            s.last_stop = s._hit_line(1)
    elif path == '/api/back':
        s.last_stop = None
        s.back()
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
            data.pop('path', None)
            save(data, args.save)
            print('Wrote %s (%d processes)' % (args.save,
                                                len(data['processes'])))
            return
        serve(args.data, args.port, not args.no_browser)
    except (ValueError, OSError) as e:
        sys.exit(str(e))


if __name__ == '__main__':
    main()

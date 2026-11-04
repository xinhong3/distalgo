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
import glob
import heapq
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

import contextlib
import io
from da.compiler import utils as dautils

HERE = os.path.dirname(os.path.abspath(__file__))
UI_ROOT = os.path.join(os.path.dirname(HERE), 'ui')
STATIC = os.path.join(UI_ROOT, 'replay')
STEP_KINDS = ('run', 'recv', 'timeout')
MAIN = 'main'


@contextlib.contextmanager
def _quiet_compiler(buf):
    """Sends the compiler's messages to `buf` (its printers bind sys.stderr
    as a default argument, so redirecting stderr is not enough; from
    DistAlgo Live)."""
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
    if isinstance(v, list):
        return [decode_value(x, pids) for x in v if not _is_more(x)]
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
        return tuple(decode_value(x, pids) for x in v['$tuple']
                     if not _is_more(x))
    if '$set' in v:
        items = [decode_value(x, pids) for x in v['$set'] if not _is_more(x)]
        try:
            return set(items)
        except TypeError:
            return items
    if '$dict' in v:
        out = {}
        for kv in v['$dict']:
            if _is_more(kv):
                continue
            k = decode_value(kv[0], pids)
            try:
                out[k] = decode_value(kv[1], pids)
            except TypeError:
                out[ReplayRepr(fmt(k))] = decode_value(kv[1], pids)
        return out
    if '$obj' in v:
        return ReplayObj(v['$obj'], {k: decode_value(x, pids)
                                     for k, x in v.get('fields', {}).items()})
    if '$repr' in v:
        return ReplayRepr(v['$repr'])
    return v


def _is_more(x):
    return isinstance(x, dict) and '$more' in x


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

_HEADER_KINDS = ((ast.FunctionDef, 'def'), (ast.For, 'for'),
                 (ast.While, 'while'), (ast.If, 'if'), (ast.With, 'with'),
                 (ast.Try, 'try'))
_CLASS_RE = re.compile(r'^class\s+(\w+)')
_HANDLER_RE = re.compile(r'^_\w+_handler_\d+$')


def _index_headers(module):
    """line -> tuple of enclosing (header line, kind), outermost first
    (from DistAlgo Live)."""
    index = {}

    def kind_of(s):
        for t, k in _HEADER_KINDS:
            if isinstance(s, t):
                return k
        return None

    def visit(stmts, chain):
        for s in stmts:
            index.setdefault(s.lineno, chain)
            k = kind_of(s)
            if isinstance(s, ast.ClassDef):
                visit(s.body, chain)
            elif k is not None:
                inner = chain + ((s.lineno, k),)
                visit(s.body, inner)
                if getattr(s, 'orelse', None):
                    visit(s.orelse, inner)
                for h in getattr(s, 'handlers', []) or []:
                    visit(h.body, inner)
                if getattr(s, 'finalbody', None):
                    visit(s.finalbody, inner)
    visit(module.body, ())
    return index


def split_blocks(lines):
    """[(name, first line, last line)] (1-based, inclusive): code before the
    first class ('setup'), one block per top-level class, and the rest
    ('main').  Unlike DistAlgo Live's split, no line is dropped, so a line
    of the file maps to a block by an offset."""
    starts = [(i + 1, _CLASS_RE.match(l).group(1))
              for i, l in enumerate(lines) if _CLASS_RE.match(l)]
    n = len(lines)
    if not starts:
        return [(MAIN, 1, max(n, 1))]
    blocks = []
    if any(l.strip() for l in lines[:starts[0][0] - 1]):
        blocks.append(('setup', 1, starts[0][0] - 1))
    else:
        starts[0] = (1, starts[0][1])
    for k, (first, name) in enumerate(starts):
        last = starts[k + 1][0] - 1 if k + 1 < len(starts) else n
        if k + 1 == len(starts):
            # the last class ends at the next unindented statement
            for j in range(first, n):
                l = lines[j]
                if l.strip() and not l[0].isspace() and \
                        not l.lstrip().startswith('#'):
                    blocks.append((name, first, j))
                    blocks.append((MAIN, j + 1, n))
                    break
            else:
                blocks.append((name, first, last))
        else:
            blocks.append((name, first, last))
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
            args = daui.parse_compiler_args([])
            with _quiet_compiler(io.StringIO()):
                pyast = daui.dastr_to_pyast(src, fn, args)
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


def data_file_name(program):
    """`<program>.replay.json` in the current directory (where viz writes
    `<program>.html`)."""
    base = os.path.splitext(os.path.basename(program or 'run'))[0]
    return base + '.replay.json'


# --------------------------------------------------------------------------
# The recorded run
# --------------------------------------------------------------------------

class Proc:
    def __init__(self, id, events):
        self.id = id
        self.events = events
        first = events[0] if events else {}
        self.cls = next((e['cls'] for e in events if e.get('cls') and
                         e['kind'] in ('run', 'setup')), id.split('-')[0])
        self.is_main = any(e['kind'] == 'run' and e.get('main') for e in events)
        self.parent = next((e.get('parent') for e in events
                            if e['kind'] in ('run', 'setup')), None)
        self.file = next((e.get('file') for e in events if e.get('file') and
                          e['kind'] in ('run', 'setup')), None)
        self.key = None
        self.pid = None           # ReplayPid
        self.segments = []        # [[event, ...]] each one a step
        self.pre = []             # events before the first step (setup)
        self.t0 = first.get('t', 0)


class Message:
    __slots__ = ('key', 'src', 'dst', 'sclk', 'payload', 'send_ev', 'send_step',
                 'recv_ev', 'recv_step', 'why', 'value')

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
        self.by_key = {p.key: p for p in self.procs.values()}

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
        segs = [(p.id, i) for p in self.procs.values()
                for i in range(len(p.segments))]
        where = {}            # (pid, seq) -> (pid, segment index)
        for p in self.procs.values():
            for i, seg in enumerate(p.segments):
                for e in seg:
                    where[(p.id, e['seq'])] = (p.id, i)
        sends = collections.defaultdict(list)    # (src, dst, sclk) -> where
        starts = {}                              # child -> where
        for p in self.procs.values():
            for i, seg in enumerate(p.segments):
                for e in seg:
                    if e['kind'] == 'send':
                        for d in e.get('to', []):
                            sends[(p.id, d, e.get('sclk'))].append((p.id, i))
                    elif e['kind'] == 'startcall':
                        for c in e.get('procs', []):
                            starts.setdefault(c, (p.id, i))
        deps = collections.defaultdict(set)
        for p in self.procs.values():
            for i, seg in enumerate(p.segments):
                me = (p.id, i)
                if i > 0:
                    deps[me].add((p.id, i - 1))
                head = seg[0]
                if head['kind'] == 'recv':
                    cands = sends.get((head['src'], p.id, head.get('sclk')))
                    if cands:
                        deps[me].add(cands[0])
                if head['kind'] == 'run' and p.id in starts:
                    deps[me].add(starts[p.id])
        for me in list(deps):
            deps[me].discard(me)
        users = collections.defaultdict(set)
        indeg = {}
        for s in segs:
            indeg[s] = len(deps[s])
            for d in deps[s]:
                users[d].add(s)
        def prio(s):
            p = self.procs[s[0]]
            return (p.segments[s[1]][0].get('t', 0), p.key, s[1])
        heap = [(prio(s), s) for s in segs if indeg[s] == 0]
        heapq.heapify(heap)
        order = []
        while heap:
            _, s = heapq.heappop(heap)
            order.append(s)
            for u in users[s]:
                indeg[u] -= 1
                if indeg[u] == 0:
                    heapq.heappush(heap, (prio(u), u))
        if len(order) < len(segs):       # inconsistent trace: keep the rest
            done = set(order)
            order += sorted((s for s in segs if s not in done), key=prio)
        self.steps = [{'pid': s[0], 'seg': s[1],
                       'events': self.procs[s[0]].segments[s[1]]}
                      for s in order]
        self.step_of = {}               # (pid, seq) -> step index
        for k, st in enumerate(self.steps):
            for e in st['events']:
                self.step_of[(st['pid'], e['seq'])] = k
        ts = [st['events'][0].get('t', 0) for st in self.steps]
        self.t0 = min(ts) if ts else 0
        # monotone step times (the order can put a later-recorded step first)
        self.times = []
        last = 0.0
        for t in ts:
            last = max(last, t - self.t0)
            self.times.append(last)

    def _messages(self):
        """Pair every send with its receive."""
        self.messages = []
        chan_count = collections.Counter()
        unmatched = collections.defaultdict(collections.deque)  # (src, dst)
        by_clock = {}
        for k, st in enumerate(self.steps):
            p = self.procs[st['pid']]
            for e in st['events']:
                if e['kind'] != 'send':
                    continue
                for d in e.get('to', []):
                    m = Message()
                    chan_count[(p.key, self.key(d))] += 1
                    m.key = '%s>%s#%d' % (p.key, self.key(d),
                                          chan_count[(p.key, self.key(d))])
                    m.src, m.dst, m.sclk = p.id, d, e.get('sclk')
                    m.payload = e.get('payload')
                    m.value = decode_value(m.payload, self.pids)
                    m.send_ev, m.send_step = e, k
                    m.recv_ev = m.recv_step = None
                    self.messages.append(m)
                    unmatched[(p.id, d)].append(m)
                    if isinstance(m.sclk, int):
                        by_clock.setdefault((p.id, d, m.sclk), m)
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
                    m = Message()
                    chan_count[(self.key(e['src']), self.key(st['pid']))] += 1
                    m.key = '%s>%s#%d' % (
                        self.key(e['src']), self.key(st['pid']),
                        chan_count[(self.key(e['src']), self.key(st['pid']))])
                    m.src, m.dst, m.sclk = e['src'], st['pid'], e.get('sclk')
                    m.payload = e.get('payload')
                    m.value = decode_value(m.payload, self.pids)
                    m.send_ev = m.send_step = None
                    self.messages.append(m)
                m.recv_ev, m.recv_step = e, k
                self.recv_msg[(st['pid'], e['seq'])] = m
        self.msg_by_key = {m.key: m for m in self.messages}
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


class ReplaySession:
    """A recorded run and a cursor into its steps.  It answers the same
    requests as DistAlgo Live's Session, so the page can be shared."""

    HISTORY = ('events', 'logs', 'changes')

    def __init__(self, path):
        self.trace = Trace(load(path))
        self.src = self.trace.src
        self.cursor = 0
        self.gen = 1
        self.now = 0.0
        self.last_stop = None
        self.conditions = []
        self.breaklines = set()           # (file, line)
        self._build_history()
        self._load_program_namespace()
        self.now = self._time(0)

    # ---- explanations ------------------------------------------------
    def _frames_locs(self, frames):
        out = []
        for fr in frames or ():
            d = self.src.loc(fr['file'], fr['line'])
            d['func'] = 'receive' if _HANDLER_RE.match(fr['func']) else fr['func']
            out.append(d)
        return out

    def _await_before(self, pid, seq):
        """Where `pid` was waiting just before its event `seq`."""
        p = self.trace.procs[pid]
        for e in reversed(p.events):
            if e['seq'] < seq and e['kind'] == 'await' and e.get('frames'):
                fr = e['frames'][0]
                return self.src.await_loc(fr['file'], fr['line'])
            if e['seq'] < seq and e['kind'] in ('run', 'setup'):
                break
        return None

    def _msg_for(self, pid, msg):
        """The Message a recorded {'src', 'sclk'} that `pid` received is."""
        if not msg:
            return None
        return self.trace.recv_index.get((pid, msg.get('src'),
                                          msg.get('sclk')))

    def _cause(self, pid, cause, seq):
        if not cause:
            return {'type': 'run'}
        if cause.get('type') == 'handler':
            m = self._msg_for(pid, cause.get('msg'))
            pids = self.trace.pids
            return {'type': 'handler',
                    'payload': fmt(m.value) if m else '?',
                    'src': self.trace.key(m.src) if m else '?',
                    'msg': m.key if m else None,
                    'deliveredStep': m.recv_step if m else None,
                    'bindings': {k: fmt(decode_value(v, pids))
                                 for k, v in (cause.get('bindings') or {}).items()}}
        after = cause.get('after') or {}
        out = {'type': 'run'}
        if after.get('type') == 'message':
            m = self._msg_for(pid, after)
            if m is not None:
                out['after'] = {'type': 'message', 'msg': m.key,
                                'src': self.trace.key(m.src),
                                'payload': fmt(m.value),
                                'deliveredStep': m.recv_step,
                                'await': self._await_before(pid, m.recv_ev['seq'])}
        elif after.get('type') == 'timeout':
            out['after'] = {'type': 'timeout',
                            'await': self._await_before(pid, seq)}
        return out

    def _why(self, pid, e, step, frames=None):
        frames = frames if frames is not None else e.get('frames')
        cause = e.get('cause')
        if frames:
            fr = frames[0]
            line = self.src.loc(fr['file'], fr['line'])
            entries = self.src.entry_chain(fr['file'], fr['line'])
        elif cause and cause.get('type') == 'handler' and cause.get('file'):
            line = self.src.loc(cause['file'], cause['line'])
            entries = []
        else:
            line, entries = None, []
        return {'line': line or {'block': None, 'line': None, 'text': ''},
                'entries': entries, 'stack': self._frames_locs(frames),
                'cause': self._cause(pid, cause, e['seq']), 'step': step}

    # ---- history (events, changes, logs), computed once ---------------
    def _build_history(self):
        T = self.trace
        events, changes, logs = [], [], []
        self._step_lines = []                 # step -> set of (file, line)
        last_state = {}
        for k, st in enumerate(T.steps):
            pid = st['pid']
            p = T.procs[pid]
            lines = set()
            if k == 0 or pid not in last_state:
                for e in p.pre:
                    if e.get('state') is not None:
                        last_state[pid] = e['state']
            for e in st['events']:
                kind = e['kind']
                for fr in (e.get('frames') or ())[:1]:
                    lines.add((fr['file'], fr['line']))
                if kind == 'handler':
                    c = e.get('cause') or {}
                    if c.get('file'):
                        lines.add((c['file'], c['line']))
                if kind == 'run':
                    logs.append(self._log(k, p.key, 'starts running',
                                          'system'))
                elif kind == 'recv':
                    m = T.recv_msg.get((pid, e['seq']))
                    if m is not None:
                        events.append({'kind': 'recv', 'step': k,
                                       'src': T.key(m.src), 'dst': p.key,
                                       'key': m.key, 'payload': fmt(m.value),
                                       'sendStep': m.send_step,
                                       'clk': e.get('clk')})
                        logs.append(self._log(k, p.key, 'delivered %s from %s'
                                              % (fmt(m.value),
                                                 '<%s>' % T.key(m.src)),
                                              'system'))
                elif kind == 'timeout':
                    logs.append(self._log(k, p.key, 'timeout fired', 'system'))
                elif kind == 'send':
                    why = self._why(pid, e, k)
                    for m in T.sent_msgs[(pid, e['seq'])]:
                            m.why = why
                            events.append({'kind': 'send', 'step': k,
                                           'src': p.key, 'dst': T.key(m.dst),
                                           'key': m.key,
                                           'payload': fmt(m.value),
                                           'clk': m.sclk, 'why': why})
                elif kind == 'output':
                    logs.append(self._log(k, p.key, e.get('text', ''),
                                          'print'))
                elif kind == 'new':
                    for c in e.get('children', []):
                        logs.append(self._log(k, p.key, 'created %r' %
                                              T.pids.get(c, c), 'system'))
                elif kind == 'end':
                    st_ = e.get('status')
                    text = {'done': 'run() finished', 'exited': 'exited',
                            'crashed': 'crashed: %s' % e.get('error')}.get(
                                st_, st_)
                    logs.append(self._log(k, p.key, text,
                                          'error' if st_ == 'crashed'
                                          else 'system'))
                if kind == 'setupcall':
                    # the children's setup() ran now
                    for c in e.get('procs', []):
                        cp = T.procs.get(c)
                        if cp is None:
                            continue
                        for ce in cp.pre:
                            if ce['kind'] == 'setup' and \
                                    ce.get('state') is not None:
                                last_state[c] = ce['state']
                if e.get('state') is not None:
                    old = last_state.get(pid, {})
                    new = e['state']
                    if old != new:
                        why = self._why(pid, e, k)
                        for f in list(new) + [f for f in old if f not in new]:
                            if old.get(f) != new.get(f):
                                changes.append({
                                    'step': k, 'pid': p.key, 'field': f,
                                    'old': fmt(decode_value(old[f], T.pids))
                                    if f in old else None,
                                    'new': fmt(decode_value(new.get(f),
                                                            T.pids)),
                                    'why': why})
                    last_state[pid] = new
            self._step_lines.append(lines)
        self.events, self.changes, self.logs = events, changes, logs

    @staticmethod
    def _log(step, pid, text, kind):
        return {'step': step, 'pid': pid, 'kind': kind, 'text': text}

    def _prefix(self, items):
        """The items of steps up to the cursor (lists are sorted by step)."""
        n = 0
        while n < len(items) and items[n]['step'] <= self.cursor:
            n += 1
        return items[:n]

    # ---- the system at the cursor ------------------------------------
    def _states(self, upto=None):
        """{pid: process state} after steps 0..upto."""
        T = self.trace
        upto = self.cursor if upto is None else upto
        out = collections.OrderedDict()

        def ensure(pid):
            if pid not in out:
                p = T.procs.get(pid)
                if p is None:
                    return None
                out[pid] = {'pid': p.pid, 'key': p.key, 'cls': p.cls,
                            'main': p.is_main, 'status': 'created',
                            'state': {}, 'frames': [], 'waiting': None,
                            'clock': None, 'sent': [], 'received': [],
                            'error': None}
            return out[pid]

        for k in range(min(upto, len(T.steps) - 1) + 1):
            st = T.steps[k]
            s = ensure(st['pid'])
            if s is None:
                continue
            if s['status'] in ('created', 'setup', 'ready'):
                for e in T.procs[st['pid']].pre:
                    if e.get('state') is not None and not s['state']:
                        s['state'] = e['state']
            s['status'] = 'running'
            s['waiting'] = None
            for e in st['events']:
                kind = e['kind']
                s['clock'] = e.get('clk', s['clock'])
                if e.get('state') is not None:
                    s['state'] = e['state']
                if e.get('frames'):
                    # where it is (with locals at awaits)
                    s['frames'] = e['frames']
                if kind == 'new':
                    for c in e.get('children', []):
                        ensure(c)
                elif kind == 'setupcall':
                    for c in e.get('procs', []):
                        cs = ensure(c)
                        if cs is None:
                            continue
                        for ce in T.procs[c].pre:
                            if ce['kind'] == 'setup' and \
                                    ce.get('state') is not None:
                                cs['state'] = ce['state']
                        if cs['status'] == 'created':
                            cs['status'] = 'setup'
                elif kind == 'startcall':
                    for c in e.get('procs', []):
                        cs = ensure(c)
                        if cs is not None and cs['status'] in ('created',
                                                               'setup'):
                            cs['status'] = 'ready'
                elif kind == 'send':
                    for m in T.sent_msgs[(st['pid'], e['seq'])]:
                        s['sent'].append((m.value, T.pids.get(m.dst, m.dst)))
                elif kind == 'recv':
                    m = T.recv_msg.get((st['pid'], e['seq']))
                    if m is not None:
                        s['received'].append((m.value, T.pids.get(m.src,
                                                                  m.src)))
                elif kind == 'await':
                    s['status'] = 'blocked'
                    s['frames'] = e.get('frames') or []
                    if e.get('frames'):
                        fr = e['frames'][0]
                        w = self.src.await_loc(fr['file'], fr['line'])
                        w['timeout'] = e.get('timeout')
                        s['waiting'] = w
                elif kind in ('handler', 'output', 'handled', 'timeout'):
                    if kind == 'timeout':
                        s['status'] = 'running'
                elif kind == 'end':
                    s['status'] = e.get('status') or 'done'
                    s['error'] = e.get('error')
                    s['frames'] = []
                    if e.get('locals'):
                        s['main_locals'] = e['locals']
        for s in out.values():
            s['values'] = {k: decode_value(v, T.pids)
                           for k, v in s['state'].items()}
        return out

    def _network(self, upto=None):
        upto = self.cursor if upto is None else upto
        return [m for m in self.trace.messages
                if m.send_step is not None and m.send_step <= upto and
                (m.recv_step is None or m.recv_step > upto)]

    def _time(self, k):
        if not self.trace.times:
            return 0.0
        return self.trace.times[max(0, min(k, len(self.trace.times) - 1))]

    def _action(self, k):
        """The step k as a DistAlgo Live scheduler action."""
        if k is None or not (0 <= k < len(self.trace.steps)):
            return None
        st = self.trace.steps[k]
        head = st['events'][0]
        key = self.trace.procs[st['pid']].key
        if head['kind'] == 'recv':
            m = self.trace.recv_msg.get((st['pid'], head['seq']))
            return {'type': 'deliver', 'msg': m.key if m else None}
        if head['kind'] == 'timeout':
            return {'type': 'timeout', 'pid': key}
        return {'type': 'run', 'pid': key}

    def snapshot(self, since=None):
        T = self.trace
        states = self._states()
        procs = []
        for pid, s in states.items():
            frames = []
            for fr in reversed(s['frames']):       # outermost first
                d = self.src.loc(fr['file'], fr['line'])
                d['func'] = MAIN if (s['main'] and fr['func'] == 'run') else (
                    'receive' if _HANDLER_RE.match(fr['func']) else fr['func'])
                d['locals'] = {k: fmt(decode_value(v, T.pids))
                               for k, v in (fr.get('locals') or {}).items()}
                frames.append(d)
            procs.append({
                'id': s['key'], 'cls': s['cls'], 'realId': pid,
                'status': s['status'], 'isMain': s['main'],
                'waitingAt': s['waiting'], 'pausedAt': None,
                'clock': s['clock'],
                'state': {k: fmt(v) for k, v in s['values'].items()},
                'frames': frames, 'queue': [],
                'sent': len(s['sent']), 'received': len(s['received']),
                'error': s['error']})
        net = []
        for m in self._network():
            sent_at = self._time(m.send_step)
            arrives = self._time(m.recv_step) if m.recv_step is not None \
                else None
            net.append({'key': m.key, 'src': T.key(m.src), 'dst': T.key(m.dst),
                        'payload': fmt(m.value), 'clk': m.sclk,
                        'sentStep': m.send_step,
                        'sentAt': round(sent_at, 6),
                        'arrives': round(arrives, 6) if arrives is not None
                        else None,
                        'recvStep': m.recv_step})
        snap = {
            'running': True, 'gen': self.gen, 'delta': False,
            'step': self.cursor, 'time': round(self.now, 6),
            'processes': procs, 'network': net,
            'events': self._prefix(self.events),
            'changes': self._prefix(self.changes),
            'logs': self._prefix(self.logs),
            'next': self._action(self.cursor + 1),
            'nextTime': self._time(self.cursor + 1)
            if self.cursor + 1 < len(T.steps) else None,
            'classes': sorted({p.cls for p in T.procs.values()
                               if not p.is_main}),
            'timers': [],
            'cursor': self.cursor,
            'redo': max(0, len(T.steps) - 1 - self.cursor),
            'total': len(T.steps),
            'lastAction': self._action(self.cursor),
            'paused': None,
            'lastStop': self.last_stop,
        }
        snap['conditions'] = [
            {'id': c['id'], 'expr': c['expr'], 'enabled': c['enabled'],
             'value': v, 'error': err}
            for c, (_, v, err) in zip(self.conditions,
                                      self.eval_conditions(states))]
        snap['conditionNames'] = self.condition_names(states)
        snap['conditionExample'] = self.condition_example(states)
        if since and since.get('gen') == self.gen:
            # The history lists only grow with the step, so the page needs
            # only what it does not have yet; after a step back, only how
            # many items to keep ('trim').
            snap['delta'] = True
            snap['trim'] = {}
            for k in self.HISTORY:
                have = max(0, int(since.get(k, 0)))
                if have <= len(snap[k]):
                    snap[k] = snap[k][have:]
                else:
                    snap['trim'][k] = len(snap[k])
                    snap[k] = []
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
        self.now = self._time(self.cursor)

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

    def _hit_line(self, k):
        hits = self._step_lines[k] & self.breaklines
        if not hits:
            return None
        fn, line = sorted(hits)[0]
        return {'reason': 'line', 'pid': self.trace.procs[
            self.trace.steps[k]['pid']].key, 'at': self.src.loc(fn, line)}

    def run_until(self, max_steps=100000):
        """Step until a step reaches a breakpoint line, a condition becomes
        true, or the run ends."""
        before = {cid: v for cid, v, _ in self.eval_conditions()}
        for n in range(max_steps):
            if not self.forward():
                self.last_stop = {'reason': 'quiet', 'steps': n}
                return self.last_stop
            hit = self._hit_line(self.cursor)
            if hit:
                hit['steps'] = n + 1
                self.last_stop = hit
                return hit
            now = {cid: v for cid, v, _ in self.eval_conditions()}
            ids = [cid for cid, v in now.items()
                   if v and not before.get(cid) and
                   self.conditions[cid]['enabled']]
            before = now
            if ids:
                self.last_stop = {'reason': 'condition', 'ids': ids,
                                  'steps': n + 1}
                return self.last_stop
        self.last_stop = {'reason': 'limit', 'steps': max_steps}
        return self.last_stop

    def advance(self, dt):
        """Real-time play: move the clock by dt (recorded seconds) and take
        the steps that happened by then."""
        self.last_stop = None
        target = self.now + max(0.0, float(dt))
        before = {cid: v for cid, v, _ in self.eval_conditions()}
        n = 0
        while self.cursor + 1 < len(self.trace.steps) and \
                self._time(self.cursor + 1) <= target:
            self.cursor += 1
            n += 1
            hit = self._hit_line(self.cursor)
            if hit:
                hit['steps'] = n
                self.last_stop = hit
                self.now = self._time(self.cursor)
                return
            now = {cid: v for cid, v, _ in self.eval_conditions()}
            ids = [cid for cid, v in now.items()
                   if v and not before.get(cid) and
                   self.conditions[cid]['enabled']]
            before = now
            if ids:
                self.last_stop = {'reason': 'condition', 'ids': ids,
                                  'steps': n}
                self.now = self._time(self.cursor)
                return
        self.now = target
        if self.cursor + 1 >= len(self.trace.steps):
            self.last_stop = {'reason': 'quiet', 'steps': n}

    # ---- breakpoints and conditions -----------------------------------
    def set_breaklines(self, blocks_lines):
        """blocks_lines: {block name: [line, ...]} (1-based, per block)."""
        out = set()
        for name, lines in (blocks_lines or {}).items():
            for ln in lines:
                fn, line = self.src.file_line(name, int(ln))
                if fn:
                    out.add((fn, line))
        self.breaklines = out

    def set_conditions(self, conds):
        self.conditions = [{'id': i, 'expr': c.get('expr', ''),
                            'enabled': c.get('enabled', True),
                            'fn': None, 'error': None}
                           for i, c in enumerate(conds or [])]
        for c in self.conditions:
            if not c['expr'].strip():
                continue
            try:
                c['fn'] = compile_query(c['expr'])
            except CompileError as e:
                c['error'] = str(e)

    def _load_program_namespace(self):
        """The program's module-level names (helpers and constants), for
        conditions.  main() is not run."""
        self.namespace = {}
        fn = self.trace.main_file
        src = self.src.text.get(fn) if fn else None
        if src is None:
            return
        try:
            args = daui.parse_compiler_args([])
            with _quiet_compiler(io.StringIO()):
                code = daui.dastr_to_pycode(src, fn, args)
                ns = {'__name__': '__dareplay__', '__file__': fn}
                if code is not None:
                    exec(code, ns)
            self.namespace = ns
        except Exception:
            self.namespace = {}

    def query_scope(self, states=None):
        states = self._states() if states is None else states
        T = self.trace
        views = {s['pid']: ProcView(s) for s in states.values()
                 if not s['main']}

        def viewify(v, depth=0):
            if isinstance(v, ReplayPid):
                return views.get(v, v)
            if depth < 2 and isinstance(v, (set, frozenset, list, tuple)):
                items = [viewify(x, depth + 1) for x in v]
                return items if not isinstance(v, tuple) else tuple(items)
            return v

        scope = {}
        for s in states.values():
            if not s['main']:
                continue
            # main()'s variables: as it returned, or at its latest await
            local_vars = s.get('main_locals')
            if not local_vars:
                local_vars = next((fr.get('locals') for fr in
                                   reversed(s['frames'])
                                   if fr.get('locals') is not None and
                                   fr['func'] == 'run'), None) or {}
            for k, v in local_vars.items():
                scope[k] = viewify(decode_value(v, T.pids))
        allv = list(views.values())
        scope.update({'procs': allv,
                      'network': [(views.get(T.pids.get(m.src), m.src),
                                   views.get(T.pids.get(m.dst), m.dst),
                                   m.value) for m in self._network()],
                      'step': self.cursor})
        for cls in sorted({p.cls for p in T.procs.values() if not p.is_main}):
            scope[cls] = [v for v in allv if v.cls == cls]
        return scope

    def condition_example(self, states=None):
        """An example condition for this program, for the input's
        placeholder: some(p in <a variable of main() holding processes>,
        has= p.<field> == <its value now>), else over a process class."""
        scope = self.query_scope(states)
        groups = []
        for name, v in scope.items():
            if name in ('procs', 'network', 'step'):
                continue
            items = [v] if isinstance(v, ProcView) else (
                list(v) if isinstance(v, (list, tuple, set, frozenset))
                else [])
            views = [x for x in items if isinstance(x, ProcView)]
            if views:
                is_class = name[:1].isupper()
                groups.append((is_class, name, views))
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

    def condition_names(self, states=None):
        scope = self.query_scope(states)
        return [n for n in scope if re.match(r'^[A-Za-z_]\w*$', n)]

    def eval_conditions(self, states=None):
        if not self.conditions:
            return []
        scope = None
        res = []
        for c in self.conditions:
            if c['fn'] is None:
                res.append((c['id'], None, c['error']))
                continue
            if scope is None:
                scope = self.query_scope(states)
            try:
                g = dict(self.namespace)
                g['da'] = da
                g.update(scope)
                fn = types.FunctionType(c['fn'].__code__, g, '_query_')
                res.append((c['id'], bool(fn()), None))
            except NameError as e:
                res.append((c['id'], None, '%s. You can use: %s' % (
                    e, ', '.join(sorted(scope)))))
            except Exception as e:
                res.append((c['id'], None, '%s: %s' % (type(e).__name__, e)))
        return res


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
        with _quiet_compiler(err):
            pyast = daui.dastr_to_pyast(src, '<condition>',
                                        daui.parse_compiler_args([]))
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
# HTTP server (from DistAlgo Live's server.py)
# --------------------------------------------------------------------------

SESSION = None


def api(path, body):
    s = SESSION
    if path == '/api/load':
        s.last_stop = None
        s.set_breaklines(body.get('breakpoints', {}))
        s.set_conditions(body.get('conditions', []))
        snap = s.snapshot()
        snap['info'] = s.info()
        return snap
    if path == '/api/breakpoints':
        s.set_breaklines(body.get('breakpoints', {}))
        s.set_conditions(body.get('conditions', []))
    elif path == '/api/step':
        s.last_stop = None
        if s.forward():
            hit = s._hit_line(s.cursor)
            if hit:
                hit['steps'] = 1
                s.last_stop = hit
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
        if path.startswith('/api/'):
            return self._handle_api(path, {})
        if path == '/':
            path = '/index.html'
        if path == '/d3.min.js':          # shared with the viz page
            full = os.path.join(UI_ROOT, 'js', 'd3.min.js')
        else:
            full = os.path.normpath(os.path.join(STATIC, path.lstrip('/')))
            if not full.startswith(STATIC):
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
        self._handle_api(self.path, body)

    def _handle_api(self, path, body):
        try:
            res = api(path, body)
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

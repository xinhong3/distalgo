'use strict';
/* DistAlgo Replay: front end.  Adapted from DistAlgo Live's page; talks to
   da/replay.py, which replays a recorded run (the steps, messages, state
   and explanations all come from the run's event logs).  Drawing uses d3
   (from DistAlgo's viz branch). */

const $ = (id) => document.getElementById(id);
const CLASS_COLORS = ['#1b8a7e', '#6a4bb3', '#b7770a', '#c0416b', '#3f6ea8',
                      '#4f7a2a', '#8a5a44', '#2f8fb0', '#9b4dca', '#5c7a00'];
const MSG_COLORS = ['#2459d6', '#c0416b', '#1b8a7e', '#b7770a', '#6a4bb3',
                    '#4f7a2a', '#d0632a', '#8a5a44', '#2f8fb0', '#9b4dca'];
const REDUCED = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
const NODE_R = 28;
const CARD_W = 236;
const DIM = '#aab3b9';     // messages of processes not picked in the network

const S = {
  blocks: [],          // {name, code, kind: 'process' | 'main' | 'code'}
  active: 0,
  bps: {},             // block name -> Set(line)
  conds: [],           // {expr, enabled}
  snap: null,
  info: null,          // the recorded run: logdir, program, steps, blocks
  sel: null,           // {type: 'proc', id} | {type: 'msg', key} | {type: 'change', i}
  view: 'network',
  auto: false,
  error: null,         // {block, line} of a compile error
  examples: [],
  msgColors: {},
  sweep: false,
  lastLogCount: 0,
  filter: new Set(),   // processes picked in the network: show only their messages
  zoom: {},            // view -> its d3 pan/zoom transform
  zoomTouched: {},     // view -> moved by the user (else it is fitted)
  size: {},            // view -> {W, H} as drawn
};

// ---------------------------------------------------------------- utilities
function esc(s) {
  return String(s == null ? '' : s).replace(/[&<>"]/g,
    (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
}
function trunc(s, n) { s = String(s); return s.length > n ? s.slice(0, n - 1) + '…' : s; }
function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

const HISTORY = ['events', 'logs', 'changes'];
let chain = Promise.resolve();
function call(path, body) {
  const p = chain.then(() => {
    body = Object.assign({}, body || {});
    if (S.snap && path !== '/api/load') {
      // ask only for history we do not have yet
      body.since = { gen: S.snap.gen };
      HISTORY.forEach((k) => { body.since[k] = S.snap[k].length; });
    }
    return fetch(path, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    }).then((r) => r.json()).then(mergeSnap);
  });
  chain = p.catch(() => {});
  return p;
}
function mergeSnap(res) {
  if (res && res.running && res.delta) {
    if (S.snap && S.snap.gen === res.gen) {
      // new items are appended; after a step back the server says how many
      // of ours to keep
      HISTORY.forEach((k) => {
        const keep = res.trim && res.trim[k] != null ? S.snap[k].slice(0, res.trim[k]) : S.snap[k];
        res[k] = keep.concat(res[k]);
      });
    }
    res.delta = false;
  }
  return res;
}
function fmtTime(t) {
  if (t == null) return '–';
  return t < 1 ? `${(t * 1000).toFixed(t < 0.01 ? 1 : 0)} ms` : `${t.toFixed(3)} s`;
}

function classColor(cls) {
  if (cls === 'main') return '#5d6b75';
  const names = S.blocks.filter((b) => b.kind === 'process').map((b) => b.name);
  let i = names.indexOf(cls);
  if (i < 0 && S.snap) i = names.length + (S.snap.classes || []).indexOf(cls);
  if (i < 0) i = [...cls].reduce((a, c) => a + c.charCodeAt(0), 0);
  return CLASS_COLORS[i % CLASS_COLORS.length];
}
function msgType(payload) {
  const m = /^\(*'([^']*)'/.exec(payload || '');
  return m ? m[1] : trunc(payload || '?', 14);
}
function msgColor(payload) {
  const t = msgType(payload);
  if (!(t in S.msgColors)) {
    S.msgColors[t] = MSG_COLORS[Object.keys(S.msgColors).length % MSG_COLORS.length];
  }
  return S.msgColors[t];
}
function locStr(loc) {
  if (!loc) return '';
  return (loc.block ? loc.block + ', ' : '') + 'line ' + loc.line;
}
function procById(id) {
  return S.snap && S.snap.processes.find((p) => p.id === id);
}
function visibleProcs() {
  if (!S.snap) return [];
  return S.snap.processes.filter((p) =>
    !(p.isMain && p.status === 'done' && p.sent === 0 && p.received === 0));
}

// ---------------------------------------------------------------- blocks
// The program is read-only: its blocks (one per process class, plus main)
// come from the recorded source files.
function isStale() { return false; }

// ---------------------------------------------------------------- status
function setStatus(text, kind, opts) {
  const el = $('status');
  el.className = 'status' + (kind ? ' ' + kind : '');
  el.textContent = text || '';
  if (opts && opts.undo) {
    const a = document.createElement('a');
    a.textContent = '  Undo';
    a.href = '#';
    a.style.color = 'var(--blue)';
    a.onclick = (e) => { e.preventDefault(); opts.undo(); setStatus(''); };
    el.appendChild(a);
  }
}

// ---------------------------------------------------------------- tabs + editor
function renderTabs() {
  const tabs = $('tabs');
  tabs.innerHTML = '';
  const marks = computeMarks();
  S.blocks.forEach((b, i) => {
    const t = document.createElement('div');
    t.className = 'tab' + (i === S.active ? ' on' : '') +
      (i !== S.active && marks[b.name] && Object.keys(marks[b.name]).length ? ' has-mark' : '');
    t.setAttribute('role', 'tab');
    t.tabIndex = 0;
    if (b.kind === 'process') {
      const sw = document.createElement('span');
      sw.className = 'swatch';
      sw.style.background = classColor(b.name);
      t.appendChild(sw);
    }
    const label = document.createElement('span');
    label.textContent = b.name;
    t.appendChild(label);
    t.onclick = () => setActive(i);
    t.onkeydown = (e) => { if (e.key === 'Enter') setActive(i); };
    tabs.appendChild(t);
  });
}
function syncCode() {}
function setActive(i, scrollLine) {
  syncCode();
  S.active = i;
  const b = S.blocks[i];
  $('code').value = b ? b.code : '';
  renderTabs();
  renderEditor();
  if (scrollLine) scrollToLine(scrollLine);
}
function scrollToLine(line) {
  const ta = $('code');
  const lh = 20;
  const top = (line - 1) * lh;
  if (top < ta.scrollTop + lh || top > ta.scrollTop + ta.clientHeight - 2 * lh) {
    ta.scrollTop = Math.max(0, top - ta.clientHeight / 3);
  }
  syncScroll();
}
function showBlock(blockName, line) {
  const i = S.blocks.findIndex((b) => b.name === blockName);
  if (i < 0) return;
  if (i !== S.active) setActive(i, line);
  else { renderEditor(); if (line) scrollToLine(line); }
}

/* Line highlights: {block: {line: class}} */
function computeMarks() {
  const marks = {};
  const put = (loc, cls, rank) => {
    if (!loc || !loc.block) return;
    const m = (marks[loc.block] = marks[loc.block] || {});
    const order = ['here2', 'here', 'mark', 'mark-line', 'err'];
    if (!m[loc.line] || order.indexOf(cls) > order.indexOf(m[loc.line])) m[loc.line] = cls;
  };
  if (S.error) put(S.error, 'err');
  if (!S.snap) return marks;
  const sel = S.sel;
  if (sel && sel.type === 'proc') {
    const p = procById(sel.id);
    if (p) {
      p.frames.forEach((f, i) => put(f, i === p.frames.length - 1 ? 'here' : 'here2'));
      if (p.pausedAt) put(p.pausedAt.line, 'here');
    }
  }
  const why = selectedWhy();
  if (why) {
    put(why.line, 'mark-line');
    (why.entries || []).forEach((e) => put(e, 'mark'));
    (why.stack || []).slice(1).forEach((f) => put(f, 'mark'));
    if (why.cause && why.cause.type === 'timer') (why.cause.setStack || []).forEach((f) => put(f, 'mark'));
  }
  return marks;
}
function procMarkers(blockName) {
  // small colored ticks in the gutter: where each process currently is
  const out = {};
  if (!S.snap) return out;
  visibleProcs().forEach((p) => {
    const f = p.pausedAt ? p.pausedAt.line : (p.frames.length ? p.frames[p.frames.length - 1] : null);
    if (f && f.block === blockName) (out[f.line] = out[f.line] || []).push(p);
  });
  return out;
}
function renderEditor() {
  const b = S.blocks[S.active];
  const hasBlocks = S.blocks.length > 0;
  $('editor').style.display = hasBlocks ? '' : 'none';
  $('empty').style.display = hasBlocks ? 'none' : '';
  if (!b) return;
  const code = $('code').value;
  const lines = code.split('\n');
  const bps = S.bps[b.name] || new Set();
  const marks = computeMarks()[b.name] || {};
  const pm = procMarkers(b.name);
  const g = $('gutter');
  const parts = [];
  for (let i = 1; i <= lines.length; i++) {
    let ticks = '';
    (pm[i] || []).slice(0, 3).forEach((p, k) => {
      ticks += `<i class="pm" style="background:${classColor(p.cls)};left:${2 + k * 3}px" title="${esc(p.id)} is here"></i>`;
    });
    parts.push(`<div data-line="${i}" class="${bps.has(i) ? 'bp' : ''}">${ticks}<span>${i}</span></div>`);
  }
  g.innerHTML = parts.join('');
  const bd = $('backdrop');
  bd.innerHTML = lines.map((l, i) => {
    const cls = marks[i + 1] || '';
    const sweep = S.sweep && cls === 'mark' ? ' sweep' : '';
    return `<div class="${cls}${sweep}">${esc(l) || ' '}</div>`;
  }).join('');
  syncScroll();
}
function syncScroll() {
  const ta = $('code');
  $('backdrop').scrollTop = ta.scrollTop;
  $('backdrop').scrollLeft = ta.scrollLeft;
  $('gutter').scrollTop = ta.scrollTop;
}
function bpPayload() {
  const out = {};
  for (const k in S.bps) out[k] = [...S.bps[k]];
  return out;
}
function toggleBreakpoint(line) {
  const b = S.blocks[S.active];
  if (!b) return;
  const set = (S.bps[b.name] = S.bps[b.name] || new Set());
  if (set.has(line)) set.delete(line); else set.add(line);
  renderEditor();
  sendBreakpoints();
}

let bpTimer = null;
function sendBreakpoints() {
  if (!S.snap) return;
  clearTimeout(bpTimer);
  bpTimer = setTimeout(pushBreakpoints, 250);
}
async function pushBreakpoints() {
  bpTimer = null;
  const res = await call('/api/breakpoints', { breakpoints: bpPayload(), conditions: S.conds });
  if (res.running) { S.snap = res; renderConds(); }
}
function flushBreakpoints() {
  // make sure the server has the latest breakpoints before time moves
  if (bpTimer) { clearTimeout(bpTimer); return pushBreakpoints(); }
  return Promise.resolve();
}

// ---------------------------------------------------------------- conditions
function renderConds() {
  const list = $('cond-list');
  const vals = (S.snap && S.snap.conditions) || [];
  if (document.activeElement && document.activeElement.closest &&
      document.activeElement.closest('#cond-list')) {
    // keep focus: only refresh the value badges
    S.conds.forEach((c, i) => {
      const row = list.children[i];
      if (row) paintCondValue(row, vals[i]);
    });
    return;
  }
  list.innerHTML = '';
  S.conds.forEach((c, i) => {
    const row = document.createElement('div');
    row.innerHTML = `<div class="cond">
        <input type="checkbox" ${c.enabled ? 'checked' : ''} title="Pause when this becomes true" aria-label="Enabled">
        <input type="text" spellcheck="false" value="${esc(c.expr)}" placeholder="e.g. ${esc(condExample())}" aria-label="Condition">
        <span class="val"></span>
        <button class="x" title="Remove condition" aria-label="Remove condition">×</button>
      </div><div class="cond-err"></div>`;
    const [chk, inp] = row.querySelectorAll('input');
    chk.onchange = () => { c.enabled = chk.checked; sendBreakpoints(); };
    inp.oninput = () => { c.expr = inp.value; sendBreakpoints(); };
    row.querySelector('.x').onclick = () => { S.conds.splice(i, 1); renderConds(); sendBreakpoints(); };
    paintCondValue(row, vals[i]);
    list.appendChild(row);
  });
  const names = S.snap && S.snap.conditionNames;
  if (names && names.length) {
    $('cond-names').innerHTML = names.map((n) => `<code>${esc(n)}</code>`).join(', ');
  }
}
function condExample() {
  // built by the server from this program: a variable of main() holding
  // processes, one of their fields and its value now
  return (S.snap && S.snap.conditionExample) || 'some(p in procs, has= len(p.received) > 0)';
}
function paintCondValue(row, v) {
  const badge = row.querySelector('.val');
  const err = row.querySelector('.cond-err');
  badge.className = 'val';
  err.textContent = '';
  if (!v || !S.snap) { badge.textContent = '–'; return; }
  if (v.error) { badge.textContent = 'error'; badge.classList.add('e'); err.textContent = v.error; }
  else if (v.value === true) { badge.textContent = 'true'; badge.classList.add('t'); }
  else if (v.value === false) { badge.textContent = 'false'; }
  else badge.textContent = '–';
}

// ---------------------------------------------------------------- loading
async function load() {
  stopAuto();
  const res = await call('/api/load', { breakpoints: bpPayload(), conditions: S.conds });
  if (!res || res.error) { setStatus(res ? res.error : 'No answer from the server.', 'error'); return; }
  S.info = res.info;
  S.blocks = res.info.blocks.map((b) => ({ name: b.name, code: b.code, kind: b.kind }));
  if (S.active >= S.blocks.length || !S.snap) {
    S.active = Math.max(0, S.blocks.findIndex((b) => b.kind === 'process'));
  }
  $('code').value = S.blocks.length ? S.blocks[S.active].code : '';
  const prog = res.info.program ? res.info.program.split(/[\\/]/).pop() : '(program)';
  $('trace-info').innerHTML = `<b>${esc(prog)}</b> · ${res.info.steps} steps · ${res.info.processes} processes · ${res.info.messages} messages · <span title="${esc(res.info.logdir)}">${esc(res.info.logdir.split(/[\\/]/).pop())}</span>`;
  $('empty').style.display = 'none';
  S.sel = null;
  S.msgColors = {};
  S.lastLogCount = 0;
  S.filter = new Set();
  S.zoomTouched = {};
  setSnap(res, { status: `Replaying ${prog}: ${res.info.steps} recorded steps. Step forward, play, or set a breakpoint or condition and run to it.` });
}
function gotoDelivery(key) {
  const m = findMsg(key);
  const fly = S.snap && S.snap.network.find((x) => x.key === key);
  const at = m && m.recvStep != null ? m.recvStep : (fly ? fly.recvStep : null);
  if (at == null) { setStatus('This message was never received in the recorded run.'); return; }
  goto(at);
}

async function step() {
  if (!S.snap) return null;
  await flushBreakpoints();
  const res = await call('/api/step', {});
  setSnap(res, { animate: true, status: res && res.lastStop && res.lastStop.reason === 'line' ? stopText(res) : undefined,
                 kind: res && res.lastStop && res.lastStop.reason === 'line' ? 'stop' : '' });
  return res;
}
async function back() {
  if (!S.snap) return;
  stopAuto();
  setSnap(await call('/api/back'), {});
}
async function reset() {
  if (!S.snap) return;
  stopAuto();
  setSnap(await call('/api/goto', { step: 0 }), { status: 'Back at step 0.' });
}
async function goto(n) {
  if (!S.snap) return;
  stopAuto();
  setSnap(await call('/api/goto', { step: n }), {});
}
async function runUntil() {
  if (!S.snap) return;
  stopAuto();
  await flushBreakpoints();
  const res = await call('/api/run_until', { maxSteps: 2000 });
  setSnap(res, { status: stopText(res), kind: res.lastStop && res.lastStop.reason !== 'limit' && res.lastStop.reason !== 'quiet' ? 'stop' : '' });
}
function stopText(snap) {
  const st = snap.lastStop;
  if (!st) return '';
  if (st.reason === 'line') {
    const after = st.steps ? ` after ${st.steps} step${st.steps === 1 ? '' : 's'}` : '';
    return `Stopped at a breakpoint${after}: at step ${snap.step}, ${st.pid} reached ${locStr(st.at)}: ${st.at.text}`;
  }
  if (st.reason === 'condition') {
    const exprs = st.ids.map((i) => snap.conditions[i] && snap.conditions[i].expr).filter(Boolean);
    return `Paused after ${st.steps} step${st.steps === 1 ? '' : 's'}: ${exprs.join(' and ')} became true.`;
  }
  if (st.reason === 'quiet') return `Ran ${st.steps} steps to the end of the recorded run.`;
  return `Ran ${st.steps} steps without reaching a breakpoint. Run again to keep going.`;
}
function toggleAuto() {
  if (S.auto) { stopAuto(); return; }
  if (!S.snap) return;
  startAuto();
}
async function startAuto() {
  S.auto = true;
  $('auto').classList.add('on');
  const speed = playSpeed();
  if (speed === null) { stopAuto(); return; }
  if (speed !== 'steps') { await realTimeLoop(speed); stopAuto(); return; }
  while (S.auto) {
    const before = S.snap;
    if (!before.next) { setStatus('End of the recorded run.'); break; }
    const res = await step();
    if (!S.auto) break;
    if (res.lastStop && res.lastStop.reason === 'line') { setStatus(stopText(res), 'stop'); break; }
    const hit = (res.conditions || []).filter((c, i) =>
      c.enabled && c.value === true && !(before.conditions[i] && before.conditions[i].value === true));
    if (hit.length) { setStatus(`Paused: ${hit.map((c) => c.expr).join(' and ')} became true.`, 'stop'); break; }
    await sleep(REDUCED ? 350 : 750);
  }
  stopAuto();
}
/* 'steps', or how many times the recorded speed to play at (null: the
   factor entered is not a positive number) */
function playSpeed() {
  const v = $('speed').value;
  if (v === 'steps') return 'steps';
  if (v !== 'x') return 1;
  const f = parseFloat($('speed-x').value);
  if (!(f > 0)) { setStatus('Enter how many times real time to play at, e.g. 0.25 or 2.', 'error'); return null; }
  return f;
}
function stopAuto() {
  S.auto = false;
  $('auto').classList.remove('on');
}
/* Real-time play: the virtual clock runs at `speed` x wall time while
   playing, and stops when paused.  Messages arrive and timers fire by
   themselves when their time comes. */
async function realTimeLoop(speed) {
  await flushBreakpoints();
  let last = performance.now();
  while (S.auto) {
    await sleep(60);
    if (!S.auto) break;
    const now = performance.now();
    const dt = Math.min(0.5, (now - last) / 1000) * speed;
    last = now;
    const prevStep = S.snap.step;
    const res = await call('/api/advance', { dt });
    if (!S.auto && !(res && res.lastStop)) { setSnap(res, {}); break; }
    const st = res.lastStop;
    if (st && (st.reason === 'line' || st.reason === 'condition')) {
      setSnap(res, { status: stopText(res), kind: 'stop' });
      break;
    }
    if (st && st.reason === 'quiet') {
      setSnap(res, { status: 'End of the recorded run.' });
      break;
    }
    if (res.step === prevStep) quickRender(res);       // only the clock moved
    else setSnap(res, { animate: false, realtime: true });
  }
}
function quickRender(snap) {
  S.snap = snap;
  renderTime();
  if (S.view === 'network') renderStage({});
}

function setSnap(snap, opts) {
  opts = opts || {};
  if (!snap || snap.error) { setStatus(snap ? snap.error : 'No answer from the server.', 'error'); return; }
  if (!snap.running) return;
  S.snap = snap;
  if (S.sel && S.sel.type === 'change' && !snap.changes[S.sel.i]) S.sel = null;
  if (S.sel && S.sel.type === 'proc' && !procById(S.sel.id)) S.sel = null;
  if (S.sel && S.sel.type === 'msg' && !findMsg(S.sel.key)) S.sel = null;
  if (snap.paused && !(S.sel && S.sel.type === 'proc' && S.sel.id === snap.paused)) {
    // show the line the paused process is at
    S.sel = { type: 'proc', id: snap.paused };
    const p = procById(snap.paused);
    if (p && p.pausedAt) {
      const l = p.pausedAt.line;
      const i = S.blocks.findIndex((b) => b.name === l.block);
      if (i >= 0 && i !== S.active) { syncCode(); S.active = i; $('code').value = S.blocks[i].code; }
      setTimeout(() => scrollToLine(l.line), 0);
    }
  }
  const ls = snap.lastStop;
  if (ls && ls.reason === 'line' && ls.at) {
    S.sel = { type: 'proc', id: ls.pid };
    const i = S.blocks.findIndex((b) => b.name === ls.at.block);
    if (i >= 0 && i !== S.active) { S.active = i; $('code').value = S.blocks[i].code; }
    setTimeout(() => scrollToLine(ls.at.line), 0);
  }
  if (opts.status !== undefined) setStatus(opts.status, opts.kind || '');
  else setStatus(describeStep(snap), snap.paused ? 'stop' : '');
  renderAll({ animate: opts.animate });
}

// ---------------------------------------------------------------- describing
function describeAction(a, snap) {
  if (!a) return '';
  if (a.type === 'run') return `${a.pid} started running`;
  if (a.type === 'timeout') return `the timeout of ${a.pid} fired`;
  if (a.type === 'continue') return `${a.pid} continued from a breakpoint`;
  if (a.type === 'timer') return `the timer ${a.id.split('~')[1] || ''} of ${a.id.split('~')[0]} fired`;
  if (a.type === 'deliver') {
    const m = findMsg(a.msg, snap);
    if (!m) return 'a message was delivered';
    const verb = m.dropped ? 'arrived but was dropped' : 'was delivered';
    return `${m.payload} from ${m.src} ${verb} to ${m.dst}`;
  }
  return '';
}
function describeNext(snap) {
  const a = snap.next;
  if (!a) return 'nothing; every process is waiting or finished';
  if (a.type === 'run') return `start ${a.pid}`;
  if (a.type === 'timeout') return `timeout of ${a.pid}`;
  if (a.type === 'continue') return `continue ${a.pid}`;
  if (a.type === 'timer') {
    const t = (snap.timers || []).find((x) => x.key === a.id);
    return `timer of ${a.id.split('~')[0]} fires${t ? ' at ' + fmtTime(t.deadline) : ''}`;
  }
  const m = snap.network.find((x) => x.key === a.msg);
  return m ? `deliver ${trunc(m.payload, 40)} from ${m.src} to ${m.dst}` : 'deliver a message';
}
function describeStep(snap) {
  if (snap.paused) {
    const p = procById(snap.paused);
    return `${snap.paused} is paused at a breakpoint (${p && p.pausedAt ? locStr(p.pausedAt.line) : ''}). Step forward to let it continue.`;
  }
  const what = describeAction(snap.lastAction, snap);
  if (snap.cursor === 0 && snap.lastAction && snap.lastAction.pid === 'main') return 'Step 0: main() ran and created the processes.';
  return `Step ${snap.step}: ${what}.`;
}
function findMsg(key, snap) {
  snap = snap || S.snap;
  if (!snap) return null;
  const send = snap.events.find((e) => e.kind === 'send' && e.key === key);
  if (!send) return null;
  const done = snap.events.find((e) => (e.kind === 'recv' || e.kind === 'drop') && e.key === key);
  const fly = snap.network.find((m) => m.key === key);
  return {
    key, src: send.src, dst: send.dst, payload: send.payload, why: send.why,
    sentStep: send.step, recvStep: done ? done.step : (fly ? fly.recvStep : null),
    received: !!done, dropped: done && done.kind === 'drop', inFlight: !!fly, arrives: fly ? fly.arrives : null,
  };
}
function selectedWhy() {
  const sel = S.sel;
  if (!sel || !S.snap) return null;
  if (sel.type === 'msg') { const m = findMsg(sel.key); return m && m.why; }
  if (sel.type === 'change') { const c = S.snap.changes[sel.i]; return c && c.why; }
  return null;
}

// ---------------------------------------------------------------- render all
function renderAll(opts) {
  opts = opts || {};
  renderTabs();
  renderEditor();
  renderConds();
  renderTime();
  renderStage(opts);
  renderLog();
  renderWhy();
  ['reset', 'back', 'step', 'until', 'auto'].forEach((id) => { $(id).disabled = !S.snap; });
  if (S.snap) {
    $('back').disabled = S.snap.cursor === 0;
    $('step').disabled = !S.snap.next;
    $('until').disabled = !S.snap.next;
  }
}

function renderTime() {
  const sc = $('scrub');
  const clk = $('clock');
  if (!S.snap) { sc.max = 0; sc.value = 0; sc.disabled = true; clk.innerHTML = ''; return; }
  sc.disabled = false;
  sc.max = S.snap.cursor + S.snap.redo;
  sc.value = S.snap.cursor;
  clk.innerHTML = `<span>Step <b>${S.snap.step}</b> of ${S.snap.cursor + S.snap.redo}</span>
    &nbsp;&nbsp; <span title="Recorded time since the run started">Time <b>${fmtTime(S.snap.time)}</b></span>
    &nbsp;&nbsp; <span>Next: ${esc(describeNext(S.snap))}</span>`;
}

// ---------------------------------------------------------------- stage
function renderStage(opts) {
  const has = !!S.snap;
  $('stage-empty').style.display = has ? 'none' : '';
  $('net').style.display = has && S.view === 'network' ? '' : 'none';
  $('timeline').hidden = $('tlhead').hidden = !(has && S.view === 'timeline');
  if (has) {
    if (S.view === 'network') renderNet(opts);
    else renderTimeline();
  }
  renderLegend();
  renderFilterBar();
}
function renderLegend() {
  const el = $('legend');
  el.innerHTML = Object.keys(S.msgColors).slice(0, 8).map((t) =>
    `<span><i style="background:${S.msgColors[t]}"></i>${esc(t)}</span>`).join('');
}

function netSize(n, stage) {
  if (n <= 4) {
    return { W: Math.max(stage.clientWidth, 2 * (CARD_W + 70) + 260),
             H: Math.max(stage.clientHeight, 360), compact: false };
  }
  // many processes: compact cards around a larger ellipse
  const ry = Math.max(170, n * 26);
  const fit = (stage.clientWidth - 2 * (CARD_W + 50)) / 2;
  const rx = n <= 8 ? Math.max(200, fit) : Math.max(260, ry * 1.55);
  return { W: Math.max(stage.clientWidth, 2 * rx + 2 * (CARD_W + 50)),
           H: Math.max(stage.clientHeight, 2 * ry + 200), compact: true, rx, ry };
}
function layout(procs, W, H, size) {
  const n = procs.length;
  const pos = {};
  const cx = W / 2, cy = H / 2;
  const rx = size && size.rx ? size.rx : Math.max(150, W / 2 - CARD_W - 70);
  const ry = size && size.ry ? size.ry : Math.max(110, H / 2 - 120);
  procs.forEach((p, i) => {
    let a;
    if (n === 1) { pos[p.id] = { x: cx, y: cy, a: 0 }; return; }
    if (n === 2) a = i === 0 ? Math.PI : 0;
    else a = -Math.PI / 2 + (2 * Math.PI * i) / n + (n % 2 ? 0 : Math.PI / n);
    pos[p.id] = { x: cx + rx * Math.cos(a), y: cy + ry * Math.sin(a), a };
  });
  return pos;
}
function curve(a, b, bend) {
  const dx = b.x - a.x, dy = b.y - a.y;
  const len = Math.hypot(dx, dy) || 1;
  const ux = dx / len, uy = dy / len;
  const sx = a.x + ux * (NODE_R + 3), sy = a.y + uy * (NODE_R + 3);
  const ex = b.x - ux * (NODE_R + 9), ey = b.y - uy * (NODE_R + 9);
  const mx = (sx + ex) / 2 - uy * bend, my = (sy + ey) / 2 + ux * bend;
  return { d: `M${sx},${sy} Q${mx},${my} ${ex},${ey}`, lx: (sx + 2 * mx + ex) / 4, ly: (sy + 2 * my + ey) / 4 };
}

function renderNet(opts) {
  const procs = visibleProcs();
  const stage = $('stage');
  const n = procs.length;
  const size = netSize(n, stage);
  const W = size.W, H = size.H;
  const pos = layout(procs, W, H, size);
  const svg = d3.select('#netsvg').attr('width', W).attr('height', H);
  $('net').style.width = W + 'px';
  $('net').style.height = H + 'px';
  svg.selectAll('*').remove();

  const defs = svg.append('defs');
  const markerFor = (color) => {
    const id = 'ah' + color.replace('#', '');
    if (defs.select('#' + id).empty()) {
      defs.append('marker').attr('id', id).attr('markerUnits', 'userSpaceOnUse')
        .attr('markerWidth', 12).attr('markerHeight', 12).attr('viewBox', '0 0 12 12')
        .attr('refX', 6).attr('refY', 6).attr('orient', 'auto')
        .append('path').attr('d', 'M2,2 L10,6 L2,10 L6,6 L2,2').attr('fill', color); // viz-branch arrow
    }
    return `url(#${id})`;
  };

  // all-to-all network
  const mesh = svg.append('g');
  for (let i = 0; i < n; i++) for (let j = i + 1; j < n; j++) {
    const a = pos[procs[i].id], b = pos[procs[j].id];
    mesh.append('line').attr('class', 'mesh').attr('x1', a.x).attr('y1', a.y).attr('x2', b.x).attr('y2', b.y);
  }

  // just delivered (this step): ghost arrows
  const snap = S.snap;
  const recent = snap.events.filter((e) => (e.kind === 'recv' || e.kind === 'drop') && e.step === snap.step && snap.cursor > 0);
  const ghosts = svg.append('g');
  recent.forEach((e) => {
    if (!pos[e.src] || !pos[e.dst]) return;
    const c = curve(pos[e.src], pos[e.dst], 26);
    const color = netColor(e);
    ghosts.append('path').attr('class', 'ghost' + (picked(e.src) ? '' : ' dim')).attr('d', c.d)
      .attr('stroke', color).attr('marker-end', markerFor(color));
  });

  // in-flight messages, grouped by channel
  const groups = new Map();
  snap.network.forEach((m) => {
    const k = m.src + '>' + m.dst;
    if (!groups.has(k)) groups.set(k, []);
    groups.get(k).push(m);
  });
  const selKey = S.sel && S.sel.type === 'msg' ? S.sel.key : null;
  const nextKey = snap.next && snap.next.type === 'deliver' ? snap.next.msg : null;
  const mg = svg.append('g');
  groups.forEach((ms) => {
    const first = ms[0];
    if (!pos[first.src] || !pos[first.dst]) return;
    const c = curve(pos[first.src], pos[first.dst], 26);
    const color = netColor(first);
    const isNext = ms.some((m) => m.key === nextKey);
    const g = mg.append('g').attr('class', 'msg' + (isNext ? ' next' : '') + (picked(first.src) ? '' : ' dim'));
    if (ms.some((m) => m.key === selKey)) g.append('path').attr('class', 'glow').attr('d', c.d);
    g.append('path').attr('class', 'line').attr('d', c.d).attr('stroke', color)
      .attr('stroke-dasharray', '6 4').attr('marker-end', markerFor(color));
    g.append('path').attr('class', 'hit').attr('d', c.d);
    g.append('text').attr('x', c.lx).attr('y', c.ly - 6).attr('text-anchor', 'middle').attr('fill', color)
      .text(trunc(first.payload, 26) + (ms.length > 1 ? `  ×${ms.length}` : ''));
    // where each message is on its way: (now - sent) / (arrives - sent)
    const pathNode = g.append('path').attr('d', c.d).attr('fill', 'none').attr('stroke', 'none').node();
    const L = pathNode.getTotalLength();
    ms.forEach((m) => {
      const span = m.arrives == null ? 1e9 : ((m.arrives - m.sentAt) || 1e-9);
      const f = Math.max(0, Math.min(1, (snap.time - m.sentAt) / span));
      const pt = pathNode.getPointAtLength(f * L);
      g.append('circle').attr('class', 'inflight-dot').attr('r', 5).attr('fill', color)
        .attr('cx', pt.x).attr('cy', pt.y);
    });
    const target = ms.find((m) => m.key === selKey) || ms[0];
    g.on('click', () => {
      if (d3.event.shiftKey) { hideTip(); gotoDelivery(target.key); return; }
      selectMsg(target.key);
    });
    g.on('mousemove', () => showTip(d3.event,
      ms.map((m) => `${m.payload}   sent at step ${m.sentStep}, ` +
        (m.recvStep != null ? `received at step ${m.recvStep}` : 'never received')).join('\n') +
      '\n\nClick to see why it was sent. Shift-click to go to the step that delivers it.'));
    g.on('mouseleave', hideTip);
  });

  // nodes
  const ng = svg.append('g');
  procs.forEach((p) => {
    const q = pos[p.id];
    const sel = S.sel && S.sel.type === 'proc' && S.sel.id === p.id;
    const g = ng.append('g').attr('class', `node ${p.status}${sel ? ' sel' : ''}`)
      .attr('transform', `translate(${q.x},${q.y})`);
    const ringColor = { blocked: '#9fb0a8', paused: '#2459d6', crashed: '#d33a2c', stuck: '#d33a2c', ready: '#b7770a' }[p.status];
    if (ringColor || sel) {
      g.append('circle').attr('class', 'ring').attr('r', NODE_R + 5)
        .attr('stroke', sel ? '#2459d6' : ringColor)
        .attr('stroke-dasharray', p.status === 'blocked' || p.status === 'ready' ? '4 3' : null);
    }
    if (S.filter.has(p.id)) g.append('circle').attr('class', 'pick').attr('r', NODE_R + 10);
    g.append('circle').attr('class', 'body').attr('r', NODE_R).attr('fill', classColor(p.cls))
      .on('click', () => toggleFilter(p.id))
      .on('mousemove', () => showTip(d3.event, `${p.id}: ${p.status}\n` + (S.filter.has(p.id)
        ? 'Click to stop showing only its messages.'
        : 'Click to see the code it is running and show only the messages it sends (click again to undo).')))
      .on('mouseleave', hideTip);
    const myTimers = (snap.timers || []).filter((x) => x.owner === p.id);
    if (myTimers.length) {
      const tb = g.append('g').attr('class', 'timer-badge').attr('transform', `translate(${NODE_R - 2},${-NODE_R + 2})`);
      tb.append('circle').attr('r', 10);
      const left = Math.min(...myTimers.map((x) => x.deadline)) - snap.time;
      tb.append('text').attr('text-anchor', 'middle').attr('dy', 3.5)
        .text(left >= 10 ? Math.round(left) : left >= 1 ? left.toFixed(1) : '<1');
      tb.on('mousemove', () => showTip(d3.event, myTimers.map((x) =>
        `timer (${x.interval}s) fires at ${fmtTime(x.deadline)}, in ${fmtTime(Math.max(0, x.deadline - snap.time))}`).join('\n')))
        .on('mouseleave', hideTip);
    }
    const parts = p.id.split(':');
    const t = g.append('text').attr('text-anchor', 'middle');
    t.append('tspan').attr('x', 0).attr('dy', parts[1] ? -2 : 4).text(trunc(parts[0], 9));
    if (parts[1]) t.append('tspan').attr('x', 0).attr('dy', 13).text(parts[1]);
  });

  // animate the messages delivered by this step
  if (opts.animate && !REDUCED) {
    recent.forEach((e) => {
      if (!pos[e.src] || !pos[e.dst]) return;
      const c = curve(pos[e.src], pos[e.dst], 26);
      const path = ghosts.append('path').attr('d', c.d).attr('fill', 'none').attr('stroke', 'none');
      const node = path.node();
      const L = node.getTotalLength();
      const dot = ghosts.append('circle').attr('r', 5).attr('fill', netColor(e));
      dot.transition().duration(480).ease(d3.easeCubicInOut)
        .attrTween('transform', () => (t) => {
          const pt = node.getPointAtLength(t * L);
          return `translate(${pt.x},${pt.y})`;
        })
        .transition().duration(200).attr('r', 0).remove();
    });
  }

  renderCards(procs, pos, size.compact);
  S.size.network = { W, H };
  syncZoom('network');
}

function renderCards(procs, pos, compact) {
  const box = $('cards');
  box.innerHTML = '';
  const snap = S.snap;
  procs.forEach((p) => {
    const q = pos[p.id];
    const card = document.createElement('div');
    const sel = S.sel && S.sel.type === 'proc' && S.sel.id === p.id;
    card.className = 'card' + (sel ? ' sel' : '') + (compact ? ' compact' : '');
    const right = Math.cos(q.a) >= -0.01;
    card.style.left = (right ? q.x + NODE_R + 14 : q.x - NODE_R - 14 - CARD_W) + 'px';
    card.style.top = Math.max(4, q.y - 34) + 'px';

    const changedNow = new Set(snap.changes.filter((c) => c.step === snap.step && c.pid === p.id).map((c) => c.field));
    let at = '';
    if (p.pausedAt) at = `paused at ${locStr(p.pausedAt.line)}: ${p.pausedAt.line.text}`;
    else if (p.waitingAt) at = `waiting at ${locStr(p.waitingAt)}: ${p.waitingAt.text}`;
    else if (p.status === 'ready') at = 'started; runs at a later step';
    else if (p.status === 'crashed' && p.error) at = 'crashed: ' + p.error;
    else if (p.status === 'setup' || p.status === 'created') at = 'created, not started';
    const rows = Object.entries(p.state).map(([k, v]) =>
      `<tr class="field${changedNow.has(k) ? ' changed' : ''}" data-field="${esc(k)}" title="Why does ${esc(k)} have this value?"><td class="k">${esc(k)}</td><td class="v">${esc(v)}</td></tr>`).join('');
    card.innerHTML = `
      <div class="head"><span class="name" style="color:${classColor(p.cls)}" title="${esc(p.realId || p.id)}">${esc(p.id)}</span>
        <span class="badge ${p.status}">${p.status}</span><span class="clk" title="Lamport clock">clock ${p.clock == null ? '–' : p.clock}</span></div>
      ${at ? `<span class="at" title="${esc(at)}">${esc(at)}</span>` : ''}
      ${rows ? `<table>${rows}</table>` : '<div class="sub">no fields</div>'}
      ${p.queue.length ? `<div class="sub">arrived, not yet handled</div><div class="inbox">${p.queue.map(esc).join('<br>')}</div>` : ''}
      ${timerLines(snap, p.id)}
      <div class="sub">sent ${p.sent}, received ${p.received}</div>`;
    card.querySelector('.name').onclick = () => selectProc(p.id);
    const atEl = card.querySelector('.at');
    if (atEl) atEl.onclick = () => selectProc(p.id);
    card.querySelectorAll('tr.field').forEach((tr) => {
      tr.onclick = () => selectField(p.id, tr.dataset.field);
    });
    box.appendChild(card);
  });
}

function timerLines(snap, pid) {
  const ts = (snap.timers || []).filter((x) => x.owner === pid);
  if (!ts.length) return '';
  return '<div class="sub">timers</div><div class="timers">' + ts.map((x) =>
    `${x.interval}s timer, fires in ${esc(fmtTime(Math.max(0, x.deadline - snap.time)))}`).join('<br>') + '</div>';
}

// timeline: the time-space diagram of the viz branch, built live
function renderTimeline() {
  const snap = S.snap;
  const procs = visibleProcs();
  const colW = 150, top = 46, rowH = 26, left = 70;
  const x = {};
  procs.forEach((p, i) => { x[p.id] = left + 40 + i * colW; });
  const steps = snap.step + 1;
  const W = Math.max($('stage').clientWidth, left + 80 + procs.length * colW);
  const H = top + (steps + 1.5) * rowH + 20;
  const y = (s) => top + s * rowH + 10;
  const svg = d3.select('#tlsvg').attr('width', W).attr('height', H);
  svg.selectAll('*').remove();
  const defs = svg.append('defs');
  const markerFor = (color) => {
    const id = 'th' + color.replace('#', '');
    if (defs.select('#' + id).empty()) {
      defs.append('marker').attr('id', id).attr('markerUnits', 'userSpaceOnUse')
        .attr('markerWidth', 12).attr('markerHeight', 12).attr('viewBox', '0 0 12 12')
        .attr('refX', 9).attr('refY', 6).attr('orient', 'auto')
        .append('path').attr('d', 'M2,2 L10,6 L2,10 L6,6 L2,2').attr('fill', color);
    }
    return `url(#${id})`;
  };
  for (let s = 0; s <= snap.step; s++) {
    svg.append('text').attr('class', 'tl-step').attr('x', 12).attr('y', y(s) + 4).text('step ' + s);
  }
  svg.append('line').attr('class', 'tl-now').attr('x1', left).attr('x2', W - 10).attr('y1', y(snap.step)).attr('y2', y(snap.step));
  // the process names: a row that stays at the top (see the zoom handler)
  const head = d3.select('#tlheadsvg');
  head.selectAll('*').remove();
  procs.forEach((p) => {
    head.append('text').attr('class', 'tl-head').datum(x[p.id]).attr('y', 20).attr('text-anchor', 'middle')
      .attr('fill', classColor(p.cls)).text(p.id).style('cursor', 'pointer').on('click', () => selectProc(p.id));
    svg.append('line').attr('class', 'tl-col').attr('x1', x[p.id]).attr('x2', x[p.id])
      .attr('y1', top - 8).attr('y2', y(snap.step) + rowH).attr('stroke', classColor(p.cls)).attr('opacity', 0.5);
  });
  const selKey = S.sel && S.sel.type === 'msg' ? S.sel.key : null;
  const sends = snap.events.filter((e) => e.kind === 'send');
  const ends = {};
  snap.events.forEach((e) => { if (e.kind === 'recv' || e.kind === 'drop') ends[e.key] = e; });
  sends.forEach((e) => {
    if (x[e.src] == null || x[e.dst] == null) return;
    const end = ends[e.key];
    const color = msgColor(e.payload);
    const x1 = x[e.src], y1 = y(e.step), x2 = x[e.dst];
    const y2 = end ? y(end.step) : y(snap.step) + rowH * 0.75;
    const cls = 'tl-msg' + (end ? (end.kind === 'drop' ? ' drop' : '') : ' fly');
    if (e.key === selKey) svg.append('line').attr('class', 'tl-msg glow').attr('x1', x1).attr('y1', y1).attr('x2', x2).attr('y2', y2);
    const ln = svg.append('line').attr('class', cls).attr('x1', x1).attr('y1', y1).attr('x2', x2).attr('y2', y2)
      .attr('stroke', color).attr('marker-end', markerFor(color));
    ln.on('click', () => selectMsg(e.key))
      .on('mousemove', () => showTip(d3.event, `${e.payload}\n${e.src} to ${e.dst}, sent at step ${e.step}` +
        (end ? `, ${end.kind === 'drop' ? 'dropped' : 'received'} at step ${end.step}` : ', still in flight')))
      .on('mouseleave', hideTip);
    svg.append('circle').attr('class', 'tl-dot').attr('cx', x1).attr('cy', y1).attr('r', 3).attr('fill', color)
      .on('click', () => selectMsg(e.key));
  });
  snap.events.filter((e) => e.kind === 'timer' && x[e.pid] != null).forEach((e) => {
    svg.append('rect').attr('class', 'tl-timer').attr('x', x[e.pid] - 6).attr('y', y(e.step) - 6)
      .attr('width', 12).attr('height', 12).attr('transform', `rotate(45 ${x[e.pid]} ${y(e.step)})`)
      .on('mousemove', () => showTip(d3.event, `${e.pid}: ${e.interval}s timer fired at step ${e.step}`))
      .on('mouseleave', hideTip);
  });
  S.size.timeline = { W, H };
  syncZoom('timeline');
}

// ---------------------------------------------------------------- picking
// Clicking processes in the network shows only the messages they send; the
// others are greyed out until they are clicked again or "Show all".
function picked(pid) { return !S.filter.size || S.filter.has(pid); }
function netColor(m) { return picked(m.src) ? msgColor(m.payload) : DIM; }
function toggleFilter(id) {
  if (S.filter.has(id)) { S.filter.delete(id); renderAll(); return; }
  S.filter.add(id);
  selectProc(id);
}
function renderFilterBar() {
  $('filter-bar').hidden = !(S.filter.size && S.view === 'network');
  $('filter-names').textContent = [...S.filter].join(', ');
}

// ---------------------------------------------------------------- pan & zoom
// Each view is drawn at its full size and shown under its own transform:
// drag to move it, the wheel scrolls, Ctrl/⌘ + wheel (or a pinch) and the
// buttons zoom.  Until it is moved, a view is fitted: the whole network, or
// the timeline's width and its latest steps (so it follows the replay).
const zoomer = d3.zoom()
  .scaleExtent([0.1, 4])
  .filter(() => {
    const e = d3.event;
    if (e.target.closest && e.target.closest('.stage-tools')) return false;
    if (e.type === 'wheel') return e.ctrlKey || e.metaKey;
    return !e.button && e.type !== 'dblclick';
  })
  .wheelDelta(() => {
    const e = d3.event;
    const d = -e.deltaY * (e.deltaMode === 1 ? 0.05 : e.deltaMode ? 1 : 0.002) * (e.ctrlKey ? 10 : 1);
    return Math.max(-0.5, Math.min(0.5, d));      // pinch, but not 4x per notch
  })
  .on('zoom', () => {
    S.zoom[S.view] = d3.event.transform;
    if (d3.event.sourceEvent) S.zoomTouched[S.view] = true;
    const el = S.view === 'network' ? $('net') : $('timeline');
    const t = d3.event.transform;
    el.style.transform = `translate(${t.x}px,${t.y}px) scale(${t.k})`;
    if (S.view === 'timeline') {      // its header row: moves only sideways
      d3.selectAll('#tlheadsvg text').attr('x', (x0) => t.x + x0 * t.k);
    }
  });
function fitTransform(view) {
  const st = $('stage'), sz = S.size[view];
  const sw = st.clientWidth, sh = st.clientHeight;
  if (view === 'network') {
    const k = Math.max(0.1, Math.min(1, (sw - 16) / sz.W, (sh - 16) / sz.H));
    return d3.zoomIdentity.translate((sw - sz.W * k) / 2, (sh - sz.H * k) / 2).scale(k);
  }
  const k = Math.max(0.1, Math.min(1, sw / sz.W));
  return d3.zoomIdentity.translate(0, Math.min(0, sh - sz.H * k)).scale(k);
}
function syncZoom(view) {
  // d3 keeps one transform on the stage: make it this view's, and keep the
  // drawing (with a margin) in view when it is moved
  const sz = S.size[view], pad = 120;
  zoomer.translateExtent([[-pad, -pad], [sz.W + pad, sz.H + pad]]);
  const t = S.zoomTouched[view] && S.zoom[view] ? S.zoom[view] : fitTransform(view);
  d3.select($('stage')).call(zoomer.transform, t);
}
function zoomBy(f) {
  if (!S.snap) return;
  S.zoomTouched[S.view] = true;
  d3.select($('stage')).transition().duration(180).call(zoomer.scaleBy, f);
}
function zoomFit() {
  if (!S.snap) return;
  S.zoomTouched[S.view] = false;
  d3.select($('stage')).transition().duration(250).call(zoomer.transform, fitTransform(S.view));
}
function wireZoom() {
  const stage = $('stage');
  d3.select(stage).call(zoomer);
  stage.addEventListener('wheel', (e) => {
    if (e.ctrlKey || e.metaKey || !S.snap) return;           // zoom: d3's
    const card = e.target.closest('.card');
    if (card && card.scrollHeight > card.clientHeight) return; // the card scrolls
    e.preventDefault();
    S.zoomTouched[S.view] = true;
    const k = (S.zoom[S.view] || d3.zoomIdentity).k;
    const dx = e.shiftKey && !e.deltaX ? e.deltaY : e.deltaX, dy = e.shiftKey && !e.deltaX ? 0 : e.deltaY;
    d3.select(stage).call(zoomer.translateBy, -dx / k, -dy / k);
  }, { passive: false });
  $('zoom-in').onclick = () => zoomBy(1.25);
  $('zoom-out').onclick = () => zoomBy(0.8);
  $('zoom-fit').onclick = zoomFit;
  $('filter-reset').onclick = () => { S.filter.clear(); renderAll(); };
}

// ---------------------------------------------------------------- log
function renderLog() {
  const el = $('log');
  if (!S.snap) { el.innerHTML = '<div class="row"><span></span><span></span><span class="t" style="color:var(--muted)">Output from output() and runtime events appear here.</span></div>'; return; }
  const showSys = $('show-sys').checked;
  const atBottom = el.scrollTop + el.clientHeight >= el.scrollHeight - 30;
  const rows = S.snap.logs.filter((l) => showSys || l.kind !== 'system').map((l) => {
    const color = l.pid ? classColor(l.pid.split(':')[0]) : 'var(--muted)';
    const now = l.step === S.snap.step && S.snap.cursor > 0 ? ' now' : '';
    return `<div class="row ${l.kind}${now}"><span class="s">${l.step}</span><span class="p" style="color:${color}">${esc(l.pid || 'runtime')}</span><span class="t">${esc(l.text)}</span></div>`;
  });
  el.innerHTML = rows.join('');
  if (atBottom || S.snap.logs.length !== S.lastLogCount) el.scrollTop = el.scrollHeight;
  S.lastLogCount = S.snap.logs.length;
}

// ---------------------------------------------------------------- why panel
function selectMsg(key) { S.sel = { type: 'msg', key }; S.sweep = true; jumpToWhy(selectedWhy()); renderAll(); S.sweep = false; }
function selectChange(i) { S.sel = { type: 'change', i }; S.sweep = true; jumpToWhy(selectedWhy()); renderAll(); S.sweep = false; }
function selectProc(id) {
  S.sel = { type: 'proc', id };
  const p = procById(id);
  if (p) {
    const at = p.pausedAt ? p.pausedAt.line : (p.frames.length ? p.frames[p.frames.length - 1] : null);
    if (at && at.block) showBlock(at.block, at.line);
  }
  renderAll();
}
function selectField(pid, field) {
  const ch = S.snap.changes;
  for (let i = ch.length - 1; i >= 0; i--) {
    if (ch[i].pid === pid && ch[i].field === field) return selectChange(i);
  }
  S.sel = { type: 'proc', id: pid };
  renderAll();
  $('why').insertAdjacentHTML('afterbegin',
    `<p class="muted"><code>${esc(field)}</code> has not changed since <code>setup()</code> gave it its value.</p>`);
}
function jumpToWhy(why) {
  if (!why) return;
  const target = (why.entries && why.entries[0]) || why.line;
  if (target && target.block) showBlock(target.block, target.line);
}

function codeLink(loc, cls) {
  return `<li class="${cls || ''}" data-block="${esc(loc.block)}" data-line="${loc.line}">
    <span class="loc">${esc(locStr(loc))}</span><code>${esc(loc.text)}</code></li>`;
}
function pathHtml(why) {
  if (!why) return '';
  let h = '<ul class="path">' + codeLink(why.line, 'self');
  (why.entries || []).forEach((e) => { h += codeLink(e); });
  (why.stack || []).slice(1).forEach((f) => { h += codeLink(f); });
  if (why.cause && why.cause.type === 'timer') {
    (why.cause.setStack || []).forEach((f) => { h += codeLink(f); });
  }
  return h + '</ul>';
}
function causeHtml(why, pid) {
  const c = why && why.cause;
  if (!c) return '';
  if (c.type === 'handler') {
    const b = c.bindings && Object.keys(c.bindings).length ?
      ` with ${Object.entries(c.bindings).map(([k, v]) => `<code>${esc(k)} = ${esc(v)}</code>`).join(', ')}` : '';
    return `<p>It ran inside the <code>receive</code> handler for <code>${esc(c.payload)}</code> from ${esc(c.src)}${b},
      which arrived at step ${c.deliveredStep}. <a data-msg="${esc(c.msg)}">Why was that sent?</a></p>`;
  }
  if (c.type === 'timer') {
    const set = c.set ? ` (<code>${esc(c.set.text)}</code>, ${esc(locStr(c.set))})` : '';
    return `<p>It ran in a <code>Timer</code> callback. ${esc(pid)} started that ${c.interval}s timer at step ${c.setStep}${set},
      and it fired ${c.interval}s later because nothing cancelled it first.</p>`;
  }
  const a = c.after;
  const runName = pid === 'main' ? 'main()' : 'run()';
  if (a && a.type === 'message') {
    const w = a.await ? ` at <code>${esc(a.await.text)}</code> (${esc(locStr(a.await))})` : '';
    return `<p>${runName} was waiting${w}. It continued once <code>${esc(a.payload)}</code> from ${esc(a.src)}
      arrived at step ${a.deliveredStep}. <a data-msg="${esc(a.msg)}">Why was that sent?</a></p>`;
  }
  if (a && a.type === 'timeout') {
    const w = a.await ? ` at <code>${esc(a.await.text)}</code> (${esc(locStr(a.await))})` : '';
    return `<p>${runName} was waiting${w}. It continued because the timeout fired.</p>`;
  }
  return `<p>${runName} reached this line on its own after the process started; nothing had to arrive first.</p>`;
}
function renderWhy() {
  const el = $('why');
  const snap = S.snap;
  if (!snap) {
    el.innerHTML = '<p class="muted">Select a message, a process or a field to see why it happened. The code that caused it is highlighted.</p>';
    return;
  }
  const sel = S.sel;
  let h = '';
  if (sel && sel.type === 'msg') {
    const m = findMsg(sel.key);
    if (m) {
      h += `<h3>Why did ${esc(m.src)} send <code>${esc(m.payload)}</code> to ${esc(m.dst)}?</h3>`;
      if (m.why && m.why.cause && m.why.cause.type === 'timer') {
        h += `<p>A timer callback of ${esc(m.src)} sent it at step ${m.sentStep}: <code>${esc(m.why.line.text)}</code>.</p>`;
      } else {
        h += `<p>${esc(m.src)} called <code>${esc(m.why ? m.why.line.text : 'send')}</code> at step ${m.sentStep}.</p>`;
      }
      h += causeHtml(m.why, m.src) + pathHtml(m.why);
      if (m.inFlight) {
        h += m.recvStep != null ?
          `<p class="muted">Still in flight at this step; it is delivered at step ${m.recvStep}.
          <button class="small" data-deliver="${esc(m.key)}">Go to step ${m.recvStep}</button></p>` :
          '<p class="muted">Still in flight: the recorded run ended before it was received.</p>';
      } else {
        h += `<p class="muted">${m.dropped ? 'Dropped' : 'Delivered'} at step ${m.recvStep}.</p>`;
      }
    }
  } else if (sel && sel.type === 'change') {
    const c = snap.changes[sel.i];
    if (c) {
      h += `<h3>Why did ${esc(c.pid)}.${esc(c.field)} change?</h3>`;
      h += `<p><code>${esc(c.old == null ? '(unset)' : c.old)}</code> became <code>${esc(c.new)}</code> at step ${c.step},
        in <code>${esc(c.why ? c.why.line.text : '')}</code>.</p>`;
      h += causeHtml(c.why, c.pid) + pathHtml(c.why);
      const older = snap.changes.map((x, i) => [x, i]).filter(([x, i]) => x.pid === c.pid && x.field === c.field && i < sel.i);
      if (older.length) {
        const [px, pi] = older[older.length - 1];
        h += `<p class="muted"><a data-change="${pi}">Previous change</a>, at step ${px.step}.</p>`;
      }
    }
  } else if (sel && sel.type === 'proc') {
    const p = procById(sel.id);
    if (p) {
      h += `<h3>${esc(p.id)} is ${esc(p.status)}</h3>`;
      if (p.frames.length) {
        h += '<p class="muted">The code it is running, outermost first:</p><ul class="path here">';
        p.frames.forEach((f) => {
          h += `<li data-block="${esc(f.block)}" data-line="${f.line}"><span class="loc">${esc(f.func)}()</span><code>${esc(f.text)}</code></li>`;
        });
        h += '</ul>';
      } else if (p.status === 'done' || p.status === 'exited') {
        h += '<p>Its <code>run()</code> has finished.</p>';
      } else if (p.status === 'crashed') {
        h += `<p>It crashed: <code>${esc(p.error || '')}</code></p>`;
      } else if (p.status === 'blocked' || p.status === 'running') {
        h += '<p class="muted">Where it is in the code was not recorded for this step (traces recorded before the fix for programs under <code>da/</code> have no positions; record the run again).</p>';
      } else {
        h += '<p>It has not started running yet.</p>';
      }
      h += stepItems(snap, p.id);
    }
  } else {
    h += `<h3>${snap.cursor === 0 ? 'Step 0' : 'Step ' + snap.step}</h3><p>${esc(describeStep(snap))}</p>`;
    h += stepItems(snap, null);
    h += '<p class="muted">Select a message arrow, a process, or a field in a process to see why it happened.</p>';
  }
  el.innerHTML = h;
  el.querySelectorAll('[data-msg]').forEach((a) => { a.onclick = () => selectMsg(a.dataset.msg); });
  el.querySelectorAll('[data-change]').forEach((a) => { a.onclick = () => selectChange(+a.dataset.change); });
  el.querySelectorAll('[data-deliver]').forEach((b) => { b.onclick = () => gotoDelivery(b.dataset.deliver); });
  el.querySelectorAll('li[data-block]').forEach((li) => { li.onclick = () => showBlock(li.dataset.block, +li.dataset.line); });
}
function stepItems(snap, pid) {
  const sends = snap.events.filter((e) => e.kind === 'send' && e.step === snap.step && (!pid || e.src === pid));
  const changes = snap.changes.map((c, i) => [c, i]).filter(([c]) => c.step === snap.step && (!pid || c.pid === pid));
  let h = '';
  if (sends.length) {
    h += '<p>Messages sent at this step:</p><ul class="items">' + sends.map((e) =>
      `<li><a data-msg="${esc(e.key)}">${esc(e.src)} to ${esc(e.dst)}: <code>${esc(trunc(e.payload, 50))}</code></a></li>`).join('') + '</ul>';
  }
  if (changes.length) {
    h += '<p>State changes at this step:</p><ul class="items">' + changes.map(([c, i]) =>
      `<li><a data-change="${i}">${esc(c.pid)}.${esc(c.field)}: <code>${esc(trunc(c.old == null ? '(unset)' : c.old, 24))}</code> to <code>${esc(trunc(c.new, 24))}</code></a></li>`).join('') + '</ul>';
  }
  return h;
}

// ---------------------------------------------------------------- tooltip
function showTip(ev, text) {
  const t = $('tip');
  t.textContent = text;
  t.style.opacity = 1;
  const x = Math.min(ev.clientX + 14, window.innerWidth - t.offsetWidth - 8);
  t.style.left = x + 'px';
  t.style.top = (ev.clientY + 14) + 'px';
}
function hideTip() { $('tip').style.opacity = 0; }

// ---------------------------------------------------------------- wiring
function wire() {
  const ta = $('code');
  ta.addEventListener('scroll', syncScroll);
  $('gutter').addEventListener('click', (e) => {
    const d = e.target.closest('div[data-line]');
    if (d) toggleBreakpoint(+d.dataset.line);
  });
  $('speed').addEventListener('change', () => {
    if (S.auto) stopAuto();
    $('speed-x-wrap').hidden = $('speed').value !== 'x';
    if ($('speed').value === 'x') $('speed-x').focus();
  });
  $('speed-x').addEventListener('change', () => { if (S.auto) stopAuto(); });
  $('step').onclick = () => { stopAuto(); step(); };
  $('back').onclick = back;
  $('reset').onclick = reset;
  $('auto').onclick = toggleAuto;
  $('until').onclick = runUntil;
  $('scrub').addEventListener('change', (e) => goto(+e.target.value));
  $('add-cond').onclick = () => { S.conds.push({ expr: '', enabled: true }); renderConds(); const ins = $('cond-list').querySelectorAll('input[type=text]'); ins[ins.length - 1].focus(); };
  $('show-sys').onchange = renderLog;
  document.querySelectorAll('.viewtabs button[data-view]').forEach((b) => {
    b.onclick = () => {
      S.view = b.dataset.view;
      document.querySelectorAll('.viewtabs button[data-view]').forEach((x) => x.classList.toggle('on', x === b));
      renderStage({});
    };
  });
  $('why-toggle').onclick = () => {
    const w = $('why-wrap');
    const c = w.classList.toggle('collapsed');
    $('why-toggle').textContent = c ? 'Show' : 'Hide';
    $('why-toggle').setAttribute('aria-expanded', String(!c));
  };
  document.addEventListener('keydown', (e) => {
    const tag = (e.target.tagName || '').toLowerCase();
    if (tag === 'textarea' || tag === 'input' || tag === 'select') return;
    if (e.key === 'ArrowRight') { e.preventDefault(); stopAuto(); step(); }
    else if (e.key === 'ArrowLeft') { e.preventDefault(); back(); }
    else if (e.key === ' ') { e.preventDefault(); toggleAuto(); }
    else if (e.key === 'Escape') { S.sel = null; renderAll(); }
  });
  window.addEventListener('resize', () => renderStage({}));
  wireZoom();
}

wire();
renderAll();
load().catch(() => setStatus('Could not reach the server. Is python -m da.replay running?', 'error'));

'use strict';

const $ = (s) => document.querySelector(s);
const $$ = (s) => [...document.querySelectorAll(s)];
const escape = (v) => String(v ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const number = (n) => Number(n || 0).toLocaleString('en-US');
const compact = (n) => n >= 1e6 ? `${(n / 1e6).toFixed(1)}M` : n >= 1e3 ? `${(n / 1e3).toFixed(1)}k` : number(n);
const bytes = (n) => n >= 1024 ** 3 ? `${(n / 1024 ** 3).toFixed(2)} GiB` : `${(n / 1024 ** 2).toFixed(1)} MiB`;
const seconds = (n) => n == null ? '执行中' : n >= 3600 ? `${(n / 3600).toFixed(1)}h` : `${Math.round(n / 60)}m ${Math.round(n % 60)}s`;
const labels = {passed:'通过', failed:'未通过', running:'运行中', paused:'已暂停', pending:'未开始', grading:'评分中', complete:'已完成', error:'执行异常', cancelled:'已中断', queued:'待续跑'};
const badge = (status) => `<span class="status ${escape(status)}">${labels[status] || escape(status)}</span>`;
const state = {overview:null, task:null, taskName:'', attemptName:'', trace:null, turn:0, turnData:null, tab:'explore', detail:'actions', filter:'all', taskQuery:'', turnQuery:'', inherited:false, busy:false, play:null, generation:0, copies:new Map(), copyId:0, demo:0};
const initial = new URLSearchParams(location.hash.slice(1));
const campaign = new URLSearchParams(location.search).get('campaign');
const agentforkOnly = () => state.overview?.configuration?.mode === 'agentfork-only';
const memoryBackend = () => state.overview?.configuration?.checkpoint_backend === 'criu';

async function api(action, params = {}) {
  const url = `/api/${action}?${new URLSearchParams(campaign ? {...params, campaign} : params)}`;
  const response = await fetch(url, {cache:'no-store'});
  if (!response.ok) throw new Error(`读取失败 (${response.status})：${action}`);
  return response.json();
}
function banner(error) {
  $('#error-banner').hidden = !error;
  $('#error-banner').textContent = error ? `${error.message || error}。将继续尝试同步。` : '';
}
function toast(message) {
  $('#toast').textContent = message; $('#toast').hidden = false;
  clearTimeout(toast.timer); toast.timer = setTimeout(() => $('#toast').hidden = true, 2300);
}
function updateHash() {
  const hash = new URLSearchParams({task:state.taskName, attempt:state.attemptName, turn:state.turn, tab:state.tab});
  history.replaceState(null, '', `#${hash}`);
}
function selectedAttempt() {return state.task?.attempts.find(a => a.name === state.attemptName);}

function renderMetrics() {
  const o = state.overview, p = o.progress, r = o.resources;
  const complete = o.tasks.filter(t => t.status === 'complete').length;
  const running = o.tasks.filter(t => t.status === 'running').length;
  const baseline = o.tasks.filter(t => t.baseline_resolved).length;
  const agentfork = o.tasks.filter(t => t.agentfork_resolved).length;
  const cards = [
    ['已完成任务', '◴', `${complete}<span> / ${o.tasks.length}</span>`, `${p.state === 'paused' ? `${o.tasks.filter(t => t.status === 'paused').length} 个任务已暂停` : `${running} 个任务运行中`} · ${o.tasks.filter(t => t.status === 'error').length} 个异常`, true],
    agentforkOnly() ? ['AgentFork · 初始轨迹', '◇', `${o.tasks.filter(t => t.root_resolved).length}<span> / ${complete || '—'}</span>`, 'Pass@4 已关闭 · 仅重跑历史未解决任务'] : ['独立采样 · Pass@4', '◇', `${baseline}<span> / ${complete || '—'}</span>`, '已完成任务中的解决数'],
    ['AgentFork · 分支搜索', '⌘', `${agentfork}<span> / ${complete || '—'}</span>`, '初始轨迹 + 最多两轮回溯'],
    ['累计创建断点', '⊞', number(r.checkpoints_created), `快照合计 ${(r.checkpoint_capture_seconds || 0).toFixed(2)} 秒`],
  ];
  $('#metrics').innerHTML = cards.map(([name,icon,value,note,progress]) => `<div class="metric"><div class="metric-top">${name}<span class="metric-symbol">${icon}</span></div><div class="metric-value">${value}</div><div class="metric-note">${note}</div>${progress ? `<div class="mini-progress"><i style="width:${complete / o.tasks.length * 100}%"></i></div>` : ''}</div>`).join('');
  const selector = $('#campaign-select');
  if (selector && !selector.options.length) {
    selector.innerHTML = (o.campaigns || []).map(c => `<option value="${escape(c.id)}" ${c.id===o.campaign_id?'selected':''}>${escape(c.label)}</option>`).join('');
    selector.addEventListener('change', () => {location.href = `?campaign=${encodeURIComponent(selector.value)}`;});
  }
  $('#task-count').textContent = o.tasks.length;
  $('#campaign-name').textContent = o.campaign;
  const age = Date.now() / 1000 - (p.updated_at || 0);
  $('#live-label').textContent = p.state === 'paused' ? '实验已暂停' : p.state === 'waiting_storage' ? '等待存储空间' : p.state === 'running' ? age < 60 ? '实时同步' : '数据待更新' : p.state === 'finished' ? '实验结束' : '进程已停止';
  $('#last-updated').textContent = `最近同步 ${new Date().toLocaleTimeString('zh-CN', {hour12:false})} · 每 10 秒刷新`;
  const health = o.health, panel = $('#health-status');
  panel.hidden = !health;
  if (health) {
    const stale = Date.now() / 1000 - health.updated_at > 90;
    const names = {checkpoint_capture_error:'断点保存失败',context_or_gateway_error:'上下文或网关异常',attempt_infrastructure_error:'轨迹执行异常',provider_failed_attempt:'API 请求失败',review_output_exhausted:'复盘输出额度耗尽',task_error:'任务执行异常',activity_needs_inspection:'执行长时间未更新',controller_absent:'执行进程已停止',monitor_absent:'进度监控已停止',supervisor_absent:'重试监督已停止',process_paused:'进程暂停',progress_stale:'进度数据未更新',storage_reserve:'存储空间不足',storage_unavailable:'存储不可用'};
    const unresolved = (health.issues || []).filter(i => !i.resolved && !i.historical);
    const recovered = (health.issues || []).filter(i => i.resolved).length;
    const checked = Object.values(health.validated_checkpoints_by_backend_revision || {}).reduce((a,b) => a+b, 0);
    const failures = health.current_revision_failure_counts || health.failure_counts || {};
    const untimed = health.untimed_failure_counts?.checkpoint_capture_error || 0;
    panel.classList.toggle('attention', stale || unresolved.length > 0);
    panel.textContent = stale ? '监控数据待更新，请检查监控进程。' : `${health.state === 'finished' ? '实验已结束' : unresolved.length ? `监控发现 ${unresolved.length} 项待检查异常` : '监控正常'} · ${number(checked)} 个断点通过恢复校验 · 本轮保存异常 ${failures.checkpoint_capture_error || 0} 次${untimed ? `（另有 ${untimed} 次异常未记录时间）` : ''} · API 失败 ${failures.provider_failed_attempt || 0} 次 · 复盘截断 ${failures.review_output_exhausted || 0} 次${recovered ? ` · 含历史的异常已恢复记录 ${recovered} 条` : ''}${unresolved.length ? ' · ' + [...new Set(unresolved.map(i => names[i.kind] || i.kind))].join('、') : ''}`;
  }
  const reviewsPanel = $('#review-progress');
  if (reviewsPanel) {
    const reviews = health?.reviewer_activity || [], now = Date.now() / 1000;
    reviewsPanel.hidden = !reviews.length;
    reviewsPanel.innerHTML = reviews.map(row => {
      const stage = row.stage === 'analysis' ? `分析轨迹 ${row.attempt || ''}` : '生成分支计划';
      const last = row.last_byte_at ? `最近数据 ${Math.max(0, Math.round(now - row.last_byte_at))} 秒前` : '等待首批数据';
      return `<div class="review-item"><div class="review-top"><strong>${escape(row.task)}</strong><span>${row.round ? `第 ${number(row.round)} 轮 · ` : ''}${escape(stage)}</span></div><p>本次生成 ${seconds(Math.max(0, now - row.started_at))} · 已接收 ${bytes(row.bytes_received || 0)} · ${last}</p></div>`;
    }).join('');
  }
}
function renderTasks() {
  const tasks = state.overview.tasks.filter(t => (state.filter === 'all' || t.status === state.filter) && t.name.toLowerCase().includes(state.taskQuery.toLowerCase()));
  $('#task-list').innerHTML = tasks.length ? tasks.map(t => `<button class="task-item ${t.name === state.taskName ? 'active' : ''}" data-task="${escape(t.name)}"><span class="task-icon ${t.status}">${{complete:'✓',running:'◉',paused:'Ⅱ',cancelled:'Ⅱ',pending:'○',error:'!'}[t.status]}</span><span class="task-item-content"><strong>${escape(t.name)}</strong><small>${t.status === 'complete' ? `${agentforkOnly() ? '' : `Pass@4 ${t.baseline_resolved ? '✓':'×'} &nbsp; `}AgentFork ${t.agentfork_resolved ? '✓':'×'}` : t.recovery?.state==='queued' ? '断点已保留 · 待续跑' : t.status === 'running' ? `${t.attempts} 条轨迹 · 运行中` : labels[t.status]}</small></span></button>`).join('') : '<div class="empty small">没有匹配的任务</div>';
}

async function selectTask(name, options = {}) {
  stopPlayback(); const generation = ++state.generation;
  state.taskName = name; state.attemptName = ''; state.trace = null; state.turnData = null;
  $('#task-title').textContent = name; $('#task-breadcrumb').textContent = name;
  $('#graph').innerHTML = '<div class="empty">正在读取分支关系…</div>';
  $('#turn-list').innerHTML = '<div class="empty small">正在读取轨迹…</div>';
  $('#detail-content').innerHTML = '<div class="empty">正在读取本地实验文件…</div>';
  $('#sidebar').classList.remove('open'); renderTasks();
  try {
    const task = await api('task', {task:name});
    if (generation !== state.generation) return;
    state.task = task;
    renderTask();
    const branch = task.attempts.find(a => a.name === 'branch-r2-0') || task.attempts.find(a => a.origin);
    const attempt = options.attempt && task.attempts.some(a => a.name === options.attempt) ? options.attempt : branch?.name || task.attempts[0]?.name;
    if (attempt) await selectAttempt(attempt, {turn:options.turn});
    else {
      state.attemptName = ''; state.trace = null;
      $('#selection-summary').innerHTML = '<span class="muted">任务尚未开始。启动后轨迹会自动出现。</span>';
      $('#turn-list').innerHTML = '<div class="empty small">尚无执行步骤</div>';
      $('#detail-content').innerHTML = '<div class="empty">此任务尚未开始，暂无轨迹数据。</div>';
    }
    updateHash(); banner(null);
  } catch (error) {banner(error);}
}
function renderTask() {
  const task = state.task, row = state.overview.tasks.find(t => t.name === task.name);
  $('#task-title').textContent = task.name; $('#task-breadcrumb').textContent = task.name;
  $('#task-subtitle').innerHTML = `${badge(row?.recovery?.state === 'queued' ? 'queued' : row?.status || 'pending')}<span>deepseek-flash</span><span>Claude Code 2.1.317</span><span>1M context · thinking max</span>${task.gate ? `<span>Oracle ${task.gate.oracle?.reward} / Nop ${task.gate.nop?.reward}</span>`:''}`;
  if ($('#instruction pre').textContent !== task.instruction) $('#instruction pre').textContent = task.instruction;
  $('#review-count').textContent = task.reviews.length;
  const diagnostic = task.diagnostics, retry = task.retry;
  const diagnosticPanel = $('#task-diagnostics');
  diagnosticPanel.hidden = !diagnostic && !retry && !task.error && !task.recovery;
  const diagnosticFingerprint = JSON.stringify([task.name, diagnostic, retry, task.error, task.result?.status, task.recovery]);
  if (!diagnosticPanel.hidden && diagnosticPanel.dataset.fingerprint !== diagnosticFingerprint) {
    diagnosticPanel.dataset.fingerprint = diagnosticFingerprint;
    const cause = diagnostic?.cause || retry?.diagnosis?.cause || task.error?.causes?.map(c => c.message).join('; ') || task.error?.error || retry?.previous_error?.error || '已保留磁盘断点与对应的原生会话';
    const heading = task.error ? '执行异常' : task.result && retry ? '异常重跑已完成' : retry ? '正在重跑' : task.recovery?.state==='queued' ? '等待续跑' : task.recovery ? '从断点续跑' : '修复验证中';
    const detail = task.error || retry?.previous_error || diagnostic?.original_error;
    diagnosticPanel.innerHTML = `<div class="notice ${task.error?'warm':task.result?'green':''}"><strong>${heading}：${escape(cause || '')}</strong><p>${escape(diagnostic?.repair || retry?.diagnosis?.repair || (task.recovery ? '此前的探索已保存。执行资源分配到此任务后，会接着保存的历史继续；修复期间的排队时间不占用 agent 执行限额。' : '错误记录已保留，正在检查。'))}</p>${diagnostic?.status==='gate_passed'?'<p>修复后原版参考解与空跑检查已通过。</p>':''}${diagnostic?.status==='controller_verified'&&!retry&&!task.result?'<p>修复已通过回归验证，等待切换控制器后重跑。</p>':''}${detail?`<details><summary>查看原始异常记录</summary><pre>${escape(JSON.stringify(detail,null,2))}</pre></details>`:''}${retry?.archive?'<p>之前的轨迹和评分记录已归档保留。</p>':''}</div>`;
  }
  renderGraph(); renderReviews();
  if (state.tab === 'how') renderHow();
}

function nodeHtml(attempt, position = '', name = '') {
  const a = attempt || {name, state:'pending', usage:{requests:0}};
  const reward = a.reward != null ? `reward ${a.reward.toFixed(1)}` : a.state === 'paused' ? '进程和断点已保留' : a.state === 'pending' ? '等待初始轨迹' : a.state === 'queued' ? '断点已保留' : a.state === 'grading' ? '原版 verifier 评分' : a.state === 'error' ? '进程异常退出' : a.state === 'cancelled' ? '随任务中断' : '模型正在执行';
  return `<button class="attempt-node ${a.name === state.attemptName ? 'selected' : ''}" ${position} ${attempt ? `data-attempt="${escape(a.name)}"`:'disabled'} aria-label="查看 ${escape(a.name)} 轨迹"><div class="node-head"><span class="node-name">${a.name === 'root-0' ? '<span class="node-start">◉</span>' : ''}${escape(a.name)}</span>${badge(a.state)}</div>${a.plan ? `<div class="node-idea" title="${escape(a.plan.name)}">${escape(a.plan.name)}</div>`:''}<div class="node-note"><span>${a.usage.requests} calls${a.checkpoints ? ` · ${a.checkpoints} ckpts`:''}</span><span>${reward}</span></div></button>`;
}
function renderGraph() {
  const attempts = state.task.attempts, root = attempts.find(a => a.name === 'root-0');
  const r1 = attempts.filter(a => a.name.startsWith('branch-r1-'));
  const r2 = attempts.filter(a => a.name.startsWith('branch-r2-'));
  const lanes = [[root].filter(Boolean), r1, r2];
  const rows = Math.max(1, r1.length, r2.length), height = Math.max(155, rows * 108 + 55);
  const width = r2.length ? 850 : r1.length ? 850 : 780;
  const x = [28, 321, 614], positions = {};
  for (let c = 0; c < 3; c++) lanes[c].forEach((a,i) => {positions[a.name] = {x:x[c], y:44 + (height - 55 - lanes[c].length * 108) / 2 + i * 108};});
  let svg = '', nodes = '', edges = '';
  for (const a of [...r1, ...r2]) {
    const from = positions[a.origin?.attempt], to = positions[a.name];
    if (!from || !to) continue;
    const active = a.name === state.attemptName;
    const sx = from.x + 205, sy = from.y + 42, tx = to.x, ty = to.y + 42;
    const mid = (sx + tx) / 2;
    svg += `<path class="edge ${active?'active':''}" d="M${sx},${sy} C${mid},${sy} ${mid},${ty} ${tx},${ty}"/>`;
    const t = a.name.startsWith('branch-r2-') ? 0.74 : 0.5, u = 1-t;
    const lx = u*u*u*sx + 3*u*u*t*mid + 3*u*t*t*mid + t*t*t*tx;
    const ly = (u*u*u + 3*u*u*t)*sy + (3*u*t*t + t*t*t)*ty;
    edges += `<button class="edge-label" style="left:${lx}px;top:${ly}px" data-origin-attempt="${escape(a.origin.attempt)}" data-origin-step="${a.origin.step}" title="查看父轨迹的断点 #${a.origin.step}">#${a.origin.step} ↗</button>`;
  }
  for (const a of lanes.flat()) {
    const pos = positions[a.name]; nodes += nodeHtml(a, `style="left:${pos.x}px;top:${pos.y}px"`);
  }
  if (!root) nodes = nodeHtml(null, `style="left:28px;top:55px"`, 'root-0');
  const headings = ['初始轨迹', '第一轮分支', '第二轮分支'];
  const titles = headings.map((title,i) => `<div class="lane-title" style="left:${x[i]}px">${title}<small>${i ? `ROUND 0${i}` : agentforkOnly() ? 'AgentFork ROOT' : 'SHARED ROOT'}</small></div>`).join('');
  const placeholder = !r1.length ? `<div class="empty" style="position:absolute;left:315px;right:20px;top:51px;padding:20px;font-size:10px">${root?.state === 'passed' ? '初始轨迹已通过，本任务无需回溯。' : root?.state === 'failed' ? '等待 Analyst 分析失败并由 Reviewer 选择分支断点。' : '初始轨迹失败后，AgentFork 会从已保存的断点探索新分支。'}</div>` : !r2.length ? '<div class="empty" style="position:absolute;left:612px;top:80px;width:205px;padding:15px;font-size:10px">第一轮仍未解决时，继续从已有分支中选择断点。</div>' : '';
  $('#graph').style.width = `${width}px`; $('#graph').style.height = `${height}px`;
  $('#graph').innerHTML = `<svg viewBox="0 0 ${width} ${height}" aria-hidden="true">${svg}</svg>${titles}${nodes}${edges}${placeholder}`;
  $('.baseline-section').hidden = agentforkOnly();
  $('#baseline').innerHTML = [1,2,3].map(i => nodeHtml(attempts.find(a => a.name === `root-${i}`), '', `root-${i}`)).join('');
  $('#graph-caption').textContent = `${attempts.length} 条轨迹 · ${r1.length + r2.length} 个回溯分支 · 点击连线上的 # 断点返回父轨迹`;
}

async function selectAttempt(name, options = {}) {
  stopPlayback();
  const taskName = state.taskName;
  state.attemptName = name; state.turnData = null; state.turnQuery = '';
  $('#turn-search').value = ''; state.detail = options.detail || 'actions';
  $$('.detail-tabs button').forEach(b => b.classList.toggle('active', b.dataset.detail === state.detail));
  renderGraph(); renderSelection();
  $('#turn-list').innerHTML = '<div class="empty small">正在读取轨迹…</div>';
  try {
    const trace = await api('attempt', {task:taskName, attempt:name});
    if (taskName !== state.taskName || name !== state.attemptName) return;
    state.trace = trace; state.inherited = false; $('#show-inherited').checked = false;
    if (options.checkpoint != null) {
      const pointTurn = trace.turns.find(t => t.checkpoints.includes(options.checkpoint));
      if (pointTurn?.inherited) {state.inherited = true; $('#show-inherited').checked = true;}
      state.turn = pointTurn?.id ?? 0;
    } else if (options.turn != null && trace.turns.some(t => t.id === +options.turn)) {
      state.turn = +options.turn;
      if (trace.turns[state.turn].inherited) {state.inherited = true; $('#show-inherited').checked = true;}
    } else state.turn = trace.turns.find(t => !t.inherited)?.id ?? 0;
    renderSelection(); renderTurns(); await renderDetail(); scrollSelected(); updateHash();
  } catch (error) {banner(error);}
}
function renderSelection() {
  const a = selectedAttempt(); if (!a) return;
  const origin = a.origin;
  $('#selection-summary').innerHTML = `<div class="attempt-label">${escape(a.name)} ${badge(a.state)}</div><div class="selection-meta">${origin ? `<button class="origin-chip" data-origin-attempt="${escape(origin.attempt)}" data-origin-step="${origin.step}">↳ ${escape(origin.attempt)} @ #${origin.step}</button>` : `<span>${a.name === 'root-0' ? (agentforkOnly() ? 'AgentFork 初始轨迹' : '共享的初始轨迹') : '从初始环境独立开始'}</span>`}<span><b>${number(a.usage.requests)}</b> API calls</span><span>输入 <b>${compact(a.usage.input_tokens)}</b></span><span>输出 <b>${compact(a.usage.output_tokens)}</b></span><span>缓存 <b>${a.usage.input_tokens ? Math.round(a.usage.cached_tokens / a.usage.input_tokens * 100):0}%</b></span></div><div class="selection-actions">${origin ? '<button class="button" data-show-audit>核对恢复证据 ↗</button>':''}<button class="button" data-share>复制当前位置</button></div>`;
}
function visibleTurns() {
  return (state.trace?.turns || []).filter(t => (state.inherited || !t.inherited) && `${t.title} ${t.tools.join(' ')} ${t.preview}`.toLowerCase().includes(state.turnQuery.toLowerCase()));
}
function renderTurns() {
  if (!state.trace) return;
  const all = state.trace.turns, turns = visibleTurns(), inherited = all.filter(t => t.inherited).length;
  $('#inherited-control').hidden = !inherited;
  $('#turn-count').textContent = `${turns.length}${inherited && !state.inherited ? ` + ${inherited} 继承`:''}`;
  $('#turn-list').innerHTML = turns.length ? turns.map(t => `<button class="turn-item ${t.id === state.turn ? 'active':''} ${t.inherited?'inherited':''} ${t.guided?'guided':''}" data-turn="${t.id}" aria-label="步骤 ${t.id+1}：${escape(t.title)}"><div class="turn-item-head"><span>T${String(t.id+1).padStart(3,'0')}</span><span class="turn-tools">${escape([...new Set(t.tools)].join(' · ') || 'response')}</span>${t.has_error?'<i class="error-dot"></i>':''}${t.checkpoints.length?`<span class="checkpoint-pill">#${t.checkpoints.join(',')}</span>`:''}</div><div class="turn-description">${escape(t.title)}</div>${t.guided?'<span class="guided-pill">↳ Reviewer 起始动作</span>':''}</button>`).join('') : '<div class="empty small">暂无匹配的执行步骤</div>';
  const index = turns.findIndex(t => t.id === state.turn);
  $('#previous-turn').disabled = index <= 0; $('#next-turn').disabled = index < 0 || index >= turns.length - 1;
  $('#play').disabled = turns.length < 2;
}
function scrollSelected() {
  const rail = $('#turn-list'), selected = $('#turn-list .turn-item.active');
  if (!selected) return;
  const item = selected.getBoundingClientRect(), box = rail.getBoundingClientRect();
  if (rail.scrollHeight > rail.clientHeight) {
    if (item.top < box.top) rail.scrollTop -= box.top - item.top + 5;
    else if (item.bottom > box.bottom) rail.scrollTop += item.bottom - box.bottom + 5;
  }
  if (rail.scrollWidth > rail.clientWidth) {
    if (item.left < box.left) rail.scrollLeft -= box.left - item.left + 5;
    else if (item.right > box.right) rail.scrollLeft += item.right - box.right + 5;
  }
}
async function selectTurn(index, playback = false) {
  if (!playback) stopPlayback();
  state.turn = index; state.turnData = null; renderTurns(); scrollSelected(); updateHash(); await renderDetail();
}
function stopPlayback() {
  clearInterval(state.play); state.play = null; $('#play').textContent = '▶ 回放轨迹';
}
function moveTurn(delta, playback = false) {
  const turns = visibleTurns(), index = turns.findIndex(t => t.id === state.turn);
  const target = turns[index + delta];
  if (target) selectTurn(target.id, playback);
  else if (playback) {stopPlayback(); toast('已播放到轨迹末尾');}
}
function play() {
  if (state.play) return stopPlayback();
  const turns = visibleTurns();
  if (turns.length < 2) return;
  if (state.turn === turns.at(-1).id) selectTurn(turns[0].id, true);
  $('#play').textContent = 'Ⅱ 暂停回放';
  state.play = setInterval(() => moveTurn(1, true), 2400);
}

function code(text, dark = false, limit = 18000) {
  text = typeof text === 'string' ? text : JSON.stringify(text, null, 2);
  const id = String(++state.copyId); state.copies.set(id, text);
  if (!state.pruneScheduled) {
    state.pruneScheduled = true;
    queueMicrotask(() => {
      const visible = new Set($$('[data-copy],[data-expand]').map(el => el.dataset.copy || el.dataset.expand));
      for (const key of state.copies.keys()) if (!visible.has(key)) state.copies.delete(key);
      state.pruneScheduled = false;
    });
  }
  return `<div class="code-block ${dark?'dark':''}"><button class="copy-btn" data-copy="${id}">复制</button><pre>${escape(text.slice(0,limit))}</pre></div>${text.length > limit ? `<button class="open-output" data-expand="${id}">展开全部 ${number(text.length)} 字符 ↓</button>`:''}`;
}
function checkpointTable(point) {
  return `<table class="data-table"><tbody>${Object.entries({断点:`#${point.step}`,磁盘快照:point.snapshot_id,恢复后端:point.snapshot_backend || 'btrfs-disk',断点资格:point.eligible === false ? '不可用：恢复失败' : point.memory_checkpoint?.validation?.passed ? '子分支恢复已验证' : '历史断点，按需验证',...(point.memory_checkpoint ? {压缩内存镜像:bytes(point.memory_checkpoint.archive_bytes)} : {}),工作目录:point.cwd,原生前缀:`${number(point.byte_length)} bytes`,模型历史:`${point.message_count} messages`,快照耗时:`${(point.capture_seconds * 1000).toFixed(2)} ms`,SHA256:point.sha256}).map(([k,v]) => `<tr><th>${k}</th><td>${escape(v)}</td></tr>`).join('')}</tbody></table>`;
}
async function renderDetail() {
  const task = state.taskName, attempt = state.attemptName, id = state.turn, mode = state.detail;
  const token = `${task}/${attempt}/${id}/${mode}`;
  state.detailToken = token;
  $('#detail-title').textContent = `${attempt || 'trajectory'} / T${String(id+1).padStart(3,'0')}`;
  const dest = $('#detail-content');
  if (!attempt) {dest.innerHTML = '<div class="empty">此任务尚未开始，暂无轨迹数据。</div>'; return;}
  try {
    if (mode === 'audit') {
      const audit = await api('audit', {task, attempt});
      if (state.detailToken !== token) return;
      const fingerprint = `audit/${task}/${attempt}/${JSON.stringify(audit)}`;
      if (dest.dataset.fingerprint !== fingerprint) {dest.innerHTML = auditHtml(audit); dest.dataset.fingerprint = fingerprint;}
      return;
    }
    if (mode === 'grade') {
      const grade = await api('grade', {task, attempt});
      if (state.detailToken !== token) return;
      const fingerprint = `grade/${task}/${attempt}/${JSON.stringify(grade)}`;
      if (dest.dataset.fingerprint === fingerprint) return;
      dest.innerHTML = grade.grade ? `<div class="notice ${grade.grade.reward === 1?'green':'warm'}">原版 Harbor verifier · reward = <strong>${grade.grade.reward}</strong></div>${grade.logs.map(log => `<div class="section-label">${escape(log.name)}</div>${code(log.content)}`).join('') || '<div class="empty small">暂无可展示的评分日志。</div>'}` : '<div class="empty">该轨迹尚未完成评分。</div>';
      dest.dataset.fingerprint = fingerprint;
      return;
    }
    if (!state.trace?.turns.length) {dest.innerHTML = '<div class="empty">模型尚未返回完整动作，稍后会自动同步。</div>'; return;}
    const turn = state.turnData?.id === id ? state.turnData : await api('turn', {task, attempt, turn:id});
    if (state.detailToken !== token) return;
    state.turnData = turn;
    dest.dataset.fingerprint = '';
    if (mode === 'thinking') {
      const thinking = turn.thinking.join('\n\n');
      dest.innerHTML = `<div class="thinking-meta"><span>${turn.guided ? 'Reviewer 撰写的起始推理' : '模型返回的 thinking 内容'}</span><span>${number(thinking.length)} characters</span></div>${turn.guided ? '<div class="notice">这是 Reviewer 为此分支编写的起始动作。之后的推理由 actor 模型继续生成。</div>':''}${thinking ? code(thinking, false, 24000) : '<div class="empty">此步骤没有记录 thinking 内容。</div>'}`;
      return;
    }
    let html = turn.inherited ? '<div class="notice">这一步来自父轨迹的恢复前缀，分支没有重新执行它。</div>' : '';
    if (turn.guided) html += '<div class="notice">↳ 分支的第一步由 Reviewer 编写；以下命令在恢复后的子环境中执行。</div>';
    html += turn.text.map(t => `<div class="text-message">${escape(t)}</div>`).join('');
    html += turn.tools.map(tool => {
      const input = tool.input || {}; let body;
      if (tool.name === 'Bash' && input.command != null) body = code(input.command, true);
      else if (tool.name === 'Edit') body = `<div class="file-label">${escape(input.file_path || '')}</div><div class="diff-block"><div class="diff-label remove">− 修改前</div>${code(input.old_string || '')}<div class="diff-label add">+ 修改后</div>${code(input.new_string || '')}</div>`;
      else if (tool.name === 'Write') body = `<div class="file-label">${escape(input.file_path || '')}</div>${code(input.content || '', true)}`;
      else body = code(input);
      const output = tool.output === null ? '<div class="empty small">当前保存的历史尚未包含工具返回。</div>' : code(tool.output, false, 12000);
      return `<section class="tool-card"><header class="tool-header"><span class="tool-icon">${tool.name === 'Bash'?'>_':tool.name === 'Edit'?'±':'▤'}</span><strong>${escape(tool.name)}</strong><span class="tool-description">${escape(input.description || input.file_path || '')}</span></header>${body}<details class="tool-output" ${tool.is_error || (tool.output || '').length < 2500 ? 'open':''}><summary class="output-label"><span class="${tool.is_error?'error':''}">${tool.is_error?'! 工具报错':'↳ 工具返回'}</span><span>${tool.output == null?'等待记录':`${number(tool.output.length)} chars`} · 点击展开</span></summary>${output}</details></section>`;
    }).join('');
    if (!turn.tools.length && !turn.text.length) html += '<div class="notice">这一步仅包含模型思考，可切换到「模型思考」查看记录。</div>';
    for (const point of turn.checkpoint_details) html += `<details class="checkpoint-details"><summary>⊞ 此处保存了断点 #${point.step} <span>${(point.capture_seconds*1000).toFixed(1)} ms</span></summary>${checkpointTable(point)}</details>`;
    dest.innerHTML = html;
  } catch (error) {if (state.detailToken === token) dest.innerHTML = `<div class="empty">${escape(error.message)}</div>`;}
}
function auditHtml(audit) {
  if (!audit.is_branch) return '<div class="notice">这是从初始环境启动的根轨迹，无需恢复父轨迹。选择任意 AgentFork 分支可查看恢复证据。</div>';
  if (!audit.available) return '<div class="empty">该分支的恢复请求还未完整落盘，稍后会自动出现。</div>';
  const checks = [
    [audit.model_prefix_equal, '模型消息前缀一致', '比较断点保存的 messages 与分支第一次实际模型请求'],
    [audit.system_equal, '系统上下文一致', '比较父断点与恢复后请求的 system 字段'],
    [audit.native_prefix_hash_valid, '原生会话前缀校验通过', '对保存的原生 JSONL 前缀重新计算 SHA256'],
  ];
  return `<div class="detail-intro">从 <button class="origin-chip" data-origin-attempt="${escape(audit.origin.attempt)}" data-origin-step="${audit.origin.step}">${escape(audit.origin.attempt)} @ #${audit.origin.step} ↗</button> 恢复${audit.snapshot_backend === 'btrfs-criu' ? '文件系统、进程内存和' : '文件系统和'}对应的会话前缀。</div><div class="audit-checks">${checks.map(([ok,title,note]) => `<div class="audit-row"><span class="audit-icon ${ok?'':'fail'}">${ok?'✓':'!'}</span><div><strong>${escape(title)}${ok?'':' · 未通过'}</strong><small>${escape(note)}</small></div></div>`).join('')}</div>${checkpointTable(audit.origin)}<div class="notice ${audit.snapshot_retained?'green':'warm'}" style="margin-top:15px">${audit.snapshot_retained?'磁盘快照当前仍保留在实验工作区。':'该任务已结束，磁盘快照已回收；模型请求和原生会话前缀仍保留供核对。'}<br>这些校验确认了会话恢复，不等同于任务已解决。${audit.snapshot_backend === 'btrfs-criu' ? 'CRIU 分支同时恢复本容器的进程内存；外部连接等无法安全保存的状态会使该断点不可用。' : '磁盘快照不保存进程内存、连接或其他容器状态。'}</div>`;
}

function renderReviews() {
  const task = state.task;
  const analysts = task.attempts.filter(a => a.analysis);
  const revision = JSON.stringify([task.name, task.reviews, analysts.map(a => [a.name,a.analysis]), task.review_usage, task.attempts.map(a => [a.name,a.state])]);
  if (state.reviewRevision === revision) return;
  state.reviewRevision = revision;
  const expanded = new Map($$('#reviews-panel details[data-review-key]').map(el => [el.dataset.reviewKey, el.open]));
  const reviewUsage = task.review_usage.reduce((a,r) => a + (r.usage?.completion_tokens || 0), 0);
  let html = `<div class="review-content"><div class="review-heading"><h3>失败分析与分支计划</h3><span>${task.reviews.length} 轮 · ${task.review_usage.length} 次评审调用 · 输出 ${compact(reviewUsage)} tokens</span></div><div class="notice">这些内容由真实 Analyst / Reviewer 调用生成。点击分支进入对应轨迹，或跳到它选中的父断点。</div>`;
  for (const attempt of analysts) {
    const a = attempt.analysis;
    html += `<details class="review-card" data-review-key="analyst-${escape(attempt.name)}"><summary><b>Analyst · ${escape(attempt.name)}</b><span>失败原因与可挽回的进展 ⌄</span></summary><div class="review-body">${[['失败原因',a.failure_reason],['修复建议',a.lesson],['可保留的进展',a.salvage]].map(([title,content]) => `<div class="section-label">${title}</div><p>${escape(content || '未记录')}</p>`).join('')}<div class="section-label">候选断点</div>${(a.branch_candidates || []).map(c => `<p><button class="origin-chip" data-origin-attempt="${escape(attempt.name)}" data-origin-step="${c.step}">#${c.step} ↗</button> ${escape(c.why)}</p>`).join('')}</div></details>`;
  }
  for (const round of task.reviews) {
    html += `<details class="review-card" data-review-key="round-${round.round}" open><summary><b>Reviewer · Round ${round.round}</b><span>${round.branches.length} 个实际分支 ⌄</span></summary><div class="review-body"><div class="section-label">综合判断</div><p>${escape(round.synthesis)}</p>${round.branches.map((b,i) => {
      const name = `branch-r${round.round}-${i}`, attempt = task.attempts.find(a => a.name === name);
      return `<div class="branch-review"><div class="branch-review-head"><strong>${escape(b.name)}</strong><button class="origin-chip" data-origin-attempt="${escape(b.base)}" data-origin-step="${b.branch_step}">${escape(b.base)} @ #${b.branch_step} ↗</button></div><p>${escape(b.why)}</p><div class="branch-review-actions">${attempt ? `<button class="button" data-attempt="${name}" data-switch-explore>查看分支 ${name} ↗</button> ${badge(attempt.state)}`:'<span class="muted">等待分支启动</span>'}</div><details data-review-key="commands-${round.round}-${i}"><summary>起始推理与执行命令 ⌄</summary><p>${escape(b.content)}</p>${b.commands.map(c => code(c, true, 6000)).join('')}</details></div>`;
    }).join('')}</div></details>`;
  }
  if (!analysts.length && !task.reviews.length) html += '<div class="empty">本任务尚未产生评审记录。初始轨迹失败后，Analyst 会分析失败，Reviewer 再选择断点并编写分支起始动作。</div>';
  $('#reviews-panel').innerHTML = html + '</div>';
  $$('#reviews-panel details[data-review-key]').forEach(el => {if (expanded.has(el.dataset.reviewKey)) el.open = expanded.get(el.dataset.reviewKey);});
}

const demoSteps = [
  {title:'执行与保存', titleLong:'在工具返回后保存一个可恢复断点', copy:'Claude Code 执行动作并收到工具结果。下一次模型请求到来时，保存只读的 Btrfs 文件系统快照，同时截取对应的原生会话前缀。两者必须指向同一个已完成动作的边界。'},
  {title:'分析失败', titleLong:'定位可以改变方向的历史位置', copy:'初始轨迹失败后，Analyst 阅读已有轨迹和评分反馈，找出失败原因以及值得保留的进展。Reviewer 选择真实存在的断点，给出新的推理与命令。每个分支都有自己的选择理由。'},
  {title:'恢复断点', titleLong:'同时恢复文件系统与对话上下文', copy:'从只读快照创建一个可写子环境，将原生会话截断至该断点，再用 Claude Code resume / fork-session 续跑。恢复后的模型请求前缀会与保存的请求逐项比较。父轨迹在断点之后的操作不会被带进子分支。'},
  {title:'探索分支', titleLong:'复用已有进展，执行新的起始动作', copy:'Reviewer 编写的首步命令在恢复后的子环境中执行，然后交回 actor 模型继续探索。继承的历史不会重新执行；子环境后续写入采用写时复制，也不会改动父分支或兄弟分支。'},
  {title:'验证与迭代', titleLong:'用同一套原版 verifier 验证每条路径', copy:'每次尝试结束后，使用未修改的 Harbor verifier 评分。若仍失败，下一轮可以从初始轨迹或已有分支的断点继续探索；若成功则停止回溯。回溯机制正确运行，也可能仍然无法解决任务。'},
];
function demoSvg(index) {
  const active = '#7b8fe0', muted = '#c9d3e6', green = '#71af99';
  const s = index >= 2 ? active : muted;
  const labels = ['root','tool','checkpoint'];
  return `<svg viewBox="0 0 420 225" role="img" aria-label="AgentFork 第 ${index+1} 阶段示意"><path d="M40 74 H302" stroke="${muted}" stroke-width="2"/>${[40,130,220,302].map((x,i) => `<circle cx="${x}" cy="74" r="${i===2?8:5}" fill="${i===2?active:muted}"/><text x="${x}" y="53" text-anchor="middle" fill="#93a2be" font-size="9" font-family="monospace">${labels[i] || 'future'}</text>`).join('')}<path d="M220 82 C220 113 276 111 295 144" fill="none" stroke="${s}" stroke-width="2" ${index < 3?'stroke-dasharray="4 4"':''}/><path d="M220 82 C220 124 145 117 132 164" fill="none" stroke="${s}" stroke-width="2" ${index < 3?'stroke-dasharray="4 4"':''}/><rect x="245" y="144" width="112" height="36" rx="6" fill="${index>=3?'#e7edff':'#e9eef7'}" stroke="${s}"/><text x="301" y="166" text-anchor="middle" fill="${index>=3?active:'#9aa9bf'}" font-size="10" font-family="monospace">branch A ${index===4?'×':'↗'}</text><rect x="76" y="164" width="112" height="36" rx="6" fill="${index===4?'#e6f3ed':'#e9eef7'}" stroke="${index===4?green:s}"/><text x="132" y="186" text-anchor="middle" fill="${index===4?green:'#9aa9bf'}" font-size="10" font-family="monospace">branch B ${index===4?'✓':'↗'}</text><rect x="164" y="91" width="112" height="29" rx="5" fill="#fff" stroke="#dbe2f0"/><text x="220" y="109" text-anchor="middle" fill="${active}" font-size="9" font-family="monospace">disk + context</text><text x="20" y="222" fill="#a4b0c3" font-size="8">机制示意 · 非新增实验结果</text></svg>`;
}
function renderHow() {
  const demo = {...demoSteps[state.demo]}, probe = state.overview?.storage_probe;
  if (memoryBackend() && state.demo === 0) demo.copy = '工具完成后先保存原生对话，结束等待 API 的 Claude Code，再用 CRIU 停止并保存环境进程，在停止期间创建 Btrfs 磁盘快照，随后恢复父环境和原对话。保存失败的位置不会进入候选断点。';
  if (memoryBackend() && state.demo === 2) {demo.titleLong = '同时恢复磁盘、进程内存与对话上下文'; demo.copy = '子环境使用独立的磁盘、网络与运行时通道。CRIU 恢复后台进程、共享内存、文件游标等状态，再从校验过的原生会话前缀启动 Claude Code。恢复请求须通过严格的对话前缀比较。';}
  $('#how-panel').innerHTML = `<div class="how-content"><div class="how-intro"><div class="eyebrow">BRANCH FROM PROGRESS</div><h3>从已有进展继续，而不是每次重头开始。</h3><p>AgentFork 的一次回溯包含三件事：选一个历史断点，恢复对应环境与上下文，再用不同的起始动作继续探索。点击下面的阶段，查看它在当前复现中的实现。</p></div><div class="how-steps">${demoSteps.map((s,i) => `${i?'<span class="how-arrow">→</span>':''}<button class="how-step ${state.demo===i?'active':''}" data-demo="${i}"><small>0${i+1}</small><strong>${s.title}</strong></button>`).join('')}</div><div class="demo-panel"><div class="demo-visual">${demoSvg(state.demo)}</div><div class="demo-copy"><div class="eyebrow">STEP 0${state.demo+1}</div><h4>${demo.titleLong}</h4><p>${demo.copy}</p><div style="margin-top:17px;display:flex;gap:8px"><button class="button" data-demo="${Math.max(0,state.demo-1)}" ${state.demo===0?'disabled':''}>← 上一步</button><button class="button" data-demo="${Math.min(4,state.demo+1)}" ${state.demo===4?'disabled':''}>下一步 →</button></div></div></div><div class="how-caveats"><div class="how-caveat"><h4>100 步就要复制 100 份完整环境吗？</h4><div class="number">${probe?bytes(probe.allocated_bytes_growth):'增量存储'}</div><p>${probe?`独立存储实测：保留 ${probe.snapshots_retained_at_measurement} 个快照，每步覆写同一个 1 MiB 文件，底层实际分配量增长如上。该数字包含元数据，并非快照独占块大小。`:'Btrfs 快照共享未修改的数据块；空间主要随写入和覆盖量增长。'} 初始环境共享，后续变更按写时复制保存。</p></div><div class="how-caveat"><h4>这个页面里的对照如何计算？</h4><p>${agentforkOnly() ? '这一轮关闭 Pass@4，只重跑历史未解决或尚未开始的 36 个任务；历史已有成功的 16 个任务保留在旧实验中。此筛选子集不能与全量分数直接比较。' : 'Pass@4 为四条独立轨迹；'}AgentFork 使用一条初始轨迹，加上最多 4 / 3 个分支的两轮搜索。${agentforkOnly() ? 'root-0 仅计入 AgentFork。' : 'root-0 是两种方法共享的初始样本，成本在各自方法中分别计入。两者尝试预算不同，需要结合实际 token 消耗比较。'}</p><p style="margin-top:9px">${memoryBackend() ? '当前使用 Btrfs + CRIU：磁盘共享未改动块，进程内存另存为压缩镜像；两部分占用分别记录。无法恢复的外部连接会拒绝保存该断点。' : '这里恢复的是磁盘与会话，不保存进程内存或网络连接。'} 已完成任务的磁盘快照会回收，校验材料继续保留。</p></div></div><div class="notice" style="margin-top:20px">这是 AgentFork 推理阶段在 Claude Code 与 Terminal-Bench 4.0 上的适配。页面只读实验数据，不会启动新的模型调用或修改正在运行的任务。</div></div>`;
}
function setTab(tab) {
  state.tab = ['explore','reviews','how'].includes(tab) ? tab : 'explore';
  $$('.tabs button').forEach(b => b.classList.toggle('active', b.dataset.tab === state.tab));
  for (const name of ['explore','reviews','how']) $(`#${name}-panel`).hidden = name !== state.tab;
  if (state.tab === 'how') renderHow();
  updateHash();
}
async function refresh() {
  if (state.busy) return;
  state.busy = true;
  try {
    state.overview = await api('overview'); renderMetrics(); renderTasks();
    if (!state.taskName) {
      const name = initial.get('task') && state.overview.tasks.some(t => t.name === initial.get('task')) ? initial.get('task') : state.overview.tasks.find(t => t.attempts > 4)?.name || state.overview.tasks.find(t => t.status === 'running')?.name || state.overview.tasks[0].name;
      await selectTask(name, {attempt:initial.get('attempt'), turn:initial.has('turn') ? initial.get('turn') : null});
      setTab(initial.get('tab') || 'explore');
    } else {
      const name = state.taskName;
      const task = await api('task', {task:name});
      if (name !== state.taskName) return;
      state.task = task; renderTask(); renderSelection();
      if (state.attemptName && !task.attempts.some(a => a.name === state.attemptName)) {
        state.attemptName = ''; state.trace = null; state.turnData = null;
      }
      if (state.attemptName) {
        const attempt = state.attemptName, trace = await api('attempt', {task:name, attempt});
        if (name !== state.taskName || attempt !== state.attemptName) return;
        if (trace.revision !== state.trace?.revision) {
          const previous = state.trace?.turns.find(t => t.id === state.turn);
          const next = trace.turns.find(t => t.id === state.turn);
          if (!next) state.turn = trace.turns.find(t => !t.inherited)?.id ?? 0;
          state.trace = trace; renderTurns();
          if (!state.turnData || JSON.stringify(previous) !== JSON.stringify(next) || ['grade','audit'].includes(state.detail)) {
            state.turnData = null; await renderDetail();
          }
        } else if (state.detail === 'grade' || state.detail === 'audit') await renderDetail();
      } else if (task.attempts.length) await selectAttempt(task.attempts[0].name);
    }
    banner(null);
  } catch (error) {banner(error); $('#live-label').textContent = '连接中断';}
  finally {state.busy = false;}
}

document.addEventListener('click', async (event) => {
  const button = event.target.closest('button'); if (!button || button.disabled) return;
  if (button.dataset.task) return selectTask(button.dataset.task);
  if (button.dataset.filter) {
    state.filter = button.dataset.filter;
    $$('.task-filters button').forEach(b => b.classList.toggle('active', b === button)); return renderTasks();
  }
  if (button.dataset.tab) return setTab(button.dataset.tab);
  if (button.dataset.attempt) {
    if ('switchExplore' in button.dataset) setTab('explore');
    return selectAttempt(button.dataset.attempt);
  }
  if (button.dataset.originAttempt) {
    setTab('explore'); return selectAttempt(button.dataset.originAttempt, {checkpoint:+button.dataset.originStep});
  }
  if (button.dataset.turn != null) return selectTurn(+button.dataset.turn);
  if (button.dataset.detail) {
    state.detail = button.dataset.detail;
    $$('.detail-tabs button').forEach(b => b.classList.toggle('active', b === button)); return renderDetail();
  }
  if ('showAudit' in button.dataset) {state.detail = 'audit'; $$('.detail-tabs button').forEach(b => b.classList.toggle('active', b.dataset.detail === 'audit')); return renderDetail();}
  if (button.dataset.demo != null) {state.demo = +button.dataset.demo; return renderHow();}
  if (button.dataset.expand) {
    const pre = button.previousElementSibling?.querySelector('pre');
    if (pre) {pre.textContent = state.copies.get(button.dataset.expand); pre.style.maxHeight = 'none'; button.remove();}
    return;
  }
  if (button.dataset.copy || 'share' in button.dataset) {
    const text = button.dataset.copy ? state.copies.get(button.dataset.copy) : location.href;
    try {
      if (navigator.clipboard) await navigator.clipboard.writeText(text);
      else {const el = document.createElement('textarea'); el.value = text; el.style.position = 'fixed'; el.style.opacity = '0'; document.body.append(el); el.select(); document.execCommand('copy'); el.remove();}
      toast(button.dataset.copy ? '已复制内容' : '已复制当前任务与步骤的链接');
    } catch {toast('浏览器未允许自动复制，请手动选择内容复制');}
  }
});
$('#task-search').addEventListener('input', e => {state.taskQuery = e.target.value; renderTasks();});
$('#turn-search').addEventListener('input', e => {state.turnQuery = e.target.value; renderTurns();});
$('#show-inherited').addEventListener('change', e => {state.inherited = e.target.checked; renderTurns();});
$('#refresh').addEventListener('click', refresh);
$('#how-link').addEventListener('click', () => setTab('how'));
$('#instruction-toggle').addEventListener('click', () => {const hidden = !$('#instruction').hidden; $('#instruction').hidden = hidden; $('#instruction-toggle').setAttribute('aria-expanded', String(!hidden));});
$('#mobile-menu').addEventListener('click', () => $('#sidebar').classList.toggle('open'));
$('#previous-turn').addEventListener('click', () => moveTurn(-1));
$('#next-turn').addEventListener('click', () => moveTurn(1));
$('#play').addEventListener('click', play);
document.addEventListener('keydown', e => {
  if (e.target.closest('input,textarea,select,[contenteditable="true"]') || state.tab !== 'explore' || e.ctrlKey || e.metaKey || e.altKey) return;
  if (e.key === 'j' || e.key === 'ArrowRight') {e.preventDefault(); moveTurn(1);}
  if (e.key === 'k' || e.key === 'ArrowLeft') {e.preventDefault(); moveTurn(-1);}
});
refresh(); setInterval(refresh, 10000);

// CloudBatch Control Plane Application Logic

let ws = null;
let latestSnapshot = null;
let memoryChart = null;
let isPlaying = false;
let benchmarkCharts = {};
let selectedJobId = null;
const commandHistory = [];
let historyIndex = -1;
window.lastHostProcesses = [];

const JOB_COLORS = [
  '#38bdf8', '#818cf8', '#c084fc', '#f472b6', '#fb7185',
  '#34d399', '#4ade80', '#a3e635', '#facc15', '#fb923c'
];

function getJobColor(jobId) {
  return JOB_COLORS[Math.abs(jobId) % JOB_COLORS.length];
}

// 1. WebSocket Connection & State Sync
async function fetchCurrentState() {
  try {
    const res = await fetch('/api/state');
    if (res.ok) {
      const snapshot = await res.json();
      renderSnapshot(snapshot);
    }
  } catch (e) {
    console.debug('Failed to fetch state:', e);
  }
}

function initWebSocket() {
  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  const wsUrl = `${protocol}//${window.location.host}/ws`;
  
  try {
    ws = new WebSocket(wsUrl);

    ws.onopen = () => {
      const badge = document.getElementById('ws-badge');
      if (badge) {
        badge.className = 'inline-flex items-center px-2 py-0.5 rounded-full text-[11px] font-mono bg-emerald-500/10 text-emerald-400 border border-emerald-500/30';
        badge.innerHTML = '<span class="w-1.5 h-1.5 rounded-full bg-emerald-400 animate-pulse mr-1.5"></span> WS CONNECTED';
      }
      fetchCurrentState();
    };

    ws.onmessage = (event) => {
      try {
        const snapshot = JSON.parse(event.data);
        renderSnapshot(snapshot);
      } catch (e) {
        console.error('Failed to parse WebSocket message:', e);
      }
    };

    ws.onclose = () => {
      const badge = document.getElementById('ws-badge');
      if (badge) {
        badge.className = 'inline-flex items-center px-2 py-0.5 rounded-full text-[11px] font-mono bg-rose-500/10 text-rose-400 border border-rose-500/30';
        badge.innerHTML = '<span class="w-1.5 h-1.5 rounded-full bg-rose-400 mr-1.5"></span> WS DISCONNECTED';
      }
      setTimeout(initWebSocket, 2000);
    };

    ws.onerror = () => {
      try { if (ws) ws.close(); } catch (_) {}
    };
  } catch (e) {
    console.warn('WebSocket init failed, fallback to polling:', e);
  }
}

// 2. Render State Snapshot
function renderSnapshot(snap) {
  latestSnapshot = snap;

  if (snap.finished) {
    isPlaying = false;
    const playIcon = document.getElementById('icon-play');
    const pauseIcon = document.getElementById('icon-pause');
    const textElem = document.getElementById('text-play-pause');
    if (playIcon) playIcon.classList.remove('hidden');
    if (pauseIcon) pauseIcon.classList.add('hidden');
    if (textElem) textElem.innerText = 'Play';
  }

  // Tick Badge
  document.getElementById('tick-counter').innerText = `#${snap.tick}`;

  // Sim Cluster Telemetry (Default 24 GB)
  const cluster = snap.cluster || {};
  const numCpus = cluster.num_cpus || 4;
  const busyCores = cluster.cores ? cluster.cores.filter(c => c !== null).length : 0;
  const cpuPct = Math.round((busyCores / numCpus) * 100);
  
  document.getElementById('telemetry-sim-cpu').innerText = `${cpuPct}%`;
  document.getElementById('telemetry-sim-cores').innerText = `(${busyCores}/${numCpus} Cores)`;
  document.getElementById('sim-cpu-bar').style.width = `${cpuPct}%`;

  const usedRam = cluster.used_memory_gb || 0;
  const totalRam = cluster.total_memory_gb || 24;
  const ramPct = Math.round((usedRam / totalRam) * 100);
  document.getElementById('telemetry-sim-ram').innerText = `${usedRam} / ${totalRam} GB`;
  document.getElementById('telemetry-sim-ram-pct').innerText = `(${ramPct}%)`;
  document.getElementById('sim-ram-bar').style.width = `${ramPct}%`;

  // Host PC Telemetry (User's 24GB system)
  const host = snap.host || {};
  const hostCpu = host.cpu_percent_per_core ? Math.round(host.cpu_percent_per_core.reduce((a, b) => a + b, 0) / host.cpu_percent_per_core.length) : 0;
  document.getElementById('telemetry-host-cpu').innerText = `${hostCpu}%`;
  document.getElementById('telemetry-host-ram').innerText = `${Math.round(host.ram_percent || 0)}%`;
  document.getElementById('telemetry-host-pids').innerText = `${host.active_workers || 0} Active, ${host.suspended_workers || 0} Susp`;

  // OS Mode Toggle sync
  document.getElementById('mode-toggle').checked = (snap.mode === 'live');

  // Queues Count Badges
  const queues = snap.queues || { new: [], ready: [], running: [], suspended: [], terminated: [] };
  document.getElementById('badge-new-count').innerText = queues.new.length;
  document.getElementById('badge-ready-count').innerText = queues.ready.length;
  document.getElementById('badge-running-count').innerText = `${busyCores}/${numCpus}`;
  document.getElementById('badge-suspended-count').innerText = queues.suspended.length;

  const jobsMap = {};
  (snap.jobs || []).forEach(j => { jobsMap[j.id] = j; });

  // Render Tier 1: NEW Queue
  renderQueueColumn('col-new-jobs', queues.new, jobsMap, 'badge-state-new', 'NEW');

  // Render Tier 2: READY Queue
  renderQueueColumn('col-ready-jobs', queues.ready, jobsMap, 'badge-state-ready', 'READY', true);

  // Render Tier 3: RUNNING Cores
  renderRunningCores(cluster.cores || [], jobsMap, snap.mode === 'live', snap.config ? (snap.config.time_quantum || 4) : 4);

  // Render Tier 4: SUSPENDED Queue
  renderSuspendedColumn('col-suspended-jobs', queues.suspended, jobsMap);

  // Render Gantt Timeline
  renderGanttChart(cluster.cores || [], snap.tick);

  // Update RAM Watermark Chart
  updateWatermarkChart(snap.tick, ramPct, queues.new.length + queues.ready.length + queues.suspended.length);

  // Render Event Log
  renderEventLog(snap.events || []);

  // Sync Active Dropdowns
  if (snap.config) {
    if (snap.config.long_term_policy) document.getElementById('cfg-long-term').value = snap.config.long_term_policy;
    if (snap.config.medium_term_policy) document.getElementById('cfg-medium-term').value = snap.config.medium_term_policy;
    if (snap.config.short_term_policy) document.getElementById('cfg-short-term').value = snap.config.short_term_policy;
    if (snap.config.time_quantum) document.getElementById('cfg-quantum').value = String(snap.config.time_quantum);
  }
}

function renderQueueColumn(elemId, jobIds, jobsMap, badgeClass, stateLabel, showAge = false) {
  const container = document.getElementById(elemId);
  if (!jobIds || jobIds.length === 0) {
    container.innerHTML = `<div class="text-xs text-slate-500 text-center py-8">Queue empty</div>`;
    return;
  }

  container.innerHTML = jobIds.map(id => {
    const j = jobsMap[id];
    if (!j) return '';
    const ageHtml = showAge ? `<span class="text-[10px] text-slate-400 bg-slate-800 px-1.5 py-0.5 rounded border border-slate-700">Age: ${j.age_counter}</span>` : '';
    const prioBoosted = (j.priority < j.base_priority) ? 'text-emerald-400 font-bold' : 'text-slate-300';
    const isLive = j.os_pid ? true : false;
    const pidBadge = isLive ? `<span class="px-1.5 py-0.5 rounded text-[9px] font-mono bg-amber-500/10 text-amber-300 border border-amber-500/30">PID: ${j.os_pid}</span>` : '';

    return `
      <div onclick="openInspector(${j.id})" class="job-card bg-slate-900/90 border border-slate-800 hover:border-sky-500/60 rounded-xl p-3 cursor-pointer text-xs shadow-md">
        <div class="flex items-center justify-between mb-1.5">
          <span class="font-mono font-bold text-white flex items-center gap-1.5">
            <span class="w-2 h-2 rounded-full" style="background-color: ${getJobColor(j.id)}"></span>
            #${j.id} <span class="text-slate-400 font-normal">(${j.user})</span>
          </span>
          <div class="flex items-center gap-1">
            <span class="px-1.5 py-0.5 rounded text-[10px] font-mono ${prioBoosted} bg-slate-800 border border-slate-700">P${j.priority}</span>
            <button onclick="event.stopPropagation(); cancelJob(${j.id});" class="text-slate-500 hover:text-rose-400 p-0.5" title="Cancel job">✕</button>
          </div>
        </div>
        <div class="text-slate-200 font-medium truncate mb-2 flex items-center justify-between">
          <span>${j.name}</span>
          ${pidBadge}
        </div>
        <div class="flex items-center justify-between text-[11px] font-mono text-slate-400">
          <span>${j.memory_gb} GB</span>
          <span>Burst: ${j.remaining_time}/${j.burst_time}</span>
          ${ageHtml}
        </div>
      </div>
    `;
  }).join('');
}

function renderRunningCores(cores, jobsMap, isLive, timeQuantum) {
  const container = document.getElementById('col-running-cores');
  container.innerHTML = cores.map((jobId, idx) => {
    if (jobId === null) {
      return `
        <div class="bg-slate-900/40 border border-dashed border-slate-800 rounded-xl p-3 text-center text-xs text-slate-500 flex flex-col justify-center items-center h-24">
          <div class="text-[10px] uppercase font-mono tracking-wider font-semibold text-slate-400 mb-1">Core ${idx}</div>
          <span>Core Idle (Free)</span>
        </div>
      `;
    }

    const j = jobsMap[jobId] || { id: jobId, name: 'Job', user: 'user', burst_time: 1, remaining_time: 1, memory_gb: 8, priority: 3, quantum_used: 0 };
    const progressPct = Math.round(((j.burst_time - j.remaining_time) / j.burst_time) * 100);
    const pidBadge = isLive && j.os_pid ? `
      <span class="px-1.5 py-0.5 rounded text-[10px] font-mono bg-amber-500/20 text-amber-300 border border-amber-500/40 flex items-center gap-1">
        <svg class="w-2.5 h-2.5" fill="currentColor" viewBox="0 0 20 20"><circle cx="10" cy="10" r="8"/></svg> PID: ${j.os_pid} (Pinned Core ${idx})
      </span>
    ` : '';

    return `
      <div onclick="openInspector(${j.id})" class="job-card core-active border border-emerald-500/50 rounded-xl p-3 shadow-md shadow-emerald-950/20 cursor-pointer text-xs">
        <div class="flex items-center justify-between mb-1.5">
          <div class="flex items-center gap-2">
            <span class="w-2 h-2 rounded-full bg-emerald-400 pulsing-dot"></span>
            <span class="font-bold text-slate-200">Core ${idx}:</span>
            <span class="font-mono font-bold text-white">#${j.id}</span>
          </div>
          <div class="flex items-center gap-1.5">
            <span class="font-mono text-[10px] text-slate-400">Q: ${j.quantum_used || 0}/${timeQuantum}</span>
            <button onclick="event.stopPropagation(); suspendJob(${j.id});" class="text-amber-400 hover:text-amber-300 p-0.5 font-bold" title="Suspend process">⏸</button>
            <button onclick="event.stopPropagation(); cancelJob(${j.id});" class="text-slate-500 hover:text-rose-400 p-0.5" title="Cancel job">✕</button>
          </div>
        </div>

        <div class="font-semibold text-white truncate mb-2">${j.name} <span class="text-slate-400 font-normal">(${j.user})</span></div>

        <!-- Progress bar -->
        <div class="w-full bg-slate-800 rounded-full h-2 mb-2 overflow-hidden">
          <div class="h-full bg-emerald-500 transition-all duration-300" style="width: ${progressPct}%"></div>
        </div>

        <div class="flex items-center justify-between text-[11px] font-mono text-slate-400">
          <span>${j.memory_gb} GB RAM</span>
          <span>${j.remaining_time} ticks rem</span>
        </div>
        ${pidBadge ? `<div class="mt-2 pt-1 border-t border-slate-800 flex justify-end">${pidBadge}</div>` : ''}
      </div>
    `;
  }).join('');
}

function renderSuspendedColumn(elemId, jobIds, jobsMap) {
  const container = document.getElementById(elemId);
  if (!jobIds || jobIds.length === 0) {
    container.innerHTML = `<div class="text-xs text-slate-500 text-center py-8">No suspended jobs</div>`;
    return;
  }

  container.innerHTML = jobIds.map(id => {
    const j = jobsMap[id];
    if (!j) return '';
    const pidBadge = j.os_pid ? `<span class="px-1.5 py-0.5 rounded text-[9px] font-mono bg-amber-500/20 text-amber-300 border border-amber-500/40">PID ${j.os_pid} (Suspended)</span>` : '';
    return `
      <div onclick="openInspector(${j.id})" class="job-card card-suspended border rounded-xl p-3 text-xs shadow-md cursor-pointer">
        <div class="flex items-center justify-between mb-1.5">
          <span class="font-mono font-bold text-white flex items-center gap-1.5">
            <span class="w-2 h-2 rounded-full bg-amber-400"></span>
            #${j.id} <span class="text-slate-400 font-normal">(${j.user})</span>
          </span>
          <span class="px-1.5 py-0.5 rounded text-[10px] font-mono bg-amber-950 text-amber-300 border border-amber-700/50">Suspended x${j.suspension_count || 1}</span>
        </div>
        <div class="text-slate-200 font-medium truncate mb-1.5 flex items-center justify-between">
          <span>${j.name}</span>
          ${pidBadge}
        </div>
        <div class="flex items-center justify-between text-[11px] font-mono text-slate-400 mb-2.5">
          <span>RAM Freed: ${j.memory_gb} GB</span>
          <span>Prio: ${j.priority}</span>
        </div>
        <div class="flex gap-2">
          <button onclick="event.stopPropagation(); resumeJob(${j.id});" class="flex-1 py-1 rounded bg-cyan-900/60 hover:bg-cyan-800 text-cyan-200 border border-cyan-700 text-[10px] font-mono font-semibold transition-colors">▶ Resume</button>
          <button onclick="event.stopPropagation(); cancelJob(${j.id});" class="px-2 py-1 rounded bg-rose-900/50 hover:bg-rose-800 text-rose-200 border border-rose-700 text-[10px] font-mono transition-colors">✕ Cancel</button>
        </div>
      </div>
    `;
  }).join('');
}

// 3. Gantt Chart Timeline
const ganttHistory = [];

function renderGanttChart(currentCores, tick) {
  ganttHistory.push([...currentCores]);
  if (ganttHistory.length > 35) ganttHistory.shift();

  const container = document.getElementById('gantt-container');
  const numCpus = currentCores.length;

  let html = '';
  for (let c = 0; c < numCpus; c++) {
    html += `
      <div class="flex items-center gap-2">
        <span class="w-14 text-right text-xs font-mono font-bold text-slate-400 select-none">CPU ${c}:</span>
        <div class="flex gap-1 overflow-x-hidden p-1 bg-slate-900/70 rounded-lg border border-slate-800">
    `;

    ganttHistory.forEach((slice, sIdx) => {
      const jid = slice[c];
      if (jid !== null && jid !== undefined) {
        const color = getJobColor(jid);
        html += `<div class="gantt-block" style="background-color: ${color}20; color: ${color}; border: 1px solid ${color}60;" title="Core ${c} | Tick ${tick - ganttHistory.length + sIdx + 1} | Job #${jid}">#${jid}</div>`;
      } else {
        html += `<div class="gantt-block bg-slate-900/40 text-slate-600 border border-slate-800" title="Core ${c} Idle">-</div>`;
      }
    });

    html += `</div></div>`;
  }
  container.innerHTML = html;
}

// 4. Memory Watermark Chart (Chart.js)
const watermarkLabels = [];
const watermarkMemoryData = [];
const watermarkQueueData = [];

function initWatermarkChart() {
  const ctx = document.getElementById('memory-watermark-chart').getContext('2d');
  memoryChart = new Chart(ctx, {
    type: 'line',
    data: {
      labels: watermarkLabels,
      datasets: [
        {
          label: 'Used RAM % (24GB)',
          data: watermarkMemoryData,
          borderColor: '#38bdf8',
          backgroundColor: 'rgba(56, 189, 248, 0.12)',
          fill: true,
          tension: 0.3,
          borderWidth: 2,
          pointRadius: 2,
        },
        {
          label: 'Total Queued Jobs',
          data: watermarkQueueData,
          borderColor: '#a78bfa',
          borderDash: [2, 2],
          tension: 0.2,
          borderWidth: 1.5,
          pointRadius: 0,
          yAxisID: 'yQueue',
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      scales: {
        x: {
          grid: { color: '#1e293b' },
          ticks: { color: '#64748b', font: { family: 'JetBrains Mono', size: 10 } }
        },
        y: {
          min: 0,
          max: 100,
          grid: { color: '#1e293b' },
          ticks: { color: '#64748b', font: { family: 'JetBrains Mono', size: 10 }, callback: v => `${v}%` }
        },
        yQueue: {
          position: 'right',
          grid: { display: false },
          ticks: { color: '#a78bfa', font: { family: 'JetBrains Mono', size: 10 } }
        }
      },
      plugins: {
        legend: {
          labels: { color: '#94a3b8', font: { family: 'Inter', size: 11 }, boxWidth: 12 }
        }
      }
    }
  });
}

function updateWatermarkChart(tick, memoryPct, queuedCount) {
  if (!memoryChart) return;
  watermarkLabels.push(`#${tick}`);
  watermarkMemoryData.push(memoryPct);
  watermarkQueueData.push(queuedCount);

  if (watermarkLabels.length > 30) {
    watermarkLabels.shift();
    watermarkMemoryData.shift();
    watermarkQueueData.shift();
  }
  memoryChart.update();
}

// 5. Scheduler Event Log
function renderEventLog(events) {
  const container = document.getElementById('event-log-container');
  if (!events || events.length === 0) return;

  container.innerHTML = events.slice(-30).reverse().map(e => {
    let badgeClass = 'bg-slate-800 text-slate-300 border-slate-700';
    if (e.scheduler === 'LT') badgeClass = 'bg-indigo-950 text-indigo-300 border-indigo-700/50';
    else if (e.scheduler === 'MT') badgeClass = 'bg-amber-950 text-amber-300 border-amber-700/50';
    else if (e.scheduler === 'ST') badgeClass = 'bg-emerald-950 text-emerald-300 border-emerald-700/50';
    else if (e.scheduler === 'CANCEL') badgeClass = 'bg-rose-950 text-rose-300 border-rose-700/50';

    return `
      <div class="flex items-start gap-2 py-1 border-b border-slate-800/40">
        <span class="text-slate-500 text-[10px] pt-0.5">T#${e.tick}</span>
        <span class="px-1.5 py-0.2 rounded text-[10px] font-mono border ${badgeClass}">[${e.scheduler}]</span>
        <span class="text-slate-300 flex-1 leading-relaxed">${e.message}</span>
      </div>
    `;
  }).join('');
}

// 6. Interactive SLURM Web Console
function appendTerminalOutput(text, isCommand = false) {
  const terminal = document.getElementById('terminal-output');
  const line = document.createElement('div');
  if (isCommand) {
    line.innerHTML = `<span class="text-emerald-400 font-bold">slurm@cluster:~$</span> <span class="text-white">${text}</span>`;
  } else {
    line.className = 'text-slate-300 whitespace-pre';
    line.textContent = text;
  }
  terminal.appendChild(line);
  terminal.scrollTop = terminal.scrollHeight;
}

async function handleTerminalCommand(cmdStr) {
  const clean = cmdStr.trim();
  if (!clean) return;
  commandHistory.push(clean);
  historyIndex = commandHistory.length;
  appendTerminalOutput(clean, true);

  const parts = clean.split(/\s+/);
  const cmd = parts[0].toLowerCase();

  if (cmd === 'clear') {
    document.getElementById('terminal-output').innerHTML = '';
    return;
  }

  if (cmd === 'help') {
    appendTerminalOutput(
      "CloudBatch SLURM Commands:\n" +
      "  squeue                        - View active jobs queue\n" +
      "  sinfo                         - View cluster partition status\n" +
      "  sacct                         - View accounting log\n" +
      "  sbatch --name <n> --mem <m>   - Submit batch job\n" +
      "  scancel <job_id>              - Cancel a job\n" +
      "  simport <pid>                 - Adopt host process from Windows Task Manager\n" +
      "  shost                         - List adoptable Windows processes\n" +
      "  benchmark [--jobs 30]         - Run 5-policy benchmark arena\n" +
      "  step [ticks]                  - Advance simulation\n" +
      "  start / pause / reset         - Control simulation clock\n" +
      "  clear                         - Clear terminal\n"
    );
    return;
  }

  try {
    if (cmd === 'squeue') {
      const res = await fetch('/api/squeue');
      const data = await res.json();
      appendTerminalOutput(formatSqueueTable(data.jobs || []));
    } else if (cmd === 'sinfo') {
      const res = await fetch('/api/sinfo');
      const data = await res.json();
      appendTerminalOutput(formatSinfoTable(data));
    } else if (cmd === 'sacct') {
      const res = await fetch('/api/sacct');
      const data = await res.json();
      appendTerminalOutput(formatSacctTable(data.jobs || []));
    } else if (cmd === 'simport') {
      const pid = parseInt(parts[1], 10);
      if (!pid) {
        appendTerminalOutput("Usage: simport <pid>");
        return;
      }
      const res = await fetch('/api/host/adopt', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ pid, burst_time: 12, priority: 3 })
      });
      const data = await res.json();
      if (res.ok) {
        appendTerminalOutput(`Adopted host process "${data.name}" (PID ${data.pid}) as batch Job #${data.job_id}`);
        await fetchCurrentState();
      } else {
        appendTerminalOutput(`Error: ${data.detail}`);
      }
    } else if (cmd === 'shost') {
      const res = await fetch('/api/host/processes');
      const data = await res.json();
      const procs = data.processes || [];
      const pad = (s, l) => String(s).padEnd(l);
      let out = `${pad('PID', 8)} ${pad('PROCESS_NAME', 25)} ${pad('RAM(MB)', 10)} CPU%\n`;
      procs.slice(0, 15).forEach(p => {
        out += `${pad(p.pid, 8)} ${pad(p.name.substring(0, 24), 25)} ${pad(p.memory_mb, 10)} ${p.cpu_percent}%\n`;
      });
      appendTerminalOutput(out);
    } else if (cmd === 'scancel') {
      const jid = parseInt(parts[1], 10);
      if (!jid) {
        appendTerminalOutput("Usage: scancel <job_id>");
        return;
      }
      const res = await fetch(`/api/jobs/${jid}`, { method: 'DELETE' });
      if (res.ok) appendTerminalOutput(`scancel: cancelled job ${jid}`);
      else appendTerminalOutput(`scancel: error cancelling job ${jid}`);
    } else if (cmd === 'sbatch') {
      let name = 'batch-job', burst = 6, mem = 8, prio = 3, user = 'alice';
      for (let i = 1; i < parts.length; i++) {
        if (parts[i] === '--name' && parts[i+1]) name = parts[++i];
        else if (parts[i] === '--burst' && parts[i+1]) burst = parseInt(parts[++i], 10);
        else if (parts[i] === '--mem' && parts[i+1]) mem = parseInt(parts[++i], 10);
        else if ((parts[i] === '--priority' || parts[i] === '--prio') && parts[i+1]) prio = parseInt(parts[++i], 10);
        else if (parts[i] === '--user' && parts[i+1]) user = parts[++i];
      }
      const res = await fetch('/api/jobs', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name, burst_time: burst, memory_gb: mem, priority: prio, user })
      });
      const data = await res.json();
      appendTerminalOutput(data.message || `Submitted batch job ${data.submitted?.id}`);
    } else if (cmd === 'step') {
      const ticks = parseInt(parts[1], 10) || 1;
      await fetch('/api/control/step', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ticks })
      });
      appendTerminalOutput(`Stepped simulation by ${ticks} tick(s).`);
    } else if (cmd === 'start') {
      await fetch('/api/control/start', { method: 'POST' });
      appendTerminalOutput("Simulation started.");
    } else if (cmd === 'pause') {
      await fetch('/api/control/pause', { method: 'POST' });
      appendTerminalOutput("Simulation paused.");
    } else if (cmd === 'reset') {
      await fetch('/api/control/reset', { method: 'POST' });
      appendTerminalOutput("Simulation reset to tick 0.");
    } else if (cmd === 'benchmark') {
      appendTerminalOutput("Running 5-policy benchmark arena...");
      const res = await fetch('/api/benchmark', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ num_jobs: 30, seed: 42 })
      });
      const data = await res.json();
      appendTerminalOutput(JSON.stringify(data.policies, null, 2));
    } else {
      appendTerminalOutput(`slurm: command not found: ${cmd}. Type 'help' for available commands.`);
    }
  } catch (err) {
    appendTerminalOutput(`Error executing command: ${err.message}`);
  }
}

function formatSqueueTable(jobs) {
  const pad = (s, l) => String(s).padEnd(l);
  let out = `${pad('JOBID', 7)} ${pad('NAME', 15)} ${pad('USER', 7)} ${pad('PRIO', 6)} ${pad('MEM(GB)', 9)} ${pad('STATE', 11)} ${pad('TIME_REM', 10)} ${pad('CORE', 7)} OS_PID\n`;
  jobs.forEach(j => {
    const core = j.assigned_cpu !== null && j.assigned_cpu !== undefined ? `CPU${j.assigned_cpu}` : '-';
    const timeRem = `${j.remaining_time}/${j.burst_time}`;
    const pidStr = j.os_pid ? (j.state === 'SUSPENDED' ? `${j.os_pid} (Susp)` : String(j.os_pid)) : '-';
    out += `${pad(j.id, 7)} ${pad(j.name.substring(0, 14), 15)} ${pad(j.user.substring(0, 6), 7)} ${pad(j.priority, 6)} ${pad(j.memory_gb, 9)} ${pad(j.state, 11)} ${pad(timeRem, 10)} ${pad(core, 7)} ${pidStr}\n`;
  });
  return out;
}

function formatSinfoTable(info) {
  const pad = (s, l) => String(s).padEnd(l);
  let out = `${pad('PARTITION', 11)} ${pad('AVAIL', 7)} ${pad('TIMELIMIT', 11)} ${pad('CPUS(A/I/T)', 13)} ${pad('MEMORY(USED/TOTAL)', 20)} ${pad('STATE', 10)} OS_BRIDGE\n`;
  const c = info.cpus || {};
  const m = info.memory_gb || {};
  out += `${pad(info.partition, 11)} ${pad(info.avail, 7)} ${pad(info.timelimit, 11)} ${pad(`${c.allocated}/${c.idle}/${c.total}`, 13)} ${pad(`${m.used}G/${m.total}G (${m.utilization_percent}%)`, 20)} ${pad(info.state, 10)} ${info.os_bridge}\n`;
  return out;
}

function formatSacctTable(jobs) {
  const pad = (s, l) => String(s).padEnd(l);
  let out = `${pad('JOBID', 7)} ${pad('NAME', 15)} ${pad('USER', 7)} ${pad('PRIO', 6)} ${pad('SUBMIT', 8)} ${pad('START', 7)} ${pad('END', 7)} ${pad('BURST', 7)} ${pad('WAIT', 6)} ${pad('TAT', 6)} ${pad('RESP', 6)} ${pad('SUSP', 6)} STATE\n`;
  jobs.forEach(j => {
    const start = j.first_run_time !== null && j.first_run_time !== undefined ? String(j.first_run_time) : '-';
    const end = j.finish_time !== null && j.finish_time !== undefined ? String(j.finish_time) : '-';
    const tat = j.finish_time !== null && j.finish_time !== undefined ? String(j.finish_time - j.arrival_time) : '-';
    const wt = j.finish_time !== null && j.finish_time !== undefined ? String((j.finish_time - j.arrival_time) - j.burst_time) : String(j.ready_wait_ticks || 0);
    const resp = j.first_run_time !== null && j.first_run_time !== undefined ? String(j.first_run_time - j.arrival_time) : '-';
    let state = j.state;
    if (j.cancelled) state = 'CANCELLED';
    else if (state === 'TERMINATED') state = 'COMPLETED';

    out += `${pad(j.id, 7)} ${pad(j.name.substring(0, 14), 15)} ${pad(j.user.substring(0, 6), 7)} ${pad(j.priority, 6)} ${pad(j.arrival_time, 8)} ${pad(start, 7)} ${pad(end, 7)} ${pad(j.burst_time, 7)} ${pad(wt, 6)} ${pad(tat, 6)} ${pad(resp, 6)} ${pad(j.suspension_count || 0, 6)} ${state}\n`;
  });
  return out;
}

// 7. Job Inspector Modal
function openInspector(jobId) {
  if (!latestSnapshot) return;
  const job = (latestSnapshot.jobs || []).find(j => j.id === jobId);
  if (!job) return;

  selectedJobId = jobId;
  document.getElementById('inspector-title').innerText = `Job #${job.id}: ${job.name}`;

  const tat = job.finish_time ? job.finish_time - job.arrival_time : (latestSnapshot.tick - job.arrival_time);
  const wt = job.finish_time ? tat - job.burst_time : job.ready_wait_ticks;
  const rt = job.first_run_time !== null ? job.first_run_time - job.arrival_time : '-';

  document.getElementById('inspector-content').innerHTML = `
    <div class="grid grid-cols-2 gap-2 bg-slate-900 p-3 rounded-xl border border-slate-800">
      <div><span class="text-slate-500">User:</span> <span class="text-white">${job.user}</span></div>
      <div><span class="text-slate-500">State:</span> <span class="text-sky-400 font-bold">${job.state}</span></div>
      <div><span class="text-slate-500">Base Priority:</span> <span class="text-white">${job.base_priority}</span></div>
      <div><span class="text-slate-500">Current Priority:</span> <span class="text-emerald-400 font-bold">${job.priority}</span></div>
      <div><span class="text-slate-500">Memory Req:</span> <span class="text-white">${job.memory_gb} GB</span></div>
      <div><span class="text-slate-500">Burst Time:</span> <span class="text-white">${job.burst_time} ticks</span></div>
      <div><span class="text-slate-500">Remaining Time:</span> <span class="text-amber-400 font-bold">${job.remaining_time} ticks</span></div>
      <div><span class="text-slate-500">Core:</span> <span class="text-white">${job.assigned_cpu !== null ? `CPU ${job.assigned_cpu}` : '-'}</span></div>
      <div><span class="text-slate-500">OS PID:</span> <span class="text-purple-400 font-bold">${job.os_pid || '-'}</span></div>
      <div><span class="text-slate-500">Suspension Count:</span> <span class="text-white">${job.suspension_count}</span></div>
    </div>
    <div class="grid grid-cols-3 gap-2 bg-slate-900 p-3 rounded-xl border border-slate-800 text-center">
      <div><div class="text-slate-500 text-[10px]">WAIT TIME (WT)</div><div class="text-white font-bold text-sm">${wt}</div></div>
      <div><div class="text-slate-500 text-[10px]">TURNAROUND (TAT)</div><div class="text-white font-bold text-sm">${tat}</div></div>
      <div><div class="text-slate-500 text-[10px]">RESPONSE (RT)</div><div class="text-white font-bold text-sm">${rt}</div></div>
    </div>
  `;

  document.getElementById('modal-inspector').classList.remove('hidden');
}

async function resumeJob(jobId) {
  await fetch(`/api/jobs/${jobId}/resume`, { method: 'POST' });
  await fetchCurrentState();
}

async function suspendJob(jobId) {
  await fetch(`/api/jobs/${jobId}/suspend`, { method: 'POST' });
  await fetchCurrentState();
}

async function cancelJob(jobId) {
  await fetch(`/api/jobs/${jobId}`, { method: 'DELETE' });
  await fetchCurrentState();
}

// 8. Host Task Manager Tasks Adoption
async function loadHostTasks() {
  const container = document.getElementById('host-tasks-table-container');
  container.innerHTML = `<div class="text-xs text-cyan-400 py-8 text-center font-mono flex items-center justify-center gap-2"><span class="w-4 h-4 border-2 border-cyan-400 border-t-transparent rounded-full animate-spin"></span> Scanning active Windows Task Manager processes...</div>`;
  try {
    const res = await fetch('/api/host/processes');
    const data = await res.json();
    window.lastHostProcesses = data.processes || [];
    renderHostTasksTable(window.lastHostProcesses);
  } catch (err) {
    container.innerHTML = `<div class="text-xs text-rose-400 py-6 text-center font-mono">Failed to scan processes: ${err.message}</div>`;
  }
}

function renderHostTasksTable(processes) {
  const container = document.getElementById('host-tasks-table-container');
  const filter = (document.getElementById('host-task-filter').value || '').toLowerCase();
  const filtered = processes.filter(p => p.name.toLowerCase().includes(filter) || String(p.pid).includes(filter));

  if (filtered.length === 0) {
    container.innerHTML = `<div class="text-xs text-slate-500 py-8 text-center font-mono">No matching host processes found.</div>`;
    return;
  }

  let html = `
    <table class="w-full text-xs font-mono text-left border border-slate-800 rounded-lg overflow-hidden">
      <thead class="bg-slate-900 text-slate-400 border-b border-slate-800 sticky top-0">
        <tr>
          <th class="p-2.5">PID</th>
          <th class="p-2.5">Process Name</th>
          <th class="p-2.5">Real RAM (MB)</th>
          <th class="p-2.5">Sim RAM Req</th>
          <th class="p-2.5">Host CPU %</th>
          <th class="p-2.5 text-right">Action</th>
        </tr>
      </thead>
      <tbody class="divide-y divide-slate-800/80 bg-slate-900/40">
  `;

  filtered.forEach(p => {
    html += `
      <tr class="hover:bg-slate-800/60 transition-colors">
        <td class="p-2.5 text-sky-400 font-bold">${p.pid}</td>
        <td class="p-2.5 text-white font-medium flex items-center gap-1.5">
          <span class="w-2 h-2 rounded-full bg-cyan-400"></span> ${p.name}
        </td>
        <td class="p-2.5 text-amber-300 font-bold">${p.memory_mb} MB</td>
        <td class="p-2.5 text-slate-300">${p.memory_gb} GB</td>
        <td class="p-2.5 text-slate-300">${p.cpu_percent}%</td>
        <td class="p-2.5 text-right">
          <button onclick="adoptHostProcess(${p.pid})" class="px-2.5 py-1 rounded bg-cyan-600 hover:bg-cyan-500 text-white font-sans font-semibold text-[11px] shadow-sm shadow-cyan-950/40 transition-colors">
            + Adopt & Schedule
          </button>
        </td>
      </tr>
    `;
  });

  html += `</tbody></table>`;
  container.innerHTML = html;
}

async function adoptHostProcess(pid) {
  try {
    const res = await fetch('/api/host/adopt', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ pid, burst_time: 12, priority: 3 })
    });
    if (res.ok) {
      const data = await res.json();
      document.getElementById('modal-host-tasks').classList.add('hidden');
      await fetchCurrentState();
      alert(`Success! Adopted "${data.name}" (PID ${data.pid}) as batch Job #${data.job_id}. The scheduler will now govern its execution and core affinity.`);
    } else {
      const err = await res.json();
      alert(`Failed to adopt process: ${err.detail}`);
    }
  } catch (e) {
    alert(`Error: ${e.message}`);
  }
}

// 9. 5-Policy Benchmark Modal & Charts
async function triggerBenchmark() {
  const jobsCount = parseInt(document.getElementById('bm-jobs-count').value, 10);
  const seed = parseInt(document.getElementById('bm-seed').value, 10);
  const spinner = document.getElementById('bm-spinner');
  spinner.classList.remove('hidden');

  try {
    const res = await fetch('/api/benchmark', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ num_jobs: jobsCount, seed })
    });
    const data = await res.json();
    renderBenchmarkResults(data.policies);
  } catch (err) {
    alert(`Benchmark error: ${err.message}`);
  } finally {
    spinner.classList.add('hidden');
  }
}

function renderBenchmarkResults(policies) {
  const policyKeys = ['fcfs', 'sjf', 'srtf', 'round_robin', 'priority'];
  const labels = ['FCFS', 'SJF', 'SRTF', 'Round Robin', 'Priority'];

  let bestWt = Infinity, bestTat = Infinity, bestUtil = -Infinity, bestSusp = Infinity;
  policyKeys.forEach(k => {
    const p = policies[k];
    if (p.avg_waiting_time < bestWt) bestWt = p.avg_waiting_time;
    if (p.avg_turnaround_time < bestTat) bestTat = p.avg_turnaround_time;
    if (p.cpu_utilization > bestUtil) bestUtil = p.cpu_utilization;
    if (p.suspensions < bestSusp) bestSusp = p.suspensions;
  });

  let tableHtml = `
    <table class="w-full text-xs font-mono text-left border border-slate-800 rounded-lg overflow-hidden">
      <thead class="bg-slate-900 text-slate-400 border-b border-slate-800">
        <tr>
          <th class="p-2.5">Policy</th>
          <th class="p-2.5">Avg Wait (WT)</th>
          <th class="p-2.5">Avg TAT</th>
          <th class="p-2.5">Avg Resp (RT)</th>
          <th class="p-2.5">CPU Util %</th>
          <th class="p-2.5">Memory Util %</th>
          <th class="p-2.5">Throughput</th>
          <th class="p-2.5">Suspensions</th>
          <th class="p-2.5">Starvations</th>
        </tr>
      </thead>
      <tbody class="divide-y divide-slate-800 bg-slate-900/40">
  `;

  policyKeys.forEach((k, idx) => {
    const p = policies[k];
    const isWtWinner = p.avg_waiting_time === bestWt;
    const isTatWinner = p.avg_turnaround_time === bestTat;
    const isUtilWinner = p.cpu_utilization === bestUtil;

    tableHtml += `
      <tr class="hover:bg-slate-800/50">
        <td class="p-2.5 font-bold text-white flex items-center gap-1.5">${labels[idx]}</td>
        <td class="p-2.5 ${isWtWinner ? 'text-emerald-400 font-bold' : 'text-slate-300'}">${p.avg_waiting_time} ${isWtWinner ? '★' : ''}</td>
        <td class="p-2.5 ${isTatWinner ? 'text-emerald-400 font-bold' : 'text-slate-300'}">${p.avg_turnaround_time} ${isTatWinner ? '★' : ''}</td>
        <td class="p-2.5 text-slate-300">${p.avg_response_time}</td>
        <td class="p-2.5 ${isUtilWinner ? 'text-emerald-400 font-bold' : 'text-slate-300'}">${p.cpu_utilization}%</td>
        <td class="p-2.5 text-slate-300">${p.memory_utilization}%</td>
        <td class="p-2.5 text-slate-300">${p.throughput}</td>
        <td class="p-2.5 text-slate-300">${p.suspensions}</td>
        <td class="p-2.5 text-slate-300">${p.starvation_count}</td>
      </tr>
    `;
  });

  tableHtml += `</tbody></table>`;
  document.getElementById('bm-table-container').innerHTML = tableHtml;

  renderBarChart('bm-chart-waiting', labels, [
    { label: 'Avg Waiting Time', data: policyKeys.map(k => policies[k].avg_waiting_time), backgroundColor: '#38bdf8' },
    { label: 'Avg Response Time', data: policyKeys.map(k => policies[k].avg_response_time), backgroundColor: '#818cf8' }
  ]);

  renderBarChart('bm-chart-tat', labels, [
    { label: 'Avg Turnaround Time', data: policyKeys.map(k => policies[k].avg_turnaround_time), backgroundColor: '#c084fc' }
  ]);

  renderBarChart('bm-chart-util', labels, [
    { label: 'CPU Utilization %', data: policyKeys.map(k => policies[k].cpu_utilization), backgroundColor: '#34d399' },
    { label: 'Throughput', data: policyKeys.map(k => policies[k].throughput), backgroundColor: '#4ade80' }
  ]);

  renderBarChart('bm-chart-susp', labels, [
    { label: 'Suspensions', data: policyKeys.map(k => policies[k].suspensions), backgroundColor: '#fbbf24' },
    { label: 'Starvation Count', data: policyKeys.map(k => policies[k].starvation_count), backgroundColor: '#f87171' }
  ]);
}

function renderBarChart(canvasId, labels, datasets) {
  if (benchmarkCharts[canvasId]) {
    benchmarkCharts[canvasId].destroy();
  }
  const ctx = document.getElementById(canvasId).getContext('2d');
  benchmarkCharts[canvasId] = new Chart(ctx, {
    type: 'bar',
    data: { labels, datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      scales: {
        x: { grid: { color: '#1e293b' }, ticks: { color: '#94a3b8', font: { family: 'Inter', size: 10 } } },
        y: { grid: { color: '#1e293b' }, ticks: { color: '#94a3b8', font: { family: 'JetBrains Mono', size: 10 } } }
      },
      plugins: {
        legend: { labels: { color: '#cbd5e1', font: { family: 'Inter', size: 10 } } }
      }
    }
  });
}

// 10. Event Listeners Initialization
document.addEventListener('DOMContentLoaded', () => {
  initWebSocket();
  initWatermarkChart();
  fetchCurrentState();

  // Polling fallback to keep dashboard fresh even if WebSocket drops or reconnects
  setInterval(async () => {
    if (!ws || ws.readyState !== WebSocket.OPEN || isPlaying) {
      await fetchCurrentState();
    }
  }, 1000);

  // Play / Pause
  document.getElementById('btn-play-pause').addEventListener('click', async () => {
    isPlaying = !isPlaying;
    const playIcon = document.getElementById('icon-play');
    const pauseIcon = document.getElementById('icon-pause');
    const textElem = document.getElementById('text-play-pause');

    if (isPlaying) {
      playIcon.classList.add('hidden');
      pauseIcon.classList.remove('hidden');
      textElem.innerText = 'Pause';
      const res = await fetch('/api/control/start', { method: 'POST' });
      const data = await res.json();
      if (data && data.snapshot) renderSnapshot(data.snapshot);
    } else {
      playIcon.classList.remove('hidden');
      pauseIcon.classList.add('hidden');
      textElem.innerText = 'Play';
      const res = await fetch('/api/control/pause', { method: 'POST' });
      const data = await res.json();
      if (data && data.snapshot) renderSnapshot(data.snapshot);
    }
  });

  // Step
  document.getElementById('btn-step').addEventListener('click', async () => {
    const res = await fetch('/api/control/step', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ticks: 1 })
    });
    const snap = await res.json();
    if (snap) renderSnapshot(snap);
  });

  // Reset
  document.getElementById('btn-reset').addEventListener('click', async () => {
    isPlaying = false;
    document.getElementById('icon-play').classList.remove('hidden');
    document.getElementById('icon-pause').classList.add('hidden');
    document.getElementById('text-play-pause').innerText = 'Play';
    const res = await fetch('/api/control/reset', { method: 'POST' });
    const snap = await res.json();
    if (snap) renderSnapshot(snap);
  });

  // Live OS Mode Toggle
  document.getElementById('mode-toggle').addEventListener('change', async (e) => {
    await fetch('/api/control/mode', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ os_bridge_enabled: e.target.checked })
    });
    await fetchCurrentState();
  });

  // Speed Buttons
  document.querySelectorAll('.btn-speed').forEach(btn => {
    btn.addEventListener('click', async () => {
      document.querySelectorAll('.btn-speed').forEach(b => {
        b.className = 'btn-speed px-1.5 py-1 rounded text-slate-400 hover:text-white transition-colors';
      });
      btn.className = 'btn-speed px-1.5 py-1 rounded bg-sky-600 text-white font-bold transition-colors';
      const dur = parseFloat(btn.dataset.speed);
      await fetch('/api/control/start', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ tick_duration_sec: dur })
      });
    });
  });

  // Apply Config Dropdowns
  document.getElementById('btn-apply-config').addEventListener('click', async () => {
    const lt = document.getElementById('cfg-long-term').value;
    const mt = document.getElementById('cfg-medium-term').value;
    const st = document.getElementById('cfg-short-term').value;
    const q = parseInt(document.getElementById('cfg-quantum').value, 10);
    const cpus = parseInt(document.getElementById('cfg-cpus').value, 10);

    const res = await fetch('/api/control/reset', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        long_term_policy: lt,
        medium_term_policy: mt,
        short_term_policy: st,
        time_quantum: q,
        num_cpus: cpus
      })
    });
    const snap = await res.json();
    if (snap) renderSnapshot(snap);
  });

  // Workload Preset Load Button
  document.getElementById('btn-apply-preset').addEventListener('click', async () => {
    const val = document.getElementById('select-workload-preset').value;
    const res = await fetch('/api/workload/preset', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ preset: val })
    });
    const data = await res.json();
    if (data && data.snapshot) renderSnapshot(data.snapshot);
  });

  // Quick Add 5 Jobs Button
  document.getElementById('btn-quick-add-5').addEventListener('click', async () => {
    const sampleNames = ['inference-eval', 'data-clean', 'audio-encode', 'matrix-mult', 'feature-extract'];
    for (const name of sampleNames) {
      await fetch('/api/jobs', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name,
          user: 'batch',
          burst_time: Math.floor(Math.random() * 8) + 4,
          memory_gb: Math.floor(Math.random() * 4) + 4,
          priority: Math.floor(Math.random() * 4) + 1
        })
      });
    }
    await fetchCurrentState();
  });

  // Host Task Manager Adoption Modal
  const modalHostTasks = document.getElementById('modal-host-tasks');
  document.getElementById('btn-open-host-tasks').addEventListener('click', () => {
    modalHostTasks.classList.remove('hidden');
    loadHostTasks();
  });
  document.getElementById('btn-close-host-tasks').addEventListener('click', () => modalHostTasks.classList.add('hidden'));
  document.getElementById('btn-refresh-host-tasks').addEventListener('click', loadHostTasks);
  document.getElementById('host-task-filter').addEventListener('input', () => {
    if (window.lastHostProcesses) renderHostTasksTable(window.lastHostProcesses);
  });

  // Clear Event Log
  document.getElementById('btn-clear-log').addEventListener('click', () => {
    document.getElementById('event-log-container').innerHTML = '<div class="text-slate-500 py-4 text-center">Event log cleared.</div>';
  });

  // Interactive Terminal Form
  const termForm = document.getElementById('terminal-form');
  const termInput = document.getElementById('terminal-input');
  termForm.addEventListener('submit', async (e) => {
    e.preventDefault();
    const val = termInput.value;
    termInput.value = '';
    await handleTerminalCommand(val);
  });

  termInput.addEventListener('keydown', (e) => {
    if (e.key === 'ArrowUp') {
      if (historyIndex > 0) {
        historyIndex--;
        termInput.value = commandHistory[historyIndex];
      }
    } else if (e.key === 'ArrowDown') {
      if (historyIndex < commandHistory.length - 1) {
        historyIndex++;
        termInput.value = commandHistory[historyIndex];
      } else {
        historyIndex = commandHistory.length;
        termInput.value = '';
      }
    }
  });

  // Modals Toggles
  const modalSubmit = document.getElementById('modal-submit');
  document.getElementById('btn-open-submit').addEventListener('click', () => modalSubmit.classList.remove('hidden'));
  document.getElementById('btn-close-submit').addEventListener('click', () => modalSubmit.classList.add('hidden'));
  document.getElementById('btn-cancel-submit').addEventListener('click', () => modalSubmit.classList.add('hidden'));

  document.getElementById('form-submit-job').addEventListener('submit', async (e) => {
    e.preventDefault();
    const name = document.getElementById('input-job-name').value;
    const user = document.getElementById('input-job-user').value;
    const burst = parseInt(document.getElementById('input-job-burst').value, 10);
    const mem = parseInt(document.getElementById('input-job-mem').value, 10);
    const prio = parseInt(document.getElementById('input-job-prio').value, 10);

    await fetch('/api/jobs', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name, user, burst_time: burst, memory_gb: mem, priority: prio })
    });
    modalSubmit.classList.add('hidden');
    await fetchCurrentState();
  });

  const modalBm = document.getElementById('modal-benchmark');
  document.getElementById('btn-open-benchmark').addEventListener('click', () => {
    modalBm.classList.remove('hidden');
    triggerBenchmark();
  });
  document.getElementById('btn-close-benchmark').addEventListener('click', () => modalBm.classList.add('hidden'));
  document.getElementById('btn-run-benchmark-now').addEventListener('click', triggerBenchmark);

  const modalInsp = document.getElementById('modal-inspector');
  document.getElementById('btn-close-inspector').addEventListener('click', () => modalInsp.classList.add('hidden'));

  document.getElementById('btn-inspector-cancel').addEventListener('click', async () => {
    if (selectedJobId) {
      await cancelJob(selectedJobId);
      modalInsp.classList.add('hidden');
    }
  });
  document.getElementById('btn-inspector-suspend').addEventListener('click', async () => {
    if (selectedJobId) {
      await suspendJob(selectedJobId);
      modalInsp.classList.add('hidden');
    }
  });
  document.getElementById('btn-inspector-resume').addEventListener('click', async () => {
    if (selectedJobId) {
      await resumeJob(selectedJobId);
      modalInsp.classList.add('hidden');
    }
  });
});

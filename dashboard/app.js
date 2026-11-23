(() => {
  "use strict";

  const LAUNCHER_BASE = `${location.protocol}//${location.hostname}:8000`;
  const NODE_BASE = (node) => `${location.protocol}//${location.hostname}:${8000 + Number(node)}`;
  const POLL_INTERVAL = 2500;
  const TRACE_DELAY = 700;
  const MAX_LOG_ROWS = 250;
  const NODE_NAMES = {1: "Pusat", 2: "Cabang A", 3: "Cabang B", 4: "Replika"};
  const PROTOCOL_COLORS = {
    "XML-RPC": "#38bdf8",
    "Pyro5-RMI": "#a855f7",
    FAILOVER: "#f59e0b",
    ROLLBACK: "#ef4444",
    "CHORD-DHT": "#10b981",
    MPI: "#10b981"
  };
  const $ = (selector, root = document) => root.querySelector(selector);
  const $$ = (selector, root = document) => Array.from(root.querySelectorAll(selector));
  const state = {
    launcherNodes: {}, heartbeat: {}, versions: {}, replica: {}, stocks: {}, gatewayNode: 1,
    polling: false, pollQueued: false, animationQueue: Promise.resolve(), logCount: 0,
    activeLocks: new Map(), logEntries: [],
    activeTimers: new Set()
  };

  function escapeHtml(value) {
    return String(value ?? "").replace(/[&<>"']/g, (char) => ({
      "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"
    })[char]);
  }

  function sleep(milliseconds) {
    return new Promise((resolve) => {
      const timer = window.setTimeout(() => { state.activeTimers.delete(timer); resolve(); }, milliseconds);
      state.activeTimers.add(timer);
    });
  }

  async function requestJson(url, options = {}) {
    const response = await fetch(url, {
      ...options,
      headers: {Accept: "application/json", ...(options.body ? {"Content-Type": "application/json"} : {}), ...(options.headers || {})},
      cache: "no-store"
    });
    const text = await response.text();
    let body;
    try { body = text ? JSON.parse(text) : {}; }
    catch (_error) { throw new Error(`Respons bukan JSON (HTTP ${response.status})`); }
    if (!response.ok || body.ok === false) {
      const message = body.error?.message || body.error || body.message || `HTTP ${response.status}`;
      const error = new Error(String(message));
      error.status = response.status;
      error.payload = body;
      throw error;
    }
    return {body, status: response.status, headers: response.headers};
  }

  function launcherFetch(path, options = {}) { return requestJson(`${LAUNCHER_BASE}${path}`, options); }

  function candidateNodes(preferredNode = 1) {
    const preferred = Number(preferredNode);
    const order = [preferred, 1, 2, 3, 4].filter((n, i, all) => n >= 1 && n <= 4 && all.indexOf(n) === i);
    const alive = order.filter((node) => {
      const processState = state.launcherNodes[String(node)]?.state;
      const status = state.heartbeat[node]?.status;
      return processState === "running" || ["hidup", "up", "healthy", "running", "aktif"].includes(status);
    });
    const uncertain = order.filter((node) => !alive.includes(node) && state.heartbeat[node]?.status !== "mati");
    return [...alive, ...uncertain, ...order.filter((node) => !alive.includes(node) && !uncertain.includes(node))];
  }

  /** Select a live REST gateway while preserving the requested operation/query unchanged. */
  async function apiFetch(path, options = {}, preferredNode = 1) {
    const failures = [];
    for (const node of candidateNodes(preferredNode)) {
      try {
        const result = await requestJson(`${NODE_BASE(node)}${path}`, options);
        state.gatewayNode = node;
        updateGatewayIndicator(node);
        if (node !== Number(preferredNode)) {
          const step = {dari: Number(preferredNode), ke: node, protokol: "FAILOVER", aksi: "gateway_failover",
            ringkasan_data: `Gateway Node ${preferredNode} tidak merespons; request diteruskan melalui Node ${node}.`, status: "ok"};
          appendLocalLog(step);
          toast(`Gateway Node ${preferredNode} tidak tersedia; memakai gateway Node ${node}.`, "warning");
        }
        return result;
      } catch (error) {
        failures.push({node, error});
        if (error.status && error.status < 500 && error.status !== 408 && error.status !== 429) throw error;
      }
    }
    const last = failures[failures.length - 1]?.error;
    throw new Error(last ? `Semua gateway node tidak tersedia. ${last.message}` : "Tidak ada gateway node yang tersedia.");
  }

  function updateGatewayIndicator(node) {
    const label = $("#gateway-node-label");
    const compact = $("#active-gateway");
    if (label) label.textContent = `Node ${node} · :${8000 + Number(node)}`;
    if (compact) compact.textContent = `Gateway :${8000 + Number(node)}`;
  }

  function toast(message, type = "info", timeout = 3800) {
    const region = $("#toast-region");
    if (!region) return;
    const element = document.createElement("div");
    element.className = `toast ${type}`;
    element.textContent = message;
    region.appendChild(element);
    window.setTimeout(() => element.remove(), timeout);
  }

  function setBusy(button, busy, text = "Memproses…") {
    if (!button) return () => {};
    const original = button.dataset.originalText || button.textContent;
    button.dataset.originalText = original;
    button.disabled = busy;
    if (busy) button.textContent = text;
    else button.textContent = original;
    return () => { button.disabled = false; button.textContent = original; };
  }

  function parseNodeStatus(data) {
    const source = data?.heartbeat || data?.nodes || {};
    const result = {};
    for (let node = 1; node <= 4; node += 1) {
      const item = source[node] || source[String(node)] || {};
      const raw = String(item.status || item.state || "unknown").toLowerCase();
      const launcherState = state.launcherNodes[String(node)]?.state;
      const status = ["hidup", "up", "healthy", "running", "aktif"].includes(raw) ? "hidup"
        : ["mati", "down", "dead", "stopped", "timeout"].includes(raw) ? "mati" : "unknown";
      result[node] = {...item, status: status === "unknown" && launcherState === "running" ? "hidup"
        : status === "unknown" && launcherState === "stopped" ? "mati" : status};
    }
    return result;
  }

  function renderNodeStatus() {
    let healthy = 0;
    for (let node = 1; node <= 4; node += 1) {
      const status = state.heartbeat[node]?.status || "unknown";
      if (status === "hidup") healthy += 1;
      const label = status === "hidup" ? "ONLINE" : status === "mati" ? "OFFLINE" : "UNKNOWN";
      const connection = $(`#connection-${node}`);
      if (connection) {
        connection.classList.remove("alive", "dead", "unknown");
        connection.classList.add(status === "hidup" ? "alive" : status === "mati" ? "dead" : "unknown");
        $("b", connection).textContent = label;
      }
      const nodeLabel = $(`#node-status-${node}`);
      if (nodeLabel) {
        nodeLabel.classList.remove("alive", "dead", "unknown");
        nodeLabel.classList.add(status === "hidup" ? "alive" : status === "mati" ? "dead" : "unknown");
        nodeLabel.innerHTML = `<i></i>${label}`;
      }
      const card = $(`[data-node-card="${node}"]`);
      if (card) {
        card.classList.toggle("heartbeat-alive", status === "hidup");
        card.classList.toggle("heartbeat-dead", status === "mati");
      }
      const controlDot = $(`#control-heart-${node}`);
      if (controlDot) {
        controlDot.classList.remove("alive", "dead", "unknown");
        controlDot.classList.add(status === "hidup" ? "alive" : status === "mati" ? "dead" : "unknown");
      }
      const toggle = $(`.toggle-node[data-node="${node}"]`);
      const delay = $(`.delay-node[data-node="${node}"]`);
      const processState = state.launcherNodes[String(node)]?.state;
      if (toggle) {
        const running = processState === "running" || (processState == null && status === "hidup");
        toggle.textContent = running ? "Matikan" : "Hidupkan";
        toggle.dataset.action = running ? "stop" : "start";
        toggle.dataset.state = running ? "running" : "stopped";
        toggle.disabled = processState === "starting";
      }
      if (delay) delay.disabled = status !== "hidup";
    }
    $("#healthy-count").textContent = String(healthy);
  }

  function getStockMap(payload) {
    const data = payload?.data ?? payload ?? {};
    if (Array.isArray(data)) return Object.fromEntries(data.filter((x) => x?.kode).map((x) => [String(x.kode), x]));
    if (data && typeof data === "object" && data.barang && typeof data.barang === "object") return data.barang;
    if (data && typeof data === "object") {
      return Object.fromEntries(Object.entries(data).filter(([, v]) => v && typeof v === "object" && ("stok" in v || "nama" in v)));
    }
    return {};
  }

  function renderStock(node, goods) {
    const host = $(`#stock-${node}`);
    if (!host) return;
    state.stocks[node] = goods || {};
    const entries = Object.entries(state.stocks[node]).sort(([a], [b]) => a.localeCompare(b));
    const count = $(`#stock-count-${node}`);
    if (count) count.textContent = String(entries.length);
    if (!entries.length) {
      host.innerHTML = `<span class="empty-stock">${state.heartbeat[node]?.status === "mati" ? "Node offline" : "Belum ada SKU"}</span>`;
    } else {
      host.innerHTML = entries.slice(0, 3).map(([code, item]) =>
        `<span class="stock-chip" title="${escapeHtml(item.nama || code)}">${escapeHtml(code)}: <b>${escapeHtml(item.stok ?? 0)}</b></span>`).join("");
    }
    updateSkuOptions();
  }

  function updateSkuOptions() {
    const node = Number($("#inventory-node")?.value || 2);
    const list = $("#inventory-sku-options");
    if (!list) return;
    list.innerHTML = Object.keys(state.stocks[node] || {}).sort().map((code) => `<option value="${escapeHtml(code)}"></option>`).join("");
  }

  function versionFrom(data) {
    const payload = data?.data ?? data ?? {};
    return payload.versi_global ?? payload.versi ?? payload.version ?? null;
  }

  function renderVersions() {
    for (let node = 1; node <= 3; node += 1) {
      const element = $(`#version-${node}`);
      if (element) element.textContent = state.versions[String(node)] == null ? "v—" : `v${state.versions[String(node)]}`;
    }
    const sources = state.replica?.replikasi || {};
    const rows = [];
    for (let node = 1; node <= 3; node += 1) {
      const replica = sources[String(node)] || sources[node] || {};
      const local = state.versions[String(node)];
      const remote = replica.versi ?? null;
      const ready = local != null && remote != null;
      const synced = ready && Number(local) === Number(remote);
      const status = !ready ? "WAIT" : synced ? "SYNC" : "STALE";
      rows.push(`<tr><td>N${node}</td><td>${escapeHtml(local == null ? "—" : `v${local}`)}</td><td>${escapeHtml(remote == null ? "—" : `v${remote}`)}</td><td><span class="replica-state ${ready && !synced ? "stale" : ""}"><i></i>${status}</span></td></tr>`);
    }
    const table = $("#replica-versions");
    if (table) table.innerHTML = rows.join("");
    const version4 = $("#version-4");
    if (version4) {
      const labels = Object.entries(sources).filter(([key]) => ["1", "2", "3"].includes(String(key)))
        .sort(([a], [b]) => Number(a) - Number(b)).map(([key, item]) => `N${key}:v${item?.versi ?? "—"}`);
      version4.textContent = labels.length ? labels.join(" · ") : "v—";
    }
  }

  function renderLocks(payload) {
    const data = payload?.data ?? payload ?? {};
    let locks = data.locks ?? data.lock_aktif ?? data.aktif ?? data;
    if (locks && !Array.isArray(locks) && typeof locks === "object") {
      locks = Object.entries(locks).map(([kode, item]) => ({kode, ...(typeof item === "object" ? item : {lease: item})}));
    }
    if (!Array.isArray(locks)) locks = [];
    const active = locks.filter((lock) => lock && lock.kode);
    state.activeLocks = new Map(active.map((lock) => [String(lock.kode), lock]));
    for (let node = 1; node <= 4; node += 1) {
      const element = $(`#node-lock-${node}`);
      if (!element) continue;
      const ownLocks = active.filter((lock) => Number(lock.node_id ?? lock.node ?? lock.sumber ?? 0) === node);
      const visible = ownLocks.length ? ownLocks : (node === 1 || node === 4 ? active : []);
      if (!visible.length) {
        element.textContent = "";
        element.classList.add("hidden");
        continue;
      }
      element.textContent = visible.map((lock) => {
        const remain = Number(lock.sisa_detik ?? lock.sisa_lease ?? lock.lease_sisa ?? lock.sisa ?? lock.lease ?? lock.expires_in ?? 10);
        return `🔒 LOCKED · ${lock.kode} · ${Math.max(0, Math.ceil(remain))}s`;
      }).join(" / ");
      element.classList.remove("hidden");
    }
  }

  function parseReplicaSnapshots(payload) {
    const data = payload?.data ?? payload ?? {};
    state.replica = data;
    const snapshots = data.replikasi || {};
    const merged = {};
    Object.values(snapshots).forEach((item) => Object.entries(item?.barang || {}).forEach(([code, goods]) => { merged[code] = goods; }));
    renderStock(4, merged);
    renderVersions();
  }

  function protocolClass(protocol) {
    const key = String(protocol || "").toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "");
    return ({"xml-rpc": "protocol-xml-rpc", "pyro5-rmi": "protocol-pyro5-rmi", failover: "protocol-failover",
      rollback: "protocol-rollback", "chord-dht": "protocol-chord-dht", mpi: "protocol-mpi"})[key] || "";
  }

  function formatTimestamp(date = new Date()) {
    return new Intl.DateTimeFormat("id-ID", {hour: "2-digit", minute: "2-digit", second: "2-digit", fractionalSecondDigits: 3}).format(date);
  }

  function renderLog() {
    const host = $("#log-rows");
    if (!host) return;
    const filter = $("#log-filter")?.value || "all";
    const shown = state.logEntries.filter((entry) => filter === "all" || entry.step.protokol === filter);
    $("#log-count").textContent = `${shown.length} event${shown.length === 1 ? "" : "s"}`;
    if (!shown.length) {
      host.innerHTML = `<div class="log-empty-row">${state.logEntries.length ? "Tidak ada event untuk filter ini." : "Belum ada aktivitas. Jalankan transaksi atau pencarian untuk melihat lineage."}</div>`;
      return;
    }
    host.innerHTML = shown.map(({step, timestamp}) => {
      const status = String(step.status || "ok").toLowerCase();
      return `<article class="log-entry"><div class="log-entry-top"><span class="log-route">N${escapeHtml(step.dari ?? "?")} → N${escapeHtml(step.ke ?? "?")}</span><time class="log-time">${escapeHtml(formatTimestamp(timestamp))}</time></div><span class="log-protocol ${protocolClass(step.protokol)}">${escapeHtml(step.protokol || "TRACE")}</span><span class="log-status ${status === "error" ? "error" : "ok"}">${status === "error" ? "ERROR" : "OK"}</span><div class="log-action">${escapeHtml(step.aksi || "event")}</div><div class="log-summary">${escapeHtml(step.ringkasan_data || "—")}</div></article>`;
    }).join("");
  }

  function appendLogRow(step, timestamp = new Date()) {
    state.logEntries.unshift({step: {...step}, timestamp});
    if (state.logEntries.length > MAX_LOG_ROWS) state.logEntries.length = MAX_LOG_ROWS;
    state.logCount = state.logEntries.length;
    renderLog();
  }

  function appendLocalLog(step) { appendLogRow(step); }

  function clearActiveBeam() {
    $$(".link-path.active").forEach((path) => path.classList.remove("active"));
    $$(".beam-particle, .beam-particle-halo", $("#beam-layer")).forEach((particle) => particle.remove());
    $$(".node-card").forEach((card) => card.style.removeProperty("--beam-color"));
  }

  function tooltipPositionForNode(node) {
    const canvas = $("#topology-canvas");
    const card = $(`[data-node-card="${node}"]`);
    const tooltip = $("#lineage-tooltip");
    if (!canvas || !card || !tooltip) return;
    const canvasRect = canvas.getBoundingClientRect();
    const cardRect = card.getBoundingClientRect();
    tooltip.style.left = `${cardRect.left - canvasRect.left + cardRect.width / 2}px`;
    tooltip.style.top = `${Math.max(26, cardRect.top - canvasRect.top + 5)}px`;
  }

  function tooltipPositionAtPoint(point) {
    const canvas = $("#topology-canvas");
    const svg = $("#topology-svg");
    const tooltip = $("#lineage-tooltip");
    if (!canvas || !svg || !tooltip) return;
    const rect = svg.getBoundingClientRect();
    const view = svg.viewBox.baseVal;
    const x = (point.x / view.width) * rect.width + (rect.left - canvas.getBoundingClientRect().left);
    const y = (point.y / view.height) * rect.height + (rect.top - canvas.getBoundingClientRect().top);
    tooltip.style.left = `${x}px`;
    tooltip.style.top = `${y}px`;
  }

  function updateTooltip(step, color) {
    const tooltip = $("#lineage-tooltip");
    tooltip.style.setProperty("--beam-color", color);
    $(".tooltip-protocol", tooltip).textContent = `[${step.protokol || "TRACE"}]`;
    $(".tooltip-heading b", tooltip).textContent = step.aksi || "event";
    $(".tooltip-summary", tooltip).textContent = step.ringkasan_data || "—";
    tooltip.classList.remove("hidden");
  }

  function makeParticle(color) {
    const layer = $("#beam-layer");
    const ns = "http://www.w3.org/2000/svg";
    const halo = document.createElementNS(ns, "circle");
    halo.setAttribute("r", "11"); halo.setAttribute("class", "beam-particle-halo"); halo.style.setProperty("--beam-color", color);
    const dot = document.createElementNS(ns, "circle");
    dot.setAttribute("r", "4.5"); dot.setAttribute("class", "beam-particle"); dot.style.setProperty("--beam-color", color);
    layer.append(halo, dot);
    return {halo, dot};
  }

  async function animateRemoteStep(step, color) {
    const from = Number(step.dari); const to = Number(step.ke);
    const pair = `${Math.min(from, to)}-${Math.max(from, to)}`;
    const path = $(`.link-path[data-pair="${pair}"]`);
    if (!path || ![1, 2, 3, 4].includes(from) || ![1, 2, 3, 4].includes(to)) {
      if ([1, 2, 3, 4].includes(to)) await animateLocalStep({...step, dari: to, ke: to}, color);
      return;
    }
    clearActiveBeam();
    path.style.setProperty("--beam-color", color);
    path.classList.add("active");
    const length = path.getTotalLength();
    const midpoint = path.getPointAtLength(length / 2);
    const particles = makeParticle(color);
    updateTooltip(step, color);
    tooltipPositionAtPoint(midpoint);
    const start = performance.now();
    const duration = Math.max(450, TRACE_DELAY - 80);
    await new Promise((resolve) => {
      const tick = (now) => {
        const progress = Math.min(1, (now - start) / duration);
        const distance = from < to ? progress * length : (1 - progress) * length;
        const point = path.getPointAtLength(Math.max(0, Math.min(length, distance)));
        [particles.halo, particles.dot].forEach((item) => { item.setAttribute("cx", String(point.x)); item.setAttribute("cy", String(point.y)); });
        if (progress < 1) requestAnimationFrame(tick); else resolve();
      };
      const point = path.getPointAtLength(from < to ? 0 : length);
      [particles.halo, particles.dot].forEach((item) => { item.setAttribute("cx", String(point.x)); item.setAttribute("cy", String(point.y)); });
      requestAnimationFrame(tick);
    });
    appendLogRow(step);
    await sleep(Math.max(0, TRACE_DELAY - duration));
  }

  async function animateLocalStep(step, color) {
    const node = Number(step.dari || step.ke);
    const card = $(`[data-node-card="${node}"]`);
    clearActiveBeam();
    if (card) {
      card.style.setProperty("--beam-color", color);
      card.classList.remove("local-pulse");
      void card.offsetWidth;
      card.classList.add("local-pulse");
    }
    updateTooltip(step, color);
    tooltipPositionForNode(node);
    appendLogRow(step);
    await sleep(TRACE_DELAY);
    if (card) card.classList.remove("local-pulse");
  }

  async function animateStep(step) {
    const protocol = String(step.protokol || "");
    const color = PROTOCOL_COLORS[protocol] || "#cbd5e1";
    const from = Number(step.dari); const to = Number(step.ke);
    if (from === to || !from || !to) await animateLocalStep(step, color);
    else await animateRemoteStep(step, color);
  }

  /** Preserve serialized trace playback so concurrent responses never overlap beams. */
  function playTraceAnimation(traceList) {
    if (!Array.isArray(traceList) || !traceList.length) return state.animationQueue;
    const run = async () => {
      for (const step of traceList) {
        try { await animateStep(step); }
        catch (error) { console.warn("Trace animation step failed", error, step); appendLogRow({...step, status: step.status || "error"}); }
      }
      clearActiveBeam();
      $("#lineage-tooltip").classList.add("hidden");
    };
    state.animationQueue = state.animationQueue.then(run, run);
    return state.animationQueue;
  }

  function consumeResponse(result) {
    if (Array.isArray(result?.body?.trace) && result.body.trace.length) playTraceAnimation(result.body.trace);
    return result.body;
  }

  async function pollCluster() {
    if (state.polling) { state.pollQueued = true; return; }
    state.polling = true;
    const started = performance.now();
    try {
      const launcherPromise = launcherFetch("/launcher/status").then((result) => result.body).catch(() => null);
      const candidates = candidateNodes(1);
      let statusResult = null; let statusNode = candidates[0] || 1;
      const failures = [];
      for (const node of candidates) {
        try { statusResult = await requestJson(`${NODE_BASE(node)}/status`); statusNode = node; state.gatewayNode = node; break; }
        catch (error) { failures.push(error); }
      }
      if (!statusResult) throw failures.at(-1) || new Error("Status API tidak tersedia");
      const launcherData = await launcherPromise;
      if (launcherData?.nodes) state.launcherNodes = launcherData.nodes;
      const statusData = statusResult.body?.data || {};
      state.heartbeat = parseNodeStatus(statusData);
      const versions = statusData.versi_global || {};
      for (let node = 1; node <= 3; node += 1) if (versions[String(node)] != null) state.versions[String(node)] = versions[String(node)];

      // Fetch cluster snapshots concurrently; rendering never waits for an active animation queue.
      const stockRequests = [1, 2, 3].map(async (node) => {
        try { return {node, result: await requestJson(`${NODE_BASE(statusNode)}/barang?node=${node}`)}; }
        catch (error) { return {node, error}; }
      });
      const lockRequest = requestJson(`${NODE_BASE(statusNode)}/lock`).catch((error) => ({error}));
      const replicaRequest = requestJson(`${NODE_BASE(statusNode)}/replika`).catch((error) => ({error}));
      const [stocks, lockResult, replicaResult] = await Promise.all([Promise.all(stockRequests), lockRequest, replicaRequest]);
      for (const entry of stocks) {
        if (entry.result) {
          const payload = entry.result.body?.data || {};
          renderStock(entry.node, getStockMap(payload));
          const version = versionFrom(payload);
          if (version != null) state.versions[String(entry.node)] = version;
          if (state.heartbeat[entry.node]?.status === "unknown" && entry.result.body?.via !== "RPC -> RMI (failover ke Replika)") state.heartbeat[entry.node].status = "hidup";
        } else if (state.heartbeat[entry.node]?.status === "unknown") state.heartbeat[entry.node].status = "mati";
      }
      if (!lockResult.error) renderLocks(lockResult.body);
      if (!replicaResult.error) parseReplicaSnapshots(replicaResult.body);
      renderNodeStatus();
      renderVersions();
      $("#last-updated").textContent = formatTimestamp();
      $("#gateway-latency").textContent = `${Math.round(performance.now() - started)}ms`;
      updateGatewayIndicator(statusNode);
    } catch (error) {
      console.warn("Cluster polling failed", error);
      $("#last-updated").textContent = "Gateway offline";
    } finally {
      state.polling = false;
      if (state.pollQueued) { state.pollQueued = false; window.setTimeout(pollCluster, 50); }
    }
  }

  async function submitInventory(quantity, button) {
    const restore = setBusy(button, true, "Memproses…");
    const node = Number($("#inventory-node").value);
    const code = $("#inventory-code").value.trim();
    try {
      if (!code || !Number.isInteger(quantity) || quantity === 0) throw new Error("Pilih SKU dan jumlah stok bukan nol.");
      $("#inventory-quantity").value = String(quantity);
      const result = await apiFetch(`/barang/${encodeURIComponent(code)}/stok?node=${node}`, {
        method: "PUT", body: JSON.stringify({jumlah: quantity, tipe: quantity < 0 ? "penjualan" : "restock"})
      }, node);
      consumeResponse(result);
      toast(quantity < 0 ? `Penjualan berhasil: ${code}, ${Math.abs(quantity)} unit terjual.` : `Restock berhasil: ${code}, ${quantity} unit ditambahkan.`, "success");
      await pollCluster();
    } catch (error) {
      if (error.payload?.trace) playTraceAnimation(error.payload.trace);
      toast(`Transaksi stok gagal: ${error.message}`, "error");
    } finally { restore(); }
  }

  async function addSku(event) {
    event.preventDefault();
    const button = $("#add-sku-button");
    const restore = setBusy(button, true, "Menambahkan…");
    const node = Number($("#inventory-node").value);
    const code = $("#manage-code").value.trim();
    try {
      const name = $("#inventory-name").value.trim();
      if (!code || !name) throw new Error("Kode SKU dan nama barang wajib diisi.");
      const result = await apiFetch(`/barang?node=${node}`, {method: "POST", body: JSON.stringify({
        kode: code, nama: name, stok: Number($("#manage-initial-stock").value || 0),
        harga: Number($("#inventory-price").value || 50000), min_stok: Number($("#inventory-min").value || 15)
      })}, node);
      consumeResponse(result);
      $("#inventory-code").value = code;
      $("#delete-code").value = code;
      toast(`SKU ${code} berhasil ditambahkan ke Node ${node}.`, "success");
      await pollCluster();
    } catch (error) {
      if (error.payload?.trace) playTraceAnimation(error.payload.trace);
      toast(error.status === 409 ? `SKU ${code} sudah ada. Gunakan kode lain.` : `Tambah SKU gagal: ${error.message}`, error.status === 409 ? "warning" : "error");
    } finally { restore(); }
  }

  async function deleteSku() {
    const button = $("#delete-sku-button");
    const code = $("#delete-code").value.trim();
    const node = Number($("#inventory-node").value);
    if (!code) { toast("Masukkan kode SKU yang akan dihapus.", "warning"); return; }
    if (!window.confirm(`Hapus SKU ${code} dari Node ${node}?`)) return;
    const restore = setBusy(button, true, "Menghapus…");
    try {
      const result = await apiFetch(`/barang/${encodeURIComponent(code)}?node=${node}`, {method: "DELETE"}, node);
      consumeResponse(result);
      toast(`SKU ${code} berhasil dihapus dari Node ${node}.`, "success");
      await pollCluster();
    } catch (error) {
      if (error.payload?.trace) playTraceAnimation(error.payload.trace);
      toast(`Penghapusan gagal: ${error.message}`, "error");
    } finally { restore(); }
  }

  async function submitTransfer(event) {
    event.preventDefault();
    const button = $("#transfer-form button[type='submit']");
    const restore = setBusy(button, true, "Transfer berjalan…");
    const source = Number($("#transfer-source").value); const target = Number($("#transfer-target").value);
    try {
      if (source === target) throw new Error("Cabang asal dan tujuan harus berbeda.");
      const body = {kode: $("#transfer-code").value.trim(), jumlah: Number($("#transfer-quantity").value), dari: source, ke: target, simulasi_gagal: $("#simulate-failure").checked};
      if (!body.kode || !Number.isInteger(body.jumlah) || body.jumlah <= 0) throw new Error("Kode SKU dan jumlah positif wajib diisi.");
      const result = await apiFetch("/transfer", {method: "POST", body: JSON.stringify(body)}, source);
      consumeResponse(result);
      toast(`Transfer ${body.kode} (${body.jumlah} unit) berhasil, Node ${source} ke Node ${target}.`, "success");
      await pollCluster();
    } catch (error) {
      if (error.payload?.trace) await playTraceAnimation(error.payload.trace);
      const rolledBack = error.payload?.trace?.some((step) => step.protokol === "ROLLBACK" || /rollback/i.test(step.aksi || ""));
      toast(rolledBack ? `Transfer gagal, stok asal ${$("#transfer-code").value.trim()} telah dipulihkan otomatis.` : `Transfer gagal: ${error.message}`, rolledBack ? "warning" : "error");
      if (rolledBack) toast("Distributed lock dilepas setelah kompensasi stok.", "warning");
    } finally { restore(); }
  }

  async function submitDht(event) {
    event.preventDefault();
    const button = $("#dht-form button[type='submit']");
    const restore = setBusy(button, true, "Mencari…");
    const code = $("#dht-code").value.trim();
    try {
      if (!code) throw new Error("Masukkan kode SKU.");
      const result = await apiFetch(`/dht/lookup/${encodeURIComponent(code)}`, {}, 1);
      const data = result.body.data || {};
      const owner = data.node_pemilik ?? data.node_id ?? "?";
      const item = data.barang || data.item || {};
      const hops = Array.isArray(data.finger_hops) ? data.finger_hops : [];
      $("#dht-result .result-content").innerHTML = `<b>Pemilik:</b> Node ${escapeHtml(owner)} · ${escapeHtml(NODE_NAMES[owner] || "Node")}<br><b>Hop:</b> ${escapeHtml(hops.length ? hops.length : 0)} <span>${hops.length ? `(${escapeHtml(hops.join(" → "))})` : ""}</span><br><b>Stok:</b> ${escapeHtml(item?.stok ?? "SKU tidak ditemukan")}`;
      $("#dht-result").classList.remove("hidden");
      consumeResponse(result);
      toast(`Rute DHT selesai. ${code} diarahkan ke Node ${owner}.`, "success");
    } catch (error) {
      $("#dht-result .result-content").textContent = error.message;
      $("#dht-result").classList.remove("hidden");
      if (error.payload?.trace) playTraceAnimation(error.payload.trace);
      toast(`Pencarian DHT gagal: ${error.message}`, "error");
    } finally { restore(); }
  }

  function discrepancyCount(data) {
    const vector = data?.discrepancy_vector || data?.discrepancy || [];
    if (Array.isArray(vector)) return vector.length;
    if (typeof vector === "number") return vector;
    return Object.keys(vector || {}).length;
  }

  function formatCurrency(value) {
    return new Intl.NumberFormat("id-ID", {style: "currency", currency: "IDR", maximumFractionDigits: 0}).format(Number(value) || 0);
  }

  function openMpiModal(data) {
    const aggregate = data.agregat || data.aggregates || {};
    const rows = Array.isArray(data.sku) ? data.sku : (Array.isArray(data.rows) ? data.rows : []);
    $("#mpi-mode").textContent = String(data.mode || "MPI").toUpperCase().replaceAll("_", " ");
    $("#audit-valuation").textContent = formatCurrency(aggregate.total_valuasi ?? aggregate.valuasi ?? 0);
    $("#audit-critical").textContent = String(aggregate.jumlah_stok_kritis ?? aggregate.stok_kritis ?? 0);
    $("#audit-discrepancy").textContent = String(discrepancyCount(data));
    $("#audit-summary").textContent = `Total stok nasional ${Number(aggregate.total_stok ?? 0).toLocaleString("id-ID")} unit · ${rows.length} SKU dianalisis · rasio safety stock ${data.parameter?.rasio_safety_stock ?? "—"}.`;
    $("#audit-ranks").textContent = `${data.jumlah_rank ?? 4} ranks · mode ${data.mode || "MPI"}`;
    const tbody = $("#audit-sku-rows");
    if (!rows.length) tbody.innerHTML = `<tr><td colspan="5" class="table-empty">Tidak ada SKU pada snapshot audit.</td></tr>`;
    else {
      const prioritized = [...rows].sort((a, b) => Number(b.stok_kritis) - Number(a.stok_kritis) || Number(b.valuasi || 0) - Number(a.valuasi || 0)).slice(0, 30);
      tbody.innerHTML = prioritized.map((item) => `<tr class="${item.stok_kritis ? "critical" : ""}"><td>${escapeHtml(item.kode || "—")}</td><td>${escapeHtml(Number(item.total_stok ?? 0).toLocaleString("id-ID"))}</td><td>${escapeHtml(formatCurrency(item.valuasi ?? 0))}</td><td>${item.stok_kritis ? `<span class="critical-badge">KRITIS</span>` : `<span class="healthy-badge">NORMAL</span>`}</td><td>${escapeHtml(item.inventory_turnover ?? "—")}</td></tr>`).join("");
    }
    $("#mpi-modal").classList.remove("hidden");
    $("#close-mpi-modal").focus();
  }

  async function seedData() {
    const restore = setBusy($("#seed-button"), true, "Seed berjalan…");
    try {
      const result = await apiFetch("/admin/seed", {method: "POST", body: JSON.stringify({})}, 1);
      consumeResponse(result);
      toast("Seed data dan sinkronisasi replika berhasil dijalankan.", "success");
      await pollCluster();
    } catch (error) {
      if (error.payload?.trace) playTraceAnimation(error.payload.trace);
      toast(`Seed gagal: ${error.message}`, "error");
    } finally { restore(); }
  }

  async function runMpiAudit() {
    const restore = setBusy($("#mpi-button"), true, "Menghitung…");
    try {
      const result = await apiFetch("/analytics/mpi", {method: "POST", body: JSON.stringify({})}, 1);
      consumeResponse(result);
      openMpiModal(result.body.data || {});
      toast(`Audit paralel selesai (${result.body.data?.mode || "MPI"}).`, "success");
    } catch (error) {
      if (error.payload?.trace) playTraceAnimation(error.payload.trace);
      toast(`Audit MPI gagal: ${error.message}`, "error");
    } finally { restore(); }
  }

  async function controlNode(node, action, button) {
    const restore = setBusy(button, true, action === "start" ? "Menyalakan…" : "Mematikan…");
    try {
      const result = await launcherFetch(`/launcher/${action}/${node}`, {method: "POST"});
      const data = result.body;
      appendLocalLog({protokol: "FAILOVER", aksi: `launcher_${action}`, dari: node, ke: node,
        ringkasan_data: `Node ${node}: ${data.state || action}, pid=${data.pid ?? "—"}`, status: "ok"});
      toast(`Node ${node} ${action === "start" ? "sedang dinyalakan" : "dimatikan"}.`, "success");
      await sleep(300);
      await pollCluster();
    } catch (error) { toast(`Kontrol Node ${node} gagal: ${error.message}`, "error"); }
    finally { restore(); }
  }

  async function injectDelay(node, button) {
    const restore = setBusy(button, true, "Mengirim…");
    try {
      const result = await apiFetch(`/admin/delay/${node}`, {method: "POST", body: JSON.stringify({detik: 4.0})}, node);
      consumeResponse(result);
      toast(`Delay 4 detik diterapkan ke Node ${node}.`, "warning");
    } catch (error) {
      if (error.payload?.trace) playTraceAnimation(error.payload.trace);
      toast(`Suntik delay gagal: ${error.message}`, "error");
    } finally { restore(); }
  }

  async function syncReplica(button) {
    const restore = setBusy(button, true, "Sinkronisasi…");
    try {
      const result = await apiFetch("/replika/sinkron_ulang", {method: "POST", body: JSON.stringify({})}, 4);
      consumeResponse(result);
      const outcomes = result.body.data?.hasil || {};
      const failed = Object.values(outcomes).filter((item) => item.status === "gagal").length;
      toast(failed ? `Sinkronisasi selesai; ${failed} sumber belum berhasil.` : "Snapshot Node 4 berhasil disinkronkan.", failed ? "warning" : "success");
      await pollCluster();
    } catch (error) {
      if (error.payload?.trace) playTraceAnimation(error.payload.trace);
      toast(`Sinkronisasi gagal: ${error.message}`, "error");
    } finally { restore(); }
  }

  function activateTab(button) {
    const group = button.dataset.tabGroup;
    const tab = button.dataset.tab;
    $$(`[data-tab-group="${group}"]`).forEach((item) => {
      const active = item === button;
      item.classList.toggle("active", active);
      item.setAttribute("aria-selected", active ? "true" : "false");
    });
    $$(`[data-panel-group="${group}"]`).forEach((panel) => {
      const active = panel.dataset.panel === tab;
      panel.classList.toggle("active", active);
      panel.hidden = !active;
    });
  }

  function showNodeDetail(node) {
    const data = state.stocks[node] || {};
    const title = $("#detail-title");
    title.textContent = `Node ${node} · ${NODE_NAMES[node]}`;
    const content = $("#detail-content");
    const entries = Object.entries(data).sort(([a], [b]) => a.localeCompare(b));
    content.innerHTML = entries.length ? entries.map(([code, item]) => `<div class="detail-item"><span>${escapeHtml(code)} · ${escapeHtml(item.nama || "Barang")}</span><b>Stok ${escapeHtml(item.stok ?? 0)}</b></div>`).join("") : `<p class="field-hint">Tidak ada data barang untuk node ini.</p>`;
    $("#node-detail").hidden = false;
  }

  function bindEvents() {
    $$('[role="tab"][data-tab-group]').forEach((button) => button.addEventListener("click", () => activateTab(button)));
    $("#inventory-node").addEventListener("change", updateSkuOptions);
    $$(".quantity-preset").forEach((button) => button.addEventListener("click", () => { $("#inventory-quantity").value = button.dataset.quantity; }));
    $("#inventory-form").addEventListener("submit", (event) => {
      event.preventDefault();
      const value = Number($("#inventory-quantity").value);
      submitInventory(value, value < 0 ? $("#sell-button") : $("#restock-button"));
    });
    $("#sell-button").addEventListener("click", () => {
      const current = Number($("#inventory-quantity").value);
      const quantity = current > 0 ? -current : (current < 0 ? current : -1);
      submitInventory(quantity, $("#sell-button"));
    });
    $("#restock-button").addEventListener("click", () => {
      const current = Number($("#inventory-quantity").value);
      const quantity = current < 0 ? -current : (current > 0 ? current : 10);
      submitInventory(quantity, $("#restock-button"));
    });
    $("#manage-sku-form").addEventListener("submit", addSku);
    $("#delete-sku-button").addEventListener("click", deleteSku);
    $("#transfer-form").addEventListener("submit", submitTransfer);
    $("#dht-form").addEventListener("submit", submitDht);
    $("#seed-button").addEventListener("click", seedData);
    $("#mpi-button").addEventListener("click", runMpiAudit);
    $("#sync-replica-button").addEventListener("click", (event) => syncReplica(event.currentTarget));
    $("#clear-log-button").addEventListener("click", () => { state.logEntries = []; state.logCount = 0; renderLog(); });
    $("#log-filter").addEventListener("change", renderLog);
    $$(".toggle-node").forEach((button) => button.addEventListener("click", () => controlNode(Number(button.dataset.node), button.dataset.action, button)));
    $$(".delay-node").forEach((button) => button.addEventListener("click", () => injectDelay(Number(button.dataset.node), button)));
    $$("[data-node-card]").forEach((card) => card.addEventListener("click", () => showNodeDetail(Number(card.dataset.nodeCard))));
    $("#close-detail").addEventListener("click", () => { $("#node-detail").hidden = true; });
    const closeModal = () => $("#mpi-modal").classList.add("hidden");
    $("#close-mpi-modal").addEventListener("click", closeModal);
    $("#close-mpi-modal-bottom").addEventListener("click", closeModal);
    $("#mpi-modal").addEventListener("click", (event) => { if (event.target.id === "mpi-modal") closeModal(); });
    document.addEventListener("keydown", (event) => { if (event.key === "Escape") { closeModal(); $("#node-detail").hidden = true; } });
    window.addEventListener("resize", () => { const tip = $("#lineage-tooltip"); if (tip && !tip.classList.contains("hidden")) tip.classList.add("hidden"); });
  }

  async function initialize() {
    bindEvents();
    updateSkuOptions();
    await pollCluster();
    window.setInterval(pollCluster, POLL_INTERVAL);
    window.setInterval(() => {
      state.activeLocks.forEach((lock, code) => {
        const expiry = Number(lock.expires_at ?? lock.expire_at ?? 0);
        if (expiry && expiry <= Date.now() / 1000) state.activeLocks.delete(code);
      });
    }, 1000);
  }

  window.DistriPOS = {apiFetch, playTraceAnimation, pollCluster};
  initialize().catch((error) => { console.error("Dashboard initialization failed", error); toast(`Dashboard gagal dimulai: ${error.message}`, "error"); });
})();
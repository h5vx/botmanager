"use strict";

// botmanager admin UI. No framework, no build step. Everything that comes
// from the server is inserted with textContent only (see h()), never as
// HTML: bot names and other values are user-controlled.

const REFRESH_MS = 5000;

const state = {
  user: null,
  tab: localStorage.getItem("bm.tab") || "overview",
  overview: null,
  loadError: null,
  openPanels: {}, // bot id -> "messages" | "ping"
  panelData: {},  // bot id -> rendered panel content state
  forms: {},      // form field name -> current value (survives re-renders)
  busy: false,
};

// ---------- DOM helpers ----------

function h(tag, attrs, ...children) {
  const el = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs || {})) {
    if (v === undefined || v === null || v === false) continue;
    if (k === "class") el.className = v;
    else if (k.startsWith("on")) el.addEventListener(k.slice(2), v);
    else if (k === "value") el.value = v;
    else el.setAttribute(k, v === true ? "" : v);
  }
  for (const c of children.flat(Infinity)) {
    if (c === null || c === undefined || c === false) continue;
    el.append(c instanceof Node ? c : document.createTextNode(String(c)));
  }
  return el;
}

// field creates an <input> or <select> whose value lives in state.forms, so
// periodic re-rendering never loses what the user typed; focus is restored
// by name (see render).
function field(tag, key, attrs, ...children) {
  const { rerender, value, ...rest } = attrs || {};
  const el = h(tag, { ...rest, name: key }, ...children);
  const initial = state.forms[key] !== undefined ? state.forms[key] : value || "";
  if (initial !== "") el.value = initial;
  if (tag === "select" && state.forms[key] === undefined) state.forms[key] = el.value;
  const update = () => {
    if (state.forms[key] === el.value) return;
    state.forms[key] = el.value;
    if (rerender) render();
  };
  el.addEventListener("input", update);
  el.addEventListener("change", update);
  return el;
}

function clearForm(...keys) {
  for (const k of keys) delete state.forms[k];
}

function toast(text, kind) {
  const t = h("div", { class: "toast " + (kind || "") }, text);
  document.getElementById("toasts").append(t);
  setTimeout(() => t.remove(), kind === "bad" ? 8000 : 4000);
}

// ---------- formatting ----------

const num = (v) => Number(v || 0);

function bytes(v) {
  let n = num(v);
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  let i = 0;
  while (n >= 1024 && i < units.length - 1) { n /= 1024; i++; }
  return (i === 0 ? n : n.toFixed(1)) + " " + units[i];
}

function duration(ms) {
  const s = Math.max(0, Math.floor(ms / 1000));
  const d = Math.floor(s / 86400), hh = Math.floor(s % 86400 / 3600), m = Math.floor(s % 3600 / 60);
  if (d) return `${d}d ${hh}h`;
  if (hh) return `${hh}h ${m}m`;
  if (m) return `${m}m`;
  return `${s}s`;
}

function since(ts) {
  if (!ts) return "—";
  return duration(Date.now() - new Date(ts).getTime());
}

function when(ts) {
  if (!ts) return "—";
  const d = new Date(ts);
  return d.toLocaleString();
}

const enumName = (v, prefix) => String(v || "").replace(prefix, "");

function botStateBadge(st) {
  const s = enumName(st, "BOT_STATE_");
  const kind = { ENABLED: "ok", DISABLED: "", BROKEN: "bad", DELETED: "warn" }[s] || "";
  return h("span", { class: "badge " + kind }, s.toLowerCase());
}

function deliveryBadge(st) {
  const s = enumName(st, "DELIVERY_STATUS_");
  const kind = { SENT: "ok", PENDING: "accent", RETRYING: "warn", FAILED: "bad", CANCELLED: "" }[s] || "";
  return h("span", { class: "badge " + kind }, s.toLowerCase());
}

// ---------- API ----------

async function api(method, path, body) {
  const opts = { method, headers: { "X-Botmanager-Request": "1" }, credentials: "same-origin" };
  if (body !== undefined) {
    opts.headers["Content-Type"] = "application/json";
    opts.body = JSON.stringify(body);
  }
  const res = await fetch(path, opts);
  if (res.status === 401 && path !== "/api/login") {
    state.user = null;
    render();
    throw new Error("session expired, please log in again");
  }
  if (res.status === 204) return null;
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.error || res.statusText);
  return data;
}

async function act(label, fn) {
  if (state.busy) return;
  state.busy = true;
  render();
  try {
    await fn();
    toast(label, "ok");
    await refresh();
  } catch (e) {
    toast(e.message, "bad");
  } finally {
    state.busy = false;
    render();
  }
}

async function refresh() {
  if (!state.user) return;
  try {
    state.overview = await api("GET", "/api/overview");
    state.loadError = null;
  } catch (e) {
    state.loadError = e.message;
  }
  render();
}

// ---------- views ----------

function loginView() {
  const err = h("div", { class: "error" });
  const user = field("input", "login.username", { autocomplete: "username", required: true, autofocus: true });
  const pass = field("input", "login.password", { type: "password", autocomplete: "current-password", required: true });
  const form = h("form", {
    onsubmit: async (e) => {
      e.preventDefault();
      err.textContent = "";
      try {
        const r = await api("POST", "/api/login", { username: state.forms["login.username"] || "", password: state.forms["login.password"] || "" });
        clearForm("login.password");
        state.user = r.username;
        await refresh();
      } catch (ex) {
        err.textContent = ex.message;
      }
    },
  },
    h("div", { class: "field" }, h("label", {}, "Username"), user),
    h("div", { class: "field" }, h("label", {}, "Password"), pass),
    h("button", { class: "primary", type: "submit" }, "Log in"),
    err,
  );
  return h("div", { class: "login card" },
    h("div", { class: "brand" }, h("img", { src: "logo.svg", alt: "" }), "botmanager"),
    form,
  );
}

function topbar() {
  const tab = (id, label) => h("button", {
    class: "tab" + (state.tab === id ? " active" : ""),
    onclick: () => { state.tab = id; localStorage.setItem("bm.tab", id); render(); },
  }, label);
  const local = state.overview && state.overview.local_node;
  return h("header", { class: "topbar" },
    h("div", { class: "brand" }, h("img", { src: "logo.svg", alt: "" }), "botmanager"),
    h("nav", { class: "tabs" }, tab("overview", "Overview"), tab("nodes", "Nodes"), tab("bots", "Bots")),
    h("div", { class: "spacer" }),
    h("span", { class: "who" }, local ? `node ${local} · ` : "", state.user),
    h("button", {
      onclick: async () => { await api("POST", "/api/logout").catch(() => {}); state.user = null; state.overview = null; render(); },
    }, "Log out"),
  );
}

function nodesWithStats() {
  const o = state.overview;
  if (!o) return [];
  return (o.cluster.nodes || []).map((n) => ({ ...n, ...(o.node_stats[n.node_id] || {}) }));
}

function leaderStats() {
  const o = state.overview;
  const leader = o && o.cluster.leader_id;
  const all = nodesWithStats();
  const pick = all.find((n) => n.node_id === leader && n.stats) || all.find((n) => n.stats);
  return pick ? pick.stats : null;
}

function overviewView() {
  const o = state.overview;
  const nodes = nodesWithStats();
  const st = leaderStats();
  const reachable = nodes.filter((n) => n.reachable).length;
  const bots = o.cluster.bots || [];
  const byState = (s) => bots.filter((b) => enumName(b.state, "BOT_STATE_") === s).length;
  const msgs = (st && st.messages_by_status) || {};
  const totalMsgs = num(st && st.messages_total);

  const tile = (label, value, sub, kind) => h("div", { class: "card tile" },
    h("div", { class: "label" }, label),
    h("div", { class: "value" }, value),
    sub ? h("div", { class: "sub" + (kind ? " " + kind : "") }, sub) : null,
  );

  const statusBars = ["PENDING", "RETRYING", "SENT", "FAILED", "CANCELLED"].map((s) => {
    const n = num(msgs[s]);
    const fill = h("span", { class: { SENT: "ok", RETRYING: "warn", FAILED: "bad" }[s] || "" });
    fill.style.width = (totalMsgs ? (n / totalMsgs) * 100 : 0) + "%";
    return h("div", { class: "bar-row" }, h("span", {}, s.toLowerCase()), h("div", { class: "bar" }, fill), h("span", { class: "num" }, n));
  });

  const maxSys = Math.max(1, ...nodes.map((n) => num(n.stats && n.stats.sys_bytes)));
  const memBars = nodes.map((n) => {
    const fill = h("span", {});
    const sys = num(n.stats && n.stats.sys_bytes);
    fill.style.width = (sys / maxSys) * 100 + "%";
    return h("div", { class: "bar-row" },
      h("span", { class: "mono" }, n.node_id),
      h("div", { class: "bar" }, fill),
      h("span", { class: "num" }, n.stats ? bytes(sys) : "—"));
  });

  return h("div", {},
    h("div", { class: "tiles" },
      tile("Leader", o.cluster.leader_id || "none", o.cluster.leader_id ? "" : "no leader elected", o.cluster.leader_id ? "" : "bad"),
      tile("Nodes", `${reachable}/${nodes.length}`, reachable === nodes.length ? "all reachable" : `${nodes.length - reachable} unreachable`),
      tile("Bots", bots.length, `${byState("ENABLED")} enabled · ${byState("BROKEN")} broken`),
      tile("Messages", totalMsgs, `${num(msgs.PENDING) + num(msgs.RETRYING)} waiting to be sent`),
      tile("Events", st ? num(st.journal_latest_sequence) : "—", st ? `${num(st.journal_retained)} retained for Subscribe` : ""),
      tile("Known chats", st ? num(st.chats_known) : "—", ""),
    ),
    h("div", { class: "split" },
      h("div", {}, h("h2", {}, "Messages by status"), h("div", { class: "card bars" }, statusBars)),
      h("div", {}, h("h2", {}, "Memory per node (from the OS)"), h("div", { class: "card bars" }, memBars.length ? memBars : h("span", { class: "hint" }, "no nodes"))),
    ),
    h("h2", {}, "Broken bots"),
    brokenBots(bots),
  );
}

function brokenBots(bots) {
  const broken = bots.filter((b) => enumName(b.state, "BOT_STATE_") === "BROKEN");
  if (!broken.length) return h("p", { class: "hint" }, "None — every enabled bot is running.");
  return h("div", { class: "table-wrap" }, h("table", {},
    h("thead", {}, h("tr", {}, h("th", {}, "Bot"), h("th", {}, "Failure"), h("th", {}, "Reason"))),
    h("tbody", {}, broken.map((b) => h("tr", {},
      h("td", {}, b.display_name || b.id),
      h("td", {}, enumName(b.last_failure_class, "FAILURE_CLASS_").toLowerCase()),
      h("td", { class: "small" }, b.last_failure_reason || "")))),
  ));
}

function nodesView() {
  const o = state.overview;
  const nodes = nodesWithStats();
  const rows = nodes.map((n) => {
    const s = n.stats;
    const role = n.is_leader ? h("span", { class: "badge accent" }, "leader") : h("span", { class: "badge" }, "follower");
    return h("tr", {},
      h("td", {}, h("div", { class: "mono nowrap" }, n.node_id), n.node_id === o.local_node ? h("div", { class: "small" }, "this node") : null),
      h("td", {}, role, " ", n.reachable ? h("span", { class: "badge ok" }, "reachable") : h("span", { class: "badge bad" }, "unreachable")),
      h("td", { class: "mono" }, h("div", {}, "raft ", n.raft_address || "—"), h("div", {}, "grpc ", n.grpc_address || "—")),
      h("td", {}, s ? [h("div", {}, bytes(s.heap_alloc_bytes), " heap"), h("div", { class: "small" }, bytes(s.sys_bytes), " from OS")] : h("span", { class: "small" }, n.error || "—")),
      h("td", {}, s ? bytes(s.data_dir_bytes) : "—"),
      h("td", {}, s ? [h("div", {}, s.raft_state, " · term ", num(s.raft_term)), h("div", { class: "small mono" }, "applied ", num(s.applied_index), " / ", num(s.last_log_index))] : "—"),
      h("td", { class: "num" }, s ? num(s.running_bots) : "—"),
      h("td", {}, s ? [h("div", { class: "nowrap" }, s.version || "—"), h("div", { class: "small nowrap" }, "up ", since(s.started_at))] : "—"),
      h("td", {}, h("div", { class: "actions" },
        h("button", {
          disabled: state.busy || n.is_leader,
          onclick: () => confirm(`Make ${n.node_id} the leader? Bots move to it within seconds.`) &&
            act(`${n.node_id} is now the leader`, () => api("POST", "/api/leader", { node_id: n.node_id })),
        }, "Make leader"),
        h("button", { disabled: state.busy, onclick: () => pingNode(n.node_id) }, "Ping Telegram"),
        h("button", {
          class: "danger",
          disabled: state.busy,
          onclick: () => confirm(`Remove ${n.node_id} from the cluster?`) &&
            act(`${n.node_id} removed`, () => api("DELETE", "/api/nodes/" + encodeURIComponent(n.node_id))),
        }, "Remove"),
      )),
    );
  });

  const id = field("input", "node.id", { placeholder: "node-4" });
  const raft = field("input", "node.raft", { placeholder: "10.0.0.4:9092" });
  const grpc = field("input", "node.grpc", { placeholder: "10.0.0.4:9090" });
  const addForm = h("form", {
    class: "form",
    onsubmit: (e) => {
      e.preventDefault();
      const f = state.forms;
      act(`${f["node.id"]} added`, async () => {
        await api("POST", "/api/nodes", { node_id: f["node.id"] || "", raft_address: f["node.raft"] || "", grpc_address: f["node.grpc"] || "" });
        clearForm("node.id", "node.raft", "node.grpc");
      });
    },
  },
    h("div", { class: "field" }, h("label", {}, "Node ID"), id),
    h("div", { class: "field" }, h("label", {}, "Raft address"), raft),
    h("div", { class: "field" }, h("label", {}, "gRPC address"), grpc),
    h("button", { class: "primary", type: "submit", disabled: state.busy }, "Add node"),
  );

  return h("div", {},
    h("h2", {}, "Cluster members"),
    h("div", { class: "table-wrap" }, h("table", {},
      h("thead", {}, h("tr", {}, ["Node", "Status", "Addresses", "Memory", "Data", "Raft", "Bots", "Version", ""].map((t) => h("th", {}, t)))),
      h("tbody", {}, rows),
    )),
    h("h2", {}, "Add a node"),
    h("div", { class: "card" },
      h("p", { class: "hint" }, "Start the new node with raft.bootstrap: false, a certificate from the cluster CA and the same token key, then add it here."),
      addForm),
  );
}

async function pingNode(nodeId, botId) {
  try {
    const r = await api("POST", "/api/ping", { node_id: nodeId, bot_id: botId || "" });
    const via = r.proxy_used ? ` via ${r.proxy_used}` : "";
    const who = botId ? `${nodeId}, bot ${botId}` : nodeId;
    if (r.success) toast(`${who}: Telegram reachable in ${num(r.latency_ms)} ms${via}`, "ok");
    else if (num(r.http_status) > 0) toast(`${who}: Telegram reachable${via}, but answered HTTP ${r.http_status}: ${r.error || ""}`, "bad");
    else toast(`${who}: Telegram not reachable${via}: ${r.error || "no response"}`, "bad");
  } catch (e) {
    toast(e.message, "bad");
  }
}

function botsView() {
  const o = state.overview;
  const bots = (o.cluster.bots || []).filter((b) => enumName(b.state, "BOT_STATE_") !== "DELETED");
  const deleted = (o.cluster.bots || []).length - bots.length;
  const nodeIds = (o.cluster.nodes || []).map((n) => n.node_id);

  const rows = [];
  for (const b of bots) {
    const st = enumName(b.state, "BOT_STATE_");
    const nodeSel = field("select", "ping." + b.id, { value: o.cluster.leader_id || "", "aria-label": "Node to ping from" },
      nodeIds.map((id) => h("option", { value: id }, id)));
    rows.push(h("tr", {},
      h("td", {}, h("div", {}, b.display_name || "(no name)"), h("div", { class: "small mono" }, b.id)),
      h("td", {}, b.username ? h("a", { href: "https://t.me/" + b.username, target: "_blank", rel: "noopener noreferrer" }, "@" + b.username) : h("span", { class: "small" }, "—")),
      h("td", {}, botStateBadge(b.state), st === "BROKEN" ? h("div", { class: "small" }, b.last_failure_reason) : null),
      h("td", { class: "small mono" }, !b.proxy ? "node default" : b.proxy.enabled ? b.proxy.address : "direct"),
      h("td", { class: "small" }, when(b.updated_at)),
      h("td", {}, h("div", { class: "actions" },
        st === "ENABLED"
          ? h("button", { disabled: state.busy, onclick: () => act(`${b.display_name || b.id} disabled`, () => api("POST", `/api/bots/${encodeURIComponent(b.id)}/state`, { state: "DISABLED" })) }, "Disable")
          : h("button", { disabled: state.busy, onclick: () => act(`${b.display_name || b.id} enabled`, () => api("POST", `/api/bots/${encodeURIComponent(b.id)}/state`, { state: "ENABLED" })) }, "Enable"),
        nodeSel,
        h("button", { onclick: () => pingNode(nodeSel.value, b.id) }, "Ping"),
        h("button", { onclick: () => toggleMessages(b.id) }, state.openPanels[b.id] ? "Hide messages" : "Messages"),
        h("button", {
          class: "danger",
          disabled: state.busy,
          onclick: () => confirm(`Delete ${b.display_name || b.id}? The bot stops; its message history is kept.`) &&
            act(`${b.display_name || b.id} deleted`, () => api("DELETE", "/api/bots/" + encodeURIComponent(b.id))),
        }, "Delete"),
      )),
    ));
    if (state.openPanels[b.id]) {
      rows.push(h("tr", {}, h("td", { colspan: "6" }, messagesPanel(b.id))));
    }
  }

  return h("div", {},
    h("h2", {}, "Bots"),
    bots.length
      ? h("div", { class: "table-wrap" }, h("table", {},
        h("thead", {}, h("tr", {}, ["Bot", "Telegram", "State", "Proxy", "Updated", ""].map((t) => h("th", {}, t)))),
        h("tbody", {}, rows)))
      : h("p", { class: "hint" }, "No bots yet."),
    deleted ? h("p", { class: "hint" }, `${deleted} deleted bot(s) hidden; their history is kept.`) : null,
    h("h2", {}, "Create a bot"),
    createBotForm(),
  );
}

function createBotForm() {
  const name = field("input", "bot.name", { placeholder: "Support bot" });
  const token = field("input", "bot.token", { type: "password", placeholder: "123456:ABC…", autocomplete: "off" });
  const mode = field("select", "bot.proxy_mode", { rerender: true },
    h("option", { value: "default" }, "Node default"),
    h("option", { value: "custom" }, "Own proxy"),
    h("option", { value: "none" }, "No proxy"));
  const proxy = field("input", "bot.proxy", { placeholder: "socks5h://user:pass@host:1080", disabled: state.forms["bot.proxy_mode"] !== "custom" });
  return h("div", { class: "card" },
    h("p", { class: "hint" }, "The token is sent to botmanager once and stored encrypted; it is never shown again. New bots start disabled."),
    h("form", {
      class: "form",
      onsubmit: (e) => {
        e.preventDefault();
        const f = state.forms;
        act("Bot created", async () => {
          await api("POST", "/api/bots", { display_name: f["bot.name"] || "", token: f["bot.token"] || "", proxy_mode: f["bot.proxy_mode"] || "default", proxy_address: f["bot.proxy"] || "" });
          clearForm("bot.name", "bot.token", "bot.proxy");
        });
      },
    },
      h("div", { class: "field" }, h("label", {}, "Name"), name),
      h("div", { class: "field" }, h("label", {}, "Token"), token),
      h("div", { class: "field" }, h("label", {}, "Proxy"), mode),
      h("div", { class: "field" }, h("label", {}, "Proxy address"), proxy),
      h("button", { class: "primary", type: "submit", disabled: state.busy }, "Create bot"),
    ),
  );
}

async function toggleMessages(botId) {
  if (state.openPanels[botId]) {
    delete state.openPanels[botId];
    render();
    return;
  }
  state.openPanels[botId] = true;
  state.panelData[botId] = { loading: true };
  render();
  try {
    const r = await api("GET", `/api/bots/${encodeURIComponent(botId)}/messages?limit=50`);
    state.panelData[botId] = { messages: r.messages };
  } catch (e) {
    state.panelData[botId] = { error: e.message };
  }
  render();
}

function messagesPanel(botId) {
  const d = state.panelData[botId] || {};
  if (d.loading) return h("div", { class: "panel hint" }, "Loading…");
  if (d.error) return h("div", { class: "panel" }, d.error);
  if (!d.messages || !d.messages.length) return h("div", { class: "panel hint" }, "No messages sent by this bot yet.");
  return h("div", { class: "panel msg-list" }, h("table", {},
    h("thead", {}, h("tr", {}, ["Created", "Chat", "Status", "Retries", "Text", "Error"].map((t) => h("th", {}, t)))),
    h("tbody", {}, d.messages.map((m) => h("tr", {},
      h("td", { class: "small" }, when(m.created_at)),
      h("td", { class: "mono" }, m.chat_id),
      h("td", {}, deliveryBadge(m.delivery && m.delivery.status)),
      h("td", { class: "num" }, num(m.delivery && m.delivery.retries)),
      h("td", {}, (m.text || "").length > 120 ? m.text.slice(0, 120) + "…" : m.text),
      h("td", { class: "small" }, (m.delivery && m.delivery.last_error) || ""),
    ))),
  ));
}

// ---------- render loop ----------

function render() {
  const app = document.getElementById("app");
  // Значения полей живут в state.forms; здесь сохраняем только фокус и
  // позицию курсора — по имени поля, а не по порядку элементов.
  const active = document.activeElement;
  const focusName = active && app.contains(active) && active.name ? active.name : null;
  let selection = null;
  try { selection = active && active.selectionStart !== undefined ? [active.selectionStart, active.selectionEnd] : null; } catch { selection = null; }

  app.replaceChildren();
  if (!state.user) {
    app.append(loginView());
    return;
  }
  app.append(topbar());
  const main = h("main", {});
  if (state.loadError && !state.overview) {
    main.append(h("div", { class: "card" }, "Cannot load cluster status: ", state.loadError));
  } else if (!state.overview) {
    main.append(h("p", { class: "hint" }, "Loading…"));
  } else {
    if (state.loadError) main.append(h("p", { class: "hint" }, "Refresh failed: ", state.loadError, " — showing the last known state."));
    main.append({ overview: overviewView, nodes: nodesView, bots: botsView }[state.tab]());
  }
  app.append(main);

  if (focusName) {
    const el = app.querySelector(`[name="${CSS.escape(focusName)}"]`);
    if (el) {
      el.focus();
      if (selection && el.setSelectionRange) { try { el.setSelectionRange(selection[0], selection[1]); } catch { /* select/password */ } }
    }
  }
}

async function start() {
  try {
    const r = await api("GET", "/api/session");
    state.user = r.username;
  } catch {
    state.user = null;
  }
  render();
  await refresh();
  setInterval(() => { if (!document.hidden && !state.busy) refresh(); }, REFRESH_MS);
}

start();

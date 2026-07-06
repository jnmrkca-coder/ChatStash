const state = {
  needsSetup: false,
  offset: 0,
  limit: 100,
  total: 0,
  rows: [],
  selected: new Set(),
  activeId: null,
};

const $ = (id) => document.getElementById(id);

async function api(path, options = {}) {
  const response = await fetch(path, {
    credentials: "same-origin",
    headers: { "Content-Type": "application/json", ...(options.headers || {}) },
    ...options,
  });
  if (!response.ok) {
    let err = {};
    try {
      err = await response.json();
    } catch {
      err = { error: response.statusText };
    }
    throw new Error(err.error || response.statusText);
  }
  return response.json();
}

function showAuth(needsSetup) {
  state.needsSetup = needsSetup;
  $("auth-view").classList.remove("hidden");
  $("app-view").classList.add("hidden");
  $("auth-copy").textContent = needsSetup
    ? "Create the first local admin account. There is no password recovery."
    : "Sign in to your local archive.";
  $("auth-form").reset();
  $("auth-username").value = "admin";
}

function showApp() {
  $("auth-view").classList.add("hidden");
  $("app-view").classList.remove("hidden");
}

function queryString() {
  const params = new URLSearchParams();
  const values = {
    q: $("q").value.trim(),
    sort: $("sort").value,
    model: $("model").value.trim(),
    mode: $("mode").value,
    project: $("project").value.trim(),
    tag: $("tag").value.trim(),
    rating: $("rating").value,
    date_from: $("date_from").value,
    date_to: $("date_to").value,
    limit: state.limit,
    offset: state.offset,
  };
  for (const [key, value] of Object.entries(values)) {
    if (value !== "" && value !== "0") params.set(key, value);
  }
  if ($("has_code").checked) params.set("has_code", "1");
  if ($("has_attachments").checked) params.set("has_attachments", "1");
  if ($("starred").checked) params.set("starred", "1");
  if ($("archived").checked) params.set("archived", "1");
  return params.toString();
}

function currentQueryWithoutPage() {
  const params = new URLSearchParams(queryString());
  params.delete("limit");
  params.delete("offset");
  return params.toString();
}

async function loadConversations() {
  const data = await api(`/api/conversations?${queryString()}`);
  state.rows = data.items;
  state.total = data.total;
  state.limit = data.limit;
  state.offset = data.offset;
  renderRows();
}

async function loadStats() {
  const data = await api("/api/stats");
  $("stats").innerHTML = `
    <dt>Conversations</dt><dd>${data.conversation_count.toLocaleString()}</dd>
    <dt>Messages</dt><dd>${data.message_count.toLocaleString()}</dd>
    <dt>Attachments</dt><dd>${data.attachment_count.toLocaleString()}</dd>
    <dt>Modes</dt><dd>${data.modes.map((m) => `${escapeHtml(m.mode || "unknown")} ${m.count}`).join(", ")}</dd>
  `;
}

async function loadJobs() {
  const data = await api("/api/jobs");
  $("jobs").innerHTML = data.items.slice(0, 6).map((job) => `
    <div class="job">
      <strong>${escapeHtml(job.kind)} / ${escapeHtml(job.status)}</strong>
      <span>${escapeHtml(job.message || job.error || "")}</span>
      <span>${job.done || 0}/${job.total || 0}</span>
    </div>
  `).join("") || `<div class="subline">No jobs yet.</div>`;
}

function renderRows() {
  const tbody = $("rows");
  tbody.innerHTML = state.rows.map((row) => {
    const tags = (row.tags || []).map((tag) => `<span class="pill">${escapeHtml(tag)}</span>`).join("");
    const selected = state.selected.has(row.id) ? "checked" : "";
    const active = state.activeId === row.id ? "active" : "";
    const date = row.updated_at ? row.updated_at.slice(0, 10) : "";
    return `
      <tr class="${active}" data-id="${row.id}">
        <td><input type="checkbox" data-select="${row.id}" ${selected}></td>
        <td class="title-cell">
          ${escapeHtml(row.display_title)}
          <div class="subline">${escapeHtml(row.id)}</div>
        </td>
        <td>${escapeHtml(date)}</td>
        <td>${escapeHtml(row.model || "")}</td>
        <td>${escapeHtml(row.mode || "")}</td>
        <td>${escapeHtml(row.display_project || row.project || "")}</td>
        <td><div class="pill-list">${tags}</div></td>
        <td><span class="stars">${row.rating || 0}/5</span></td>
        <td>${row.message_count} msg / ${row.code_block_count} code / ${row.attachment_count} files</td>
      </tr>
    `;
  }).join("");
  $("result-count").textContent = `${state.total.toLocaleString()} conversations`;
  $("selection-count").textContent = `${state.selected.size.toLocaleString()} selected`;
  $("bulk-count").textContent = state.selected.size.toLocaleString();
  $("select-page").checked = state.rows.length > 0 && state.rows.every((r) => state.selected.has(r.id));
}

async function openDetail(id) {
  state.activeId = id;
  renderRows();
  const item = await api(`/api/conversations/${encodeURIComponent(id)}`);
  $("empty-detail").classList.add("hidden");
  const tags = (item.tags || []).map((tag) => `<span class="pill">${escapeHtml(tag)}</span>`).join("");
  $("detail-content").classList.remove("hidden");
  $("detail-content").innerHTML = `
    <div class="detail-head">
      <h2>${escapeHtml(item.display_title)}</h2>
      <div class="pill-list">${tags}</div>
      <div class="subline">${escapeHtml(item.id)}</div>
    </div>
    <div class="meta-grid">
      <div><b>Created</b>${escapeHtml(item.created_at || "")}</div>
      <div><b>Updated</b>${escapeHtml(item.updated_at || "")}</div>
      <div><b>Model</b>${escapeHtml(item.model || "")}</div>
      <div><b>Mode</b>${escapeHtml(item.mode || "")}</div>
      <div><b>Project</b>${escapeHtml(item.display_project || item.project || "")}</div>
      <div><b>Source Project</b>${escapeHtml(item.source_project_id || "")}</div>
      <div><b>Rating</b>${item.rating || 0}/5</div>
      <div><b>Messages</b>${item.message_count}</div>
      <div><b>Files</b>${item.attachment_count}</div>
    </div>
    <pre class="conversation-text">${escapeHtml(item.text_content || "")}</pre>
  `;
}

async function bulk(action, extra = {}) {
  const ids = [...state.selected];
  if (!ids.length) return;
  const result = await api("/api/conversations/bulk", {
    method: "POST",
    body: JSON.stringify({ ids, action, ...extra }),
  });
  await loadConversations();
  await loadStats();
  flash(`${result.updated} updated`);
}

async function exportSelected() {
  const ids = [...state.selected];
  if (!ids.length) return;
  const response = await fetch("/api/export", {
    method: "POST",
    credentials: "same-origin",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      ids,
      format: $("export-format").value,
      filename_pattern: $("title-pattern").value || "{date} - {title}",
    }),
  });
  if (!response.ok) throw new Error("export_failed");
  const blob = await response.blob();
  const disposition = response.headers.get("Content-Disposition") || "";
  const match = disposition.match(/filename="([^"]+)"/);
  const filename = match ? match[1] : "chatstash-export";
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

async function loadSettings() {
  const [cfg, sourceProjects] = await Promise.all([api("/api/config"), api("/api/source-projects")]);
  $("library-paths").value = (cfg.library_paths || []).join("\n");
  $("watch-enabled").checked = !!cfg.watch_enabled;
  $("watch-interval").value = cfg.watch_interval_seconds || 60;
  $("managed-library-path").value = cfg.managed_library_path || "";
  $("copy-imports").checked = !!cfg.copy_imports_to_library;
  const aliases = cfg.project_aliases || {};
  $("source-projects").innerHTML = (sourceProjects.items || []).map((project) => `
    <label class="source-project-row">
      <span>
        <strong>${escapeHtml(project.label || project.id)}</strong>
        <small>${escapeHtml(project.id)} / ${project.count} conversations</small>
      </span>
      <input data-project-alias="${escapeHtml(project.id)}" value="${escapeHtml(aliases[project.id] || project.label || "")}" />
    </label>
  `).join("") || `<div class="subline">No ChatGPT project buckets found in indexed exports.</div>`;
}

async function saveSettings() {
  const projectAliases = {};
  for (const input of document.querySelectorAll("[data-project-alias]")) {
    if (input.value.trim()) projectAliases[input.dataset.projectAlias] = input.value.trim();
  }
  await api("/api/config", {
    method: "POST",
    body: JSON.stringify({
      library_paths: $("library-paths").value.split(/\n+/).map((v) => v.trim()).filter(Boolean),
      watch_enabled: $("watch-enabled").checked,
      watch_interval_seconds: Number($("watch-interval").value || 60),
      managed_library_path: $("managed-library-path").value.trim(),
      copy_imports_to_library: $("copy-imports").checked,
      project_aliases: projectAliases,
    }),
  });
  flash("Settings saved");
  await refreshAll();
}

function clearFilters() {
  for (const id of ["q", "model", "project", "tag", "date_from", "date_to"]) $(id).value = "";
  $("mode").value = "";
  $("rating").value = "0";
  $("has_code").checked = false;
  $("has_attachments").checked = false;
  $("starred").checked = false;
  $("archived").checked = false;
  state.offset = 0;
  loadConversations();
}

function flash(message) {
  $("selection-count").textContent = message;
  setTimeout(() => {
    $("selection-count").textContent = `${state.selected.size.toLocaleString()} selected`;
  }, 1400);
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function bindEvents() {
  $("auth-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    $("auth-error").textContent = "";
    try {
      await api(state.needsSetup ? "/api/setup" : "/api/login", {
        method: "POST",
        body: JSON.stringify({
          username: $("auth-username").value.trim() || "admin",
          password: $("auth-password").value,
        }),
      });
      if (state.needsSetup) {
        await api("/api/login", {
          method: "POST",
          body: JSON.stringify({
            username: $("auth-username").value.trim() || "admin",
            password: $("auth-password").value,
          }),
        });
      }
      await bootApp();
    } catch (error) {
      $("auth-error").textContent = error.message === "password_too_short"
        ? "Use at least 10 characters."
        : "Could not continue.";
    }
  });

  $("search-form").addEventListener("submit", (event) => {
    event.preventDefault();
    state.offset = 0;
    loadConversations();
  });

  for (const id of ["sort", "mode", "has_code", "has_attachments", "starred", "archived"]) {
    $(id).addEventListener("change", () => {
      state.offset = 0;
      loadConversations();
    });
  }

  $("clear-filters").addEventListener("click", clearFilters);
  $("prev-page").addEventListener("click", () => {
    state.offset = Math.max(0, state.offset - state.limit);
    loadConversations();
  });
  $("next-page").addEventListener("click", () => {
    if (state.offset + state.limit < state.total) {
      state.offset += state.limit;
      loadConversations();
    }
  });

  $("rows").addEventListener("click", (event) => {
    const checkbox = event.target.closest("input[type='checkbox']");
    if (checkbox) {
      const id = checkbox.dataset.select;
      if (checkbox.checked) state.selected.add(id);
      else state.selected.delete(id);
      renderRows();
      return;
    }
    const row = event.target.closest("tr[data-id]");
    if (row) openDetail(row.dataset.id);
  });

  $("select-page").addEventListener("change", (event) => {
    for (const row of state.rows) {
      if (event.target.checked) state.selected.add(row.id);
      else state.selected.delete(row.id);
    }
    renderRows();
  });

  $("scan-btn").addEventListener("click", async () => {
    const job = await api("/api/scan", { method: "POST", body: "{}" });
    flash(`Scan ${job.job_id} queued`);
    setTimeout(refreshAll, 1200);
  });

  $("settings-toggle").addEventListener("click", async () => {
    await loadSettings();
    $("settings").showModal();
  });
  $("save-settings").addEventListener("click", saveSettings);

  $("add-tags").addEventListener("click", () => bulk("add_tags", { tags: $("bulk-tags").value }));
  $("set-tags").addEventListener("click", () => bulk("set_tags", { tags: $("bulk-tags").value }));
  $("set-project").addEventListener("click", () => bulk("set_project", { project: $("bulk-project").value }));
  $("set-rating").addEventListener("click", () => bulk("set_rating", { rating: $("bulk-rating").value }));
  $("rename-title").addEventListener("click", () => bulk("set_title_pattern", { pattern: $("title-pattern").value }));
  $("export-btn").addEventListener("click", () => exportSelected().catch((error) => flash(error.message)));
}

async function refreshAll() {
  await Promise.all([loadConversations(), loadStats(), loadJobs()]);
}

async function bootApp() {
  showApp();
  await refreshAll();
  setInterval(loadJobs, 5000);
}

async function boot() {
  bindEvents();
  const status = await api("/api/setup/status");
  if (status.needs_setup) {
    showAuth(true);
    return;
  }
  try {
    await api("/api/me");
    await bootApp();
  } catch {
    showAuth(false);
  }
}

boot();

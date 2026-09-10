// S3 Explorer front end. Talks to the Rust side through two Tauri commands:
//   list_buckets()                      -> [{ name, region, created }]
//   list_dir({ bucket, prefix, token }) -> { bucket, prefix, folders, files, next_token }
// All DOM is built with textContent, never innerHTML, because S3 keys can
// contain arbitrary characters.

const { invoke } = window.__TAURI__.core;

const $ = (id) => document.getElementById(id);

const ICONS = {
  folder: '<path d="M1.5 3.5a1 1 0 0 1 1-1h3.6l1.5 1.6h5.9a1 1 0 0 1 1 1v7.4a1 1 0 0 1-1 1H2.5a1 1 0 0 1-1-1z"/>',
  file: '<path d="M3.5 1.5h6l3 3v10h-9z M9.5 1.5v3h3"/>',
  bucket: '<path d="M2.5 3.5h11l-1.3 10a1 1 0 0 1-1 .9H4.8a1 1 0 0 1-1-.9z"/><ellipse cx="8" cy="3.5" rx="5.5" ry="1.6"/>',
};

const state = {
  buckets: [],
  bucket: null,
  prefix: "",
  folders: [],
  files: [],
  nextToken: null,
  back: [],
  forward: [],
  sort: { key: "name", dir: 1 },
  selected: -1,
  rows: [], // what is currently rendered, in order
  seq: 0, // guards against out-of-order responses
};

// ---------- helpers ----------

function icon(kind) {
  const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
  svg.setAttribute("viewBox", "0 0 16 16");
  svg.classList.add("kind-icon", kind);
  svg.innerHTML = ICONS[kind]; // static markup, no user data
  return svg;
}

function el(tag, props = {}, ...children) {
  const node = document.createElement(tag);
  Object.assign(node, props);
  for (const c of children) node.append(c);
  return node;
}

function formatBytes(bytes) {
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  if (bytes < 1024) return `${bytes} B`;
  let value = bytes;
  let unit = 0;
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024;
    unit += 1;
  }
  return `${value.toFixed(value < 10 ? 2 : 1)} ${units[unit]}`;
}

function formatDate(rfc3339) {
  if (!rfc3339) return "";
  const d = new Date(rfc3339);
  return Number.isNaN(d.getTime()) ? rfc3339 : d.toLocaleString();
}

function setLoading(on) {
  document.body.classList.toggle("loading", on);
}

function showError(message) {
  const box = $("error");
  box.textContent = message;
  box.hidden = !message;
}

// ---------- data ----------

async function loadBuckets() {
  setLoading(true);
  $("status").textContent = "Loading buckets…";
  try {
    state.buckets = await invoke("list_buckets");
    showError("");
    renderBuckets();
    renderStatus();
  } catch (e) {
    showError(
      `Could not list buckets:\n${e}\n\n` +
        "You can still open a bucket you have access to with the “Open s3://bucket/path” box.",
    );
    $("status").textContent = "";
  } finally {
    setLoading(false);
  }
}

/** Go to a location; `record` pushes the current one onto the back stack. */
function navigate(bucket, prefix, { record = true } = {}) {
  if (record && state.bucket !== null) {
    state.back.push({ bucket: state.bucket, prefix: state.prefix });
    state.forward = [];
  }
  state.bucket = bucket;
  state.prefix = prefix;
  state.folders = [];
  state.files = [];
  state.nextToken = null;
  state.selected = -1;
  $("filter").value = "";
  renderBuckets();
  renderBreadcrumb();
  renderNav();
  return fetchPage(false);
}

async function fetchPage(append) {
  const seq = ++state.seq;
  const { bucket, prefix } = state;
  setLoading(true);
  $("status").textContent = append ? "Loading more…" : "Loading…";
  $("btn-more").hidden = true;
  if (!append) {
    $("listing").hidden = true;
    $("empty").hidden = true;
  }

  try {
    const page = await invoke("list_dir", {
      bucket,
      prefix,
      token: append ? state.nextToken : null,
    });
    if (seq !== state.seq) return; // user navigated away meanwhile
    showError("");
    state.folders = append ? state.folders.concat(page.folders) : page.folders;
    state.files = append ? state.files.concat(page.files) : page.files;
    state.nextToken = page.next_token;
    state.prefix = page.prefix;
    renderListing();
  } catch (e) {
    if (seq !== state.seq) return;
    showError(`Could not open s3://${bucket}/${prefix}\n${e}`);
    $("status").textContent = "";
  } finally {
    if (seq === state.seq) setLoading(false);
  }
}

// ---------- rendering ----------

function renderBuckets() {
  const query = $("bucket-filter").value.trim().toLowerCase();
  const list = $("bucket-list");
  list.replaceChildren();
  const shown = state.buckets.filter((b) => b.name.toLowerCase().includes(query));
  for (const b of shown) {
    const item = el(
      "li",
      { className: "bucket-item", title: b.name },
      icon("bucket"),
      el("span", { className: "name", textContent: b.name }),
      el("span", { className: "region", textContent: b.region ?? "" }),
    );
    if (b.name === state.bucket) item.classList.add("active");
    item.addEventListener("click", () => navigate(b.name, ""));
    list.append(item);
  }
  $("bucket-count").textContent = state.buckets.length ? String(state.buckets.length) : "";
}

function renderBreadcrumb() {
  const nav = $("breadcrumb");
  nav.replaceChildren();
  if (state.bucket === null) {
    nav.append(el("span", { className: "muted", textContent: "Select a bucket" }));
    return;
  }
  const crumb = (label, prefix) => {
    const b = el("button", { className: "crumb", textContent: label });
    b.addEventListener("click", () => {
      if (prefix !== state.prefix) navigate(state.bucket, prefix);
    });
    return b;
  };
  nav.append(crumb(`s3://${state.bucket}`, ""));
  const parts = state.prefix.split("/").slice(0, -1); // prefix ends with "/"
  let acc = "";
  for (const part of parts) {
    acc += `${part}/`;
    nav.append(el("span", { className: "crumb-sep", textContent: "›" }), crumb(part || "/", acc));
  }
  nav.scrollLeft = nav.scrollWidth;
}

function renderNav() {
  $("btn-back").disabled = state.back.length === 0;
  $("btn-forward").disabled = state.forward.length === 0;
  $("btn-up").disabled = state.bucket === null || state.prefix === "";
}

function sortedRows() {
  const query = $("filter").value.trim().toLowerCase();
  const match = (e) => e.name.toLowerCase().includes(query);
  const { key, dir } = state.sort;

  const folders = state.folders
    .filter(match)
    .map((f) => ({ kind: "folder", ...f }))
    .sort((a, b) => (key === "name" ? dir : 1) * a.name.localeCompare(b.name));

  const fileValue = {
    name: (f) => f.name,
    size: (f) => f.size,
    modified: (f) => f.last_modified ?? "",
    class: (f) => f.storage_class ?? "",
  }[key];
  const files = state.files
    .filter(match)
    .map((f) => ({ kind: "file", ...f }))
    .sort((a, b) => {
      const va = fileValue(a);
      const vb = fileValue(b);
      const c = typeof va === "number" ? va - vb : String(va).localeCompare(String(vb));
      return dir * (c || a.name.localeCompare(b.name));
    });

  // Explorer keeps folders on top regardless of sort column.
  return folders.concat(files);
}

function renderListing() {
  state.rows = sortedRows();
  const tbody = $("rows");
  tbody.replaceChildren();

  state.rows.forEach((row, i) => {
    const tr = el("tr");
    if (row.kind === "folder") {
      tr.append(
        el("td", { title: row.prefix }, el("div", { className: "entry" }, icon("folder"), el("span", { textContent: row.name }))),
        el("td", { className: "num muted", textContent: "—" }),
        el("td", { className: "muted", textContent: "" }),
        el("td", { className: "muted", textContent: "Folder" }),
      );
    } else {
      tr.append(
        el("td", { title: row.key }, el("div", { className: "entry" }, icon("file"), el("span", { textContent: row.name }))),
        el("td", { className: "num", textContent: formatBytes(row.size), title: `${row.size.toLocaleString()} bytes` }),
        el("td", { textContent: formatDate(row.last_modified), title: row.last_modified ?? "" }),
        el("td", { className: "muted", textContent: row.storage_class ?? "" }),
      );
    }
    tr.addEventListener("click", () => select(i));
    tr.addEventListener("dblclick", () => open(i));
    tbody.append(tr);
  });

  for (const th of document.querySelectorAll("th[data-sort]")) {
    const on = th.dataset.sort === state.sort.key;
    th.classList.toggle("sorted", on);
    th.classList.toggle("desc", on && state.sort.dir < 0);
  }

  const hasRows = state.rows.length > 0;
  $("listing").hidden = !hasRows;
  $("empty").hidden = hasRows;
  if (!hasRows) {
    const filtered = $("filter").value.trim() !== "";
    $("empty").textContent = filtered ? "No items match the filter." : "This folder is empty.";
  }
  state.selected = Math.min(state.selected, state.rows.length - 1);
  highlight();
  renderStatus();
}

function renderStatus() {
  if (state.bucket === null) {
    $("status").textContent = state.buckets.length ? `${state.buckets.length} bucket(s)` : "";
    $("btn-more").hidden = true;
    return;
  }
  const bytes = state.files.reduce((sum, f) => sum + f.size, 0);
  const more = state.nextToken ? " (more available)" : "";
  let text = `${state.folders.length} folder(s), ${state.files.length} file(s), ${formatBytes(bytes)}${more}`;
  if (state.rows.length !== state.folders.length + state.files.length) {
    text = `${state.rows.length} shown · ${text}`;
  }
  $("status").textContent = text;
  $("btn-more").hidden = !state.nextToken;
}

// ---------- interaction ----------

function highlight() {
  const trs = $("rows").children;
  for (let i = 0; i < trs.length; i++) trs[i].classList.toggle("selected", i === state.selected);
  trs[state.selected]?.scrollIntoView({ block: "nearest" });
}

function select(i) {
  state.selected = i;
  highlight();
}

function open(i) {
  const row = state.rows[i];
  if (row?.kind === "folder") navigate(state.bucket, row.prefix);
}

function goUp() {
  if (state.bucket === null || state.prefix === "") return;
  const parts = state.prefix.split("/").slice(0, -2);
  navigate(state.bucket, parts.length ? `${parts.join("/")}/` : "");
}

function goBack() {
  const loc = state.back.pop();
  if (!loc) return;
  state.forward.push({ bucket: state.bucket, prefix: state.prefix });
  navigate(loc.bucket, loc.prefix, { record: false });
}

function goForward() {
  const loc = state.forward.pop();
  if (!loc) return;
  state.back.push({ bucket: state.bucket, prefix: state.prefix });
  navigate(loc.bucket, loc.prefix, { record: false });
}

/** Open "s3://bucket/some/prefix" or "bucket/some/prefix" typed by the user. */
function openPath(text) {
  const path = text.trim().replace(/^s3:\/\//i, "");
  if (!path) return;
  const slash = path.indexOf("/");
  const bucket = slash < 0 ? path : path.slice(0, slash);
  const prefix = slash < 0 ? "" : path.slice(slash + 1); // backend appends a trailing "/"
  if (!bucket) return;
  // Buckets opened by name stay in the sidebar for this session.
  if (!state.buckets.some((b) => b.name === bucket)) {
    state.buckets.push({ name: bucket, region: null, created: null });
    state.buckets.sort((a, b) => a.name.localeCompare(b.name));
  }
  navigate(bucket, prefix);
}

function refresh() {
  if (state.bucket === null) loadBuckets();
  else fetchPage(false);
}

$("btn-back").addEventListener("click", goBack);
$("btn-forward").addEventListener("click", goForward);
$("btn-up").addEventListener("click", goUp);
$("btn-refresh").addEventListener("click", refresh);
$("btn-more").addEventListener("click", () => fetchPage(true));
$("filter").addEventListener("input", renderListing);
$("bucket-filter").addEventListener("input", renderBuckets);
$("open-form").addEventListener("submit", (e) => {
  e.preventDefault();
  openPath($("open-path").value);
  $("open-path").blur();
});

for (const th of document.querySelectorAll("th[data-sort]")) {
  th.addEventListener("click", () => {
    const key = th.dataset.sort;
    state.sort = { key, dir: state.sort.key === key ? -state.sort.dir : 1 };
    renderListing();
  });
}

document.addEventListener("keydown", (e) => {
  const typing = e.target instanceof HTMLInputElement;
  if (e.key === "F5" || ((e.metaKey || e.ctrlKey) && e.key === "r")) {
    e.preventDefault();
    refresh();
  } else if (e.altKey && e.key === "ArrowLeft") {
    goBack();
  } else if (e.altKey && e.key === "ArrowRight") {
    goForward();
  } else if (e.altKey && e.key === "ArrowUp") {
    goUp();
  } else if (typing) {
    if (e.key === "Escape") e.target.blur();
  } else if (e.key === "Backspace") {
    goUp();
  } else if (e.key === "ArrowDown" && state.rows.length) {
    e.preventDefault();
    select(Math.min(state.selected + 1, state.rows.length - 1));
  } else if (e.key === "ArrowUp" && state.rows.length) {
    e.preventDefault();
    select(Math.max(state.selected - 1, 0));
  } else if (e.key === "Enter") {
    open(state.selected);
  }
});

renderBreadcrumb();
renderNav();
loadBuckets();

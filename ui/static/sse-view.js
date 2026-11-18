const params = new URLSearchParams(window.location.search);
const jobId = params.get("job");

const jobIdLabel = document.getElementById("job-id-label");
const rawStreamLink = document.getElementById("raw-stream-link");
const streamStatus = document.getElementById("stream-status");
const jobStatus = document.getElementById("job-status");
const jobStage = document.getElementById("job-stage");
const jobVersion = document.getElementById("job-version");
const variantGrid = document.getElementById("variant-grid");
const rawLog = document.getElementById("raw-log");

function variantLabel(name) {
  return ({ thumbnail: "Thumbnail", preview: "Preview", display: "Display" })[name] || name;
}

function renderVariants(variants) {
  const expected = ["thumbnail", "preview", "display"];
  const byName = new Map((variants || []).map((variant) => [variant.name, variant]));

  variantGrid.replaceChildren(...expected.map((name) => {
    const variant = byName.get(name);
    const card = document.createElement("article");
    card.className = "variant-card";

    if (variant) {
      const mat = document.createElement("a");
      mat.className = "variant-mat";
      mat.href = variant.url;
      mat.target = "_blank";
      mat.rel = "noopener";
      mat.setAttribute("aria-label", `Open ${variantLabel(name)} variant`);

      const image = document.createElement("img");
      image.src = variant.url;
      image.alt = `${variantLabel(name)} variant`;
      mat.appendChild(image);
      card.appendChild(mat);
    } else {
      const mat = document.createElement("div");
      mat.className = "variant-mat variant-mat--pending";
      mat.textContent = "Pending";
      card.appendChild(mat);
    }

    const info = document.createElement("div");
    info.className = "variant-info";
    const head = document.createElement("div");
    head.className = "variant-head";

    const title = document.createElement("h3");
    title.className = "variant-name";
    title.textContent = variantLabel(name);

    const chip = document.createElement("span");
    chip.className = `chip ${variant ? "chip--ready" : "chip--pending"}`;
    chip.textContent = variant ? "Ready" : "Pending";

    head.append(title, chip);
    info.appendChild(head);

    const dims = document.createElement("p");
    dims.className = "variant-dims";
    dims.textContent = variant ? `${variant.width} × ${variant.height}` : "Waiting for variant";
    info.appendChild(dims);

    if (variant) {
      const actions = document.createElement("div");
      actions.className = "variant-actions";

      const view = document.createElement("a");
      view.className = "btn btn--ghost btn--sm";
      view.href = variant.url;
      view.target = "_blank";
      view.rel = "noopener";
      view.textContent = "View";

      const download = document.createElement("a");
      download.className = "btn btn--primary btn--sm";
      download.href = variant.url;
      download.download = `${name}.jpg`;
      download.textContent = "Download";

      actions.append(view, download);
      info.appendChild(actions);
    }

    card.appendChild(info);
    return card;
  }));
}

function appendRaw(eventName, raw, data) {
  const entry = {
    received_at: new Date().toISOString(),
    event: eventName,
    raw,
    parsed: data,
  };
  rawLog.textContent = `${JSON.stringify(entry, null, 2)}\n\n${rawLog.textContent}`;
}

if (!jobId) {
  streamStatus.textContent = "Missing job ID";
  streamStatus.dataset.state = "error";
  jobIdLabel.textContent = "not provided";
} else {
  jobIdLabel.textContent = `#${jobId.slice(0, 8)}`;
  rawStreamLink.href = `/v1/jobs/${encodeURIComponent(jobId)}/events`;

  const source = new EventSource(`/v1/jobs/${encodeURIComponent(jobId)}/events`);

  source.onopen = () => {
    streamStatus.textContent = "Connected · receiving live snapshots";
    streamStatus.dataset.state = "connected";
  };

  source.addEventListener("job-update", (event) => {
    let data;
    try { data = JSON.parse(event.data); } catch { data = null; }

    appendRaw("job-update", event.data, data);
    if (!data) return;

    jobStatus.textContent = data.status || "-";
    jobStage.textContent = data.stage || "-";
    jobVersion.textContent = data.version != null ? String(data.version) : "-";
    renderVariants(data.variants || []);

    if (data.status === "completed" || data.status === "failed") {
      streamStatus.textContent = data.status === "completed" ? "Completed" : "Processing failed";
      source.close();
      streamStatus.dataset.state = data.status === "completed" ? "connected" : "error";
    }
  });

  source.onerror = () => {
    if (source.readyState === EventSource.CLOSED) {
      streamStatus.textContent = "Stream closed";
      streamStatus.dataset.state = "error";
    } else {
      streamStatus.textContent = "Connection interrupted · reconnecting";
      streamStatus.dataset.state = "error";
    }
  };
}

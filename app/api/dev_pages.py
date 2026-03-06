from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import HTMLResponse

router = APIRouter(tags=["dev-pages"])


@router.get("/dev/admin", response_class=HTMLResponse)
async def admin_landing_page() -> str:
    return """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Admin Landing</title>
  <style>
    body { font-family: "Segoe UI", sans-serif; margin: 24px; max-width: 900px; }
    .card { border: 1px solid #ddd; border-radius: 8px; padding: 14px; margin-bottom: 12px; }
    a { text-decoration: none; color: #0b57d0; }
  </style>
</head>
<body>
  <h1>Admin Landing (Dev)</h1>
  <div class="card">
    <h3>Form Templates</h3>
    <div><a href="/dev/admin-form-templates">Open Form Template Manager</a></div>
    <div style="margin-top:6px;">Create/import/export template JSON, view versions, and save new versions.</div>
  </div>
  <div class="card">
    <h3>Active Review Requests</h3>
    <div><a href="/dev/active-forms-admin">Open Active Forms Admin</a></div>
    <div style="margin-top:6px;">View in-flight requests and reassign template/version.</div>
  </div>
  <div class="card">
    <h3>Reviewer Test Form</h3>
    <div><a href="/dev/review-form">Open Reviewer Form Test Page</a></div>
    <div style="margin-top:6px;">Preview reviewer rendering and validate/submit payloads.</div>
  </div>
</body>
</html>
"""


@router.get("/dev/admin-form-templates", response_class=HTMLResponse)
async def admin_form_templates_page() -> str:
    return """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Admin Form Template Editor</title>
  <style>
    body { font-family: "Segoe UI", sans-serif; margin: 24px; max-width: 1100px; }
    textarea { width: 100%; min-height: 380px; font-family: Consolas, monospace; }
    input, button { margin-right: 8px; margin-bottom: 8px; }
    .row { display: flex; gap: 12px; flex-wrap: wrap; }
    .card { border: 1px solid #ddd; padding: 12px; border-radius: 6px; }
    pre { background: #f8f8f8; padding: 12px; white-space: pre-wrap; }
  </style>
</head>
<body>
  <div style="margin-bottom:12px;">
    <a href="/dev/admin">Admin Home</a> |
    <a href="/dev/active-forms-admin">Active Forms</a>
  </div>
  <h1>Admin Form Template Editor (Dev)</h1>
  <div class="row">
    <div class="card">
      <div><label>Template Key <input id="templateKey" value="qc_subconsultant_review" /></label></div>
      <div>
        <label>Version
          <select id="versionSelect">
            <option value="1">v1</option>
          </select>
        </label>
      </div>
      <button onclick="loadVersion()">Load Version</button>
      <button onclick="listVersions()">List Versions</button>
      <button onclick="saveVersion()">Save New Version</button>
      <button onclick="importTemplate()">Import JSON</button>
      <button onclick="exportTemplate()">Export JSON</button>
    </div>
  </div>
  <textarea id="jsonArea"></textarea>
  <h3>Result</h3>
  <pre id="result"></pre>
  <script>
    const starter = {
      schema_json: {
        schema_version: "1.0",
        template_key: "qc_subconsultant_review",
        display_name: "QC Subconsultant Review Form",
        branding: { org_name: "Gresham Smith", logo_url: "/static/images/logo.png" },
        auto_fields: ["project_name","project_number","owner_end_user","submittal_name","submittal_date","reviewer_name"],
        discipline_repeat: {
          source: "review_request_discipline",
          label_field: "discipline_name",
          items: [
            {
              section_key: "off_team_qualified_review",
              section_label: "Off-Team Qualified Review",
              choice: { type: "single_select", options: ["complete", "na"], required: true },
              signature: {
                required_when_choice_selected: true,
                type_name_must_match_reviewer: true,
                match_mode: "case_insensitive_exact",
                capture_timestamp: true
              },
              notes: { type: "text", required: false, max_length: 4000 }
            },
            {
              section_key: "constructability_review",
              section_label: "Constructability Review",
              choice: { type: "single_select", options: ["complete", "na"], required: true },
              signature: {
                required_when_choice_selected: true,
                type_name_must_match_reviewer: true,
                match_mode: "case_insensitive_exact",
                capture_timestamp: true
              },
              notes: { type: "text", required: false, max_length: 4000 }
            }
          ]
        }
      }
    };
    const result = document.getElementById("result");
    const area = document.getElementById("jsonArea");
    const versionSelect = document.getElementById("versionSelect");
    area.value = JSON.stringify(starter, null, 2);
    function show(value) {
      result.textContent = typeof value === "string" ? value : JSON.stringify(value, null, 2);
    }
    async function loadVersion(versionOverride = null) {
      const key = document.getElementById("templateKey").value.trim();
      const version = versionOverride ?? versionSelect.value;
      versionSelect.value = String(version);
      const response = await fetch(`/admin/form-templates/${encodeURIComponent(key)}/versions/${version}`);
      const body = await response.json();
      if (!response.ok) return show(body);
      area.value = JSON.stringify({ schema_json: body.version.schema_json }, null, 2);
      show(body);
    }
    async function listVersions() {
      const key = document.getElementById("templateKey").value.trim();
      const response = await fetch(`/admin/form-templates/${encodeURIComponent(key)}/versions`);
      const body = await response.json();
      if (!response.ok) {
        versionSelect.innerHTML = '<option value="1">v1</option>';
        return show(body);
      }
      if (!Array.isArray(body) || body.length === 0) {
        versionSelect.innerHTML = '<option value="">No versions found</option>';
        return;
      }
      body.sort((a, b) => {
        const da = new Date(a.created_at).getTime();
        const db = new Date(b.created_at).getTime();
        if (db !== da) return db - da;
        return Number(b.version) - Number(a.version);
      });
      const selectedVersion = versionSelect.value;
      versionSelect.innerHTML = body.map(v => {
        const activeLabel = v.is_active ? " (active)" : "";
        return `<option value="${v.version}">v${v.version}${activeLabel}</option>`;
      }).join("");
      const found = body.some(v => String(v.version) === String(selectedVersion));
      if (found) {
        versionSelect.value = selectedVersion;
      }
    }
    async function saveVersion() {
      const key = document.getElementById("templateKey").value.trim();
      const payload = JSON.parse(area.value);
      const response = await fetch(`/admin/form-templates/${encodeURIComponent(key)}/versions`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });
      const body = await response.json();
      show(body);
      if (response.ok && body?.version?.version) {
        await listVersions();
        versionSelect.value = String(body.version.version);
      }
    }
    async function importTemplate() {
      const payload = JSON.parse(area.value);
      const response = await fetch("/admin/form-templates/import", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });
      const body = await response.json();
      show(body);
      if (response.ok && body?.template?.template_key && body?.version?.version) {
        document.getElementById("templateKey").value = body.template.template_key;
        await listVersions();
        versionSelect.value = String(body.version.version);
      }
    }
    async function exportTemplate() {
      const key = document.getElementById("templateKey").value.trim();
      const version = versionSelect.value;
      const response = await fetch(`/admin/form-templates/${encodeURIComponent(key)}/versions/${version}/export`);
      const body = await response.json();
      show(body);
      area.value = JSON.stringify(body, null, 2);
    }
    listVersions();
  </script>
</body>
</html>
"""


@router.get("/dev/review-form", response_class=HTMLResponse)
async def review_form_page() -> str:
    return """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Reviewer Form Test</title>
  <style>
    body { font-family: "Segoe UI", sans-serif; margin: 24px; max-width: 1200px; }
    .card { border: 1px solid #ddd; border-radius: 6px; margin: 12px 0; padding: 12px; }
    .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; }
    textarea { width: 100%; min-height: 80px; }
    .muted { color: #666; font-size: 0.9rem; }
    pre { background: #f8f8f8; padding: 12px; white-space: pre-wrap; }
    .error-banner { border: 1px solid #b42318; background: #fef3f2; color: #7a271a; padding: 10px 12px; border-radius: 6px; margin: 12px 0; display: none; }
    .error-banner h3 { margin: 0 0 6px 0; font-size: 1rem; }
    .error-banner ul { margin: 0; padding-left: 18px; }
    .error-section { border-color: #b42318; box-shadow: 0 0 0 1px #b42318 inset; }
    .info-banner { border: 1px solid #0f766e; background: #ecfdf5; color: #115e59; padding: 10px 12px; border-radius: 6px; margin: 12px 0; display: none; }
  </style>
</head>
<body>
  <h1>Reviewer Form (Dev)</h1>
  <label>Review Request ID <input id="reviewRequestId" size="42" /></label>
  <button onclick="loadForm()">Load Form</button>
  <button onclick="validateForm()">Validate</button>
  <button onclick="submitForm()">Submit</button>
  <div id="validationBanner" class="error-banner"></div>
  <div id="infoBanner" class="info-banner"></div>
  <div id="brandHeader" class="card"></div>
  <div id="autoFields" class="card"></div>
  <div id="disciplineBlocks"></div>
  <h3>Result</h3>
  <pre id="result"></pre>
  <script>
    let context = null;
    let sectionLabelToKey = new Map();
    function show(value) {
      document.getElementById("result").textContent = typeof value === "string" ? value : JSON.stringify(value, null, 2);
    }
    function clearValidationUi() {
      document.getElementById("validationBanner").style.display = "none";
      document.getElementById("validationBanner").innerHTML = "";
      document.getElementById("infoBanner").style.display = "none";
      document.getElementById("infoBanner").innerHTML = "";
      document.querySelectorAll(".error-section").forEach(el => el.classList.remove("error-section"));
    }
    function showInfo(message) {
      const banner = document.getElementById("infoBanner");
      banner.textContent = message;
      banner.style.display = "block";
    }
    function setFormDisabled(disabled) {
      const selector = "input, textarea, button";
      document.querySelectorAll(selector).forEach(el => {
        if (el.id === "reviewRequestId") return;
        if (el.getAttribute("onclick") === "loadForm()") return;
        el.disabled = disabled;
      });
    }
    function renderValidationErrors(errors) {
      if (!Array.isArray(errors) || errors.length === 0) return;
      const banner = document.getElementById("validationBanner");
      const items = errors.map(err => `<li>${err}</li>`).join("");
      banner.innerHTML = `<h3>Validation failed</h3><ul>${items}</ul>`;
      banner.style.display = "block";

      const pattern = /Section '([^']+)' for discipline '([^']+)'/;
      errors.forEach(err => {
        const match = err.match(pattern);
        if (!match) return;
        const sectionLabel = match[1];
        const disciplineName = match[2];
        const sectionKey = sectionLabelToKey.get(sectionLabel) || sectionLabel;
        const card = [...document.querySelectorAll("[data-discipline-id]")]
          .find(item => item.getAttribute("data-discipline-name") === disciplineName);
        if (!card) return;
        const section = card.querySelector(`[data-section="${sectionKey}"]`);
        if (section) section.classList.add("error-section");
      });
    }
    async function loadForm() {
      clearValidationUi();
      const id = document.getElementById("reviewRequestId").value.trim();
      const response = await fetch(`/review-forms/${id}`);
      const body = await response.json();
      if (!response.ok) return show(body);
      context = body;
      sectionLabelToKey = new Map(
        (body.schema_json?.discipline_repeat?.items || []).map(item => [item.section_label, item.section_key])
      );
      const brand = body.schema_json.branding || {};
      const brandDiv = document.getElementById("brandHeader");
      const logoHtml = brand.logo_url ? `<img src="${brand.logo_url}" alt="logo" style="display:block;margin-bottom:8px;" />` : "";
      brandDiv.innerHTML = `${logoHtml}<div><strong>${brand.org_name || ""}</strong></div>`;
      const autoDiv = document.getElementById("autoFields");
      autoDiv.innerHTML = "<h3>Auto Fields</h3>" + Object.entries(body.auto_values).map(([k,v]) => `<div><strong>${k}</strong>: ${v ?? ""}</div>`).join("");
      const sections = body.schema_json.discipline_repeat.items;
      const blocks = body.disciplines.map((d, idx) => `
        <div class="card" data-discipline-id="${d.discipline_id}" data-discipline-name="${d.discipline_name}">
          <h3>${d.discipline_name}</h3>
          ${sections.map(s => `
            <div class="card" data-section="${s.section_key}">
              <div><strong>${s.section_label}</strong></div>
              <label><input type="radio" name="${idx}-${s.section_key}" value="complete"> Complete</label>
              <label><input type="radio" name="${idx}-${s.section_key}" value="na"> N/A</label>
              <div><label>Signature Name <input data-field="signature_name" /></label></div>
              <div class="muted">Must match reviewer name: ${body.reviewer_name}</div>
              <div><label>Signed At <input data-field="signed_at" readonly /></label> <button type="button" onclick="stamp(this)">Stamp</button></div>
              <div><label>Notes</label><textarea data-field="notes"></textarea></div>
            </div>
          `).join("")}
        </div>
      `).join("");
      document.getElementById("disciplineBlocks").innerHTML = blocks;
      setFormDisabled(false);
      if (body.status === "submitted") {
        showInfo("This review request has already been submitted and is locked.");
        setFormDisabled(true);
      }
      show(body);
    }
    function stamp(button) {
      const section = button.closest("[data-section]");
      const field = section.querySelector("[data-field='signed_at']");
      field.value = new Date().toISOString();
    }
    function collectPayload() {
      const disciplineCards = [...document.querySelectorAll("[data-discipline-id]")];
      return {
        review_request_id: context.review_request_id,
        reviewer_name_expected: context.reviewer_name,
        discipline_responses: disciplineCards.map(card => {
          const sections = {};
          [...card.querySelectorAll("[data-section]")].forEach(section => {
            const sectionKey = section.getAttribute("data-section");
            const checked = section.querySelector("input[type='radio']:checked");
            const signatureRaw = section.querySelector("[data-field='signature_name']").value;
            const signedAtRaw = section.querySelector("[data-field='signed_at']").value;
            const signatureName = signatureRaw.trim() ? signatureRaw : null;
            const signedAt = signedAtRaw.trim() ? signedAtRaw : null;
            sections[sectionKey] = {
              status: checked ? checked.value : null,
              signature_name: signatureName,
              signed_at: signedAt,
              notes: section.querySelector("[data-field='notes']").value
            };
          });
          return {
            discipline_id: card.getAttribute("data-discipline-id"),
            discipline_name: card.getAttribute("data-discipline-name"),
            sections
          };
        })
      };
    }
    async function validateForm() {
      clearValidationUi();
      if (context && context.status === "submitted") {
        showInfo("This review request has already been submitted and cannot be validated.");
        return;
      }
      const id = document.getElementById("reviewRequestId").value.trim();
      const response = await fetch(`/review-forms/${id}/validate`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(collectPayload())
      });
      const body = await response.json();
      if (!response.ok) {
        show(body);
        return;
      }
      show(body);
      if (body && body.valid === false) {
        renderValidationErrors(body.errors);
      }
    }
    async function submitForm() {
      clearValidationUi();
      if (context && context.status === "submitted") {
        showInfo("This review request has already been submitted and cannot be updated.");
        return;
      }
      const id = document.getElementById("reviewRequestId").value.trim();
      const response = await fetch(`/review-forms/${id}/submit`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(collectPayload())
      });
      const body = await response.json();
      show(body);
      if (!response.ok && body && Array.isArray(body.detail)) {
        renderValidationErrors(body.detail);
      } else if (!response.ok && response.status === 409) {
        showInfo(body?.detail || "This review request has already been submitted.");
      }
    }
  </script>
</body>
</html>
"""


@router.get("/dev/active-forms-admin", response_class=HTMLResponse)
async def active_forms_admin_page() -> str:
    return """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Active Review Requests Admin</title>
  <style>
    body { font-family: "Segoe UI", sans-serif; margin: 24px; max-width: 1400px; }
    table { border-collapse: collapse; width: 100%; }
    th, td { border: 1px solid #ddd; padding: 8px; vertical-align: top; font-size: 0.9rem; }
    th { background: #f5f5f5; text-align: left; }
    input, select, button { margin-right: 6px; margin-bottom: 4px; }
    .toolbar { margin-bottom: 12px; }
    pre { background: #f8f8f8; padding: 12px; white-space: pre-wrap; }
  </style>
</head>
<body>
  <div style="margin-bottom:12px;">
    <a href="/dev/admin">Admin Home</a> |
    <a href="/dev/admin-form-templates">Template Manager</a>
  </div>
  <h1>Active Review Requests (Dev Admin)</h1>
  <div class="toolbar">
    <button onclick="loadActiveRequests()">Refresh Active Requests</button>
  </div>
  <div id="tableWrap"></div>
  <h3>Result</h3>
  <pre id="result"></pre>
  <script>
    const result = document.getElementById("result");
    function show(value) {
      result.textContent = typeof value === "string" ? value : JSON.stringify(value, null, 2);
    }
    async function getVersions(templateKey) {
      const response = await fetch(`/admin/form-templates/${encodeURIComponent(templateKey)}/versions`);
      const body = await response.json();
      if (!response.ok || !Array.isArray(body)) return [];
      return body.sort((a, b) => Number(b.version) - Number(a.version));
    }
    async function loadActiveRequests() {
      const response = await fetch("/admin/review-requests/active");
      const body = await response.json();
      if (!response.ok) {
        document.getElementById("tableWrap").innerHTML = "";
        return show(body);
      }
      if (!Array.isArray(body) || body.length === 0) {
        document.getElementById("tableWrap").innerHTML = "<div>No active review requests found.</div>";
        return;
      }
      const rows = await Promise.all(body.map(async (item) => {
        const versions = await getVersions(item.template_key);
        const versionOptions = versions.map(v => {
          const selected = Number(v.version) === Number(item.expected_form_version) ? "selected" : "";
          const activeLabel = v.is_active ? " (active)" : "";
          return `<option value="${v.version}" ${selected}>v${v.version}${activeLabel}</option>`;
        }).join("") || `<option value="${item.expected_form_version}">v${item.expected_form_version}</option>`;
        return `
          <tr data-request-id="${item.review_request_id}">
            <td>${item.review_request_id}</td>
            <td>${item.status}</td>
            <td>${item.reviewer_name || ""}<br>${item.reviewer_email || ""}</td>
            <td>${item.project_number || ""}<br>${item.project_name || ""}</td>
            <td>${item.submittal_name || ""}<br>${item.submittal_date || ""}</td>
            <td>${item.template_key}<br>current: v${item.expected_form_version}<br>active: ${item.active_template_version ? "v" + item.active_template_version : "-"}</td>
            <td>${item.discipline_count}</td>
            <td>${item.updated_at}</td>
            <td>
              <div><input data-role="template-key" value="${item.template_key}" /></div>
              <div><button type="button" onclick="reloadVersions(this)">Load Versions</button></div>
              <div><select data-role="version-select">${versionOptions}</select></div>
              <div><button type="button" onclick="reassignTemplate(this)">Reassign</button></div>
            </td>
          </tr>
        `;
      }));
      document.getElementById("tableWrap").innerHTML = `
        <table>
          <thead>
            <tr>
              <th>Review Request ID</th>
              <th>Status</th>
              <th>Reviewer</th>
              <th>Project</th>
              <th>Submittal</th>
              <th>Template</th>
              <th>Disciplines</th>
              <th>Updated</th>
              <th>Reassign</th>
            </tr>
          </thead>
          <tbody>${rows.join("")}</tbody>
        </table>
      `;
      show(body);
    }
    async function reloadVersions(button) {
      const row = button.closest("tr");
      const key = row.querySelector("[data-role='template-key']").value.trim();
      const select = row.querySelector("[data-role='version-select']");
      const versions = await getVersions(key);
      if (!versions.length) {
        select.innerHTML = '<option value="">No versions found</option>';
        return;
      }
      select.innerHTML = versions.map(v => {
        const activeLabel = v.is_active ? " (active)" : "";
        return `<option value="${v.version}">v${v.version}${activeLabel}</option>`;
      }).join("");
    }
    async function reassignTemplate(button) {
      const row = button.closest("tr");
      const reviewRequestId = row.getAttribute("data-request-id");
      const templateKey = row.querySelector("[data-role='template-key']").value.trim();
      const version = Number(row.querySelector("[data-role='version-select']").value);
      const response = await fetch(`/admin/review-requests/${reviewRequestId}/reassign-template`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ template_key: templateKey, version })
      });
      const body = await response.json();
      show(body);
      if (response.ok) {
        await loadActiveRequests();
      }
    }
    loadActiveRequests();
  </script>
</body>
</html>
"""

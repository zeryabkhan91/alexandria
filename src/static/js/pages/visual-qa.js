window.Pages = window.Pages || {};

function esc(value) {
  return String(value || '').replace(/[&<>"]/g, (char) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[char]));
}

const VisualQAState = {
  rows: [],
  summary: { total: 0, passed: 0, failed: 0, not_compared: 0, generated: 0 },
  showFailuresOnly: false,
  loading: false,
  generatedAt: '',
  message: '',
  requestedBook: 0,
  autoOpenBook: false,
};

function visualQaRouteState() {
  const hash = String(window.location.hash || '');
  const queryString = hash.includes('?') ? hash.split('?').slice(1).join('?') : '';
  const params = new URLSearchParams(queryString);
  const bookRaw = Number(params.get('book') || 0);
  const openToken = String(params.get('open') || '').trim().toLowerCase();
  return {
    book: Number.isFinite(bookRaw) && bookRaw > 0 ? Math.trunc(bookRaw) : 0,
    autoOpen: ['1', 'true', 'yes', 'on'].includes(openToken),
  };
}

function syncVisualQaRouteState() {
  const route = visualQaRouteState();
  VisualQAState.requestedBook = route.book;
  VisualQAState.autoOpenBook = route.autoOpen;
  return route;
}

function rowVerdict(row) {
  if (row && row.structural_passed === true) return 'PASS';
  if (row && row.structural_passed === false) return 'FAIL';
  return String(row?.verdict || 'UNKNOWN').toUpperCase();
}

function filteredRows() {
  if (!VisualQAState.showFailuresOnly) return VisualQAState.rows;
  return VisualQAState.rows.filter((row) => rowVerdict(row) === 'FAIL');
}

function verdictTagClass(verdict) {
  const token = String(verdict || '').toUpperCase();
  if (token === 'PASS') return 'tag-success';
  if (token === 'FAIL') return 'tag-failed';
  return 'tag-status';
}

function renderCards() {
  const container = document.getElementById('visualQaCards');
  if (!container) return;
  const rows = filteredRows();
  if (!rows.length) {
    const fallback = VisualQAState.message || 'No comparison grids available yet.';
    container.innerHTML = `<div class="text-muted">${esc(fallback)}</div>`;
    return;
  }
  container.innerHTML = rows.map((row) => {
    const book = Number(row.book_number || 0);
    const title = row.book_title || `Book ${book}`;
    const verdict = rowVerdict(row);
    const metrics = `Frame changed: ${Number(row.frame_changed_pct || 0).toFixed(2)}% · Mean delta: ${Number(row.frame_mean_delta || 0).toFixed(2)}`;
    const failedChecks = Array.isArray(row.structural_failed_checks) ? row.structural_failed_checks : [];
    const structuralText = row.structural_passed === null || row.structural_passed === undefined
      ? 'Structural QA: not yet available'
      : row.structural_passed
        ? 'Structural QA: PASS'
        : `Structural QA: FAIL (${failedChecks.join(', ') || 'see report'})`;
    const imageUrl = `${String(row.image_url || '').trim()}?ts=${Date.now()}`;
    return `
      <div class="card visual-qa-card" data-book="${book}">
        <div class="card-header">
          <h3 class="card-title">Book ${book}: ${esc(title)}</h3>
          <span class="tag ${verdictTagClass(verdict)}">${esc(verdict)}</span>
        </div>
        <div class="text-sm text-muted mb-12">${esc(metrics)}</div>
        <div class="text-sm text-muted mb-12">${esc(structuralText)}</div>
        ${row.has_image ? `<img class="visual-qa-thumb" src="${esc(imageUrl)}" alt="${esc(title)}" />` : '<div class="thumb-fallback">Comparison image missing</div>'}
      </div>
    `;
  }).join('');

  container.querySelectorAll('.visual-qa-card[data-book]').forEach((card) => {
    card.addEventListener('click', () => {
      const book = Number(card.dataset.book || 0);
      const row = VisualQAState.rows.find((item) => Number(item.book_number || 0) === book);
      if (!row) return;
      openLightbox(row);
    });
  });
}

function renderSummary() {
  const badge = document.getElementById('visualQaSummary');
  if (!badge) return;
  const summary = VisualQAState.summary || {};
  badge.innerHTML = `
    <span class="tag tag-success">PASS ${Number(summary.passed || 0)}</span>
    <span class="tag tag-failed">FAIL ${Number(summary.failed || 0)}</span>
    <span class="tag tag-status">TOTAL ${Number(summary.total || 0)}</span>
    <span class="tag tag-status">NOT COMPARED ${Number(summary.not_compared || 0)}</span>
    <span class="tag tag-status">STRUCT PASS ${Number(summary.structural_passed || 0)}</span>
    <span class="tag tag-status">STRUCT FAIL ${Number(summary.structural_failed || 0)}</span>
  `;
  const stamp = document.getElementById('visualQaGeneratedAt');
  if (stamp) stamp.textContent = VisualQAState.generatedAt ? `Updated ${new Date(VisualQAState.generatedAt).toLocaleString()}` : 'Not generated yet';
}

async function loadVisualQa({ force = false } = {}) {
  if (VisualQAState.loading) return;
  VisualQAState.loading = true;
  const route = syncVisualQaRouteState();
  const status = document.getElementById('visualQaStatus');
  if (status) status.textContent = force ? 'Generating comparison grids...' : 'Loading comparison grids...';
  try {
    const params = new URLSearchParams({ catalog: 'classics' });
    if (force) params.set('force', '1');
    if (route.book > 0) params.set('book_number', String(route.book));
    const response = await fetch(`/api/visual-qa?${params.toString()}`, { cache: 'no-store' });
    const payload = await response.json();
    VisualQAState.rows = Array.isArray(payload.comparisons) ? payload.comparisons : [];
    VisualQAState.summary = payload.summary || {};
    VisualQAState.generatedAt = String(payload.generated_at || '');
    VisualQAState.message = String(payload.message || '').trim();
    renderSummary();
    renderCards();
    if (status) {
      if (VisualQAState.rows.length > 0) {
        status.textContent = route.book > 0
          ? `Loaded ${VisualQAState.rows.length} comparison grid(s) for book ${route.book}.`
          : `Loaded ${VisualQAState.rows.length} comparison grid(s).`;
      } else {
        status.textContent = VisualQAState.message || 'No comparison grids available yet.';
      }
    }
    if (route.autoOpen && route.book > 0) {
      const row = VisualQAState.rows.find((item) => Number(item.book_number || 0) === route.book && item.has_image);
      if (row) {
        window.setTimeout(() => openLightbox(row), 0);
      }
    }
  } catch (error) {
    if (status) status.textContent = `Failed to load Visual QA: ${error.message}`;
    window.Toast?.error(`Visual QA load failed: ${error.message}`);
  } finally {
    VisualQAState.loading = false;
  }
}

async function generateVisualQa() {
  const button = document.getElementById('visualQaGenerateBtn');
  const status = document.getElementById('visualQaStatus');
  if (button) button.disabled = true;
  if (status) status.textContent = 'Generating comparison grids...';
  try {
    const response = await fetch('/api/visual-qa/generate?catalog=classics', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({}),
    });
    const payload = await response.json();
    if (!response.ok || payload.ok === false) {
      throw new Error(payload.error || payload.error_message || 'Generation failed');
    }
    window.Toast?.success(
      `Visual QA generated: ${Number(payload.generated || 0)} books · ${Number(payload.failed || 0)} failures`,
      5500,
    );
    await loadVisualQa({ force: true });
  } catch (error) {
    if (status) status.textContent = `Visual QA generation failed: ${error.message}`;
    window.Toast?.error(`Visual QA generation failed: ${error.message}`);
  } finally {
    if (button) button.disabled = false;
  }
}

function openLightbox(row) {
  const imageUrl = String(row.image_url || '').trim();
  if (!imageUrl) return;
  const overlay = document.createElement('div');
  overlay.className = 'view-modal';
  overlay.innerHTML = `
    <div class="view-modal-inner">
      <div class="modal-header">
        <h3 class="modal-title">Book ${Number(row.book_number || 0)} · ${esc(row.book_title || '')} · ${esc(row.verdict || '')}</h3>
        <button class="close-btn" id="visualQaCloseBtn">x</button>
      </div>
      <div class="modal-body">
        <img src="${esc(`${imageUrl}?ts=${Date.now()}`)}" alt="${esc(row.book_title || 'comparison')}" style="width:100%;height:auto;border-radius:8px;border:1px solid var(--border);" />
      </div>
    </div>
  `;
  overlay.querySelector('#visualQaCloseBtn')?.addEventListener('click', () => overlay.remove());
  overlay.addEventListener('click', (event) => { if (event.target === overlay) overlay.remove(); });
  document.body.appendChild(overlay);
}

window.Pages['visual-qa'] = {
  async render() {
    syncVisualQaRouteState();
    const content = document.getElementById('content');
    if (!content) return;
    const scopedBanner = VisualQAState.requestedBook > 0
      ? `<div class="text-sm text-muted mb-12">Scoped to book ${VisualQAState.requestedBook} from the Compare action.</div>`
      : '';
    content.innerHTML = `
      <div class="card">
        <div class="card-header">
          <h2 class="card-title">Visual QA Dashboard</h2>
          <div class="filters-bar">
            <button class="btn btn-primary btn-sm" id="visualQaGenerateBtn">Generate All Comparisons</button>
            <button class="filter-chip ${VisualQAState.showFailuresOnly ? '' : 'active'}" id="visualQaAllBtn">All</button>
            <button class="filter-chip ${VisualQAState.showFailuresOnly ? 'active' : ''}" id="visualQaFailBtn">Failures Only</button>
          </div>
        </div>
        <div id="visualQaSummary" class="flex gap-8 mb-8"></div>
        <div id="visualQaGeneratedAt" class="text-sm text-muted mb-12">Loading...</div>
        ${scopedBanner}
        <div id="visualQaStatus" class="text-sm text-muted mb-12">Loading comparison grids...</div>
        <div id="visualQaCards" class="compare-grid"></div>
      </div>
    `;

    content.querySelector('#visualQaGenerateBtn')?.addEventListener('click', () => {
      generateVisualQa();
    });
    content.querySelector('#visualQaAllBtn')?.addEventListener('click', () => {
      VisualQAState.showFailuresOnly = false;
      content.querySelector('#visualQaAllBtn')?.classList.add('active');
      content.querySelector('#visualQaFailBtn')?.classList.remove('active');
      renderCards();
    });
    content.querySelector('#visualQaFailBtn')?.addEventListener('click', () => {
      VisualQAState.showFailuresOnly = true;
      content.querySelector('#visualQaFailBtn')?.classList.add('active');
      content.querySelector('#visualQaAllBtn')?.classList.remove('active');
      renderCards();
    });

    await loadVisualQa({ force: false });
  },
};

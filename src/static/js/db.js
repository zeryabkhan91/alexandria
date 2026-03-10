const _stores = {
  books: {},
  jobs: {},
  winners: {},
  prompts: {},
  settings: {},
  cost_ledger: {},
  batches: {},
};

const STORE_CONFIGS = {
  books: { keyPath: 'id', autoIncrement: false },
  jobs: { keyPath: 'id', autoIncrement: false },
  winners: { keyPath: 'book_id', autoIncrement: false },
  prompts: { keyPath: 'id', autoIncrement: true },
  settings: { keyPath: 'key', autoIncrement: false },
  cost_ledger: { keyPath: 'id', autoIncrement: true },
  batches: { keyPath: 'id', autoIncrement: false },
};

const _autoIncrements = { prompts: 1, cost_ledger: 1 };
const CGI_SETTINGS = '/cgi-bin/settings.py';
let _persistTimer = null;

function _nextId(storeName) {
  _autoIncrements[storeName] = Number(_autoIncrements[storeName] || 1);
  const next = _autoIncrements[storeName];
  _autoIncrements[storeName] += 1;
  return next;
}

function _normalizeBook(raw) {
  if (!raw) return null;
  const id = raw.id ?? raw.number ?? raw.book_number;
  const resolvedNumber = raw.number ?? raw.book_number ?? id;
  const fallbackSource = Boolean(raw.local_cover_available || raw.cover_jpg_id) ? 'catalog' : 'drive';
  const fallbackOriginal = resolvedNumber !== undefined && resolvedNumber !== null
    ? `/api/books/${encodeURIComponent(String(resolvedNumber))}/cover-preview?source=${fallbackSource}`
    : '';
  return {
    id,
    number: resolvedNumber,
    title: raw.title || `Book ${id}`,
    author: raw.author || '',
    folder_name: raw.folder || raw.folder_name || '',
    cover_jpg_id: raw.cover_jpg_id || '',
    original: raw.original || raw.thumbnail_url || fallbackOriginal,
    winner_selected: Boolean(raw.winner_selected || raw.winner_variant),
    winner_variant: raw.winner_variant || null,
    synced_at: raw.synced_at || new Date().toISOString(),
    ...raw,
  };
}

function _normalizedTextList(value) {
  if (!Array.isArray(value)) return [];
  return value
    .map((item) => String(item ?? '').trim())
    .filter(Boolean);
}

function _bookHasPromptEnrichment(raw) {
  if (!raw || typeof raw !== 'object') return false;
  const enrichment = (raw.enrichment && typeof raw.enrichment === 'object') ? raw.enrichment : {};
  const promptComponents = (raw.prompt_components && typeof raw.prompt_components === 'object') ? raw.prompt_components : {};
  const era = enrichment.era;
  const eraValues = Array.isArray(era) ? _normalizedTextList(era) : [String(era ?? '').trim()].filter(Boolean);
  return (
    _normalizedTextList(enrichment.iconic_scenes).length > 0
    || Boolean(String(enrichment.emotional_tone ?? enrichment.mood ?? '').trim())
    || eraValues.length > 0
    || _normalizedTextList(promptComponents.title_keywords).length > 0
    || Boolean(String(raw.composed_prompt ?? '').trim())
  );
}

function _mergeBookRecord(existing, incoming) {
  const merged = { ...(existing || {}), ...(incoming || {}) };
  if (!_bookHasPromptEnrichment(existing) || _bookHasPromptEnrichment(incoming)) {
    return merged;
  }
  if (existing?.enrichment && typeof existing.enrichment === 'object') merged.enrichment = existing.enrichment;
  if (existing?.prompt_components && typeof existing.prompt_components === 'object') merged.prompt_components = existing.prompt_components;
  if (String(existing?.composed_prompt ?? '').trim()) merged.composed_prompt = existing.composed_prompt;
  if (!String(incoming?.default_prompt ?? '').trim() && String(existing?.default_prompt ?? '').trim()) {
    merged.default_prompt = existing.default_prompt;
  }
  if (!String(incoming?.genre ?? '').trim() && String(existing?.genre ?? '').trim()) {
    merged.genre = existing.genre;
  }
  return merged;
}

async function _loadServerSettings() {
  try {
    const resp = await fetch(CGI_SETTINGS, { cache: 'no-store' });
    if (!resp.ok) return;
    const data = await resp.json();
    if (!data || typeof data !== 'object') return;
    for (const [key, value] of Object.entries(data)) {
      _stores.settings[key] = { key, value };
    }
  } catch (err) {
    try {
      const local = localStorage.getItem('alexandria_settings_store');
      if (!local) return;
      const parsed = JSON.parse(local);
      for (const [key, value] of Object.entries(parsed || {})) {
        _stores.settings[key] = { key, value };
      }
    } catch {
      // ignore
    }
  }
}

async function _persistSettings() {
  const flat = {};
  for (const [key, obj] of Object.entries(_stores.settings)) flat[key] = obj.value;
  try {
    await fetch(CGI_SETTINGS, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(flat),
    });
  } catch {
    try {
      localStorage.setItem('alexandria_settings_store', JSON.stringify(flat));
    } catch {
      // ignore
    }
  }
}

window.DB = {
  openDB() {
    return true;
  },

  dbPut(storeName, item) {
    const cfg = STORE_CONFIGS[storeName];
    if (!cfg) throw new Error(`Unknown store: ${storeName}`);
    const copy = { ...item };
    if (cfg.autoIncrement && !copy[cfg.keyPath]) {
      copy[cfg.keyPath] = _nextId(storeName);
    }
    if (storeName === 'books') {
      const normalized = _normalizeBook(copy);
      if (!normalized || normalized[cfg.keyPath] === undefined || normalized[cfg.keyPath] === null) return null;
      const existing = _stores.books[normalized[cfg.keyPath]] || null;
      const merged = _mergeBookRecord(existing, normalized);
      _stores.books[merged[cfg.keyPath]] = merged;
      return merged;
    }
    _stores[storeName][copy[cfg.keyPath]] = copy;
    return copy;
  },

  dbGet(storeName, key) {
    return _stores[storeName][key] ?? null;
  },

  dbGetAll(storeName) {
    return Object.values(_stores[storeName] || {});
  },

  dbDelete(storeName, key) {
    delete _stores[storeName][key];
  },

  dbClear(storeName) {
    _stores[storeName] = {};
    if (_autoIncrements[storeName] !== undefined) _autoIncrements[storeName] = 1;
  },

  replaceBooks(rows) {
    const next = {};
    const current = _stores.books || {};
    (Array.isArray(rows) ? rows : []).forEach((raw) => {
      const normalized = _normalizeBook(raw);
      if (!normalized || normalized.id === undefined || normalized.id === null) return;
      const existing = current[normalized.id] || null;
      next[normalized.id] = _mergeBookRecord(existing, normalized);
    });
    _stores.books = next;
    return this.dbGetAll('books');
  },

  bookHasPromptEnrichment(book) {
    return _bookHasPromptEnrichment(book);
  },

  dbGetByIndex(storeName, indexName, value) {
    return Object.values(_stores[storeName] || {}).filter((item) => item[indexName] === value);
  },

  dbCount(storeName) {
    return Object.keys(_stores[storeName] || {}).length;
  },

  getSetting(key, defaultValue = null) {
    return _stores.settings[key]?.value ?? defaultValue;
  },

  setSetting(key, value) {
    _stores.settings[key] = { key, value };
    clearTimeout(_persistTimer);
    _persistTimer = setTimeout(_persistSettings, 300);
  },

  async loadBooks(catalog = 'classics') {
    const resp = await fetch(`/api/iterate-data?catalog=${encodeURIComponent(catalog)}&view=books&limit=9999&offset=0`, { cache: 'no-store' });
    const data = await resp.json();
    const books = Array.isArray(data.books) ? data.books : [];
    return this.replaceBooks(books);
  },

  async loadPrompts(catalog = 'classics') {
    try {
      const resp = await fetch(`/api/prompts?catalog=${encodeURIComponent(catalog)}`, { cache: 'no-store' });
      const data = await resp.json();
      this.dbClear('prompts');
      const prompts = Array.isArray(data.prompts) ? data.prompts : [];
      prompts.forEach((prompt) => {
        this.dbPut('prompts', {
          id: prompt.id,
          name: prompt.name,
          prompt_template: prompt.prompt_template,
          negative_prompt: prompt.negative_prompt || '',
          style_profile: (Array.isArray(prompt.style_anchors) ? prompt.style_anchors.join(', ') : '') || prompt.style_profile || '',
          category: prompt.category || 'Saved',
          created_at: prompt.created_at || new Date().toISOString(),
          usage_count: Number(prompt.usage_count || 0),
          win_count: Number(prompt.win_count || 0),
          ...prompt,
        });
      });
      const maxId = prompts.reduce((acc, p) => Math.max(acc, Number(p.id || 0)), 0);
      _autoIncrements.prompts = Math.max(_autoIncrements.prompts, maxId + 1);
      return this.dbGetAll('prompts');
    } catch {
      return this.dbGetAll('prompts');
    }
  },

  async initDefaults() {
    await _loadServerSettings();
    const defaults = {
      openrouter_key: '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',
      google_api_key: 'AIzaSyAY6XvPxrdS_fMNMZEUkJd7UW9b9yuJDgI',
      drive_source_folder: '1ybFYDJk7Y3VlbsEjRAh1LOfdyVsHM_cS',
      drive_output_folder: '1Vr184ZsX3k38xpmZkd8g2vwB5y9LYMRC',
      drive_winner_folder: '1vOGdGjryzErrzB0kT3qmu3PJrRLOoqBg',
      budget_limit: 50,
      default_variant_count: 1,
      quality_threshold: 0.6,
      medallion_cx: 2850,
      medallion_cy: 1350,
      medallion_radius: 520,
    };
    for (const [key, val] of Object.entries(defaults)) {
      if (!_stores.settings[key]) {
        _stores.settings[key] = { key, value: val };
      }
    }
  },
};

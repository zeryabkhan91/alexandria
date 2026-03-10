window.Drive = {
  _lastCatalogSyncSummary: {},

  async catalogCacheStatus() {
    try {
      const resp = await fetch('/cgi-bin/catalog.py/status', { cache: 'no-store' });
      if (resp.ok) return resp.json();
    } catch {
      // ignore
    }
    try {
      const resp = await fetch('/api/drive/status', { cache: 'no-store' });
      if (!resp.ok) throw new Error('Drive status failed');
      const data = await resp.json();
      return {
        cached: true,
        age_seconds: 0,
        count: Number(data.status?.source_count || 0),
        stale: false,
        synced_at: data.status?.last_sync || null,
      };
    } catch {
      return { cached: false, stale: false, count: 0, synced_at: null, age_seconds: 0 };
    }
  },

  async loadCachedCatalog() {
    try {
      const resp = await fetch('/cgi-bin/catalog.py', { cache: 'no-store' });
      if (!resp.ok) throw new Error('No CGI cache');
      const data = await resp.json();
      const books = Array.isArray(data.books) ? data.books : [];
      return DB.replaceBooks(books.map((book) => ({ id: book.number, number: book.number, ...book })));
    } catch {
      return DB.loadBooks('classics');
    }
  },

  async refreshCatalogCache() {
    try {
      await fetch('/cgi-bin/catalog.py/refresh', { method: 'POST' });
    } catch {
      // ignore
    }
    return DB.loadBooks('classics');
  },

  async syncCatalog(onProgress, options = {}) {
    let progressCb = onProgress;
    let opts = options;
    if (onProgress && typeof onProgress === 'object' && !Array.isArray(onProgress)) {
      progressCb = null;
      opts = onProgress;
    }
    const catalog = String(opts.catalog || 'classics').trim() || 'classics';
    const rawLimit = Number(opts.limit || 20000);
    const limit = Number.isFinite(rawLimit) ? Math.max(1, Math.min(50000, Math.round(rawLimit))) : 20000;
    const force = opts.force === undefined ? true : Boolean(opts.force);
    if (typeof progressCb === 'function') progressCb({ step: 'start', catalog });
    const resp = await fetch(`/api/drive/catalog-sync?catalog=${encodeURIComponent(catalog)}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ force, limit }),
    });
    if (!resp.ok) {
      const text = await resp.text();
      throw new Error(text || `Catalog sync failed (HTTP ${resp.status})`);
    }
    const summary = await resp.json();
    this._lastCatalogSyncSummary = (summary && typeof summary === 'object') ? summary : {};
    let books = [];
    const syncedBooks = Array.isArray(this._lastCatalogSyncSummary.books) ? this._lastCatalogSyncSummary.books : [];
    if (syncedBooks.length) {
      books = DB.replaceBooks(
        syncedBooks.map((book) => {
          if (!book || typeof book !== 'object') return null;
          const id = book.id ?? book.number ?? book.book_number;
          const number = book.number ?? book.book_number ?? id;
          if (id === undefined || id === null) return null;
          return { id, number, ...book };
        }).filter(Boolean),
      );
    } else {
      books = await DB.loadBooks(catalog);
    }
    if (typeof progressCb === 'function') {
      progressCb({
        step: 'done',
        count: books.length,
        catalog,
        summary: this.getLastCatalogSyncSummary(),
      });
    }
    return books;
  },

  getLastCatalogSyncSummary() {
    const summary = this._lastCatalogSyncSummary;
    if (!summary || typeof summary !== 'object') return {};
    return { ...summary };
  },

  getDriveThumbnailUrl(fileId, _apiKey, size = 280) {
    if (!fileId) return '';
    return `https://drive.google.com/thumbnail?id=${encodeURIComponent(fileId)}&sz=w${Number(size)}`;
  },

  async listDriveSubfolders(folderId, _apiKey) {
    const resp = await fetch(`/api/drive/input-covers?limit=1000&force=0&input_folder_id=${encodeURIComponent(folderId || '')}`, { cache: 'no-store' });
    if (!resp.ok) throw new Error('Unable to list Drive subfolders');
    const data = await resp.json();
    return Array.isArray(data.covers) ? data.covers : [];
  },

  async downloadCoverForBook(bookNumber, source = 'catalog') {
    const resp = await fetch(`/api/books/${encodeURIComponent(bookNumber)}/cover-preview?source=${encodeURIComponent(source)}&catalog=classics`, { cache: 'no-store' });
    if (!resp.ok) throw new Error('Cover preview unavailable');
    const blob = await resp.blob();
    const url = URL.createObjectURL(blob);
    const img = await new Promise((resolve, reject) => {
      const el = new Image();
      el.crossOrigin = 'anonymous';
      el.onload = () => resolve(el);
      el.onerror = () => reject(new Error('Failed to decode cover preview'));
      el.src = url;
    });
    return { img, url };
  },

  validateCoverTemplate(_img) {
    const cx = Number(DB.getSetting('medallion_cx', 2850));
    const cy = Number(DB.getSetting('medallion_cy', 1350));
    const radius = Number(DB.getSetting('medallion_radius', 520));
    return { valid: true, medallion: { cx, cy, radius } };
  },
};

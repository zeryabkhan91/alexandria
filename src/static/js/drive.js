window.Drive = {
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
      DB.dbClear('books');
      books.forEach((book) => DB.dbPut('books', { id: book.number, number: book.number, ...book }));
      return books;
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

  async syncCatalog(onProgress) {
    if (typeof onProgress === 'function') onProgress({ step: 'start' });
    const resp = await fetch('/api/drive/catalog-sync?catalog=classics', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ force: true, limit: 5000 }),
    });
    if (!resp.ok) {
      const text = await resp.text();
      throw new Error(text || `Catalog sync failed (HTTP ${resp.status})`);
    }
    await resp.json();
    const books = await DB.loadBooks('classics');
    if (typeof onProgress === 'function') onProgress({ step: 'done', count: books.length });
    return books;
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

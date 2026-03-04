window.Pages = window.Pages || {};

let _filter = 'all';

function _fallbackCoverPreview(book) {
  const number = Number(book?.number || book?.id || 0);
  if (!Number.isFinite(number) || number <= 0) return '';
  const source = (book?.local_cover_available || book?.cover_jpg_id) ? 'catalog' : 'drive';
  return `/api/books/${encodeURIComponent(String(number))}/cover-preview?source=${source}`;
}

function getBookThumb(book, winner) {
  if (winner) {
    const job = DB.dbGet('jobs', winner.job_id);
    if (job) return getBlobUrl(job.composited_image_blob || job.generated_image_blob, `${job.id}-winner-thumb`);
  }
  return book.original || _fallbackCoverPreview(book);
}

window.Pages.review = {
  async render() {
    let books = DB.dbGetAll('books');
    if (!books.length) books = await DB.loadBooks('classics');

    const content = document.getElementById('content');
    content.innerHTML = `
      <div class="card">
        <div class="flex justify-between items-center mb-8">
          <div class="filters-bar">
            <button class="filter-chip active" data-filter="all">All</button>
            <button class="filter-chip" data-filter="has-variants">Has Variants</button>
            <button class="filter-chip" data-filter="needs-review">Needs Review</button>
            <button class="filter-chip" data-filter="approved">Approved</button>
          </div>
          <div class="flex gap-8" style="flex-wrap:wrap">
            <a href="https://drive.google.com/drive/folders/${encodeURIComponent(DB.getSetting('drive_winner_folder', ''))}" target="_blank" class="btn btn-secondary">Winner Covers (Drive)</a>
            <button class="btn btn-secondary" id="reviewDownloadZip">Download ZIP</button>
            <button class="btn btn-secondary" id="reviewAutoApproveBtn">Batch Auto-Approve</button>
          </div>
        </div>

        <div id="autoApprovePanel" class="hidden">
          <div class="card" style="margin-bottom:0">
            <div class="form-group">
              <label class="form-label">Quality Threshold</label>
              <input type="range" min="0" max="100" value="60" id="autoApproveThreshold" class="w-full" />
              <span id="autoApproveThresholdVal" class="text-muted">60%</span>
            </div>
            <div class="text-muted" id="autoApprovePreview">0 books would be auto-approved</div>
            <button id="autoApproveConfirmBtn" class="btn btn-primary mt-8">Apply Auto-Approve</button>
          </div>
        </div>
      </div>

      <div class="grid-auto" id="reviewGrid"></div>
    `;

    content.querySelectorAll('[data-filter]').forEach((chip) => {
      chip.addEventListener('click', () => {
        _filter = chip.dataset.filter;
        content.querySelectorAll('[data-filter]').forEach((c) => c.classList.toggle('active', c === chip));
        this._renderGrid(books);
      });
    });

    document.getElementById('reviewDownloadZip')?.addEventListener('click', () => this.downloadZip());
    document.getElementById('reviewAutoApproveBtn')?.addEventListener('click', () => {
      document.getElementById('autoApprovePanel')?.classList.toggle('hidden');
      this._updateAutoPreview(books);
    });
    document.getElementById('autoApproveThreshold')?.addEventListener('input', (e) => {
      document.getElementById('autoApproveThresholdVal').textContent = `${e.target.value}%`;
      this._updateAutoPreview(books);
    });
    document.getElementById('autoApproveConfirmBtn')?.addEventListener('click', () => this.autoApprove(books));

    this._renderGrid(books);
  },

  _filteredBooks(books) {
    return books.filter((book) => {
      const jobs = DB.dbGetByIndex('jobs', 'book_id', book.id).filter((j) => j.status === 'completed');
      const winner = DB.dbGet('winners', book.id);
      if (_filter === 'has-variants') return jobs.length > 0;
      if (_filter === 'needs-review') return jobs.length > 0 && !winner;
      if (_filter === 'approved') return Boolean(winner);
      return true;
    });
  },

  _renderGrid(books) {
    const grid = document.getElementById('reviewGrid');
    if (!grid) return;
    const rows = this._filteredBooks(books);
    if (!rows.length) {
      grid.innerHTML = '<div class="text-muted">No books for this filter.</div>';
      return;
    }

    grid.innerHTML = rows.map((book) => {
      const jobs = DB.dbGetByIndex('jobs', 'book_id', book.id).filter((j) => j.status === 'completed');
      const winner = DB.dbGet('winners', book.id);
      const status = winner ? 'Winner' : (jobs.length ? 'Needs Review' : 'No Variants');
      return `
        <div class="book-card" data-book-open="${book.id}">
          <img class="book-thumb" src="${getBookThumb(book, winner)}" alt="${book.title}" />
          <div class="book-info">
            <div class="book-title">${book.title}</div>
            <div class="book-author">${book.author || ''}</div>
            <div class="flex justify-between mt-8">
              <span class="text-sm text-muted">${jobs.length} variants</span>
              <span class="tag ${winner ? 'tag-gold' : (jobs.length ? 'tag-pending' : 'tag-status')}">${status}</span>
            </div>
          </div>
        </div>
      `;
    }).join('');

    grid.querySelectorAll('[data-book-open]').forEach((el) => {
      el.addEventListener('click', () => this.showBookVariants(Number(el.dataset.bookOpen)));
    });
  },

  showBookVariants(bookId) {
    const book = DB.dbGet('books', bookId);
    const jobs = DB.dbGetByIndex('jobs', 'book_id', bookId).filter((j) => j.status === 'completed').sort((a, b) => Number(b.quality_score || 0) - Number(a.quality_score || 0));
    if (!book || !jobs.length) {
      Toast.info('No variants available for this book yet.');
      return;
    }

    const overlay = document.createElement('div');
    overlay.className = 'modal-overlay';
    overlay.innerHTML = `
      <div class="modal">
        <div class="modal-header">
          <h2 class="modal-title">Pick Winner · ${book.number}. ${book.title}</h2>
          <button class="close-btn">×</button>
        </div>
        <div class="modal-body">
          <div class="grid-auto">
            ${jobs.map((job) => `
              <div class="result-card" data-pick="${job.id}">
                <img class="thumb" src="${getBlobUrl(job.composited_image_blob || job.generated_image_blob, `${job.id}-review`) }" />
                <div class="card-body">
                  <div class="flex justify-between"><span class="tag tag-model">${job.model}</span><span class="tag tag-gold">${Math.round(Number(job.quality_score || 0) * 100)}%</span></div>
                </div>
              </div>
            `).join('')}
          </div>
        </div>
      </div>
    `;

    const close = () => overlay.remove();
    overlay.querySelector('.close-btn')?.addEventListener('click', close);
    overlay.addEventListener('click', (e) => { if (e.target === overlay) close(); });

    overlay.querySelectorAll('[data-pick]').forEach((el) => {
      el.addEventListener('click', () => {
        const job = DB.dbGet('jobs', el.dataset.pick);
        if (!job) return;
        DB.dbPut('winners', {
          book_id: bookId,
          job_id: job.id,
          variant_index: job.variant,
          quality_score: Number(job.quality_score || 0),
          auto_approved: false,
          selected_at: new Date().toISOString(),
        });
        Toast.success('Winner selected');
        close();
        this._renderGrid(DB.dbGetAll('books'));
      });
    });

    document.body.appendChild(overlay);
  },

  _updateAutoPreview(books) {
    const threshold = Number(document.getElementById('autoApproveThreshold')?.value || 60) / 100;
    let count = 0;
    books.forEach((book) => {
      const hasWinner = DB.dbGet('winners', book.id);
      if (hasWinner) return;
      const top = DB.dbGetByIndex('jobs', 'book_id', book.id)
        .filter((j) => j.status === 'completed')
        .sort((a, b) => Number(b.quality_score || 0) - Number(a.quality_score || 0))[0];
      if (top && Number(top.quality_score || 0) >= threshold) count += 1;
    });
    const preview = document.getElementById('autoApprovePreview');
    if (preview) preview.textContent = `${count} books would be auto-approved`;
  },

  autoApprove(books) {
    const threshold = Number(document.getElementById('autoApproveThreshold')?.value || 60) / 100;
    let applied = 0;
    books.forEach((book) => {
      const top = DB.dbGetByIndex('jobs', 'book_id', book.id)
        .filter((j) => j.status === 'completed')
        .sort((a, b) => Number(b.quality_score || 0) - Number(a.quality_score || 0))[0];
      if (top && Number(top.quality_score || 0) >= threshold) {
        DB.dbPut('winners', {
          book_id: book.id,
          job_id: top.id,
          variant_index: top.variant,
          quality_score: Number(top.quality_score || 0),
          auto_approved: true,
          selected_at: new Date().toISOString(),
        });
        applied += 1;
      }
    });
    this._renderGrid(books);
    Toast.success(`${applied} winners auto-approved`);
  },

  async downloadZip() {
    const winners = DB.dbGetAll('winners');
    if (!winners.length) {
      Toast.warning('No winners selected yet.');
      return;
    }
    const zip = new JSZip();

    for (const winner of winners) {
      const book = DB.dbGet('books', winner.book_id);
      const job = DB.dbGet('jobs', winner.job_id);
      if (!book || !job) continue;
      const folder = zip.folder(`book_${book.number}_${String(book.title).replace(/[^a-z0-9]+/gi, '_')}`);
      const src = getBlobUrl(job.composited_image_blob || job.generated_image_blob, `${job.id}-zip`);
      const imageBlob = await fetch(src).then((r) => r.blob());
      folder.file('illustration.jpg', imageBlob);
      folder.file('metadata.json', JSON.stringify({
        title: book.title,
        author: book.author,
        model: job.model,
        quality: winner.quality_score,
        cost: job.cost_usd,
        date: winner.selected_at,
      }, null, 2));
    }

    const blob = await zip.generateAsync({ type: 'blob' });
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = 'alexandria-winners.zip';
    a.click();
  },
};

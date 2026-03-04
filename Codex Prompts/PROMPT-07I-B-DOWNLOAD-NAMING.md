# PROMPT-07I-B — Download Naming: Match Source Cover Names Exactly

## Goal
Make download file names and ZIP folder names match the original source cover naming exactly, using `file_base` from the book catalog instead of constructing names from title/author.

## Root Cause
The current `resolveBookMetadataForJob()` function in `iterate.js` constructs `baseName` as `sanitizeDownloadName(title + ' — ' + author)`. This doesn't always match the source cover `file_base` in `book_catalog.json` because:
- Some source covers use em dash `—`, others use hyphen `-`
- Some have special characters or abbreviated names
- The `sanitizeDownloadName()` function strips characters that may be part of the original name

The `file_base` field is already available in the frontend DB (synced from the API via `_normalizeBook()` + spread operator).

## File to Change
`src/static/js/pages/iterate.js`

## Exact Changes

### Change 1: Update `resolveBookMetadataForJob` to use `file_base` when available

**Replace the current function (around lines 157-168):**
```javascript
function resolveBookMetadataForJob(job) {
  const bookId = Number(job?.book_id || 0);
  let book = DB.dbGet('books', bookId);
  if (!book) {
    book = DB.dbGetAll('books').find((row) => Number(row.id) === bookId) || null;
  }
  const title = sanitizeDownloadName(book?.title || `Book ${bookId || 'Unknown'}`);
  const author = sanitizeDownloadName(book?.author || 'Unknown');
  const number = sanitizeDownloadName(book?.number || job?.book_id || 'Unknown');
  const baseName = sanitizeDownloadName(`${title} — ${author}`);
  return { title, author, number, baseName };
}
```

**With:**
```javascript
function resolveBookMetadataForJob(job) {
  const bookId = Number(job?.book_id || 0);
  let book = DB.dbGet('books', bookId);
  if (!book) {
    book = DB.dbGetAll('books').find((row) => Number(row.id) === bookId) || null;
  }
  const title = sanitizeDownloadName(book?.title || `Book ${bookId || 'Unknown'}`);
  const author = sanitizeDownloadName(book?.author || 'Unknown');
  const number = sanitizeDownloadName(book?.number || job?.book_id || 'Unknown');
  // Use file_base from catalog when available — it exactly matches the source cover file names.
  const catalogBase = String(book?.file_base || '').trim();
  const baseName = catalogBase
    ? sanitizeDownloadName(catalogBase)
    : sanitizeDownloadName(`${title} \u2014 ${author}`);
  return { title, author, number, baseName };
}
```

### Change 2: Update ZIP structure to mirror source cover folder naming

**Replace the `downloadComposite` method (around lines 848-891):**

Find the line:
```javascript
    const zipName = `${number}. ${baseName}.zip`;
```

Replace with:
```javascript
    // Mirror source cover folder naming: "{number}. {file_base}"
    const folderName = `${number}. ${baseName}`;
    const zipName = `${folderName}.zip`;
```

And update the zip.file() calls to include the folder prefix. Replace:
```javascript
      if (compositeHref) {
        const compositeBlob = await fetchDownloadBlob(compositeHref);
        if (compositeBlob) {
          zip.file(`${baseName}.jpg`, compositeBlob);
        }
      }

      if (rawHref) {
        const rawBlob = await fetchDownloadBlob(rawHref);
        if (rawBlob) {
          zip.file(`${baseName} (illustration).jpg`, rawBlob);
        }
      }
```

With:
```javascript
      if (compositeHref) {
        const compositeBlob = await fetchDownloadBlob(compositeHref);
        if (compositeBlob) {
          zip.file(`${folderName}/${baseName}.jpg`, compositeBlob);
        }
      }

      if (rawHref) {
        const rawBlob = await fetchDownloadBlob(rawHref);
        if (rawBlob) {
          zip.file(`${folderName}/${baseName} (illustration).jpg`, rawBlob);
        }
      }
```

### Change 3: Also update the Raw download button's filename

In the `downloadGenerated` method (around lines 893-903), replace:
```javascript
    a.download = `${baseName} (illustration).jpg`;
```
With:
```javascript
    a.download = `${number}. ${baseName} (illustration).jpg`;
```

## Expected Download Output

For book #1 "A Room with a View" by E. M. Forster (where `file_base` = `"A Room with a View - E. M. Forster"`):

**Download button** → `1. A Room with a View - E. M. Forster.zip` containing:
```
1. A Room with a View - E. M. Forster/
  ├── A Room with a View - E. M. Forster.jpg          (composite, 3784×2777 @ 300 DPI)
  └── A Room with a View - E. M. Forster (illustration).jpg  (raw generated image)
```

**Raw button** → `1. A Room with a View - E. M. Forster (illustration).jpg`

This mirrors the source cover naming:
```
1. A Room with a View - E. M. Forster copy/
  ├── A Room with a View - E. M. Forster.jpg
  ├── A Room with a View - E. M. Forster.pdf
  └── A Room with a View - E. M. Forster.ai
```

## How to Verify
1. Sync the catalog (click Sync button)
2. Generate a cover for any book
3. Click Download — verify the ZIP name matches `{number}. {file_base}.zip`
4. Extract the ZIP — verify the folder name and file names inside match the pattern above
5. Click Raw — verify the filename matches `{number}. {file_base} (illustration).jpg`

```
git add -A && git commit && git push
```

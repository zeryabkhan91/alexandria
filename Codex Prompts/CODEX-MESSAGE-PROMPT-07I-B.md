## Codex Task — PROMPT-07I-B: Download Naming Fix

Make download file names match the original source covers exactly by using the `file_base` field from the book catalog.

### Context
Downloads currently construct names as `title — author` which doesn't match source cover files (which use various separators like `-` or `—`). The `file_base` field from `book_catalog.json` already contains the exact original name and is available in the frontend DB.

### Changes (single file: `src/static/js/pages/iterate.js`)
1. Update `resolveBookMetadataForJob()` to prefer `book.file_base` over constructed name
2. ZIP structure: add folder matching source cover folder naming
3. Raw download: prefix with book number

See `PROMPT-07I-B-DOWNLOAD-NAMING.md` for exact code changes.

### Verify
Generate + download a cover. ZIP should be named `{number}. {file_base}.zip` containing a folder with `{file_base}.jpg` (composite) and `{file_base} (illustration).jpg` (raw).

```
git add -A && git commit && git push
```

from __future__ import annotations

import json
from pathlib import Path
import shutil
import subprocess
import textwrap

import pytest


PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _run_browser_script(script_body: str) -> dict | list | str:
    if shutil.which("node") is None:
        pytest.skip("node not installed")

    node_script = textwrap.dedent(
        f"""
        const fs = require('fs');
        const vm = require('vm');

        global.window = {{}};
        global.document = {{}};
        global.localStorage = {{
          getItem: () => null,
          setItem: () => undefined,
        }};
        global.fetch = async () => {{ throw new Error('unexpected fetch'); }};

        vm.runInThisContext(fs.readFileSync('src/static/js/db.js', 'utf8'), {{ filename: 'db.js' }});
        vm.runInThisContext(fs.readFileSync('src/static/js/drive.js', 'utf8'), {{ filename: 'drive.js' }});
        global.DB = window.DB;
        global.Drive = window.Drive;

        (async () => {{
          {script_body}
        }})().then((result) => {{
          process.stdout.write(JSON.stringify(result));
        }}).catch((error) => {{
          console.error(error && error.stack ? error.stack : String(error));
          process.exit(1);
        }});
        """
    )
    proc = subprocess.run(
        ["node", "-e", node_script],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr or proc.stdout
    return json.loads(proc.stdout)


def test_replace_books_preserves_existing_prompt_enrichment_when_incoming_row_is_bare():
    result = _run_browser_script(
        """
        DB.dbPut('books', {
          id: 3,
          number: 3,
          title: "Gulliver's Travels",
          author: 'Jonathan Swift',
          enrichment: {
            iconic_scenes: ['Gulliver wakes bound on the beach while Lilliputians swarm over him'],
            emotional_tone: 'satirical wonder with unease',
            era: '1726',
          },
          prompt_components: { title_keywords: ['gulliver', 'lilliput'] },
          composed_prompt: 'The illustration must depict: Gulliver wakes bound on the beach while Lilliputians swarm over him.',
        });

        const replaced = DB.replaceBooks([
          {
            id: 3,
            number: 3,
            title: "Gulliver's Travels",
            author: 'Jonathan Swift',
            folder_name: '3. Gulliver',
            cover_jpg_id: 'drive-cover-123',
          },
        ]);

        return {
          replaced,
          book: DB.dbGet('books', 3),
          enriched: DB.bookHasPromptEnrichment(DB.dbGet('books', 3)),
        };
        """
    )

    assert result["enriched"] is True
    assert result["book"]["cover_jpg_id"] == "drive-cover-123"
    assert result["book"]["enrichment"]["iconic_scenes"][0].startswith("Gulliver wakes bound on the beach")
    assert "The illustration must depict:" in result["book"]["composed_prompt"]


def test_drive_load_cached_catalog_does_not_strip_enrichment_from_existing_books():
    result = _run_browser_script(
        """
        DB.dbPut('books', {
          id: 3,
          number: 3,
          title: "Gulliver's Travels",
          author: 'Jonathan Swift',
          enrichment: {
            iconic_scenes: ['Gulliver wakes bound on the beach while Lilliputians swarm over him'],
            emotional_tone: 'satirical wonder with unease',
            era: '1726',
          },
          prompt_components: { title_keywords: ['gulliver', 'lilliput'] },
          composed_prompt: 'The illustration must depict: Gulliver wakes bound on the beach while Lilliputians swarm over him.',
        });

        global.fetch = async (url) => {
          if (String(url) === '/cgi-bin/catalog.py') {
            return {
              ok: true,
              async json() {
                return {
                  books: [
                    {
                      number: 3,
                      title: "Gulliver's Travels",
                      author: 'Jonathan Swift',
                      folder_name: '3. Gulliver',
                      cover_jpg_id: 'drive-cover-456',
                    },
                  ],
                };
              },
            };
          }
          throw new Error(`unexpected fetch: ${url}`);
        };

        const rows = await Drive.loadCachedCatalog();
        return {
          rows,
          book: DB.dbGet('books', 3),
          enriched: DB.bookHasPromptEnrichment(DB.dbGet('books', 3)),
        };
        """
    )

    assert len(result["rows"]) == 1
    assert result["enriched"] is True
    assert result["book"]["cover_jpg_id"] == "drive-cover-456"
    assert result["book"]["enrichment"]["emotional_tone"] == "satirical wonder with unease"
    assert result["book"]["prompt_components"]["title_keywords"] == ["gulliver", "lilliput"]

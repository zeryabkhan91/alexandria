from __future__ import annotations

import json
from pathlib import Path
import shutil
import subprocess
import textwrap

import pytest


PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _run_iterate_prompt_builder(payload: dict) -> dict:
    if shutil.which("node") is None:
        pytest.skip("node not installed")

    node_script = textwrap.dedent(
        f"""
        const fs = require('fs');
        const vm = require('vm');

        global.window = {{ Pages: {{}}, __ITERATE_TEST_HOOKS__: {{}} }};
        global.document = {{}};
        global.DB = {{ dbGetAll: () => [], dbGet: () => null }};
        global.OpenRouter = {{ MODELS: [] }};
        global.Toast = {{}};
        global.JobQueue = {{}};
        global.escapeHtml = (value) => String(value ?? '');
        global.getBlobUrl = () => '';
        global.fetchDownloadBlob = async () => {{ throw new Error('unused'); }};
        global.ensureJSZip = async () => {{ throw new Error('unused'); }};
        global.uuid = () => 'job-1';
        global.StyleDiversifier = {{
          buildDiversifiedPrompt: () => 'Create a breathtaking legacy prompt.',
          selectDiverseStyles: () => [{{ id: 'romantic-sublime', label: 'Romantic Sublime' }}],
        }};

        const source = fs.readFileSync('src/static/js/pages/iterate.js', 'utf8');
        vm.runInThisContext(source, {{ filename: 'iterate.js' }});
        const fn = window.__ITERATE_TEST_HOOKS__.buildGenerationJobPrompt;
        const result = fn({json.dumps(payload)});
        process.stdout.write(JSON.stringify(result));
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


def test_iterate_prompt_builder_keeps_library_prompt_precomposed():
    result = _run_iterate_prompt_builder(
        {
            "book": {
                "title": "A Room with a View",
                "author": "E. M. Forster",
                "default_prompt": "A scene from the piazza",
            },
            "templateObj": {
                "id": "alexandria-base-romantic-realism",
                "name": "BASE 4 Romantic Realism",
                "prompt_template": (
                    "Book cover illustration only - no text. "
                    "Centered medallion illustration: {SCENE}. "
                    "The mood is {MOOD}. Era reference: {ERA}."
                ),
            },
            "promptId": "alexandria-base-romantic-realism",
            "customPrompt": (
                "Book cover illustration only - no text. "
                "Centered medallion illustration: {SCENE}. "
                "The mood is {MOOD}. Era reference: {ERA}."
            ),
            "sceneVal": "Lucy Honeychurch on a Florentine terrace",
            "moodVal": "classical, timeless, evocative",
            "eraVal": "Edwardian Italy",
            "style": {"id": "romantic-sublime", "label": "Romantic Sublime"},
        }
    )

    assert "Create a breathtaking legacy prompt." not in result["prompt"]
    assert "Lucy Honeychurch on a Florentine terrace" in result["prompt"]
    assert "Edwardian Italy" in result["prompt"]
    assert result["styleLabel"] == "BASE 4 Romantic Realism"
    assert result["styleId"] == "none"
    assert result["libraryPromptId"] == "alexandria-base-romantic-realism"
    assert result["composePrompt"] is False
    assert result["backendPromptSource"] == "custom"


def test_iterate_prompt_builder_keeps_legacy_style_diversifier_for_default_auto():
    result = _run_iterate_prompt_builder(
        {
            "book": {
                "title": "A Room with a View",
                "author": "E. M. Forster",
            },
            "templateObj": None,
            "promptId": "",
            "customPrompt": "",
            "sceneVal": "",
            "moodVal": "",
            "eraVal": "",
            "style": {"id": "romantic-sublime", "label": "Romantic Sublime"},
        }
    )

    assert result["prompt"].startswith("Create a breathtaking legacy prompt.")
    assert 'Create a colorful circular medallion illustration for "A Room with a View" by E. M. Forster.' in result["prompt"]
    assert result["styleLabel"] == "Romantic Sublime"
    assert result["styleId"] == "romantic-sublime"
    assert result["libraryPromptId"] == ""

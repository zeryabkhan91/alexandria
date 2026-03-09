from __future__ import annotations

import json
from pathlib import Path
import shutil
import subprocess
import textwrap

import pytest


PROJECT_ROOT = Path(__file__).resolve().parent.parent


TEST_PROMPTS = [
    {"id": "alexandria-base-classical-devotion", "name": "BASE 1 — Classical Devotion"},
    {"id": "alexandria-base-philosophical-gravitas", "name": "BASE 2 — Philosophical Gravitas"},
    {"id": "alexandria-base-gothic-atmosphere", "name": "BASE 3 — Gothic Atmosphere"},
    {"id": "alexandria-base-romantic-realism", "name": "BASE 4 — Romantic Realism"},
    {"id": "alexandria-base-esoteric-mysticism", "name": "BASE 5 — Esoteric Mysticism"},
    {"id": "alexandria-wildcard-edo-meets-alexandria", "name": "WILDCARD 1 — Edo Meets Alexandria"},
    {"id": "alexandria-wildcard-pre-raphaelite-garden", "name": "WILDCARD 2 — Pre-Raphaelite Garden"},
    {"id": "alexandria-wildcard-illuminated-manuscript", "name": "WILDCARD 3 — Illuminated Manuscript"},
    {"id": "alexandria-wildcard-celestial-cartography", "name": "WILDCARD 4 — Celestial Cartography"},
    {"id": "alexandria-wildcard-temple-of-knowledge", "name": "WILDCARD 5 — Temple of Knowledge"},
]


def _run_iterate_hook(hook_name: str, payload, prompts=None) -> dict | list | str:
    if shutil.which("node") is None:
        pytest.skip("node not installed")

    prompt_rows = prompts if prompts is not None else []
    node_script = textwrap.dedent(
        f"""
        const fs = require('fs');
        const vm = require('vm');
        const prompts = {json.dumps(prompt_rows)};

        global.window = {{ Pages: {{}}, __ITERATE_TEST_HOOKS__: {{}} }};
        global.document = {{}};
        global.DB = {{
          dbGetAll: (table) => table === 'prompts' ? prompts : [],
          dbGet: (table, id) => table === 'prompts' ? (prompts.find((row) => String(row.id) === String(id)) || null) : null,
        }};
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
        const fn = window.__ITERATE_TEST_HOOKS__[{json.dumps(hook_name)}];
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


def _run_iterate_prompt_builder(payload: dict) -> dict:
    return _run_iterate_hook("buildGenerationJobPrompt", payload)


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
    assert result["preservePromptText"] is True
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
    assert result["preservePromptText"] is False
    assert result["libraryPromptId"] == ""


def test_iterate_prompt_builder_uses_evocative_scene_fallback_when_scene_missing():
    result = _run_iterate_prompt_builder(
        {
            "book": {
                "title": "A Room with a View",
                "author": "E. M. Forster",
            },
            "templateObj": {
                "id": "alexandria-base-romantic-realism",
                "name": "BASE 4 Romantic Realism",
                "prompt_template": "Book cover illustration only — {SCENE}. The mood is {MOOD}. Era reference: {ERA}.",
            },
            "promptId": "alexandria-base-romantic-realism",
            "customPrompt": "",
            "sceneVal": "",
            "moodVal": "",
            "eraVal": "",
            "style": {"id": "romantic-sublime", "label": "Romantic Sublime"},
        }
    )

    assert 'A pivotal dramatic moment from the literary work "A Room with a View" by E. M. Forster' in result["prompt"]
    assert "centered and fully contained" not in result["prompt"]


def test_build_scene_pool_uses_enrichment_sources_and_variation_prefixes():
    result = _run_iterate_hook(
        "buildScenePool",
        {
            "title": "A Room with a View",
            "enrichment": {
                "iconic_scenes": [
                    "Lucy Honeychurch at the pension window overlooking Florence",
                    "George Emerson and Lucy in the Italian countryside",
                ],
                "protagonist": "Lucy Honeychurch",
                "setting_primary": "Edwardian Florence terraces",
                "setting_details": "cypress trees and sunlit courtyards",
                "visual_motifs": ["violet flowers", "open window", "travel guidebook"],
                "symbolic_elements": ["view over the Arno", "threshold between freedom and convention"],
                "key_characters": ["Lucy Honeychurch", "George Emerson", "Charlotte Bartlett"],
            },
            "count": 8,
        },
    )

    assert len(result) == 8
    assert result[0] == "Lucy Honeychurch at the pension window overlooking Florence"
    assert result[1] == "George Emerson and Lucy in the Italian countryside"
    assert "Lucy Honeychurch in a pivotal moment" in result[2]
    assert result[3].startswith("Edwardian Florence terraces")
    assert result[4].startswith("symbolic arrangement of violet flowers")
    assert result[5] == "Lucy Honeychurch, George Emerson, Charlotte Bartlett — a dramatic ensemble scene from the story"
    assert result[6].startswith("intimate close-up view of ")
    assert result[7].startswith("intimate close-up view of ")


def test_build_genre_aware_rotation_matches_literature_and_varies_scenes():
    result = _run_iterate_hook(
        "buildGenreAwareRotation",
        {
            "book": {
                "title": "A Room with a View",
                "genre": "Literary Fiction",
                "enrichment": {
                    "iconic_scenes": [
                        "Lucy at the pension window overlooking Florence",
                        "George and Lucy in the Italian countryside",
                        "The Pension Bertolini courtyard scene",
                    ],
                    "protagonist": "Lucy Honeychurch",
                    "setting_primary": "Edwardian Florence terraces",
                    "setting_details": "cypress trees and sunlit courtyards",
                    "visual_motifs": ["open window", "violet flowers"],
                    "symbolic_elements": ["view over the Arno", "threshold between freedom and convention"],
                    "key_characters": ["Lucy Honeychurch", "George Emerson", "Charlotte Bartlett"],
                },
            },
            "variantCount": 10,
        },
        prompts=TEST_PROMPTS,
    )

    assert [row["promptId"] for row in result] == [
        "alexandria-base-romantic-realism",
        "alexandria-wildcard-pre-raphaelite-garden",
        "alexandria-base-romantic-realism",
        "alexandria-wildcard-edo-meets-alexandria",
        "alexandria-base-romantic-realism",
        "alexandria-wildcard-illuminated-manuscript",
        "alexandria-base-romantic-realism",
        "alexandria-wildcard-celestial-cartography",
        "alexandria-base-romantic-realism",
        "alexandria-wildcard-temple-of-knowledge",
    ]
    assert len({row["sceneOverride"] for row in result}) == 10
    assert all(row["promptId"] != "alexandria-base-classical-devotion" for row in result)
    assert all(row["promptId"] != "alexandria-base-gothic-atmosphere" for row in result)


def test_build_genre_aware_rotation_defaults_to_romantic_realism_when_genre_unknown():
    result = _run_iterate_hook(
        "buildGenreAwareRotation",
        {
            "book": {
                "title": "Unknown Text",
                "enrichment": {
                    "iconic_scenes": ["A mysterious scene"],
                },
            },
            "variantCount": 1,
        },
        prompts=TEST_PROMPTS,
    )

    assert result == [
        {
            "promptId": "alexandria-base-romantic-realism",
            "sceneOverride": "A mysterious scene",
        }
    ]

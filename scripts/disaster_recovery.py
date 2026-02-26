#!/usr/bin/env python3
"""CLI wrapper for snapshot/restore disaster recovery tooling."""

from __future__ import annotations

from src.disaster_recovery import main


if __name__ == "__main__":
    raise SystemExit(main())

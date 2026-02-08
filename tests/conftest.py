"""Configure pytest for local development.

This test suite imports the package directly from the repository's `src/` folder.
"""

from __future__ import annotations

import sys
from pathlib import Path


def _ensure_src_on_path() -> None:
    """Add the repository's `src/` directory to `sys.path` for imports."""
    repo_root = Path(__file__).resolve().parents[1]
    src = repo_root / "src"
    sys.path.insert(0, str(src))


_ensure_src_on_path()

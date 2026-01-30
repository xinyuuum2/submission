from __future__ import annotations

import sys
from pathlib import Path


def _ensure_flat_src_imports_work() -> None:
    """
    This project keeps modules directly under `src/` (e.g. `cli.py`, `db.py`).

    Evaluators often run `python -m src.main ...` from the repo root.
    In that mode, `src/` is treated as a package, so `import cli` would fail unless
    we also add the `src/` directory to sys.path.
    """

    src_dir = Path(__file__).resolve().parent
    src_str = str(src_dir)
    if src_str not in sys.path:
        sys.path.insert(0, src_str)


def main(argv: list[str] | None = None) -> int:
    _ensure_flat_src_imports_work()

    # Import after sys.path patch so `cli.py`'s flat imports work.
    from cli import main as cli_main  # noqa: PLC0415

    return int(cli_main(argv))


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

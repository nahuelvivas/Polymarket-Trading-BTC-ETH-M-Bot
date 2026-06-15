"""Windows launcher — run POLY-BTC-ETH-BOT.exe with config/default.yaml next to the exe."""

from __future__ import annotations

import os
import sys
from pathlib import Path


def app_root() -> Path:
    """Directory containing the exe (frozen) or project root (dev)."""
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parents[2]


def main() -> None:
    root = app_root()
    os.chdir(root)

    config = root / "config" / "default.yaml"
    if not config.is_file():
        print(f"ERROR: config not found: {config}")
        print("Place config/default.yaml next to POLY-BTC-ETH-BOT.exe and try again.")
        input("Press Enter to exit...")
        sys.exit(1)

    from polybot5m.cli import cli

    sys.argv = ["POLY-BTC-ETH-BOT", "run", "-c", str(config)]
    cli()


if __name__ == "__main__":
    main()

"""Allow the documented `python scripts/name.py` commands from any directory."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

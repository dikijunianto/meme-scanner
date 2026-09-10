"""Manual bounded replay; shares the writer lock and never changes either cursor."""
import argparse
import asyncio
from app.main import main


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from-block", type=int, required=True)
    parser.add_argument("--to-block", type=int, required=True)
    args = parser.parse_args()
    if not 0 <= args.from_block <= args.to_block:
        parser.error("Require 0 <= from-block <= to-block")
    return args.from_block, args.to_block


if __name__ == "__main__":
    selected = parse_args()
    try:
        asyncio.run(main(backfill=selected))
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    except Exception as exc:
        raise SystemExit(f"Backfill stopped ({type(exc).__name__}); inspect sanitized scanner log") from None

"""Explicit migrations and local submission/query commands."""
import argparse
import json
from pathlib import Path
from uuid import UUID

import psycopg

from monitoring.events import loads_json
from monitoring.store import EventStore, database_url, migrate


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("migrate")
    submit = commands.add_parser("submit")
    submit.add_argument("--event", type=Path, required=True)
    get = commands.add_parser("get")
    get.add_argument("--pipeline-id", required=True)
    get.add_argument("--run-id", type=UUID, required=True)
    listing = commands.add_parser("list")
    listing.add_argument("--pipeline-id")
    listing.add_argument("--status")
    listing.add_argument("--limit", type=int, default=20)
    listing.add_argument("--offset", type=int, default=0)
    args = parser.parse_args()
    try:
        dsn = database_url()
        store = EventStore(dsn)
        if args.command == "migrate":
            result = {"applied": migrate(dsn)}
        elif args.command == "submit":
            result = {"outcome": store.submit(loads_json(args.event.read_text(encoding="utf-8")))}
        elif args.command == "get":
            result = store.get(args.pipeline_id, str(args.run_id))
            if result is None:
                parser.exit(1, "Execution not found\n")
        else:
            result = store.list(pipeline_id=args.pipeline_id, status=args.status, limit=args.limit, offset=args.offset)
    except psycopg.Error:
        parser.exit(2, "Database operation failed; check configuration, server availability and migrations.\n")
    except (ValueError, OSError) as error:
        parser.exit(2, f"Monitoring command failed: {error}\n")
    print(json.dumps(result, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()

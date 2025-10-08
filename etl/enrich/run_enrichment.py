import argparse
import logging
import os
import sys

from dotenv import load_dotenv

from .client import load_client_from_env
from .runner import EnrichmentRunner


def build_pg_config() -> dict[str, str]:
    return {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": os.getenv("POSTGRES_PORT", "5432"),
        "dbname": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run LLM enrichment for CustomerTarget businesses.")
    parser.add_argument("--limit", type=int, default=50, help="Maximum number of businesses to enrich in this run.")
    parser.add_argument("--force", action="store_true", help="Ignore TTL and re-enrich even if data is recent.")
    parser.add_argument("--ttl-days", type=int, default=int(os.getenv("ENRICHMENT_TTL_DAYS", "30")),
                        help="Days before an enrichment is considered stale.")
    parser.add_argument("--dry-run", action="store_true", help="Show prompts without calling the provider.")
    parser.add_argument("--log-level", default=os.getenv("ENRICHMENT_LOG_LEVEL", "INFO"),
                        help="Logging level (DEBUG, INFO, ...).")
    return parser.parse_args()


def main() -> int:
    root_dir = os.path.dirname(os.path.dirname(__file__))
    load_dotenv(os.path.join(root_dir, "..", ".env"))

    args = parse_args()
    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    client = load_client_from_env()
    runner = EnrichmentRunner(pg=build_pg_config(), client=client)
    runner.run(limit=args.limit, dry_run=args.dry_run, force=args.force, ttl_days=args.ttl_days)
    return 0


if __name__ == "__main__":
    sys.exit(main())

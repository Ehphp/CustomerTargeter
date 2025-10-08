from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
from dataclasses import dataclass

import psycopg2
from dotenv import load_dotenv

logger = logging.getLogger("automation.auto_refresh")


def load_env() -> None:
    project_root = os.path.dirname(__file__)
    env_path = os.path.abspath(os.path.join(project_root, "..", ".env"))
    load_dotenv(env_path)


def build_pg_config() -> dict[str, str]:
    return {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": os.getenv("POSTGRES_PORT", "5432"),
        "dbname": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
    }


@dataclass
class StalenessReport:
    enrichment_candidates: int
    metrics_missing: int
    metrics_stale: int

    @property
    def metrics_needed(self) -> bool:
        return (self.metrics_missing + self.metrics_stale) > 0


def compute_staleness(conn: psycopg2.extensions.connection, ttl_days: int) -> StalenessReport:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)
            FROM places_clean p
            LEFT JOIN business_facts bf ON bf.business_id = p.place_id
            WHERE bf.business_id IS NULL OR bf.updated_at < now() - %s::interval
            """,
            (f"{ttl_days} days",),
        )
        enrichment_candidates = int(cur.fetchone()[0])

        cur.execute(
            """
            SELECT
              SUM(CASE WHEN bm.business_id IS NULL THEN 1 ELSE 0 END) AS missing_metrics,
              SUM(
                CASE
                  WHEN bm.business_id IS NOT NULL
                   AND bf.updated_at IS NOT NULL
                   AND bm.updated_at < bf.updated_at THEN 1
                  ELSE 0
                END
              ) AS stale_metrics
            FROM places_clean p
            LEFT JOIN business_metrics bm ON bm.business_id = p.place_id
            LEFT JOIN business_facts bf ON bf.business_id = p.place_id
            """,
        )
        row = cur.fetchone()
        metrics_missing = int(row[0] or 0)
        metrics_stale = int(row[1] or 0)

    return StalenessReport(
        enrichment_candidates=enrichment_candidates,
        metrics_missing=metrics_missing,
        metrics_stale=metrics_stale,
    )


def run_enrichment(limit: int, ttl_days: int | None, force: bool) -> None:
    cmd = [sys.executable, "-m", "etl.enrich.run_enrichment", "--limit", str(limit)]
    if ttl_days is not None:
        cmd.extend(["--ttl-days", str(ttl_days)])
    if force:
        cmd.append("--force")
    logger.info("Launching enrichment: %s", " ".join(cmd))
    subprocess.run(cmd, check=True)


def run_metrics_builder() -> None:
    cmd = [sys.executable, "-m", "feature_builder.build_metrics"]
    logger.info("Launching metrics builder: %s", " ".join(cmd))
    subprocess.run(cmd, check=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run enrichment and metrics builders only when data is stale."
    )
    parser.add_argument(
        "--enrich-limit",
        type=int,
        default=int(os.getenv("AUTO_REFRESH_ENRICH_LIMIT", "100")),
        help="Limit for enrichment batch size.",
    )
    parser.add_argument(
        "--enrich-ttl-days",
        type=int,
        default=None,
        help="Override TTL in days for enrichment staleness checks.",
    )
    parser.add_argument(
        "--force-enrichment",
        action="store_true",
        help="Skip staleness checks and always run the enrichment step.",
    )
    parser.add_argument(
        "--always-run-metrics",
        action="store_true",
        help="Run metrics builder even when no stale rows are detected.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only report what would run without executing external commands.",
    )
    parser.add_argument(
        "--log-level",
        default=os.getenv("AUTO_REFRESH_LOG_LEVEL", "INFO"),
        help="Logging level (DEBUG, INFO, ...).",
    )
    return parser.parse_args()


def main() -> int:
    load_env()
    args = parse_args()

    logging.basicConfig(level=args.log_level.upper(), format="%(asctime)s [%(levelname)s] %(message)s")

    ttl_env = os.getenv("ENRICHMENT_TTL_DAYS", "30")
    ttl_override = args.enrich_ttl_days or os.getenv("AUTO_REFRESH_ENRICH_TTL")
    ttl_days = int(ttl_override or ttl_env)

    pg = build_pg_config()
    try:
        with psycopg2.connect(**pg) as conn:
            report = compute_staleness(conn, ttl_days)
    except psycopg2.OperationalError as exc:
        logger.error("Failed to connect to Postgres: %s", exc)
        return 1

    logger.info(
        "Staleness report — enrichment:%d metrics_missing:%d metrics_stale:%d",
        report.enrichment_candidates,
        report.metrics_missing,
        report.metrics_stale,
    )

    should_run_enrichment = args.force_enrichment or report.enrichment_candidates > 0
    should_run_metrics = args.always_run_metrics or report.metrics_needed or should_run_enrichment

    if args.dry_run:
        logger.info(
            "Dry run: enrichment=%s metrics=%s",
            "yes" if should_run_enrichment else "no",
            "yes" if should_run_metrics else "no",
        )
        return 0

    try:
        if should_run_enrichment:
            run_enrichment(args.enrich_limit, ttl_days if not args.force_enrichment else None, args.force_enrichment)
        else:
            logger.info("Skipping enrichment: data is within TTL.")

        if should_run_metrics:
            run_metrics_builder()
        else:
            logger.info("Skipping metrics builder: metrics already up to date.")
    except subprocess.CalledProcessError as exc:
        logger.error("Command failed with exit code %s", exc.returncode)
        return exc.returncode

    return 0


if __name__ == "__main__":
    sys.exit(main())

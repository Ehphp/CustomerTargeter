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


def _env_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


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


def run_enrichment_batches(
    pg: dict[str, str],
    initial_report: StalenessReport,
    ttl_days: int,
    limit: int,
    force: bool,
    max_batches: int,
    metrics_each_batch: bool,
) -> tuple[StalenessReport, bool]:
    remaining = initial_report.enrichment_candidates
    batch = 0
    metrics_ran = False
    current_report = initial_report

    while True:
        if not force and remaining <= 0:
            break
        if max_batches and batch >= max_batches:
            logger.info("Reached max enrichment batches (%d); stopping loop.", max_batches)
            break

        batch += 1
        logger.info("=== Enrichment batch %d (limit=%d) ===", batch, limit)
        run_enrichment(limit, ttl_days if not force else None, force)

        if metrics_each_batch:
            logger.info("Running metrics builder after batch %d", batch)
            run_metrics_builder()
            metrics_ran = True

        with psycopg2.connect(**pg) as conn:
            current_report = compute_staleness(conn, ttl_days)
        remaining = current_report.enrichment_candidates
        logger.info(
            "Staleness after batch %d -> enrichment_candidates=%d, metrics_missing=%d, metrics_stale=%d",
            batch,
            current_report.enrichment_candidates,
            current_report.metrics_missing,
            current_report.metrics_stale,
        )

        if not force and remaining <= 0:
            break
        if force and remaining <= 0:
            break

    return current_report, metrics_ran


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
    parser.add_argument(
        "--max-enrich-batches",
        type=int,
        default=int(os.getenv("AUTO_REFRESH_MAX_ENRICH_BATCHES", "0")),
        help="Numero massimo di batch consecutivi di enrichment da eseguire (0 = finché ci sono candidati).",
    )
    parser.add_argument(
        "--metrics-each-batch",
        action="store_true",
        default=_env_flag("AUTO_REFRESH_METRICS_EACH_BATCH"),
        help="Esegue il calcolo delle metriche dopo ogni batch di enrichment.",
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
        metrics_ran_during_batches = False
        if should_run_enrichment:
            report, metrics_ran_during_batches = run_enrichment_batches(
                pg=pg,
                initial_report=report,
                ttl_days=ttl_days,
                limit=args.enrich_limit,
                force=args.force_enrichment,
                max_batches=args.max_enrich_batches,
                metrics_each_batch=args.metrics_each_batch,
            )
            if not args.force_enrichment:
                logger.info("Remaining enrichment candidates after loop: %d", report.enrichment_candidates)
        else:
            logger.info("Skipping enrichment: data is within TTL.")

        if should_run_metrics and not (args.metrics_each_batch and metrics_ran_during_batches):
            run_metrics_builder()
        elif should_run_metrics:
            logger.info("Metrics already executed during enrichment batches.")
        else:
            logger.info("Skipping metrics builder: metrics already up to date.")
    except subprocess.CalledProcessError as exc:
        logger.error("Command failed with exit code %s", exc.returncode)
        return exc.returncode

    return 0


if __name__ == "__main__":
    sys.exit(main())

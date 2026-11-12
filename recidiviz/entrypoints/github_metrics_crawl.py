# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""Entrypoint that crawls pull request history for the engineering metrics repos
from the GitHub GraphQL API into the github_api_raw tables.

Runs nightly as the github-metrics-crawl Cloud Run job. A repo with no rows yet
starts from --since, so the first run in a project, or the first run after a repo
joins the allowlist, loads its history and may take more than one run to finish.
For a local check that writes no BigQuery rows, set GITHUB_METRICS_TOKEN and run:

    uv run python -m recidiviz.entrypoints.github_metrics_crawl \\
        --since 2026-10-01 --repo Recidiviz/claude-skills --output-dir /tmp/crawl
"""
import argparse
import logging
import uuid
from datetime import date, datetime, timezone
from pathlib import Path

from recidiviz.big_query.big_query_client import BigQueryClient, BigQueryClientImpl
from recidiviz.entrypoints.entrypoint_interface import EntrypointInterface
from recidiviz.github.metrics.crawler import DEFAULT_CRAWL_SINCE, GitHubMetricsCrawler
from recidiviz.github.metrics.credentials import resolve_github_client
from recidiviz.github.metrics.graphql_client import GitHubGraphQLClient
from recidiviz.github.metrics.row_sinks import BigQueryRowSink, NdjsonRowSink, RowSink
from recidiviz.github.metrics.rows import CrawlContext
from recidiviz.monitoring.eng_metrics.exclusions import ENG_METRICS_REPO_ALLOWLIST
from recidiviz.utils.environment import GCP_PROJECT_STAGING, in_development
from recidiviz.utils.metadata import set_development_project_id_override


class GitHubMetricsCrawl(EntrypointInterface):
    """Entrypoint that crawls pull requests for the engineering metrics repos into
    the github_api_raw tables in the current GCP project."""

    @staticmethod
    def get_parser() -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--since",
            type=date.fromisoformat,
            default=DEFAULT_CRAWL_SINCE,
            help="First update date crawled for a repo with no rows in "
            "pull_requests yet, as YYYY-MM-DD. Ignored for a repo that has rows.",
        )
        parser.add_argument(
            "--repo",
            action="append",
            choices=ENG_METRICS_REPO_ALLOWLIST,
            default=None,
            help="Repo to crawl as owner/name. Repeat for several. Defaults to "
            "every repo in the engineering metrics allowlist.",
        )
        parser.add_argument(
            "--output-dir",
            type=Path,
            default=None,
            help="Write one newline-delimited JSON file per table here instead of "
            "loading BigQuery.",
        )
        return parser

    @staticmethod
    def run_entrypoint(*, args: argparse.Namespace) -> None:
        """Crawls the requested repos."""
        ctx = CrawlContext(
            crawl_id=str(uuid.uuid4()), crawled_at=datetime.now(timezone.utc)
        )
        bq_client: BigQueryClient | None
        sink: RowSink
        if args.output_dir is not None:
            bq_client = None
            sink = NdjsonRowSink(args.output_dir)
        else:
            bq_client = BigQueryClientImpl()
            sink = BigQueryRowSink(bq_client)
        repositories = args.repo or list(ENG_METRICS_REPO_ALLOWLIST)
        logging.info("Starting crawl %s of %s", ctx.crawl_id, repositories)
        crawler = GitHubMetricsCrawler(
            client=GitHubGraphQLClient(resolve_github_client()),
            bq_client=bq_client,
            sink=sink,
            repositories=repositories,
            ctx=ctx,
        )
        crawler.crawl(args.since)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    if in_development():
        set_development_project_id_override(GCP_PROJECT_STAGING)

    GitHubMetricsCrawl.run_entrypoint(args=GitHubMetricsCrawl.get_parser().parse_args())

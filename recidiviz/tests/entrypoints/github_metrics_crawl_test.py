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
"""Tests for recidiviz/entrypoints/github_metrics_crawl.py"""
from datetime import date
from pathlib import Path
from typing import Iterator
from unittest import mock

import attr
import pytest

from recidiviz.entrypoints.github_metrics_crawl import GitHubMetricsCrawl
from recidiviz.github.metrics.row_sinks import NdjsonRowSink
from recidiviz.monitoring.eng_metrics.exclusions import ENG_METRICS_REPO_ALLOWLIST

_MODULE = "recidiviz.entrypoints.github_metrics_crawl"


@attr.define
class _Patches:
    resolve_github_client: mock.MagicMock
    graphql_client: mock.MagicMock
    bq_client: mock.MagicMock
    crawler: mock.MagicMock


@pytest.fixture(name="patches")
def patches_fixture() -> Iterator[_Patches]:
    with mock.patch(f"{_MODULE}.resolve_github_client") as resolve, mock.patch(
        f"{_MODULE}.GitHubGraphQLClient"
    ) as graphql_client, mock.patch(
        f"{_MODULE}.BigQueryClientImpl"
    ) as bq_client, mock.patch(
        f"{_MODULE}.GitHubMetricsCrawler"
    ) as crawler:
        yield _Patches(resolve, graphql_client, bq_client, crawler)


def _run(argv: list[str]) -> None:
    GitHubMetricsCrawl.run_entrypoint(
        args=GitHubMetricsCrawl.get_parser().parse_args(argv)
    )


def test_parser_rejects_repo_outside_allowlist() -> None:
    with pytest.raises(SystemExit):
        GitHubMetricsCrawl.get_parser().parse_args(
            ["--repo", "Recidiviz/synthetic-repo"]
        )


def test_parser_rejects_mode() -> None:
    with pytest.raises(SystemExit):
        GitHubMetricsCrawl.get_parser().parse_args(["--mode", "incremental"])


def test_parser_defaults() -> None:
    args = GitHubMetricsCrawl.get_parser().parse_args([])

    assert args.since == date(2025, 5, 1)
    assert args.repo is None
    assert args.output_dir is None


def test_run_entrypoint_crawls_from_since(patches: _Patches) -> None:
    _run(["--since", "2026-10-01"])

    patches.crawler.return_value.crawl.assert_called_once_with(date(2026, 10, 1))


def test_output_dir_uses_ndjson_sink_and_no_bq_client(
    patches: _Patches, tmp_path: Path
) -> None:
    _run(["--output-dir", str(tmp_path)])

    kwargs = patches.crawler.call_args.kwargs
    assert kwargs["bq_client"] is None
    assert isinstance(kwargs["sink"], NdjsonRowSink)
    assert kwargs["repositories"] == ENG_METRICS_REPO_ALLOWLIST
    patches.bq_client.assert_not_called()
    patches.crawler.return_value.crawl.assert_called_once_with(date(2025, 5, 1))

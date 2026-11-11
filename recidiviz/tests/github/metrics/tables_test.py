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
"""Tests for the github_api_raw table addresses."""
import os

from recidiviz.github.metrics.tables import (
    ALL_TABLE_ADDRESSES,
    CHILD_TABLE_ADDRESSES,
    CRAWL_RUNS_ADDRESS,
    PULL_REQUESTS_ADDRESS,
)
from recidiviz.source_tables import yaml_managed
from recidiviz.source_tables.source_table_config import SourceTableConfig
from recidiviz.source_tables.yaml_managed.datasets import GITHUB_API_RAW_DATASET


def _config(table_id: str) -> SourceTableConfig:
    return SourceTableConfig.from_file(
        os.path.join(
            os.path.dirname(yaml_managed.__file__),
            GITHUB_API_RAW_DATASET,
            f"{table_id}.yaml",
        )
    )


def test_every_table_yaml_matches_tables_module() -> None:
    for address in ALL_TABLE_ADDRESSES:
        assert address.dataset_id == GITHUB_API_RAW_DATASET
        config = _config(address.table_id)
        assert config.address == address
        names = [field.name for field in config.schema_fields]
        assert names[:4] == [
            "repository_full_name",
            "pr_number",
            "crawl_id",
            "crawled_at",
        ]
        assert names[-1] == "raw"
        assert config.schema_fields[-1].field_type == "JSON"
        assert config.time_partitioning is not None
        assert config.time_partitioning.field == "crawled_at"
        assert config.require_partition_filter is False


def test_tables_load_children_first_and_pull_requests_last() -> None:
    assert [address.table_id for address in ALL_TABLE_ADDRESSES] == [
        "reviews",
        "review_comments",
        "issue_comments",
        "timeline_items",
        "commits",
        "files",
        "pull_requests",
    ]
    assert ALL_TABLE_ADDRESSES[:-1] == CHILD_TABLE_ADDRESSES
    assert ALL_TABLE_ADDRESSES[-1] == PULL_REQUESTS_ADDRESS


def test_crawl_runs_yaml_matches_tables_module() -> None:
    assert CRAWL_RUNS_ADDRESS not in ALL_TABLE_ADDRESSES
    config = _config(CRAWL_RUNS_ADDRESS.table_id)

    assert config.address == CRAWL_RUNS_ADDRESS
    assert [(f.name, f.field_type, f.mode) for f in config.schema_fields] == [
        ("crawl_id", "STRING", "REQUIRED"),
        ("mode", "STRING", "REQUIRED"),
        ("started_at", "TIMESTAMP", "REQUIRED"),
        ("finished_at", "TIMESTAMP", "REQUIRED"),
        ("repositories_requested", "INTEGER", "REQUIRED"),
        ("repositories_crawled", "INTEGER", "REQUIRED"),
        ("pull_requests_crawled", "INTEGER", "REQUIRED"),
    ]
    assert config.time_partitioning is not None
    assert config.time_partitioning.field == "started_at"
    assert config.require_partition_filter is False

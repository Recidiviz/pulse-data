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
"""Addresses of the github_api_raw tables that the GitHub metrics crawl writes.

The crawl writes one row per pull request to pull_requests and one row per
review, comment, timeline item, commit and changed file to the six child tables.
When a run finishes every repo, it writes one row to crawl_runs.
"""
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.source_tables.yaml_managed.datasets import GITHUB_API_RAW_DATASET

PULL_REQUESTS_ADDRESS = BigQueryAddress(
    dataset_id=GITHUB_API_RAW_DATASET, table_id="pull_requests"
)

REVIEWS_ADDRESS = BigQueryAddress(dataset_id=GITHUB_API_RAW_DATASET, table_id="reviews")
REVIEW_COMMENTS_ADDRESS = BigQueryAddress(
    dataset_id=GITHUB_API_RAW_DATASET, table_id="review_comments"
)
ISSUE_COMMENTS_ADDRESS = BigQueryAddress(
    dataset_id=GITHUB_API_RAW_DATASET, table_id="issue_comments"
)
TIMELINE_ITEMS_ADDRESS = BigQueryAddress(
    dataset_id=GITHUB_API_RAW_DATASET, table_id="timeline_items"
)
COMMITS_ADDRESS = BigQueryAddress(dataset_id=GITHUB_API_RAW_DATASET, table_id="commits")
FILES_ADDRESS = BigQueryAddress(dataset_id=GITHUB_API_RAW_DATASET, table_id="files")

# The crawl loads these before PULL_REQUESTS_ADDRESS, so a pull_requests row never
# lands without the child rows from the same crawl.
CHILD_TABLE_ADDRESSES: list[BigQueryAddress] = [
    REVIEWS_ADDRESS,
    REVIEW_COMMENTS_ADDRESS,
    ISSUE_COMMENTS_ADDRESS,
    TIMELINE_ITEMS_ADDRESS,
    COMMITS_ADDRESS,
    FILES_ADDRESS,
]

ALL_TABLE_ADDRESSES: list[BigQueryAddress] = [
    *CHILD_TABLE_ADDRESSES,
    PULL_REQUESTS_ADDRESS,
]

# One row per completed run. Not a pull request snapshot table, so it is in neither
# list above.
CRAWL_RUNS_ADDRESS = BigQueryAddress(
    dataset_id=GITHUB_API_RAW_DATASET, table_id="crawl_runs"
)

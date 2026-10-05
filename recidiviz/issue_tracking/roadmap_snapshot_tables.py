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
"""Addresses and shared column names for the launch momentum roadmap snapshot
tables: linear_snapshots.linear_project_snapshots, written from the Linear API,
and notion_snapshots.notion_roadmap_snapshots, uploaded from a Notion CSV
export. The notion_linear_roadmap_latest view joins the two on
LINEAR_PROJECT_SLUG_ID_COLUMN. Column names used by only one module live in
that module.

This module only defines constants, so view and entrypoint code can import it
without depending on the code that builds and writes the snapshots.
"""
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.source_tables.yaml_managed.datasets import (
    LINEAR_SNAPSHOTS_DATASET,
    NOTION_SNAPSHOTS_DATASET,
)

LINEAR_PROJECT_SNAPSHOTS_ADDRESS = BigQueryAddress(
    dataset_id=LINEAR_SNAPSHOTS_DATASET, table_id="linear_project_snapshots"
)
NOTION_ROADMAP_SNAPSHOTS_ADDRESS = BigQueryAddress(
    dataset_id=NOTION_SNAPSHOTS_DATASET, table_id="notion_roadmap_snapshots"
)

# Columns that appear in both snapshot tables.
LINEAR_PROJECT_SLUG_ID_COLUMN = "linear_project_slug_id"
SNAPSHOT_DATE_COLUMN = "snapshot_date"
UPLOAD_DATETIME_COLUMN = "upload_datetime"

# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Helpers for generating datasets related to raw state data."""
from typing import Optional


def raw_tables_dataset_for_region(
    region_code: str, sandbox_dataset_prefix: Optional[str]
) -> str:
    """Returns the dataset containing raw data tables for this region."""
    prefix = f"{sandbox_dataset_prefix}_" if sandbox_dataset_prefix else ""
    return f"{prefix}{region_code.lower()}_raw_data"


def raw_latest_views_dataset_for_region(
    region_code: str, sandbox_dataset_prefix: Optional[str]
) -> str:
    """Returns the dataset containing raw data "latest" views for this region."""
    prefix = f"{sandbox_dataset_prefix}_" if sandbox_dataset_prefix else ""
    return f"{prefix}{region_code.lower()}_raw_data_up_to_date_views"

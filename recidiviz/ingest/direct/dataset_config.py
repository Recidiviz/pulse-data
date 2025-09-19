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

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance


def raw_tables_dataset_for_region(
    state_code: StateCode,
    instance: DirectIngestInstance,
    sandbox_dataset_prefix: Optional[str] = None,
) -> str:
    """Returns the dataset containing raw data tables for this region."""
    suffix = "_secondary" if instance == DirectIngestInstance.SECONDARY else ""
    prefix = f"{sandbox_dataset_prefix}_" if sandbox_dataset_prefix else ""
    return f"{prefix}{state_code.value.lower()}_raw_data{suffix}"


def raw_latest_views_dataset_for_region(
    state_code: StateCode,
    instance: DirectIngestInstance,
    sandbox_dataset_prefix: Optional[str] = None,
) -> str:
    """Returns the dataset containing raw data "latest" views for this region."""
    suffix = "_secondary" if instance == DirectIngestInstance.SECONDARY else ""
    prefix = f"{sandbox_dataset_prefix}_" if sandbox_dataset_prefix else ""
    return f"{prefix}{state_code.value.lower()}_raw_data_up_to_date_views{suffix}"


def raw_data_pruning_new_raw_data_dataset(
    state_code: StateCode, instance: DirectIngestInstance
) -> str:
    """Returns the dataset containing new raw data temporarily housed in BQ for raw data
    diff queries that used in raw data pruning."""
    return f"{state_code.value.lower()}_new_pruned_raw_data_{instance.value.lower()}"


def raw_data_pruning_raw_data_diff_results_dataset(
    state_code: StateCode,
    instance: DirectIngestInstance,
    sandbox_dataset_prefix: Optional[str] = None,
) -> str:
    """Returns the dataset containing the temporary results of raw data diff queries
    that are used in raw data pruning.
    """
    prefix = f"{sandbox_dataset_prefix}_" if sandbox_dataset_prefix else ""
    return f"{prefix}{state_code.value.lower()}_raw_data_pruning_diff_results_{instance.value.lower()}"


def raw_data_temp_load_dataset(
    state_code: StateCode,
    instance: DirectIngestInstance,
    sandbox_dataset_prefix: Optional[str] = None,
) -> str:
    """Returns the dataset containing the temporary raw file load results"""
    prefix = f"{sandbox_dataset_prefix}_" if sandbox_dataset_prefix else ""
    return f"{prefix}{state_code.value.lower()}_{instance.value.lower()}_raw_data_temp_load"

# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Helpers for getting ingest pipeline output datasets."""
from typing import Optional

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.common.constants.states import StateCode


def ingest_view_materialization_results_dataset(
    state_code: StateCode, sandbox_dataset_prefix: Optional[str] = None
) -> str:
    """Returns the dataset of the ingest view results Dataflow tables for this state's
    ingest pipeline.
    """
    dataset_prefix = f"{sandbox_dataset_prefix}_" if sandbox_dataset_prefix else ""
    return f"{dataset_prefix}{state_code.value.lower()}_ingest_view_results"


def state_dataset_for_state_code(
    state_code: StateCode, sandbox_dataset_prefix: Optional[str] = None
) -> str:
    """Where the output of the state-specific ingest pipelines is stored."""
    dataset_prefix = f"{sandbox_dataset_prefix}_" if sandbox_dataset_prefix else ""
    return f"{dataset_prefix}{state_code.value.lower()}_state"


def normalized_state_dataset_for_state_code(
    state_code: StateCode, sandbox_dataset_prefix: str | None = None
) -> str:
    """Where the output of state-specific ingest pipeline normalized entities output is
    stored.
    """
    base_dataset = f"{state_code.value.lower()}_normalized_state"
    if not sandbox_dataset_prefix:
        return base_dataset
    return BigQueryAddressOverrides.format_sandbox_dataset(
        sandbox_dataset_prefix, base_dataset
    )

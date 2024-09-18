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
"""Helpers for getting pipeline output datasets for normalized entities."""
from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.common.constants.states import StateCode


# TODO(#31741): Move this function to recidiviz/pipelines/ingest once we've shipped
#  combined ingest and normalization.
def normalized_state_dataset_for_state_code_ingest_pipeline_output(
    state_code: StateCode, sandbox_dataset_prefix: str | None = None
) -> str:
    """Where the output of state-specific ingest pipeline normalized entities output is
    stored.
    """
    # TODO(#31741): Rename this back to `us_xx_normalized_state` once we're only reading
    #  from this dataset for all states and have deleted the legacy
    #  `us_xx_normalized_state` source table collections.
    base_dataset = f"{state_code.value.lower()}_normalized_state_new"
    if not sandbox_dataset_prefix:
        return base_dataset
    return BigQueryAddressOverrides.format_sandbox_dataset(
        sandbox_dataset_prefix, base_dataset
    )

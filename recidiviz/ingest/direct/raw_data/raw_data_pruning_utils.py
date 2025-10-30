#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2025 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""Utilities for raw data pruning operations."""
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.gating import (
    file_tag_exempt_from_automatic_raw_data_pruning,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import DirectIngestRawFileConfig
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance


def automatic_raw_data_pruning_enabled_for_file_config(
    state_code: StateCode,
    raw_data_instance: DirectIngestInstance,
    raw_file_config: DirectIngestRawFileConfig,
) -> bool:
    """Boolean return for if automatic raw data pruning can be run for the given raw |file_tag|."""
    # TODO(#12390): Delete once raw data pruning is live.
    file_tag_is_exempt = file_tag_exempt_from_automatic_raw_data_pruning(
        state_code, raw_data_instance, raw_file_config.file_tag
    )
    if file_tag_is_exempt:
        return False

    return raw_file_config.eligible_for_automatic_raw_data_pruning()

# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Contains factory class for creating StateRawFileChunkingMetadata objects"""
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.raw_file_chunking_metadata_history import (
    StateRawFileChunkingMetadata,
)
from recidiviz.ingest.direct.regions.us_tn.us_tn_raw_file_chunking_metadata import (
    US_TN_CHUNKING_METADATA_BY_FILE_TAG,
)
from recidiviz.ingest.direct.regions.us_ut.us_ut_raw_file_chunking_metadata import (
    US_UT_CHUNKING_METADATA_BY_FILE_TAG,
)

_STATES_WITH_NO_CHUNKED_FILES: set[StateCode] = {
    StateCode.US_AR,
    StateCode.US_AZ,
    StateCode.US_CA,
    StateCode.US_CO,
    StateCode.US_IA,
    StateCode.US_ID,
    StateCode.US_IX,
    StateCode.US_MA,
    StateCode.US_ME,
    StateCode.US_MI,
    StateCode.US_MO,
    StateCode.US_NC,
    StateCode.US_ND,
    StateCode.US_NE,
    StateCode.US_NY,
    StateCode.US_OR,
    StateCode.US_OZ,
    StateCode.US_PA,
    StateCode.US_TX,
}


class StateRawFileChunkingMetadataFactory:
    """Factory class for creating StateRawFileChunkingMetadata objects"""

    @classmethod
    def build(cls, *, region_code: str) -> StateRawFileChunkingMetadata:
        """Builds a StateRawFileChunkingMetadata configured for the provided |region_code|.

        Returns:
            A StateRawFileChunkingMetadata configured with the appropriate chunking metadata
            for the given state, or with None if the state has no chunking metadata.
        """
        state_code = StateCode(region_code.upper())

        if state_code == StateCode.US_TN:
            return StateRawFileChunkingMetadata(
                state_code=state_code,
                chunking_metadata_by_file_tag=US_TN_CHUNKING_METADATA_BY_FILE_TAG,
            )
        if state_code == StateCode.US_UT:
            return StateRawFileChunkingMetadata(
                state_code=state_code,
                chunking_metadata_by_file_tag=US_UT_CHUNKING_METADATA_BY_FILE_TAG,
            )

        if state_code in _STATES_WITH_NO_CHUNKED_FILES:
            return StateRawFileChunkingMetadata(
                state_code=state_code, chunking_metadata_by_file_tag=None
            )

        raise ValueError(f"Unexpected region code provided: [{region_code}]")

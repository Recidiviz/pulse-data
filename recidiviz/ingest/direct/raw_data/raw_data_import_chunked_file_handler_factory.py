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
"""Contains factory class for creating RawDataImportChunkedFileHandler objects"""
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.raw_data_import_chunked_file_handler import (
    RawDataImportChunkedFileHandler,
)
from recidiviz.ingest.direct.regions.us_tn.us_tn_raw_file_chunking_metadata import (
    US_TN_CHUNKING_METADATA_BY_FILE_TAG,
)
from recidiviz.ingest.direct.regions.us_ut.us_ut_raw_file_chunking_metadata import (
    US_UT_CHUNKING_METADATA_BY_FILE_TAG,
)

_STATES_WITH_NO_CHUNKED_FILES: set[str] = {
    StateCode.US_AR.value,
    StateCode.US_AZ.value,
    StateCode.US_CA.value,
    StateCode.US_CO.value,
    StateCode.US_IA.value,
    StateCode.US_ID.value,
    StateCode.US_IX.value,
    StateCode.US_MA.value,
    StateCode.US_ME.value,
    StateCode.US_MI.value,
    StateCode.US_MO.value,
    StateCode.US_NC.value,
    StateCode.US_ND.value,
    StateCode.US_NE.value,
    StateCode.US_NY.value,
    StateCode.US_OR.value,
    StateCode.US_OZ.value,
    StateCode.US_PA.value,
    StateCode.US_TX.value,
}


class RawDataImportChunkedFileHandlerFactory:
    """Factory class for creating RawDataImportChunkedFileHandler objects"""

    @classmethod
    def build(cls, *, region_code: str) -> RawDataImportChunkedFileHandler:
        """Builds a RawDataImportChunkedFileHandler configured for the provided |region_code|.

        Returns:
            A RawDataImportChunkedFileHandler configured with the appropriate chunking metadata
            for the given state, or with None if the state has no chunking metadata.
        """
        region_code = region_code.upper()

        if region_code == StateCode.US_TN.value:
            return RawDataImportChunkedFileHandler(
                chunking_metadata_by_file_tag=US_TN_CHUNKING_METADATA_BY_FILE_TAG
            )
        if region_code == StateCode.US_UT.value:
            return RawDataImportChunkedFileHandler(
                chunking_metadata_by_file_tag=US_UT_CHUNKING_METADATA_BY_FILE_TAG
            )

        if region_code in _STATES_WITH_NO_CHUNKED_FILES:
            return RawDataImportChunkedFileHandler(chunking_metadata_by_file_tag=None)

        raise ValueError(f"Unexpected region code provided: [{region_code}]")

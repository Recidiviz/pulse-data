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
"""Factory to create raw table validations."""
import datetime

import attr

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.raw_file_configs import DirectIngestRawFileConfig
from recidiviz.ingest.direct.raw_data.validations.distinct_primary_key_table_validation import (
    DistinctPrimaryKeyTableValidation,
)
from recidiviz.ingest.direct.raw_data.validations.stable_historical_raw_data_counts_table_validation import (
    StableHistoricalRawDataCountsTableValidation,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.raw_data_import_blocking_validation import (
    RawDataImportBlockingValidation,
)
from recidiviz.ingest.direct.types.raw_data_import_blocking_validation_type import (
    RawDataImportBlockingValidationType,
)


@attr.define
class RawDataImportBlockingValidationFactory:
    """Factory for creating raw data import blocking validations."""

    @staticmethod
    def create_table_validation(
        validation_type: RawDataImportBlockingValidationType,
        *,
        state_code: StateCode,
        file_tag: str,
        project_id: str,
        temp_table_address: BigQueryAddress,
        raw_file_config: DirectIngestRawFileConfig,
        raw_data_instance: DirectIngestInstance,
        file_update_datetime: datetime.datetime,
    ) -> RawDataImportBlockingValidation:
        """Creates a RawDataImportBlockingValidation for the given table if applicable."""
        if validation_type == RawDataImportBlockingValidationType.DISTINCT_PRIMARY_KEYS:
            return DistinctPrimaryKeyTableValidation.create_table_validation(
                state_code=state_code,
                file_tag=file_tag,
                project_id=project_id,
                temp_table_address=temp_table_address,
                primary_key_cols=raw_file_config.primary_key_cols,
            )
        if (
            validation_type
            == RawDataImportBlockingValidationType.STABLE_HISTORICAL_RAW_DATA_COUNTS
        ):
            return StableHistoricalRawDataCountsTableValidation.create_table_validation(
                state_code=state_code,
                file_tag=file_tag,
                project_id=project_id,
                temp_table_address=temp_table_address,
                file_update_datetime=file_update_datetime,
                raw_data_instance=raw_data_instance,
            )

        raise ValueError(f"Unsupported table validation type: [{validation_type}]")

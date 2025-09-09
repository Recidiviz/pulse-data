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
"""Collects all applicable import-blocking validations for a given file_tag."""
import datetime
from typing import Type

import attr

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.raw_file_configs import DirectIngestRawFileConfig
from recidiviz.ingest.direct.raw_data.validations.datetime_parsers_validation import (
    DatetimeParsersValidation,
)
from recidiviz.ingest.direct.raw_data.validations.distinct_primary_key_validation import (
    DistinctPrimaryKeyValidation,
)
from recidiviz.ingest.direct.raw_data.validations.expected_type_validation import (
    ExpectedTypeValidation,
)
from recidiviz.ingest.direct.raw_data.validations.known_values_validation import (
    KnownValuesValidation,
)
from recidiviz.ingest.direct.raw_data.validations.nonnull_values_validation import (
    NonNullValuesValidation,
)
from recidiviz.ingest.direct.raw_data.validations.stable_historical_raw_data_counts_validation import (
    StableHistoricalRawDataCountsValidation,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.raw_data_import_blocking_validation import (
    BaseRawDataImportBlockingValidation,
    RawDataImportBlockingValidationContext,
)
from recidiviz.ingest.direct.types.raw_data_import_blocking_validation_type import (
    RawDataImportBlockingValidationType,
)

VALIDATION_TYPE_TO_CLASS: dict[
    RawDataImportBlockingValidationType, Type[BaseRawDataImportBlockingValidation]
] = {
    RawDataImportBlockingValidationType.DATETIME_PARSERS: DatetimeParsersValidation,
    RawDataImportBlockingValidationType.EXPECTED_TYPE: ExpectedTypeValidation,
    RawDataImportBlockingValidationType.KNOWN_VALUES: KnownValuesValidation,
    RawDataImportBlockingValidationType.NONNULL_VALUES: NonNullValuesValidation,
    RawDataImportBlockingValidationType.DISTINCT_PRIMARY_KEYS: DistinctPrimaryKeyValidation,
    RawDataImportBlockingValidationType.STABLE_HISTORICAL_RAW_DATA_COUNTS: StableHistoricalRawDataCountsValidation,
}


@attr.define
class RawDataImportBlockingValidationCollector:
    """Collect applicable import-blocking validations for a given file_tag."""

    @staticmethod
    def collect_validations_for_file(
        *,
        state_code: StateCode,
        file_tag: str,
        project_id: str,
        temp_table_address: BigQueryAddress,
        raw_file_config: DirectIngestRawFileConfig,
        raw_data_instance: DirectIngestInstance,
        file_update_datetime: datetime.datetime,
    ) -> list[BaseRawDataImportBlockingValidation]:
        """Collects and creates instances of all applicable raw data import blocking validations
        for a given file tag based on its configuration and the current file update datetime.
        """
        validation_context = RawDataImportBlockingValidationContext(
            state_code=state_code,
            file_tag=file_tag,
            project_id=project_id,
            temp_table_address=temp_table_address,
            raw_file_config=raw_file_config,
            raw_data_instance=raw_data_instance,
            file_update_datetime=file_update_datetime,
        )

        validations = []
        for validation_type in RawDataImportBlockingValidationType:
            if validation_type not in VALIDATION_TYPE_TO_CLASS:
                raise ValueError(f"Unsupported validation type: [{validation_type}]")
            validation_cls = VALIDATION_TYPE_TO_CLASS[validation_type]
            if validation_cls.validation_applies_to_file(validation_context):
                validations.append(validation_cls.create_validation(validation_context))

        return validations

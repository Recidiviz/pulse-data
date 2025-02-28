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
"""Run import-blocking validations on a raw data temp table in BigQuery."""
from concurrent import futures
from datetime import datetime, timezone
from typing import List, Type

from google.api_core import retry

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.big_query.big_query_job_labels import RawDataImportStepBQLabel
from recidiviz.big_query.big_query_utils import bq_query_job_result_to_list_of_row_dicts
from recidiviz.common.constants.states import StateCode
from recidiviz.common.retry_predicate import ssl_error_retry_predicate
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRegionRawFileConfig,
)
from recidiviz.ingest.direct.raw_data.validations.datetime_parsers_column_validation import (
    DatetimeParsersColumnValidation,
)
from recidiviz.ingest.direct.raw_data.validations.expected_type_column_validation import (
    ExpectedTypeColumnValidation,
)
from recidiviz.ingest.direct.raw_data.validations.known_values_column_validation import (
    KnownValuesColumnValidation,
)
from recidiviz.ingest.direct.raw_data.validations.nonnull_values_column_validation import (
    NonNullValuesColumnValidation,
)
from recidiviz.ingest.direct.raw_data.validations.stable_historical_raw_data_counts_table_validation import (
    StableHistoricalRawDataCountsTableValidation,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.raw_data_import_blocking_validation import (
    RawDataColumnImportBlockingValidation,
    RawDataImportBlockingValidation,
    RawDataImportBlockingValidationError,
    RawDataImportBlockingValidationFailure,
)

MAX_THREADS = 3  # TODO(#29946) determine reasonable default

COLUMN_VALIDATION_CLASSES: List[Type[RawDataColumnImportBlockingValidation]] = [
    NonNullValuesColumnValidation,
    KnownValuesColumnValidation,
    ExpectedTypeColumnValidation,
    DatetimeParsersColumnValidation,
]


class DirectIngestRawTablePreImportValidator:
    """Validator class responsible for executing import-blocking validations on raw data
    loaded into temporary BigQuery tables."""

    def __init__(
        self,
        region_raw_file_config: DirectIngestRegionRawFileConfig,
        raw_data_instance: DirectIngestInstance,
        region_code: str,
        project_id: str,
        big_query_client: BigQueryClient,
    ):
        self.region_raw_file_config = region_raw_file_config
        self.region_code = region_code
        self.raw_data_instance = raw_data_instance
        self.project_id = project_id
        self.big_query_client = big_query_client

    def _collect_validations_to_run(
        self,
        file_tag: str,
        file_update_datetime: datetime,
        temp_table_address: BigQueryAddress,
    ) -> List[RawDataImportBlockingValidation]:
        """Collect all validations to be run on the temp table.
        This method gathers both table-level and column-level validations, checking if
        they apply to the file and are not exempt based on the raw file configuration.
        Column validations are only collected for columns that are relevant at the given
        file update datetime, meaning they are present in the temp table and have not been
        since deleted from the raw table.
        """
        all_validations: List[RawDataImportBlockingValidation] = []
        raw_file_config = self.region_raw_file_config.raw_file_configs[file_tag]

        state_code = StateCode(self.region_code.upper())
        if not raw_file_config.file_is_exempt_from_validation(
            StableHistoricalRawDataCountsTableValidation.validation_type()
        ) and StableHistoricalRawDataCountsTableValidation.should_run_validation(
            raw_file_config, state_code, file_tag, file_update_datetime
        ):
            all_validations.append(
                StableHistoricalRawDataCountsTableValidation.create_table_validation(
                    file_tag,
                    self.project_id,
                    temp_table_address,
                    state_code,
                    self.raw_data_instance,
                    file_update_datetime,
                )
            )

        for column in raw_file_config.columns_at_datetime(file_update_datetime):
            if not column.name_at_datetime(datetime.now(tz=timezone.utc)):
                # We don't care about running this validation if the column doesn't exist in this file anymore
                continue
            for col_validation_cls in COLUMN_VALIDATION_CLASSES:
                if not raw_file_config.column_is_exempt_from_validation(
                    column.name, col_validation_cls.validation_type()
                ) and col_validation_cls.validation_applies_to_column(
                    column, raw_file_config
                ):
                    all_validations.append(
                        col_validation_cls.create_column_validation(
                            file_tag,
                            self.project_id,
                            temp_table_address,
                            file_update_datetime,
                            column,
                        )
                    )

        return all_validations

    def _execute_validation_queries_concurrently(
        self, validations_to_run: List[RawDataImportBlockingValidation]
    ) -> List[RawDataImportBlockingValidationFailure]:
        """Executes |validations_to_run| concurrently, returning any errors we encounter."""
        job_to_validation = {
            self.big_query_client.run_query_async(
                query_str=validation.query,
                use_query_cache=True,
                job_labels=[
                    RawDataImportStepBQLabel.RAW_DATA_PRE_IMPORT_VALIDATIONS.value
                ],
            ): validation
            for validation in validations_to_run
        }

        ssl_retry_policy = retry.Retry(predicate=ssl_error_retry_predicate)

        errors = []
        with futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            job_futures = {
                executor.submit(job.result, retry=ssl_retry_policy): validation_info
                for job, validation_info in job_to_validation.items()
            }
            for f in futures.as_completed(job_futures):
                validation_info: RawDataImportBlockingValidation = job_futures[f]

                error = validation_info.get_error_from_results(
                    bq_query_job_result_to_list_of_row_dicts(f.result())
                )
                if error:
                    errors.append(error)
        return errors

    def run_raw_data_temp_table_validations(
        self,
        file_tag: str,
        file_update_datetime: datetime,
        temp_table_address: BigQueryAddress,
    ) -> None:
        """Run all applicable validation queries against the temp raw table in BigQuery and
        raise a RawDataImportBlockingValidationError if any validations don't meet the success criteria.
        """

        validations_to_run: List[
            RawDataImportBlockingValidation
        ] = self._collect_validations_to_run(
            file_tag, file_update_datetime, temp_table_address
        )

        failures = self._execute_validation_queries_concurrently(validations_to_run)

        if failures:
            raise RawDataImportBlockingValidationError(file_tag, failures)

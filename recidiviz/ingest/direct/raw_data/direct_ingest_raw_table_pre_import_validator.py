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
"""Run pre-import validations on a raw data temp table in BigQuery."""

from concurrent import futures
from datetime import datetime

from google.api_core import retry

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.big_query.big_query_utils import bq_query_job_result_to_list_of_row_dicts
from recidiviz.cloud_resources.platform_resource_labels import (
    RawDataImportStepResourceLabel,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.common.retry_predicate import rate_limit_retry_predicate
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRegionRawFileConfig,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.raw_data_pre_import_validation import (
    BaseRawDataPreImportValidation,
    RawDataBlockingValidationFailure,
    RawDataNonBlockingValidationFailure,
    RawDataPreImportValidationError,
    RawDataPreImportValidationFailure,
)
from recidiviz.ingest.direct.types.raw_data_pre_import_validation_collector import (
    RawDataPreImportValidationCollector,
)
from recidiviz.ingest.direct.types.raw_data_pre_import_validation_type import (
    RawDataPreImportValidationType,
)
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION

MAX_THREADS = 8
DEFAULT_INITIAL_DELAY = 15.0  # 15 seconds
DEFAULT_MAXIMUM_DELAY = 60.0 * 2  # 2 minutes, in seconds
DEFAULT_TOTAL_TIMEOUT = 60.0 * 8  # 8 minutes, in seconds

NON_IMPORT_BLOCKING_VALIDATIONS_BY_PROJECT: dict[
    str, set[RawDataPreImportValidationType]
] = {
    GCP_PROJECT_PRODUCTION: set()  # TODO(#71014) Add RawDataPreImportValidationType.KNOWN_VALUES
}


class DirectIngestRawTablePreImportValidator:
    """Validator class responsible for executing pre-import validations on raw data
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

    def _execute_validation_queries_concurrently(
        self, validations_to_run: list[BaseRawDataPreImportValidation]
    ) -> list[RawDataPreImportValidationFailure]:
        """Executes |validations_to_run| concurrently, returning any errors we encounter."""
        job_to_validation = {
            self.big_query_client.run_query_async(
                query_str=validation.build_query(),
                use_query_cache=True,
                job_labels=[
                    RawDataImportStepResourceLabel.RAW_DATA_PRE_IMPORT_VALIDATIONS.value
                ],
            ): validation
            for validation in validations_to_run
        }

        rate_limit_retry_policy = retry.Retry(
            initial=DEFAULT_INITIAL_DELAY,
            maximum=DEFAULT_MAXIMUM_DELAY,
            timeout=DEFAULT_TOTAL_TIMEOUT,
            predicate=rate_limit_retry_predicate,
        )

        errors = []
        with futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            job_futures = {
                executor.submit(
                    job.result, retry=rate_limit_retry_policy
                ): validation_info
                for job, validation_info in job_to_validation.items()
            }
            for f in futures.as_completed(job_futures):
                validation_info: BaseRawDataPreImportValidation = job_futures[f]

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
    ) -> list[RawDataNonBlockingValidationFailure]:
        """Run all applicable validation queries against the temp raw table in BigQuery.
        Raises a RawDataPreImportValidationError if any blocking validation failures are found;
        otherwise returns a list of non-blocking validation failures.
        """

        validations_to_run: list[
            BaseRawDataPreImportValidation
        ] = RawDataPreImportValidationCollector.collect_validations_for_file(
            state_code=StateCode(self.region_code.upper()),
            file_tag=file_tag,
            project_id=self.project_id,
            temp_table_address=temp_table_address,
            raw_file_config=self.region_raw_file_config.raw_file_configs[file_tag],
            raw_data_instance=self.raw_data_instance,
            file_update_datetime=file_update_datetime,
        )

        failures = self._execute_validation_queries_concurrently(validations_to_run)

        blocking_failures: list[RawDataBlockingValidationFailure] = []
        non_blocking_failures: list[RawDataNonBlockingValidationFailure] = []
        for failure in failures:
            if (
                self.project_id in NON_IMPORT_BLOCKING_VALIDATIONS_BY_PROJECT
                and failure.validation_type
                in NON_IMPORT_BLOCKING_VALIDATIONS_BY_PROJECT[self.project_id]
            ):
                non_blocking_failures.append(failure.to_non_blocking_failure())
            else:
                blocking_failures.append(failure.to_blocking_failure())

        if blocking_failures:
            raise RawDataPreImportValidationError(
                file_tag, failures=blocking_failures, warnings=non_blocking_failures
            )

        return non_blocking_failures

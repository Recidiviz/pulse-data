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
"""Validation to check if the current raw data row count is within an acceptable range of the historical median for that file tag."""
import datetime
import logging
from typing import List

import attr
from more_itertools import one

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.raw_file_configs import DirectIngestRawFileConfig
from recidiviz.ingest.direct.raw_data.validations.import_blocking_validations_query_runner import (
    RawDataImportBlockingValidationQueryRunner,
)
from recidiviz.ingest.direct.raw_data.validations.stable_historical_raw_data_counts_table_validation_config import (
    STABLE_HISTORICAL_COUNTS_TABLE_VALIDATION_CONFIG_YAML,
    StableHistoricalCountsDateRangeExclusion,
    StableHistoricalRawDataCountsTableValidationConfig,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.raw_data_import_blocking_validation import (
    RawDataImportBlockingValidationFailure,
    RawDataImportBlockingValidationType,
    RawDataTableImportBlockingValidation,
)
from recidiviz.utils.string import StrictStringFormatter
from recidiviz.utils.types import assert_type

CLOUDSQL_CONNECTION_REGION = "us-east1"

RAW_ROWS_MEDIAN_KEY = "raw_rows_median"
TEMP_TABLE_ROW_COUNT_KEY = "temp_table_row_count"

HISTORICAL_MEDIAN_QUERY = """
WITH historical_data AS (
    SELECT 
        raw_rows
    FROM 
        `{project_id}.operations_v2_cloudsql_connection.direct_ingest_raw_file_import`
    WHERE 
        file_id IN (
            SELECT file_id 
            FROM `{project_id}.operations_v2_cloudsql_connection.direct_ingest_raw_big_query_file_metadata`
            WHERE file_tag = '{file_tag}'
            AND is_invalidated = False
            AND region_code = '{region_code}'
            AND raw_data_instance = '{raw_data_instance}'
            {datetime_filter}
        )
    AND import_status = 'SUCCEEDED'
)
SELECT
    APPROX_QUANTILES(raw_rows, 2)[OFFSET(1)] AS {RAW_ROWS_MEDIAN_KEY},
FROM 
    historical_data;
"""

RAW_ROW_COUNTS_QUERY = """
SELECT
    COUNT(*)  AS {TEMP_TABLE_ROW_COUNT_KEY}
FROM
    `{project_id}.{dataset_id}.{table_id}`;
"""

_TIME_WINDOW_TEMPLATE = " AND update_datetime > TIMESTAMP_SUB(CAST('{file_update_timestamp}' AS TIMESTAMP), INTERVAL {time_window_lookback_days} DAY)"
_DATE_EXCLUSION_TEMPLATE = " AND update_datetime NOT BETWEEN PARSE_TIMESTAMP('%FT%T', '{datetime_start_inclusive}') AND PARSE_TIMESTAMP('%FT%T', '{datetime_end_exclusive}')"


@attr.define
class StableHistoricalRawDataCountsTableValidation(
    RawDataTableImportBlockingValidation
):
    """Verify that the current raw data row count is within an acceptable range of the historical median for that file tag."""

    row_count_percent_change_tolerance: float = attr.ib(init=False)
    time_window_lookback_days: int = attr.ib(init=False)
    date_range_exclusions: List[StableHistoricalCountsDateRangeExclusion] = attr.ib(
        init=False
    )
    validation_config: StableHistoricalRawDataCountsTableValidationConfig = attr.ib(
        factory=StableHistoricalRawDataCountsTableValidationConfig
    )

    def __attrs_post_init__(self) -> None:
        self.row_count_percent_change_tolerance = (
            self.validation_config.get_custom_percent_change_tolerance(
                self.state_code, self.file_tag
            )
        )
        self.date_range_exclusions = self.validation_config.get_date_range_exclusions(
            self.state_code, self.file_tag
        )
        self.time_window_lookback_days = (
            self.validation_config.get_time_window_lookback_days()
        )

    @classmethod
    def create_table_validation(
        cls,
        *,
        file_tag: str,
        project_id: str,
        temp_table_address: BigQueryAddress,
        state_code: StateCode,
        raw_data_instance: DirectIngestInstance,
        file_update_datetime: datetime.datetime,
        bq_client: BigQueryClient,
    ) -> "RawDataTableImportBlockingValidation":
        """Factory method to create a table validation."""
        return cls(
            project_id=project_id,
            temp_table_address=temp_table_address,
            file_tag=file_tag,
            state_code=state_code,
            raw_data_instance=raw_data_instance,
            file_update_datetime=file_update_datetime,
            query_runner=RawDataImportBlockingValidationQueryRunner(
                bq_client=bq_client
            ),
        )

    @staticmethod
    def validation_type() -> RawDataImportBlockingValidationType:
        return RawDataImportBlockingValidationType.STABLE_HISTORICAL_RAW_DATA_COUNTS

    @staticmethod
    def validation_applies_to_table(
        file_config: DirectIngestRawFileConfig,
    ) -> bool:
        return file_config.always_historical_export and not file_config.is_code_file

    @staticmethod
    def should_run_validation(
        file_config: DirectIngestRawFileConfig,
        state_code: StateCode,
        file_tag: str,
        file_update_datetime: datetime.datetime,
    ) -> bool:
        """Returns True if always_historical_export is True for the file tag,
        is_code_file is False for the file tag, and file's update_datetime doesn't fall
        within a date range that is excluded for that file in
        stable_historical_counts_table_validation_config.yaml.

        Excluding code files prevents very small code files from failing to import
        when one or two rows are added or removed. This is relatively safe to do since
        these files are not expected to change much and are not highly consequential on
        their own.
        """

        return StableHistoricalRawDataCountsTableValidation.validation_applies_to_table(
            file_config
        ) and not StableHistoricalRawDataCountsTableValidationConfig().datetime_is_excluded(
            state_code,
            file_tag,
            datetime_to_check=file_update_datetime,
        )

    def _build_datetime_filter(self) -> str:
        return "".join(
            [
                StrictStringFormatter().format(
                    _TIME_WINDOW_TEMPLATE,
                    file_update_timestamp=self.file_update_datetime.isoformat(),
                    time_window_lookback_days=self.time_window_lookback_days,
                )
            ]
            + [
                StrictStringFormatter().format(
                    _DATE_EXCLUSION_TEMPLATE,
                    datetime_start_inclusive=exlusion.format_for_query(
                        exlusion.datetime_start_inclusive
                    ),
                    datetime_end_exclusive=exlusion.format_for_query(
                        exlusion.datetime_end_exclusive
                    ),
                )
                for exlusion in self.date_range_exclusions
            ]
        )

    def build_historical_median_query(self) -> str:
        return StrictStringFormatter().format(
            HISTORICAL_MEDIAN_QUERY,
            project_id=self.project_id,
            file_tag=self.file_tag,
            region_code=self.state_code.value,
            raw_data_instance=self.raw_data_instance.value,
            datetime_filter=self._build_datetime_filter(),
            RAW_ROWS_MEDIAN_KEY=RAW_ROWS_MEDIAN_KEY,
        )

    def build_temp_table_row_count_query(self) -> str:
        return StrictStringFormatter().format(
            RAW_ROW_COUNTS_QUERY,
            project_id=self.project_id,
            dataset_id=self.temp_table_address.dataset_id,
            table_id=self.temp_table_address.table_id,
            TEMP_TABLE_ROW_COUNT_KEY=TEMP_TABLE_ROW_COUNT_KEY,
        )

    def run_validation(
        self,
    ) -> RawDataImportBlockingValidationFailure | None:
        """Runs the validation query and returns an error if the validation fails."""
        historical_counts_query = self.build_historical_median_query()
        median_results = self.query_runner.run_query_in_region(
            query=historical_counts_query, region=CLOUDSQL_CONNECTION_REGION
        )
        median = one(median_results).get(RAW_ROWS_MEDIAN_KEY)
        if not bool(median):
            logging.info(
                "No historical data found for [%s] in running stable historical counts validation. "
                "Treating as a success.",
                self.file_tag,
            )
            return None
        median = assert_type(median, int)

        temp_table_row_count_results = self.query_runner.run_query(
            query=self.build_temp_table_row_count_query()
        )
        temp_table_row_count = one(temp_table_row_count_results).get(
            TEMP_TABLE_ROW_COUNT_KEY
        )
        temp_table_row_count = assert_type(temp_table_row_count, int)

        if (
            abs(median - temp_table_row_count) / float(median)
            > self.row_count_percent_change_tolerance
        ):
            return RawDataImportBlockingValidationFailure(
                validation_type=self.validation_type(),
                validation_query=historical_counts_query,
                error_msg=(
                    f"Median historical raw rows count [{median}] is more than [{self.row_count_percent_change_tolerance}]"
                    f" different than the current count [{temp_table_row_count}] for file [{self.file_tag}]."
                    " If you want to alter the percent change threshold or add a date range to be excluded when calculating the historical median,"
                    f" please add an entry for [{self.file_tag}] in {STABLE_HISTORICAL_COUNTS_TABLE_VALIDATION_CONFIG_YAML}"
                    " If you want the validation to be skipped for this import, you can add a date range exclusion that"
                    " includes the file's update_datetime."
                ),
            )

        # The current row count is within an acceptable range of the historical median
        return None

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
from typing import Any, ClassVar, Dict, List

import attr
from more_itertools import one

from recidiviz.ingest.direct.raw_data.validations.stable_historical_raw_data_counts_validation_config import (
    STABLE_HISTORICAL_COUNTS_TABLE_VALIDATION_CONFIG_YAML,
    StableHistoricalCountsDateRangeExclusion,
    StableHistoricalRawDataCountsValidationConfig,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.raw_data_import_blocking_validation import (
    BaseRawDataImportBlockingValidation,
    RawDataColumnValidationMixin,
    RawDataImportBlockingValidationContext,
    RawDataImportBlockingValidationFailure,
)
from recidiviz.ingest.direct.types.raw_data_import_blocking_validation_type import (
    RawDataImportBlockingValidationType,
)
from recidiviz.utils.string import StrictStringFormatter

RAW_ROWS_MEDIAN_KEY = "raw_rows_median"
TEMP_TABLE_ROW_COUNT_KEY = "temp_table_row_count"

HISTORICAL_STABLE_COUNTS_QUERY = """
WITH historical_data AS (
    SELECT 
        raw_rows
    FROM 
        `{project_id}.operations.direct_ingest_raw_file_import`
    WHERE 
        file_id IN (
            SELECT file_id 
            FROM `{project_id}.operations.direct_ingest_raw_big_query_file_metadata`
            WHERE file_tag = '{file_tag}'
            AND is_invalidated = False
            AND region_code = '{region_code}'
            AND raw_data_instance = '{raw_data_instance}'
            {datetime_filter}
        )
    AND import_status = 'SUCCEEDED'
),
median_data AS (
    SELECT
        APPROX_QUANTILES(raw_rows, 2)[OFFSET(1)] AS {RAW_ROWS_MEDIAN_KEY},
    FROM 
        historical_data
)

SELECT 
    (SELECT COUNT(*) FROM `{project_id}.{dataset_id}.{table_id}`) AS {TEMP_TABLE_ROW_COUNT_KEY},
    {RAW_ROWS_MEDIAN_KEY},
FROM 
    median_data;
"""
_TIME_WINDOW_TEMPLATE = " AND update_datetime > TIMESTAMP_SUB(CAST('{file_update_timestamp}' AS TIMESTAMP), INTERVAL {time_window_lookback_days} DAY)"
_DATE_EXCLUSION_TEMPLATE = " AND update_datetime NOT BETWEEN PARSE_TIMESTAMP('%FT%T', '{datetime_start_inclusive}') AND PARSE_TIMESTAMP('%FT%T', '{datetime_end_exclusive}')"


@attr.define
class StableHistoricalRawDataCountsValidation(
    BaseRawDataImportBlockingValidation, RawDataColumnValidationMixin
):
    """Verify that the current raw data row count is within an acceptable range of the historical median for that file tag."""

    VALIDATION_TYPE: ClassVar[
        RawDataImportBlockingValidationType
    ] = RawDataImportBlockingValidationType.STABLE_HISTORICAL_RAW_DATA_COUNTS
    raw_data_instance: DirectIngestInstance
    file_update_datetime: datetime.datetime
    validation_config: StableHistoricalRawDataCountsValidationConfig = attr.ib(
        factory=StableHistoricalRawDataCountsValidationConfig
    )

    @classmethod
    def create_validation(
        cls, context: RawDataImportBlockingValidationContext
    ) -> "StableHistoricalRawDataCountsValidation":
        """Factory method to create a StableHistoricalRawDataCountsValidation."""
        return cls(
            project_id=context.project_id,
            temp_table_address=context.temp_table_address,
            file_tag=context.file_tag,
            state_code=context.state_code,
            raw_data_instance=context.raw_data_instance,
            file_update_datetime=context.file_update_datetime,
        )

    @property
    def row_count_percent_change_tolerance(self) -> float:
        return self.validation_config.get_custom_percent_change_tolerance(
            self.state_code, self.file_tag
        )

    @property
    def time_window_lookback_days(self) -> int:
        return self.validation_config.get_time_window_lookback_days()

    @property
    def date_range_exclusions(self) -> List[StableHistoricalCountsDateRangeExclusion]:
        return self.validation_config.get_date_range_exclusions(
            self.state_code, self.file_tag
        )

    @classmethod
    def validation_applies_to_file(
        cls,
        context: RawDataImportBlockingValidationContext,
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
        return (
            context.raw_file_config.always_historical_export
            and not context.raw_file_config.is_code_file
            and not StableHistoricalRawDataCountsValidationConfig().datetime_is_excluded(
                context.state_code,
                context.file_tag,
                datetime_to_check=context.file_update_datetime,
            )
            and not context.raw_file_config.file_is_exempt_from_validation(
                cls.VALIDATION_TYPE
            )
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

    def build_query(self) -> str:
        return StrictStringFormatter().format(
            HISTORICAL_STABLE_COUNTS_QUERY,
            project_id=self.project_id,
            file_tag=self.file_tag,
            dataset_id=self.temp_table_address.dataset_id,
            table_id=self.temp_table_address.table_id,
            region_code=self.state_code.value,
            raw_data_instance=self.raw_data_instance.value,
            RAW_ROWS_MEDIAN_KEY=RAW_ROWS_MEDIAN_KEY,
            TEMP_TABLE_ROW_COUNT_KEY=TEMP_TABLE_ROW_COUNT_KEY,
            datetime_filter=self._build_datetime_filter(),
        )

    def get_error_from_results(
        self, results: List[Dict[str, Any]]
    ) -> RawDataImportBlockingValidationFailure | None:
        if not results:
            raise RuntimeError(
                "No results found for stable historical counts validation."
                f"\nFile tag: [{self.file_tag}]."
                f"\nValidation query: {self.build_query()}"
            )

        stats = one(results)

        median = stats[RAW_ROWS_MEDIAN_KEY]
        if not median:
            logging.info(
                "No historical data found for [%s] in running stable historical counts validation. "
                "Treating as a success.",
                self.file_tag,
            )
            return None

        temp_table_row_count = stats[TEMP_TABLE_ROW_COUNT_KEY]
        if (
            abs(median - temp_table_row_count) / float(median)
            > self.row_count_percent_change_tolerance
        ):
            return RawDataImportBlockingValidationFailure(
                validation_type=self.VALIDATION_TYPE,
                validation_query=self.build_query(),
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

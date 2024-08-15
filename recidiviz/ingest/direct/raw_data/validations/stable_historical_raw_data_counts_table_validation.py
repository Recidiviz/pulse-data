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
import logging
from typing import Any, Dict, List

import attr

from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.raw_data_import_blocking_validation_types import (
    RawDataTableImportBlockingValidation,
    RawDataTableImportBlockingValidationFailure,
    RawDataTableImportBlockingValidationType,
)
from recidiviz.utils.string import StrictStringFormatter

ROW_COUNT_PERCENT_CHANGE_TOLERANCE = 0.1
ROW_COUNT_LOOKBACK_DAYS = 90

RAW_ROWS_MEDIAN_KEY = "raw_rows_median"
TEMP_TABLE_ROW_COUNT_KEY = "temp_table_row_count"

HISTORICAL_STABLE_COUNTS_QUERY = """
WITH historical_data AS (
    SELECT 
        raw_rows
    FROM 
        `{project_id}.operations.direct_ingest_raw_data_import_session`
    WHERE 
        file_id IN (
            SELECT file_id 
            FROM `{project_id}.operations.direct_ingest_raw_big_query_file_metadata`
            WHERE file_tag = '{file_tag}'
            AND is_invalidated = False
            AND region_code = '{region_code}'
            AND raw_data_instance = '{raw_data_instance}'
            AND update_datetime > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {ROW_COUNT_LOOKBACK_DAYS} DAY)
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


@attr.define
class StableHistoricalRawDataCountsTableValidation(
    RawDataTableImportBlockingValidation
):
    """Verify that the current raw data row count is within an acceptable range of the historical median for that file tag."""

    region_code: str
    raw_data_instance: DirectIngestInstance
    validation_type: RawDataTableImportBlockingValidationType = (
        RawDataTableImportBlockingValidationType.STABLE_HISTORICAL_RAW_DATA_COUNTS
    )

    def build_query(self) -> str:
        return StrictStringFormatter().format(
            HISTORICAL_STABLE_COUNTS_QUERY,
            project_id=self.project_id,
            file_tag=self.file_tag,
            dataset_id=self.temp_table_address.dataset_id,
            table_id=self.temp_table_address.table_id,
            region_code=self.region_code,
            raw_data_instance=self.raw_data_instance.value,
            RAW_ROWS_MEDIAN_KEY=RAW_ROWS_MEDIAN_KEY,
            TEMP_TABLE_ROW_COUNT_KEY=TEMP_TABLE_ROW_COUNT_KEY,
            ROW_COUNT_LOOKBACK_DAYS=ROW_COUNT_LOOKBACK_DAYS,
        )

    def get_error_from_results(
        self, results: List[Dict[str, Any]]
    ) -> RawDataTableImportBlockingValidationFailure | None:
        if not results:
            raise RuntimeError(
                "No results found for stable historical counts validation."
                f"\nFile tag: [{self.file_tag}]."
                f"\nValidation query: {self.query}"
            )

        stats = results[0]

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
            > ROW_COUNT_PERCENT_CHANGE_TOLERANCE
        ):
            return RawDataTableImportBlockingValidationFailure(
                validation_type=self.validation_type,
                error_msg=(
                    f"Median historical raw rows count [{median}] is more than [{ROW_COUNT_PERCENT_CHANGE_TOLERANCE}] different than the current count [{temp_table_row_count}]."
                    f"\nFile tag: [{self.file_tag}]."
                    f"\nValidation query: {self.query}"
                ),
            )

        # The current row count is within an acceptable range of the historical median
        return None

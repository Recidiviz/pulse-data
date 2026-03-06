# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Shared SQL templates and helpers for ingest pipeline watermark operations.

These are used both by the Airflow pipeline (via the CloudSqlQueryGenerator
classes) and by local diagnostic tools (e.g. check_watermarks.py).
"""

import datetime
from typing import TypeVar

# Selects the max update_datetime per file tag for non-invalidated processed
# files. The {raw_data_instance} and {region_code} placeholders must be filled
# in by callers (e.g. via StrictStringFormatter).
MAX_UPDATE_DATETIMES_QUERY_TEMPLATE = """
SELECT file_tag, MAX(update_datetime) AS max_update_datetime
FROM direct_ingest_raw_big_query_file_metadata
WHERE raw_data_instance = '{raw_data_instance}'
AND is_invalidated IS FALSE
AND file_processed_time IS NOT NULL
AND region_code = '{region_code}'
GROUP BY file_tag;
"""

# Selects watermarks and job_id for the most recent non-invalidated job.
# The {region_code} and {ingest_instance} placeholders must be filled in by
# callers (e.g. via StrictStringFormatter).
WATERMARKS_QUERY_TEMPLATE = """
SELECT raw_data_file_tag, watermark_datetime, job_id
FROM direct_ingest_dataflow_raw_table_upper_bounds
WHERE job_id IN (
    SELECT MAX(job_id)
    FROM direct_ingest_dataflow_job
    WHERE region_code = '{region_code}'
    AND ingest_instance = '{ingest_instance}'
    AND is_invalidated = FALSE
)
"""

_Comparable = TypeVar("_Comparable", datetime.datetime, str)


def get_problematic_watermark_tags(
    watermarks: dict[str, _Comparable],
    max_update_datetimes: dict[str, _Comparable],
) -> tuple[list[str], list[str]]:
    """Returns (missing_tags, stale_tags) for watermarks that would block the pipeline.

    missing_tags: tags with a watermark but no current data in max_update_datetimes.
    stale_tags: tags where the watermark is newer than the current max update datetime.

    Accepts either datetime or str values; string comparison works correctly for
    the ISO-format datetime strings produced by the Airflow XCom serializers.
    """
    missing = sorted(tag for tag in watermarks if tag not in max_update_datetimes)
    stale = sorted(
        tag
        for tag in watermarks
        if tag in max_update_datetimes and watermarks[tag] > max_update_datetimes[tag]
    )
    return missing, stale

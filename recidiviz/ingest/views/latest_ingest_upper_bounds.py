# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""A view that reports back on the ingest "high water mark", i.e. the latest date
where all files on or before that date are processed for a given state."""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.operations.dataset_config import OPERATIONS_BASE_DATASET
from recidiviz.ingest.views.dataset_config import VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# TODO(#11424): Delete this view once BQ materialization has shipped to all states
#  and the reference to this view in `ingest_metadata_store.py` has been deleted.
LATEST_INGESTED_UPPER_BOUNDS_QUERY_TEMPLATE = """
WITH
primary_ingest_file_dates AS (
    SELECT
        DISTINCT
            region_code AS state_code,
            EXTRACT(DATE FROM datetimes_contained_upper_bound_inclusive) AS ingest_file_date,
            processed_time IS NOT NULL AS is_processed
    FROM `{project_id}.{operations_dataset}.direct_ingest_ingest_file_metadata`
    WHERE NOT is_invalidated AND ingest_database_name LIKE '%primary%'
),
min_unprocessed_dates AS (
    SELECT state_code, COALESCE(ingest_file_date, DATE(3000, 01, 01)) AS ingest_file_date
    FROM (SELECT DISTINCT state_code FROM primary_ingest_file_dates)
    LEFT OUTER JOIN
    (
        SELECT state_code, MIN(ingest_file_date) AS ingest_file_date
        FROM primary_ingest_file_dates
        WHERE NOT is_processed
        GROUP BY state_code
    )
    USING (state_code)
),
processed_dates AS (
    SELECT state_code, ingest_file_date
    FROM primary_ingest_file_dates
    WHERE is_processed
),
max_processed_dates AS (
    SELECT
        processed_dates.state_code,
        MAX(processed_dates.ingest_file_date) AS processed_date
    FROM processed_dates
    LEFT OUTER JOIN
        min_unprocessed_dates
    ON
        processed_dates.state_code = min_unprocessed_dates.state_code AND
        processed_dates.ingest_file_date < min_unprocessed_dates.ingest_file_date
    WHERE min_unprocessed_dates.state_code IS NOT NULL
    GROUP BY state_code
)
SELECT
    state_code,
    processed_date
FROM (SELECT DISTINCT state_code FROM primary_ingest_file_dates)
LEFT OUTER JOIN max_processed_dates
USING (state_code)
ORDER BY state_code
"""

LATEST_INGESTED_UPPER_BOUNDS_DESCRIPTION = """A view that reports back on the ingest
 'high water mark', i.e. the latest date where all files on or before that date are
  processed for a given state in the PRIMARY ingest instance."""


LATEST_INGESTED_UPPER_BOUNDS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=VIEWS_DATASET,
    view_id="ingest_metadata_latest_ingested_upper_bounds",
    description=LATEST_INGESTED_UPPER_BOUNDS_DESCRIPTION,
    view_query_template=LATEST_INGESTED_UPPER_BOUNDS_QUERY_TEMPLATE,
    operations_dataset=OPERATIONS_BASE_DATASET,
)


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        LATEST_INGESTED_UPPER_BOUNDS_VIEW_BUILDER.build_and_print()

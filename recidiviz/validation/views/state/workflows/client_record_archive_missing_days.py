# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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

"""A view revealing date gaps in client_record_archive"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

CLIENT_RECORD_ARCHIVE_MISSING_DAYS_VIEW_NAME = "client_record_archive_missing_days"

CLIENT_RECORD_ARCHIVE_MISSING_DAYS_DESCRIPTION = (
    """Date gaps found for a given region in client_record_archive"""
)

CLIENT_RECORD_ARCHIVE_MISSING_DAYS_QUERY_TEMPLATE = """
    /*{description}*/
    WITH 
    start_date_by_state AS (
        SELECT
            state_code AS region_code,
            MIN(date_of_supervision) as records_start,
        FROM `{project_id}.{workflows_dataset}.client_record_archive_materialized`
        GROUP BY 1
    )
    , all_expected_dates_by_state AS (
        SELECT
            region_code,
            date_of_supervision,
        FROM start_date_by_state,
        UNNEST(
            GENERATE_DATE_ARRAY(
                records_start,
                -- it's OK if today is missing, it may not have processed yet
                DATE_SUB(CURRENT_DATE("US/Eastern"), INTERVAL 1 DAY)
            )
        ) date_of_supervision
    )
    , actual_dates_by_state AS (
        SELECT
            state_code AS region_code,
            date_of_supervision,
        FROM `{project_id}.{workflows_dataset}.client_record_archive_materialized`
        GROUP BY 1, 2
    )

    SELECT * FROM all_expected_dates_by_state
    EXCEPT DISTINCT
    SELECT * FROM actual_dates_by_state
"""

CLIENT_RECORD_ARCHIVE_MISSING_DAYS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=CLIENT_RECORD_ARCHIVE_MISSING_DAYS_VIEW_NAME,
    view_query_template=CLIENT_RECORD_ARCHIVE_MISSING_DAYS_QUERY_TEMPLATE,
    description=CLIENT_RECORD_ARCHIVE_MISSING_DAYS_DESCRIPTION,
    workflows_dataset=state_dataset_config.WORKFLOWS_VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CLIENT_RECORD_ARCHIVE_MISSING_DAYS_VIEW_BUILDER.build_and_print()

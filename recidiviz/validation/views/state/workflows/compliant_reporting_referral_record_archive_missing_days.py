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

"""A view revealing date gaps in compliant_reporting_referral_record_archive"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

COMPLIANT_REPORTING_REFERRAL_RECORD_ARCHIVE_MISSING_DAYS_VIEW_NAME = (
    "compliant_reporting_referral_record_archive_missing_days"
)

COMPLIANT_REPORTING_REFERRAL_RECORD_ARCHIVE_MISSING_DAYS_DESCRIPTION = """Date gaps found for a given region in compliant_reporting_referral_record_archive"""

COMPLIANT_REPORTING_REFERRAL_RECORD_ARCHIVE_MISSING_DAYS_QUERY_TEMPLATE = """
    /*{description}*/
    WITH 
    archive_start_date AS (
        SELECT
            state_code AS region_code,
            MIN(export_date) AS records_start,
        FROM `{project_id}.{workflows_dataset}.compliant_reporting_referral_record_archive_materialized`
        GROUP BY 1
    )
    , all_expected_dates AS (
        SELECT
            region_code,
            export_date,
        FROM archive_start_date,
        UNNEST(
            GENERATE_DATE_ARRAY(
                records_start,
                -- it's OK if today is missing, it may not have processed yet
                DATE_SUB(CURRENT_DATE("US/Eastern"), INTERVAL 1 DAY)
            )
        ) export_date
    )
    , all_actual_dates AS (
        -- deduplicate repeat uploads for the same date
        SELECT DISTINCT
            state_code AS region_code,
            export_date,
        FROM `{project_id}.{workflows_dataset}.compliant_reporting_referral_record_archive_materialized`
    )

    SELECT * FROM all_expected_dates
    EXCEPT DISTINCT
    SELECT * FROM all_actual_dates
"""

COMPLIANT_REPORTING_REFERRAL_RECORD_ARCHIVE_MISSING_DAYS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=COMPLIANT_REPORTING_REFERRAL_RECORD_ARCHIVE_MISSING_DAYS_VIEW_NAME,
    view_query_template=COMPLIANT_REPORTING_REFERRAL_RECORD_ARCHIVE_MISSING_DAYS_QUERY_TEMPLATE,
    description=COMPLIANT_REPORTING_REFERRAL_RECORD_ARCHIVE_MISSING_DAYS_DESCRIPTION,
    workflows_dataset=state_dataset_config.WORKFLOWS_VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        COMPLIANT_REPORTING_REFERRAL_RECORD_ARCHIVE_MISSING_DAYS_VIEW_BUILDER.build_and_print()

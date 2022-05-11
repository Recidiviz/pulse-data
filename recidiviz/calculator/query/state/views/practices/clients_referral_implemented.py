#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2022 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""View of workflow referrals implemented by agency staff."""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

CLIENTS_REFERRAL_IMPLEMENTED_VIEW_NAME = "clients_referral_implemented"

CLIENTS_REFERRAL_IMPLEMENTED_DESCRIPTION = """
    View of workflow referrals implemented by agency staff.
    """


CLIENTS_REFERRAL_IMPLEMENTED_QUERY_TEMPLATE = """
    /*{description}*/
    WITH
    downgrade_dates AS (
        SELECT
            LPAD(CAST(Offender_ID AS STRING), 8, '0') AS person_external_id,
            # don't really expect multiple rows per person, but if it happens take the earlier
            MIN(date_of_standards) AS date_of_standards,
        FROM (
            SELECT 
                Offender_ID,
                date_of_standards,
                Supervision_Level,
                LAG(Supervision_Level) OVER person_window AS supervision_level_last_report,
                LAG(date_of_standards) OVER person_window AS previous_date_of_standards,
            FROM `{project_id}.{static_reference_dataset}.us_tn_standards_due` 
            WINDOW person_window AS (PARTITION BY Offender_ID ORDER BY date_of_standards ASC)
        )
        WHERE Supervision_Level = 'STANDARD: TELEPHONE RPT'
            AND supervision_level_last_report != 'STANDARD: TELEPHONE RPT'
            # First Workflows live date
            AND date_of_standards >= '2022-03-31'
        GROUP BY 1
    )
    , downgrade_events AS (
        SELECT
            "US_TN" AS state_code,
            person_external_id,
            date_of_standards AS event_date,
            "compliantReporting" AS opportunity_type,
        FROM downgrade_dates
    )

    SELECT
        person_id,
        downgrade_events.*,
    FROM downgrade_events
    LEFT JOIN `{project_id}.{state_base_dataset}.state_person_external_id` pei
        ON downgrade_events.state_code = pei.state_code
        AND downgrade_events.person_external_id = pei.external_id
"""

CLIENTS_REFERRAL_IMPLEMENTED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.PRACTICES_VIEWS_DATASET,
    view_id=CLIENTS_REFERRAL_IMPLEMENTED_VIEW_NAME,
    view_query_template=CLIENTS_REFERRAL_IMPLEMENTED_QUERY_TEMPLATE,
    description=CLIENTS_REFERRAL_IMPLEMENTED_DESCRIPTION,
    static_reference_dataset=dataset_config.STATIC_REFERENCE_TABLES_DATASET,
    state_base_dataset=dataset_config.STATE_BASE_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CLIENTS_REFERRAL_IMPLEMENTED_VIEW_BUILDER.build_and_print()

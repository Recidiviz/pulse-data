# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Reference table for overdue discharge alert exclusions.

python -m recidiviz.calculator.query.state.reference.overdue_discharge_alert_exclusions
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

OVERDUE_DISCHARGE_ALERT_EXCLUSIONS_VIEW_NAME = "overdue_discharge_alert_exclusions"

OVERDUE_DISCHARGE_ALERT_EXCLUSIONS_DESCRIPTION = (
    """Reference table for overdue discharge alert exclusions."""
)


OVERDUE_DISCHARGE_ALERT_EXCLUSIONS_QUERY_TEMPLATE = """
/*{description}*/
WITH preliminary_eligibility AS (
    SELECT
        state_code,
        person_id,
        person_external_id,
        district_name,
        supervising_officer_external_id,
        projected_end_date,
        date_of_supervision,
        CONCAT(first_name, ' ', last_name) AS full_name,
    FROM `{project_id}.{analyst_data_dataset}.projected_discharges_materialized`
    WHERE state_code = 'US_ID' AND DATE_ADD(date_of_supervision, INTERVAL 60 DAY) >= projected_end_date
),
case_notes AS (
    SELECT
        'US_ID' AS state_code,
        ofndr_num AS person_external_id,
        MAX(GREATEST(
            violation,
            sanction,
            extend,
            abscond,
            custody,
            agents_warning,
            revoke,
            other,
            new_investigation
        )) AS case_notes_flag
    FROM `{project_id}.analyst_data_scratch_space.us_id_case_notes_flag_24`
    WHERE SAFE_CAST(create_dt AS date) >= DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL 1 YEAR)
    GROUP BY 1, 2
),
cis_ftrd AS (
    SELECT
        'US_ID' AS state_code,
        CAST(person_external_id AS STRING) AS person_external_id,
        UPPER(status_description) AS cis_status,
        cis_ftrd,
        life_death_flag AS cis_life_death_flag,
    FROM `{project_id}.analyst_data_scratch_space.us_id_cis_ftrd_20211130`
)
SELECT
    CURRENT_DATE('US/Eastern') AS date_of_exclusion,
    state_code,
    person_external_id,
    CASE
        WHEN case_notes.case_notes_flag = 1 THEN 'MATCHED_CASE_NOTES_FLAG'
        WHEN cis_ftrd.cis_ftrd > projected_end_date THEN 'CIS_FTRD_LATER'
        WHEN cis_ftrd.cis_life_death_flag IN ('L', 'D') THEN 'LIFE_DEATH_FLAG'
    END AS exclusion_reason
FROM preliminary_eligibility
LEFT JOIN case_notes USING (state_code, person_external_id)
LEFT JOIN cis_ftrd USING (state_code, person_external_id)
WHERE
    case_notes_flag = 1
    OR (cis_ftrd.cis_ftrd IS NOT NULL AND projected_end_date < cis_ftrd.cis_ftrd)
    OR (cis_ftrd.cis_life_death_flag IN ('L', 'D'))
"""


OVERDUE_DISCHARGE_ALERT_EXCLUSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.SHARED_METRIC_VIEWS_DATASET,
    view_id=OVERDUE_DISCHARGE_ALERT_EXCLUSIONS_VIEW_NAME,
    view_query_template=OVERDUE_DISCHARGE_ALERT_EXCLUSIONS_QUERY_TEMPLATE,
    description=OVERDUE_DISCHARGE_ALERT_EXCLUSIONS_DESCRIPTION,
    analyst_data_dataset=dataset_config.ANALYST_VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        OVERDUE_DISCHARGE_ALERT_EXCLUSIONS_VIEW_BUILDER.build_and_print()

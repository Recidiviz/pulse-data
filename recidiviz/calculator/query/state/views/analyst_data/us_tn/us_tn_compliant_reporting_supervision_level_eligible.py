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
"""Creates a view that surfaces sessions during which clients are eligible for
Compliant Reporting due to supervision level."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SESSIONS_DATASET,
    STATE_BASE_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TN_COMPLIANT_REPORTING_SUPERVISION_LEVEL_ELIGIBLE_VIEW_NAME = (
    "us_tn_compliant_reporting_supervision_level_eligible"
)

US_TN_COMPLIANT_REPORTING_SUPERVISION_LEVEL_ELIGIBLE_VIEW_DESCRIPTION = "Creates a view that surfaces sessions during which clients are eligible for Compliant Reporting due to supervision level"

US_TN_COMPLIANT_REPORTING_SUPERVISION_LEVEL_ELIGIBLE_QUERY_TEMPLATE = """
    WITH cte0 AS 
    /*
    All TN time in supervision at min or medium level. Create an indicator for whether there is a gap in time between min/med periods 
    which creates a new session
    */
    (
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        supervision_level,
        IF(LAG(end_date) OVER(PARTITION BY person_id ORDER BY start_date) != DATE_SUB(start_date, INTERVAL 1 DAY),1,0) AS new_session
    FROM `{project_id}.{sessions_dataset}.supervision_level_raw_text_sessions_materialized`
    WHERE state_code = 'US_TN'
    AND supervision_level IN ('MINIMUM','MEDIUM')
    AND supervision_level_raw_text != '6P3'
    )
    ,
    cte1 AS
    /*
    Create an id using the new session indicator that can be used to group continuous periods of time under min or medium supervision. A person
    can go between min and medium levels and I call the continuous time spent at either a "super session" throughout the rest of this query
    */
    (
    SELECT
        *,
        SUM(new_session) OVER(PARTITION BY person_id ORDER BY start_date)+1 AS min_max_session_id,
    FROM cte0
    )
    ,
    cte2 AS
    /*
    Calculate characteristics of the start of this min/max supersession - date that the super session started and level that the supersession started at. 
    These are used to check for the appropriate 12 and 18 month statuses.
    */
    (
    SELECT 
        *,
        MIN(start_date) OVER(PARTITION BY person_id, min_max_session_id ORDER BY start_date) AS super_session_start_date,
        FIRST_VALUE(supervision_level) OVER(PARTITION BY person_id, min_max_session_id ORDER BY start_date) AS supervision_level_start,
    FROM cte1
    )
    ,
    cte3 AS
    /*
    Do the date calculations to determine when CR eligibility is reached. Two date fields are calculated here: 
    (1) super_session_start_eligibility_date - takes the super session start date and adds the relevant number of months based on the level that a person 
        started at (12 if minumum, 18 if medium).
    (2) min_session_start_eligibility_date - this is to account for the edge case where a person starting on medium can be eligible less than 18 months 
        after starting at medium if they transition to minimum in less than 6 months and then spend 12+ months at minimum. For example a person could start at
        medium on 1/1/18, transition to min on 3/1/18 and then be eligible for CR on 3/1/19, which is only 14 months after starting at medium. This field adds 
        12 months to all minimum level sessions (not super sessions)
    The lesser of the two date fields represents the eligibility date. Most frequently it is 12 or 18 months after a min/max super-session start, but can also be 
    12 months after the start of a min session, in the edge case described above.
    */
    (
    SELECT 
        *,
        CASE 
            WHEN supervision_level_start = 'MEDIUM'
                THEN DATE_ADD(super_session_start_date, INTERVAL 18 MONTH)
            WHEN supervision_level_start = 'MINIMUM'
                THEN DATE_ADD(super_session_start_date, INTERVAL 12 MONTH) END AS super_session_start_eligibility_date,
        IF(supervision_level = 'MINIMUM', DATE_ADD(start_date, INTERVAL 12 MONTH), NULL) AS min_session_start_eligibility_date,
    FROM cte2
    )
    /*
    Aggregate sessions to super sessions and use the min of the least value between super_session_start_eligibility_date and min_session_start_eligibility_date as the eligibility start_date
    End date is the date that a person transitions out of min/med which is just the max end date of the super session
    */
    SELECT 
        cte3.person_id,
        external_id AS offender_id,
        cte3.state_code,
        TRUE AS supervision_level_eligible,
        MIN(LEAST(COALESCE(min_session_start_eligibility_date,'9999-01-01'),super_session_start_eligibility_date)) AS start_date,
        CASE WHEN LOGICAL_AND(end_date IS NOT NULL) THEN MAX(end_date) END AS end_date,
    FROM cte3
    JOIN `{project_id}.{base_dataset}.state_person_external_id` ex
        on ex.person_id = cte3.person_id
        AND ex.state_code = cte3.state_code
    GROUP BY person_id, offender_id, state_code, min_max_session_id
    --eligibility sessions will only be when the start date is less than the end date
    --if this is not true the person became ineligible before they became eligible
    HAVING start_date<COALESCE(end_date,'9999-01-01')
"""

US_TN_COMPLIANT_REPORTING_SUPERVISION_LEVEL_ELIGIBLE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    base_dataset=STATE_BASE_DATASET,
    view_id=US_TN_COMPLIANT_REPORTING_SUPERVISION_LEVEL_ELIGIBLE_VIEW_NAME,
    description=US_TN_COMPLIANT_REPORTING_SUPERVISION_LEVEL_ELIGIBLE_VIEW_DESCRIPTION,
    view_query_template=US_TN_COMPLIANT_REPORTING_SUPERVISION_LEVEL_ELIGIBLE_QUERY_TEMPLATE,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_COMPLIANT_REPORTING_SUPERVISION_LEVEL_ELIGIBLE_VIEW_BUILDER.build_and_print()

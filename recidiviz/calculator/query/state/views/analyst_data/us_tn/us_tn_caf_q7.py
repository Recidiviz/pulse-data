# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Computes Q7 of TN's CAF form (most severe disciplinary in last 18 months) at any point in time"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TN_CAF_Q7_VIEW_NAME = "us_tn_caf_q7"

US_TN_CAF_Q7_VIEW_DESCRIPTION = """Computes Q7 of TN's CAF form (most severe disciplinary in last 18 months) at any point in time
    See details of CAF Form here https://drive.google.com/file/d/18ez172yx3Tpx-rFaNseJv3dgMh7cGezg/view?usp=sharing
    """

US_TN_CAF_Q7_QUERY_TEMPLATE = f"""
    -- This CTE keeps unique disciplinaries for a given person-day where there is a guilty disposition, 
    -- prioritizing more severe disciplinaries when there are multiple disciplinaries on the same day
    WITH unique_person_dates AS (
        SELECT
            state_code,
            person_id,
            incident_date AS start_date,
            DATE_ADD(incident_date, INTERVAL 18 MONTH) AS end_date,
            CASE WHEN incident_class = 'A' THEN 7
                WHEN incident_class = 'B' THEN 5
                WHEN incident_class = 'C' THEN 2
                END AS disciplinary_score
        FROM 
            `{{project_id}}.{{analyst_dataset}}.incarceration_incidents_preprocessed_materialized` dis
        WHERE
            state_code = "US_TN"
            AND disposition = 'GU'
            AND incident_class IS NOT NULL
        QUALIFY ROW_NUMBER() OVER(PARTITION BY state_code, person_id, incident_date
                                  ORDER BY disciplinary_score DESC) = 1
    )
    ,
    {create_sub_sessions_with_attributes('unique_person_dates')}
    /* If someone has more than one disciplinary in 18 months, then they will have
    multiple sub-sessions during the overlapping span. We deduplicate by taking the maximum
    score associated with each sub-session */
    
    SELECT
        person_id,
        state_code,
        start_date,
        end_date,
        MAX(disciplinary_score) AS q7_score,
    FROM
        sub_sessions_with_attributes
    GROUP BY
        1,2,3,4
        
"""

US_TN_CAF_Q7_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    view_id=US_TN_CAF_Q7_VIEW_NAME,
    description=US_TN_CAF_Q7_VIEW_DESCRIPTION,
    view_query_template=US_TN_CAF_Q7_QUERY_TEMPLATE,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_CAF_Q7_VIEW_BUILDER.build_and_print()

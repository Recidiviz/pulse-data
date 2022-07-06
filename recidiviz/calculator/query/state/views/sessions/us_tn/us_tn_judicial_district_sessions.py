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
"""Sessionized view of judicial district off of pre-processed raw TN sentencing data"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    SESSIONS_DATASET,
    STATE_BASE_DATASET,
    US_TN_RAW_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TN_JUDICIAL_DISTRICT_SESSIONS_VIEW_NAME = "us_tn_judicial_district_sessions"

US_TN_JUDICIAL_DISTRICT_SESSIONS_VIEW_DESCRIPTION = """Sessionized view of judicial district off of pre-processed raw TN sentencing data"""

US_TN_JUDICIAL_DISTRICT_SESSIONS_QUERY_TEMPLATE = """
    /*{description}*/ 
    --TODO(#10747): Remove judicial district preprocessing once hydrated in population metrics   
    WITH cte AS
    (
    SELECT
        person_id,
        state_code,
        judicial_district_code,
        sentence_effective_date AS judicial_district_start_date,
        LEAD(DATE_SUB(sentence_effective_date, INTERVAL 1 DAY)) OVER(PARTITION BY person_id ORDER BY sentence_effective_date) AS judicial_district_end_date,
    FROM
        (
        SELECT 
            ex.person_id,
            'US_TN' AS state_code,
            CAST(jd.JudicialDistrict AS STRING) AS judicial_district_code,
            CAST(CAST(s.SentenceEffectiveDate AS datetime) AS DATE) AS sentence_effective_date,
        FROM `{project_id}.{raw_dataset}.Sentence_latest` s
        JOIN `{project_id}.{raw_dataset}.JOIdentification_latest` jd
            USING(OffenderID, ConvictionCounty, CaseYear, CaseNumber, CountNumber) 
        JOIN `{project_id}.{state_base_dataset}.state_person_external_id` ex
            ON ex.external_id = s.OffenderID
            AND ex.state_code = 'US_TN'
        --This needs to be unique on person id and sentence effective date in order to work properly. It is very
        --rare (0.2% of cases) that a person has more than one judicial district on the same start date and when that
        --occurs deterministically take the first code
        QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id, sentence_effective_date ORDER BY judicial_district_code)=1
        )
    )
    SELECT
        person_id,
        state_code,
        judicial_district_session_id,
        judicial_district_code,
        MIN(judicial_district_start_date) AS judicial_district_start_date,
        CASE WHEN LOGICAL_AND(judicial_district_end_date IS NOT NULL) THEN MAX(judicial_district_end_date) END AS judicial_district_end_date,
    FROM
        (  
        SELECT
            *,
            SUM(new_session) OVER(PARTITION BY person_id ORDER BY judicial_district_start_date) AS judicial_district_session_id
        FROM
            (
            SELECT
              *,
              IF(COALESCE(LAG(judicial_district_code) OVER(PARTITION BY person_id ORDER BY judicial_district_start_date) != judicial_district_code, TRUE),1,0) as new_session
            FROM cte
            )
        )
    GROUP BY 1,2,3,4
"""

US_TN_JUDICIAL_DISTRICT_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=US_TN_JUDICIAL_DISTRICT_SESSIONS_VIEW_NAME,
    view_query_template=US_TN_JUDICIAL_DISTRICT_SESSIONS_QUERY_TEMPLATE,
    description=US_TN_JUDICIAL_DISTRICT_SESSIONS_VIEW_DESCRIPTION,
    raw_dataset=US_TN_RAW_DATASET,
    state_base_dataset=STATE_BASE_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_JUDICIAL_DISTRICT_SESSIONS_VIEW_BUILDER.build_and_print()

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
# ============================================================================
"""Describes spans of time when a resident of Arizona has not had a transition release in their current sentence span"""
from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.us_az_query_fragments import (
    us_az_sentences_preprocessed_query_template,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_AZ_NO_TRANSITION_RELEASE_IN_CURRENT_SENTENCE_SPAN"

_DESCRIPTION = __doc__

_REASONS_FIELDS = [
    ReasonsField(
        name="statutes_with_transition_release",
        type=bigquery.enums.StandardSqlTypeNames.DATE,
        description="Relevant statutes associated with the transition release",
    ),
    ReasonsField(
        name="descriptions_with_transition_release",
        type=bigquery.enums.StandardSqlTypeNames.DATE,
        description="Descriptions of relevant statutes associated with the transition release",
    ),
    ReasonsField(
        name="most_recent_transition_date",
        type=bigquery.enums.StandardSqlTypeNames.DATE,
        description="Most recent transition release date",
    ),
    ReasonsField(
        name="most_recent_transition_type",
        type=bigquery.enums.StandardSqlTypeNames.DATE,
        description="Most recent transition release type",
    ),
]

_QUERY_TEMPLATE = f"""
WITH tpr_and_dtp_dates AS (
  SELECT *
  FROM `{{project_id}}.analyst_data.us_az_early_releases_from_incarceration_materialized`
),

sentences_preprocessed AS (
    {us_az_sentences_preprocessed_query_template()}
),

sentences_with_a_tr AS (
    -- This identifies all sentences who have already had a Transition Release. They can't have a second one.
    SELECT 
        sent.person_id,
        sent.state_code,
        tr.release_date AS start_date,
        sent.end_date,
        sent.statute,
        sent.description,
        tr.release_date,
        tr.release_type,
    FROM sentences_preprocessed sent
    INNER JOIN tpr_and_dtp_dates tr
        ON tr.release_date BETWEEN sent.start_date AND IFNULL(DATE_SUB(sent.end_date, INTERVAL 1 DAY), '9999-12-31')
            AND sent.person_id = tr.person_id
            AND sent.state_code = tr.state_code
    WHERE sent.state_code = 'US_AZ'
),

{create_sub_sessions_with_attributes('sentences_with_a_tr')}
 
SELECT 
    state_code,
    person_id,
    start_date,
    end_date,
    False AS meets_criteria,
    TO_JSON(STRUCT(
        STRING_AGG(statute, ', ' ORDER BY statute) AS statutes_with_transition_release,
        STRING_AGG(description, ', ' ORDER BY description) AS descriptions_with_transition_release,
        MAX(release_date) AS most_recent_transition_date,
        ANY_VALUE(release_type HAVING MAX release_date) AS most_recent_transition_type
    )) AS reason,
    STRING_AGG(statute, ', ' ORDER BY statute) AS statutes_with_transition_release,
    STRING_AGG(description, ', ' ORDER BY description) AS descriptions_with_transition_release,
    MAX(release_date) AS most_recent_transition_date,
    ANY_VALUE(release_type HAVING MAX release_date) AS most_recent_transition_type,
FROM sub_sessions_with_attributes
GROUP BY 1,2,3,4
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        state_code=StateCode.US_AZ,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        meets_criteria_default=True,
        reasons_fields=_REASONS_FIELDS,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()

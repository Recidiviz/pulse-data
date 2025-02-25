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
"""
Defines a criteria view that shows spans of time for which residents are 
compliant with the requisites needed to be approved by the work-release committee. 
These requisites are: 
    A. within 6 months of release/prd/cpp
    B. no level 2 or 3 infractions in the past 6 months
"""
from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_clause,
    nonnull_start_date_clause,
    revert_nonnull_end_date_clause,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_criteria_state_specific_dataset,
)
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.state_dataset_query_fragments import (
    extract_object_from_json,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_ND_WORK_RELEASE_COMMITTEE_REQUIREMENTS"

_DESCRIPTION = """
Defines a criteria view that shows spans of time for which residents are 
compliant with two criteria needed to be approved by the work-release committee. 
These criteria are: 
    A. within 6 months of release/prd/cpp
    B. no level 2 or 3 infractions in the past 6 months
"""
_QUERY_TEMPLATE = f"""
WITH commmitte_plus_6mo AS (
    -- Takes residents who are required to be approved by the work-release committee
    --  and joins it to criteria A (see description above)
    SELECT 
        ca.state_code,
        ca.person_id,
        -- We want the intersection of both spans. That's why we use GREATEST and LEAST
        GREATEST(ca.start_date, iw6.start_date) AS start_date,
        LEAST({nonnull_end_date_clause('ca.end_date')}, {nonnull_end_date_clause('iw6.end_date')}) AS end_date,
        iw6.meets_criteria AS six_months_away_from_relevant_date,
        {extract_object_from_json(object_column='full_term_completion_date', 
                                  object_type='STRING',
                                  json_column='iw6.reason')} AS full_term_completion_date,
        {extract_object_from_json(object_column='parole_review_date', 
                                  object_type='STRING',
                                  json_column='iw6.reason')} AS parole_review_date,
        -- TODO(#27763): Add CPP date as an additional column when the field works
    FROM `{{project_id}}.{{task_eligibility_criteria_us_nd}}.requires_committee_approval_for_work_release_materialized` ca
    INNER JOIN `{{project_id}}.{{task_eligibility_criteria_us_nd}}.incarceration_within_6_months_of_ftcd_or_prd_or_cpp_release_materialized` iw6
        ON ca.person_id = iw6.person_id
            AND ca.state_code = iw6.state_code
            AND ca.start_date < {nonnull_end_date_clause('iw6.end_date')}
            AND iw6.start_date < {nonnull_end_date_clause('ca.end_date')}
    -- Only keep spans where the resident requires committee approval.
    WHERE ca.meets_criteria
),
commmitte_plus_6mo_plus_no_infraction AS (    
    -- Takes the CTE above and joins it to criteria B (see description above)
    SELECT 
        cp6.state_code,
        cp6.person_id,
        -- We want the intersection of both spans. That's why we use GREATEST and LEAST
        GREATEST(cp6.start_date, {nonnull_start_date_clause('ni.start_date')}) AS start_date,
        LEAST(cp6.end_date, {nonnull_end_date_clause('ni.end_date')}) AS end_date,
        six_months_away_from_relevant_date,
        full_term_completion_date,
        parole_review_date,
        IFNULL(ni.meets_criteria, True) AS no_level_2_or_3_infractions,
        {extract_object_from_json(object_column='infraction_categories', 
                                  object_type='STRING',
                                  json_column='ni.reason')} AS infraction_categories,
        {extract_object_from_json(object_column='most_recent_infraction_date', 
                                  object_type='STRING',
                                  json_column='ni.reason')} AS most_recent_infraction_date,
    FROM commmitte_plus_6mo cp6
    LEFT JOIN `{{project_id}}.{{task_eligibility_criteria_us_nd}}.no_level_2_or_3_infractions_for_6_months_materialized` ni
        ON cp6.person_id = ni.person_id
            AND cp6.state_code = ni.state_code
            AND cp6.start_date < {nonnull_end_date_clause('ni.end_date')}
            AND ni.start_date < cp6.end_date
)
SELECT 
    state_code,
    person_id,
    start_date,
    {revert_nonnull_end_date_clause('end_date')} AS end_date,
    -- Folks who are required to be reviewed by the committee need to meet both criteria
    (six_months_away_from_relevant_date AND no_level_2_or_3_infractions) AS meets_criteria,
    TO_JSON(STRUCT(TRUE AS requires_committee_approval,
                   six_months_away_from_relevant_date AS six_months_away_from_relevant_date,
                   no_level_2_or_3_infractions AS no_level_2_or_3_infractions,
                   parole_review_date AS parole_review_date,
                   full_term_completion_date AS full_term_completion_date,
                   infraction_categories AS infraction_categories,
                   most_recent_infraction_date AS most_recent_infraction_date)) 
        AS reason,
    TRUE AS requires_committee_approval,
    six_months_away_from_relevant_date AS six_months_away_from_relevant_date,
    no_level_2_or_3_infractions,
    parole_review_date,
    full_term_completion_date,
    infraction_categories,
    most_recent_infraction_date,
FROM commmitte_plus_6mo_plus_no_infraction
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    state_code=StateCode.US_ND,
    criteria_name=_CRITERIA_NAME,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    description=_DESCRIPTION,
    task_eligibility_criteria_us_nd=task_eligibility_criteria_state_specific_dataset(
        StateCode.US_ND
    ),
    meets_criteria_default=True,
    reasons_fields=[
        ReasonsField(
            name="requires_committee_approval",
            type=bigquery.enums.StandardSqlTypeNames.BOOL,
            description="Does this person require approval from the work-release committee?",
        ),
        ReasonsField(
            name="six_months_away_from_relevant_date",
            type=bigquery.enums.StandardSqlTypeNames.BOOL,
            description="Is the person within 6 months of the relevant date (full term completion date or parole review date)?",
        ),
        ReasonsField(
            name="no_level_2_or_3_infractions",
            type=bigquery.enums.StandardSqlTypeNames.BOOL,
            description="Does the person have no level 2 or 3 infractions in the past 6 months?",
        ),
        ReasonsField(
            name="parole_review_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Parole review date: The date when the person is reviewed for parole.",
        ),
        ReasonsField(
            name="full_term_completion_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Full term completion date: The date when the person is expected to complete their full term.",
        ),
        ReasonsField(
            name="infraction_categories",
            type=bigquery.enums.StandardSqlTypeNames.STRING,
            description="Categories of infractions that the person has committed.",
        ),
        ReasonsField(
            name="most_recent_infraction_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Date of the most recent level 2 or 3 infraction.",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()

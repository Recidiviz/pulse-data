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
# =============================================================================
"""
Defines a criteria span view that shows spans of time during which
someone is within 5 years of their parole hearing date.
"""
from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_clause,
    nonnull_start_date_clause,
    revert_nonnull_end_date_clause,
    revert_nonnull_start_date_clause,
)
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_IX_PAROLE_HEARING_DATE_WITHIN_5_YEARS"

_DESCRIPTION = """
Defines a criteria span view that shows spans of time during which
someone is within 5 years of their parole hearing date.
"""

_QUERY_TEMPLATE = f"""
WITH min_max_parole_hearing_date_spans AS (
        -- Get the max and min parole hearing dates for each person.
        SELECT 
            * EXCEPT(min_parole_hearing_date, max_parole_hearing_date),
            {revert_nonnull_end_date_clause('min_parole_hearing_date')} AS min_parole_hearing_date,
            {revert_nonnull_start_date_clause('max_parole_hearing_date')} AS max_parole_hearing_date
        FROM (
            SELECT
                state_code,
                person_id,
                {nonnull_start_date_clause('start_date')} as start_datetime,
                end_date as end_datetime,
                -- We can have at most two parole hearing dates. We first categorize them
                --    as min and max. This way we can use this info to create relevant spans. 
                LEAST({nonnull_end_date_clause('initial_parole_hearing_date')},
                    {nonnull_end_date_clause('next_parole_hearing_date')}) AS min_parole_hearing_date,
                GREATEST({nonnull_start_date_clause('initial_parole_hearing_date')},
                        {nonnull_start_date_clause('next_parole_hearing_date')}) AS max_parole_hearing_date
            FROM `{{project_id}}.{{analyst_dataset}}.us_ix_parole_dates_spans_preprocessing_materialized`
        )
),

critical_date_spans AS (
    -- Get the critical date for each person.

    -- If they only have one date, we use that as a critical date
    SELECT
        state_code,
        person_id,
        start_datetime,
        end_datetime,
        min_parole_hearing_date AS critical_date,
    FROM min_max_parole_hearing_date_spans
    WHERE min_parole_hearing_date = max_parole_hearing_date

    UNION ALL

    -- MINIMUM DATE SPANS
    -- If they have two dates, the person was not granted parole at the first date. So
    --      the earlier date is the critical date until that first hearing happened. Just after
    --      it happened, the later date becomes the critical date. So these spans will end
    --      at the first parole hearing date.
    SELECT 
        state_code,
        person_id,
        start_datetime,
        min_parole_hearing_date AS end_datetime,
        min_parole_hearing_date AS critical_date,
    FROM min_max_parole_hearing_date_spans
    WHERE min_parole_hearing_date < max_parole_hearing_date

    UNION ALL

    -- MAXIMUM DATE SPANS
    -- After the first Parole Hearing, the later date is the critical date.
    SELECT 
        state_code,
        person_id,
        min_parole_hearing_date AS start_datetime,
        end_datetime,
        max_parole_hearing_date AS critical_date,
    FROM min_max_parole_hearing_date_spans
    WHERE min_parole_hearing_date < max_parole_hearing_date
),

{critical_date_has_passed_spans_cte(meets_criteria_leading_window_time=5,
                                    date_part='YEAR')},

{create_sub_sessions_with_attributes('critical_date_has_passed_spans')}

SELECT 
    state_code,
    person_id,
    start_date,
    end_date,
    LOGICAL_OR(critical_date_has_passed) AS meets_criteria,
    TO_JSON(STRUCT(MIN(critical_date) AS next_parole_hearing_date)) AS reason,
    MIN(critical_date) AS next_parole_hearing_date,
FROM sub_sessions_with_attributes
WHERE start_date != {nonnull_end_date_clause('end_date')}
GROUP BY 1,2,3,4
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    description=_DESCRIPTION,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    state_code=StateCode.US_IX,
    reasons_fields=[
        ReasonsField(
            name="next_parole_hearing_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Next Parole Hearing Date (PHD): The date on which the person is scheduled for a parole hearing.",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()

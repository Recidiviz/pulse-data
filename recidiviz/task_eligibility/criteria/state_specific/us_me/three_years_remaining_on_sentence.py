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

"""
Defines a criteria view that shows spans of time for
which clients are 3 years away from expected release date.
"""

from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)
from recidiviz.task_eligibility.utils.raw_table_import import cis_319_after_csswa
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_ME_THREE_YEARS_REMAINING_ON_SENTENCE"

_DESCRIPTION = """
Defines a criteria view that shows spans of time for
which clients are 3 years away from expected release date.
"""

_QUERY_TEMPLATE = f"""
# TODO(#17247) Make this a general/state-agnostic criteria
WITH term_with_critical_date AS (
-- Combine case load with term data
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        DATE_SUB(end_date, INTERVAL 3 YEAR) AS critical_date,
        status,
        term_id,
    FROM `{{project_id}}.{{analyst_dataset}}.us_me_sentence_term_materialized` tc
),

-- Create sub-sessions w/attributes
{create_sub_sessions_with_attributes('term_with_critical_date')},

critical_date_spans AS (
    -- Drop additional repeated subsessions: if concurrent keep the longest one, drop
    --   completed sessions over active ones
    {cis_319_after_csswa()}
),

-- Critical date has passed
{critical_date_has_passed_spans_cte()}
SELECT
    cd.state_code,
    cd.person_id,
    CASE
        WHEN (start_date IS NULL) AND (critical_date_has_passed) THEN cd.critical_date
                                -- When there was no intake date in us_me_sentence_term,
                                -- start_date of our subsession is NULL for the
                                -- period for which the criteria is met. But
                                -- we know the end date and we can calculate the
                                -- start_date (eligible_date)
        ELSE start_date
    END start_date,
        -- if the most recent subsession is True, then end_date should be NULL
    IF((ROW_NUMBER() OVER (PARTITION BY cd.person_id, cd.state_code
                           ORDER BY start_date DESC) =  1)
            AND (critical_date_has_passed),
        NULL,
        end_date) AS end_date,
    critical_date_has_passed AS meets_criteria,
    TO_JSON(STRUCT(critical_date AS eligible_date)) AS reason,
FROM critical_date_has_passed_spans cd
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        state_code=StateCode.US_ME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        analyst_dataset=ANALYST_VIEWS_DATASET,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()

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
"""Defines a criteria span view that shows spans of time during which someone
has completed half their full term supervision or supervision out of state sentence.
"""
from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import (
    list_to_query_string,
    nonnull_end_date_clause,
)
from recidiviz.calculator.query.sessions_query_fragments import (
    join_sentence_spans_to_compartment_sessions,
)
from recidiviz.calculator.query.state.dataset_config import (
    NORMALIZED_STATE_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.calculator.query.state.views.sessions.state_sentence_configurations import (
    STATES_WITH_NO_INCARCERATION_SENTENCES_ON_SUPERVISION,
)
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = (
    "SUPERVISION_OR_SUPERVISION_OUT_OF_STATE_PAST_HALF_FULL_TERM_RELEASE_DATE"
)

_DESCRIPTION = """Defines a criteria span view that shows spans of time during which
someone has completed half their full term supervision or supervision out of state sentence"""

_QUERY_TEMPLATE = f"""
WITH critical_date_spans AS (
    SELECT
        span.state_code,
        span.person_id,
        span.start_date AS start_datetime,
        span.end_date AS end_datetime,
        (DATE_ADD(MAX(sent.effective_date),INTERVAL
            CAST(CEILING(DATE_DIFF(MAX(sent.projected_completion_date_max),MAX(sent.effective_date),DAY))/2 AS INT64) DAY)) AS critical_date
    {join_sentence_spans_to_compartment_sessions(compartment_level_1_to_overlap=["SUPERVISION", "SUPERVISION_OUT_OF_STATE"])} 
    WHERE
    -- due to sentence data quality issues, we exclude sentences where the effective date comes before the projected completion date max
    -- validation errors and information can be found in this epic (https://app.zenhub.com/workspaces/analysis-5f8f1c625afb1c0011c7222a/issues/gh/recidiviz/pulse-data/16206) 
        effective_date < projected_completion_date_max
        -- Exclude incarceration sentences for states that store all supervision
        -- sentence data (including parole)
        -- separately in supervision sentences
        AND (sent.state_code NOT IN ({{excluded_incarceration_states}}) OR sent.sentence_type = "SUPERVISION")
    GROUP BY 1, 2, 3, 4
),
{critical_date_has_passed_spans_cte()}
SELECT
    cd.state_code,
    cd.person_id,
    cd.start_date,
    cd.end_date,
    cd.critical_date_has_passed AS meets_criteria,
    TO_JSON(STRUCT(
        sup_type.supervision_type AS sentence_type,
        cd.critical_date AS eligible_date
    )) AS reason,
    sup_type.supervision_type AS sentence_type,
    cd.critical_date AS half_full_term_release_date,
FROM critical_date_has_passed_spans cd
LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_supervision_period` sup_type
    ON sup_type.state_code = cd.state_code
    AND sup_type.person_id = cd.person_id
    AND sup_type.start_date < {nonnull_end_date_clause('cd.end_date')}
    AND cd.start_date < {nonnull_end_date_clause('sup_type.termination_date')}
-- Prioritize the latest supervision period
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY state_code, person_id, cd.start_date
    ORDER BY
        sup_type.start_date DESC,
        {nonnull_end_date_clause('sup_type.termination_date')} DESC
) = 1
"""

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = StateAgnosticTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=_DESCRIPTION,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    sessions_dataset=SESSIONS_DATASET,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    excluded_incarceration_states=list_to_query_string(
        string_list=STATES_WITH_NO_INCARCERATION_SENTENCES_ON_SUPERVISION,
        quoted=True,
    ),
    reasons_fields=[
        ReasonsField(
            name="sentence_type",
            type=bigquery.enums.StandardSqlTypeNames.STRING,
            description="Indicates a client's supervision level",
        ),
        ReasonsField(
            name="half_full_term_release_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Date where a client has served half of their full term supervision sentence",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()

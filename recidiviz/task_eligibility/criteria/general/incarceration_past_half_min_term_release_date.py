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
has completed half their minimum term incarceration sentence.
"""
from recidiviz.calculator.query.sessions_query_fragments import (
    join_sentence_spans_to_compartment_sessions,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "INCARCERATION_PAST_HALF_MIN_TERM_RELEASE_DATE"

_DESCRIPTION = """Defines a criteria span view that shows spans of time during which someone
has completed half their minimum term incarceration sentence."""

_QUERY_TEMPLATE = f"""
WITH critical_date_spans AS (
    SELECT
        span.state_code,
        span.person_id,
        span.start_date AS start_datetime,
        span.end_date AS end_datetime,
        (DATE_ADD(MAX(sent.effective_date),INTERVAL
            CAST(CEILING(DATE_DIFF(MAX(sent.projected_completion_date_min),MAX(sent.effective_date),DAY))/2 AS INT64) DAY)) AS critical_date
    {join_sentence_spans_to_compartment_sessions(compartment_level_1_to_overlap="INCARCERATION")}
    WHERE
        sent.sentence_type = 'INCARCERATION'
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
        cd.critical_date AS eligible_date
    )) AS reason,
FROM critical_date_has_passed_spans cd
"""

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = (
    StateAgnosticTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        sessions_dataset=SESSIONS_DATASET,
    )
)
if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()

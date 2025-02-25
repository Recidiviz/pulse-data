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
Defines a criteria span view that calculates half time across all supervision sentences; 
where start_date is the first time a person began probation (based on super_sessions 
after partitioning ME SCCP sessions) and end_date is the final projected_release_date 
all supervision sentences considered.

This view was created because SUPERVISION_PAST_HALF_FULL_TERM_RELEASE_DATE didn't use
the right start_date in ME.
"""
from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)
from recidiviz.task_eligibility.utils.us_me_query_fragments import (
    compartment_level_1_super_sessions_without_me_sccp,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = (
    "US_ME_SUPERVISION_PAST_HALF_FULL_TERM_RELEASE_DATE_FROM_PROBATION_START"
)

_DESCRIPTION = """
Defines a criteria span view that calculates half time across all supervision sentences; 
where start_date is the first time a person began probation (based on super_sessions 
after partitioning ME SCCP sessions) and end_date is the final projected_release_date 
all supervision sentences considered.

This view was created because SUPERVISION_PAST_HALF_FULL_TERM_RELEASE_DATE didn't use
the right start_date in ME.
"""

_QUERY_TEMPLATE = f"""
WITH {compartment_level_1_super_sessions_without_me_sccp()},

critical_date_spans AS (
    SELECT
        sss.state_code,
        sss.person_id,
        GREATEST(sss.start_date, proj_end.start_date) AS start_datetime,
        LEAST(
            {nonnull_end_date_clause('sss.end_date_exclusive')},
            {nonnull_end_date_clause('proj_end.end_date_exclusive')}
        ) AS end_datetime,
        proj_end.projected_completion_date_max,
        DATE_ADD(sss.start_date,
                INTERVAL CAST(FLOOR(DATE_DIFF(projected_completion_date_max, sss.start_date, DAY)/2) AS INT64) DAY)
        AS critical_date,
    FROM partitioning_compartment_l1_ss_with_sccp sss
    LEFT JOIN `{{project_id}}.{{sessions_dataset}}.supervision_projected_completion_date_spans_materialized` proj_end
        ON sss.compartment_level_1 = "SUPERVISION"
            AND sss.state_code = proj_end.state_code 
            AND sss.person_id = proj_end.person_id 
            AND sss.start_date < {nonnull_end_date_clause('proj_end.end_date_exclusive')}
            AND proj_end.start_date < {nonnull_end_date_clause('sss.end_date_exclusive')}
    WHERE COALESCE(compartment_level_2, "") != 'COMMUNITY_CONFINEMENT'
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
    cd.critical_date AS eligible_date,
FROM critical_date_has_passed_spans cd
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        state_code=StateCode.US_ME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        sessions_dataset=SESSIONS_DATASET,
        reasons_fields=[
            ReasonsField(
                name="eligible_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Date when the client has served 1/2 of their sentence.",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()

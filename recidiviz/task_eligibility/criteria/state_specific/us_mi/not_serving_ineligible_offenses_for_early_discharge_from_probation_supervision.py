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
# ============================================================================
"""Defines a criteria span view that shows spans of time during which someone is not serving
ineligible offenses on probation supervision
"""
from recidiviz.calculator.query.sessions_query_fragments import (
    join_sentence_spans_to_compartment_sessions,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_MI_NOT_SERVING_INELIGIBLE_OFFENSES_FOR_EARLY_DISCHARGE_FROM_PROBATION_SUPERVISION"

_DESCRIPTION = """Defines a criteria span view that shows spans of time during which
someone is not serving on probation for any ineligible offense listed below: 
    - Not serving for MCL 750.81
    - Not serving for MC 750.84 (Assault with intent to commit Great bodily harm less than murder)
    - Not serving for an offense that requires a mandatory probation term (750.411H, 750.411I, 750.136b) 
"""
_QUERY_TEMPLATE = f"""
 SELECT
        span.state_code,
        span.person_id,
        span.start_date,
        span.end_date,
        FALSE AS meets_criteria,
        TO_JSON(STRUCT(ARRAY_AGG(DISTINCT statute) AS ineligible_offenses)) AS reason,
    {join_sentence_spans_to_compartment_sessions(compartment_level_2_to_overlap="PROBATION")}
    WHERE span.state_code = "US_MI"
        AND sent.sentence_sub_type = "PROBATION" 
        --only include spans with ineligible offenses 
        --include all statutes that have 750.81 followed by a digit (but not by a letter) since those are not substatutes
        AND ((sent.statute LIKE '750.81%' AND REGEXP_CONTAINS(statute, r'750\\.81(\\d+)*$'))
        OR sent.statute LIKE '750.84%'
        OR sent.statute LIKE '750.411H%'
        OR sent.statute LIKE '750.411I%'
        OR sent.statute LIKE '750.136B%')
    GROUP BY 1, 2, 3, 4,5
    """

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        state_code=StateCode.US_MI,
        sessions_dataset=SESSIONS_DATASET,
        meets_criteria_default=True,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()

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
"""Defines a criteria span view that shows spans of time during which someone has no ineligible offenses
for supervision level downgrade
"""
from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import (
    join_sentence_spans_to_compartment_sessions,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)

_CRITERIA_NAME = (
    "US_MI_NOT_SERVING_INELIGIBLE_OFFENSES_FOR_DOWNGRADE_FROM_SUPERVISION_LEVEL"
)

_DESCRIPTION = """Defines a criteria span view that shows spans of time during which someone has no ineligible offenses
(below) for supervision level downgrade
        - not currently serving for a sex offense
        - not serving for failure to register for SORA offense
        - not currently serving for aggravated stalking or Domestic Violence 3rd
        - not currently serving a life or commuted sentence
"""

_QUERY_TEMPLATE = f"""
 SELECT
        span.state_code,
        span.person_id,
        span.start_date,
        span.end_date,
        FALSE as meets_criteria,
        TO_JSON(STRUCT(ARRAY_AGG(DISTINCT statute IGNORE NULLS ORDER BY statute) AS ineligible_offenses,
                        ARRAY_AGG(DISTINCT sent.status IGNORE NULLS ORDER BY status) AS sentence_status,
                         LOGICAL_OR(sent.life_sentence) AS is_life_sentence,
                         LOGICAL_OR(sent.is_sex_offense) AS is_sex_offense )) AS reason,
        ARRAY_AGG(DISTINCT statute IGNORE NULLS ORDER BY statute) AS ineligible_offenses,
        ARRAY_AGG(DISTINCT sent.status IGNORE NULLS ORDER BY status) AS sentence_status,
        LOGICAL_OR(sent.life_sentence) AS is_life_sentence,
        LOGICAL_OR(sent.is_sex_offense) AS is_sex_offense,
    {join_sentence_spans_to_compartment_sessions(compartment_level_1_to_overlap="SUPERVISION")}
    WHERE span.state_code = "US_MI"
    AND (sent.is_sex_offense
        --failure to register for sex offense
        OR sent.statute LIKE '28.729%'
        --aggravated stalking
        OR sent.statute LIKE '750.411I%'
        --Domestic Violence 3rd 
        OR sent.statute LIKE '750.814%'
        OR sent.status = 'COMMUTED'
        OR sent.life_sentence
        )
    GROUP BY 1, 2, 3, 4, 5
    """

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        state_code=StateCode.US_MI,
        sessions_dataset=SESSIONS_DATASET,
        meets_criteria_default=True,
        reasons_fields=[
            ReasonsField(
                name="ineligible_offenses",
                type=bigquery.enums.StandardSqlTypeNames.ARRAY,
                description="#TODO(#29059): Add reasons field description",
            ),
            ReasonsField(
                name="sentence_status",
                type=bigquery.enums.StandardSqlTypeNames.ARRAY,
                description="#TODO(#29059): Add reasons field description",
            ),
            ReasonsField(
                name="is_life_sentence",
                type=bigquery.enums.StandardSqlTypeNames.BOOL,
                description="#TODO(#29059): Add reasons field description",
            ),
            ReasonsField(
                name="is_sex_offense",
                type=bigquery.enums.StandardSqlTypeNames.BOOL,
                description="#TODO(#29059): Add reasons field description",
            ),
        ],
    )
)

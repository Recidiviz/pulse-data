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
"""Spans of time when someone is serving a sentence eligible for AR administrative transfer,
based on the statute ("Target Offenses").
"""
from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import (
    join_sentence_spans_to_compartment_sessions,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_AR_SENTENCE_STATUTE_ELIGIBLE_FOR_ADMIN_TRANSFER"


_DESCRIPTION = """Spans of time when someone is serving a sentence eligible for AR administrative
transfer, based on the statute ("Target Offenses")."""

_QUERY_TEMPLATE = f"""
    WITH sentences AS (
        SELECT
            span.state_code,
            span.person_id,
            span.start_date,
            span.end_date,
            statute,
        {join_sentence_spans_to_compartment_sessions(compartment_level_1_to_overlap=["INCARCERATION"])}
    )
    ,
    statute_eligibility AS (
        SELECT
            LOWER(LTRIM(sentence_component_statute, '0')) AS statute,
            is_target AS target_group,
        FROM `{{project_id}}.us_ar_raw_data_up_to_date_views.RECIDIVIZ_REFERENCE_TARGET_STATUTES_latest`
    )
    ,
    -- Unnest to get one row per statute
    sentences_with_statutes AS (
        SELECT
            state_code,
            person_id,
            start_date,
            end_date,
            -- Normalize statute to remove non-numeric characters and leading zeros
            LOWER(LTRIM(statute, '0')) AS statute,
        FROM sentences,
        -- Multiple statutes appear in the `statute` field, separated by "@@"
        UNNEST(SPLIT(statute, "@@")) AS statute
    )
    ,
    statutes_with_eligibility AS (
        SELECT
            state_code,
            person_id,
            start_date,
            end_date,
            statute,
            -- Y: "Yes", is eligible. "M": "Maybe", is possibly eligible. "N": "No", is not eligible.
            target_group IN ('Y', 'M') AS eligible_statute,
            COALESCE(target_group, 'N') IN ('Y', 'M') AS m_or_y_statute,
            target_group
        FROM sentences_with_statutes
        LEFT JOIN statute_eligibility
        USING(statute)
    )
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        LOGICAL_AND(eligible_statute) AS meets_criteria,
        LOGICAL_AND(m_or_y_statute) AS meets_criteria_with_positive_statute_match,
        ARRAY_AGG(statute ORDER BY statute) AS statutes,
        ARRAY_AGG(target_group ORDER BY target_group) AS target_groups,
        TO_JSON(STRUCT(ARRAY_AGG(statute ORDER BY statute) AS statutes, ARRAY_AGG(target_group ORDER BY target_group) AS target_groups, LOGICAL_AND(m_or_y_statute) AS meets_criteria_with_positive_statute_match)) AS reason,
    FROM statutes_with_eligibility
    GROUP BY 1, 2, 3, 4
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=_DESCRIPTION,
    reasons_fields=[
        ReasonsField(
            name="statutes",
            type=bigquery.enums.StandardSqlTypeNames.STRUCT,
            description="List of statutes of sentences being served",
        ),
        ReasonsField(
            name="target_groups",
            type=bigquery.enums.StandardSqlTypeNames.STRUCT,
            description="List of target groups for each statute",
        ),
        ReasonsField(
            name="meets_criteria_with_positive_statute_match",
            type=bigquery.enums.StandardSqlTypeNames.BOOL,
            description="Whether the person is serving a sentence with a positive (M or Y) statute match",
        ),
    ],
    state_code=StateCode.US_AR,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    meets_criteria_default=True,
    sessions_dataset="sessions",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()

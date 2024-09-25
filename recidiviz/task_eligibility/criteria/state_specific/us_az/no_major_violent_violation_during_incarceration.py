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
"""Describes spans of time when someone has not had a violent violation during their incarceration period"""
from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_AZ_NO_MAJOR_VIOLENT_VIOLATION_DURING_INCARCERATION"

_DESCRIPTION = """Describes spans of time when someone has not had a violent violation during their incarceration
period"""

_QUERY_TEMPLATE = f"""
    WITH major_violent_violations AS (
        SELECT
            state_code,
            person_id,
            incident_date AS violation_date,
        FROM
            `{{project_id}}.{{normalized_state_dataset}}.state_incarceration_incident`
        WHERE
            state_code = 'US_AZ'
            AND incident_type = 'VIOLENCE'
    ),
    map_to_incarceration AS (
        SELECT 
            mvv.*,
            sesh.start_date,
            sesh.end_date,
            FALSE as meets_criteria,
        FROM major_violent_violations mvv
        LEFT JOIN `{{project_id}}.{{sessions_dataset}}.compartment_sessions_materialized` sesh
            ON mvv.person_id = sesh.person_id
            AND mvv.violation_date BETWEEN sesh.start_date AND {nonnull_end_date_clause('sesh.end_date')}
    ),
    {create_sub_sessions_with_attributes('map_to_incarceration')}
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        LOGICAL_AND(meets_criteria) AS meets_criteria,
        TO_JSON(STRUCT(ARRAY_AGG(violation_date IGNORE NULLS ORDER BY violation_date DESC) 
            AS associated_violations)) AS reason,
        ARRAY_AGG(violation_date IGNORE NULLS ORDER BY violation_date DESC) AS associated_violations,
    FROM sub_sessions_with_attributes
    GROUP BY 1,2,3,4
"""

_REASONS_FIELDS = [
    ReasonsField(
        name="associated_violations",
        type=bigquery.enums.StandardSqlTypeNames.ARRAY,
        description="A list of violations committed by a given resident, if applicable",
    ),
]

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        reasons_fields=_REASONS_FIELDS,
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        sessions_dataset=SESSIONS_DATASET,
        state_code=StateCode.US_AZ,
        meets_criteria_default=True,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()

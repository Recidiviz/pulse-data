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
"""Spans of time when someone is in county jail backup in Arkansas.
"""
from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_AR_IN_COUNTY_JAIL_BACKUP"

_DESCRIPTION = """Spans of time when someone is in county jail backup in Arkansas."""

_QUERY_TEMPLATE = f"""
    WITH location_subtype_sessions AS (
        SELECT
            ls.state_code,
            ls.person_id,
            ls.start_date,
            ls.end_date_exclusive AS end_date,
            -- TODO(#32293): Update once we have a better way to handle location subtypes
            COALESCE(JSON_EXTRACT_SCALAR(location_metadata, '$.location_subtype') IN ("B8", "BC"), FALSE) AS is_county_jail_backup,
        FROM `{{project_id}}.sessions.location_sessions_materialized` ls
        LEFT JOIN `{{project_id}}.reference_views.location_metadata_materialized` lm
        ON
            ls.state_code = lm.state_code
            AND ls.location = lm.location_external_id
        WHERE ls.state_code = 'US_AR'
    )
    ,
    {create_sub_sessions_with_attributes(
        table_name="location_subtype_sessions",
    )}
    ,
    dedup_cte AS (
        -- Prioritize location spans that are county jail backup where there are overlapping spans
        -- TODO(#31919): Remove or refine this deduplication once we have a better way to handle overlapping spans
        SELECT
            state_code,
            person_id,
            start_date,
            end_date,
            LOGICAL_OR(is_county_jail_backup) AS is_county_jail_backup,
        FROM
            sub_sessions_with_attributes
        GROUP BY 1,2,3,4
    )
    ,
    county_jail_backup_sessions AS ({aggregate_adjacent_spans(
        table_name='dedup_cte',
        attribute='is_county_jail_backup',
    )})
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        is_county_jail_backup AS meets_criteria,
        is_county_jail_backup,
        TO_JSON(STRUCT(is_county_jail_backup)) AS reason
    FROM county_jail_backup_sessions
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        reasons_fields=[
            ReasonsField(
                name="is_county_jail_backup",
                type=bigquery.enums.StandardSqlTypeNames.BOOL,
                description="Whether the person is in county jail backup.",
            )
        ],
        state_code=StateCode.US_AR,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        meets_criteria_default=False,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()

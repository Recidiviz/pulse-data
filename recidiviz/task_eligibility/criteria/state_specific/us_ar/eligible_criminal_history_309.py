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
"""Spans of time when someone has a criminal history eligible for 309 (institutional worker status) in Arkansas.
"""
from google.cloud import bigquery

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.general_criteria_builders import (
    create_sub_sessions_with_attributes,
    num_events_within_time_interval_spans,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_AR_ELIGIBLE_CRIMINAL_HISTORY_309"

_QUERY_TEMPLATE = f"""
    WITH sentences AS (
        SELECT
            state_code,
            person_id,
            date_imposed,
            offense_type,
            offense_type LIKE "%ATTEMPT_FLAG%" AS is_attempt,
            offense_type LIKE "%SOLICITATION_FLAG%" AS is_solicitation,
            offense_type LIKE "%CONSPIRACY_FLAG%" AS is_conspiracy,
        FROM `{{project_id}}.sessions.sentences_preprocessed_materialized`
        WHERE state_code = 'US_AR'
    )
    ,
    -- Unnest to get one row per offense type
    sentences_with_offense_types AS (
        SELECT
            state_code,
            person_id,
            date_imposed,
            offense_type,
            is_attempt,
            is_conspiracy,
            is_solicitation,
        FROM sentences,
        -- Multiple offense types appear in the `offense_type` field, separated by "@@"
        UNNEST(SPLIT(offense_type, "@@")) AS offense_type
    )
    ,
    aggravated_robbery_events AS (
        SELECT
            state_code,
            person_id,
            date_imposed AS event_date,
        FROM sentences_with_offense_types
        WHERE offense_type = "AGG_ROBBERY"
        -- For aggravated robbery, do not count inchoate offenses, except attempts
        AND (NOT is_conspiracy) AND (NOT is_solicitation)
    )
    ,
    multiple_aggravated_robbery_spans AS (
        WITH {num_events_within_time_interval_spans('aggravated_robbery_events')}
        SELECT
            state_code,
            person_id,
            start_date,
            CAST(NULL AS DATE) AS end_date,
            TRUE AS ineligible_offense_occurred,
            "AGG_ROBBERY" AS offense_type,
        FROM event_count_spans
        WHERE event_count >= 2
    )
    ,
    ineligible_offense_spans AS (
        SELECT
            state_code,
            person_id,
            date_imposed AS start_date,
            CAST(NULL AS DATE) AS end_date,
            TRUE AS ineligible_offense_occurred,
            offense_type,
        FROM sentences_with_offense_types
        -- TODO(#34320): Refine to include other sexual offenses once ingested
        WHERE offense_type IN ("CAPITAL_OFFENSE", "MURDER_1", "ESCAPE", "RAPE")
        -- For kidnapping, do not count inchoate offenses, except attempts
        OR (offense_type IN ("KIDNAPPING") AND (NOT is_conspiracy) AND (NOT is_solicitation))

        UNION ALL

        SELECT * FROM multiple_aggravated_robbery_spans
    )
    ,
    {create_sub_sessions_with_attributes(
        table_name="ineligible_offense_spans",
    )}
    ,
    dedup_cte AS (
        SELECT
            state_code,
            person_id,
            start_date,
            end_date,
            LOGICAL_OR(ineligible_offense_occurred) AS ineligible_offense_occurred,
            ARRAY_TO_STRING(ARRAY_AGG(DISTINCT offense_type ORDER BY offense_type), ', ') AS ineligible_offense_types,
        FROM
            sub_sessions_with_attributes
        GROUP BY 1,2,3,4
    )
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        NOT ineligible_offense_occurred AS meets_criteria,
        ineligible_offense_types,
        TO_JSON(STRUCT(ineligible_offense_types)) AS reason
    FROM dedup_cte
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=__doc__,
    reasons_fields=[
        ReasonsField(
            name="ineligible_offense_types",
            type=bigquery.enums.StandardSqlTypeNames.ARRAY,
            description="Ineligible offense types someone has been charged with for 309 (institutional worker status) in Arkansas.",
        )
    ],
    state_code=StateCode.US_AR,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    meets_criteria_default=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()

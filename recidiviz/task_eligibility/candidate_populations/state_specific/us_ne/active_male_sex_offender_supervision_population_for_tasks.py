# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
# =============================================================================
"""Candidate population of NE clients on active parole supervision with a
SEX_OFFENSE case type and male gender. Used by sex-offender-specific tasks
(e.g., STABLE assessment compliance) so the male/SO filtering does not need to
live inside individual criteria.
"""
from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.task_candidate_population_big_query_view_builder import (
    StateSpecificTaskCandidatePopulationBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_POPULATION_NAME = "US_NE_ACTIVE_MALE_SEX_OFFENDER_SUPERVISION_POPULATION_FOR_TASKS"

_QUERY_TEMPLATE = f"""
WITH active_supervision_population AS (
    SELECT
        css.state_code,
        css.person_id,
        css.start_date,
        css.end_date_exclusive AS end_date,
        SAFE_CAST(NULL AS STRING) AS case_type,
    FROM `{{project_id}}.sessions.compartment_sub_sessions_materialized` css
    WHERE css.state_code = 'US_NE'
        AND css.compartment_level_1 = 'SUPERVISION'
        AND css.metric_source != "INFERRED"
        AND css.compartment_level_2 = 'PAROLE'
        AND (
            css.correctional_level NOT IN
                ('IN_CUSTODY','WARRANT','ABSCONDED','ABSCONSION','EXTERNAL_UNKNOWN')
            OR css.correctional_level IS NULL
        )
        AND css.sex = 'MALE'
),

sex_offender_case_type AS (
    SELECT
        ctsl.state_code,
        ctsl.person_id,
        ctsl.start_date,
        ctsl.end_date,
        ctsl.case_type,
    FROM `{{project_id}}.tasks_views.case_type_supervision_level_spans_materialized` ctsl
    WHERE ctsl.state_code = 'US_NE'
        AND ctsl.case_type = 'SEX_OFFENSE'
),

combine AS (
    SELECT * FROM active_supervision_population
    UNION ALL
    SELECT * FROM sex_offender_case_type
),

{create_sub_sessions_with_attributes(table_name="combine")}

SELECT
    state_code,
    person_id,
    start_date,
    end_date,
FROM sub_sessions_with_attributes
WHERE start_date != {nonnull_end_date_clause('end_date')}
GROUP BY 1, 2, 3, 4
-- Span must appear in both active-supervision and sex-offender CTEs to qualify
HAVING COUNT(*) >= 2
"""

VIEW_BUILDER = StateSpecificTaskCandidatePopulationBigQueryViewBuilder(
    state_code=StateCode.US_NE,
    population_name=_POPULATION_NAME,
    population_spans_query_template=_QUERY_TEMPLATE,
    description=__doc__,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()

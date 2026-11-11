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
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""Helper SQL queries and view builders for Idaho supervision tasks, covering
supervision levels, the active supervision population, and initial residence and
employment verifications."""

from typing import List, Optional

from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.views.tasks.contact_type import ContactType
from recidiviz.calculator.query.state.views.tasks.state_specific_tasks_criteria_builders import (
    non_recurring_contact_compliance_builder,
)
from recidiviz.calculator.query.state.views.tasks.tasks_criteria_utils import (
    get_contact_events_for_type,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.task_candidate_population_big_query_view_builder import (
    StateSpecificTaskCandidatePopulationBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)


def supervision_level_criteria_query(
    excluded_levels: List[str],
) -> str:
    return f"""
#TODO(#22511) refactor to build off of a general criteria view builder
WITH so_spans AS (
SELECT
        state_code,
        person_id,
        start_date,
        end_date_exclusive AS end_date,
    #TODO(#20035) replace with supervision level raw text sessions once views agree
    FROM `{{project_id}}.{{sessions_dataset}}.compartment_sub_sessions_materialized`
    WHERE compartment_level_1 = 'SUPERVISION'
    AND correctional_level_raw_text IN {tuple(excluded_levels)}
    AND state_code = 'US_IX'
)
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        FALSE AS meets_criteria,
    TO_JSON(STRUCT(TRUE AS supervision_level_is_so)) AS reason,
    TRUE AS supervision_level_is_so,
    FROM ({aggregate_adjacent_spans(table_name='so_spans')})
"""


def us_ix_active_supervision_population_view_builder(
    population_name: str,
    description: str,
    case_types: List[str],
    supervision_levels: List[str],
    additional_where_clause: Optional[str] = None,
) -> StateSpecificTaskCandidatePopulationBigQueryViewBuilder:
    """
    Creates a VIEW_BUILDER for Idaho active supervision population with specified filters.

    This includes people on PROBATION, PAROLE, DUAL, or COMMUNITY_CONFINEMENT
    with specified case types and supervision levels.

    Args:
        population_name: The name of the population (e.g., 'US_IX_ACTIVE_SUPERVISION_POPULATION_FOR_TASKS')
        description: The description/docstring for the population
        case_types: List of case types to include (e.g., ['GENERAL', 'SEX_OFFENSE'])
        supervision_levels: List of supervision levels to include (e.g., ['MINIMUM', 'MEDIUM', 'HIGH'])
        additional_where_clause: Optional additional WHERE clause to apply to compartment_sub_sessions.
            Should start with 'AND'. Example: "AND css.sex = 'MALE'"
    """
    case_types_str = "('" + "', '".join(case_types) + "')"
    supervision_levels_str = "('" + "', '".join(supervision_levels) + "')"

    # Build additional where clause if specified
    where_clause_addition = additional_where_clause or ""

    query_template = f"""WITH active_supervision_population AS (
    -- Active supervision population on PROBATION, PAROLE, or DUAL supervision
    SELECT
        css.state_code,
        css.person_id,
        css.start_date,
        css.end_date_exclusive AS end_date,
        css.compartment_level_1,
        css.compartment_level_2,
        SAFE_CAST(NULL AS STRING) AS case_type,
        SAFE_CAST(NULL AS STRING) AS supervision_level
    FROM
        `{{project_id}}.sessions.compartment_sub_sessions_materialized` css
    WHERE css.compartment_level_1 IN ('SUPERVISION')
        AND css.metric_source != "INFERRED"
        AND css.compartment_level_2 IN ('PROBATION', 'PAROLE', 'DUAL', 'COMMUNITY_CONFINEMENT')
        AND css.correctional_level NOT IN ('IN_CUSTODY','WARRANT','ABSCONDED','ABSCONSION','EXTERNAL_UNKNOWN')
        AND css.start_date >= '1900-01-01'
        {where_clause_addition}
),

supervision_case_and_level AS (
    -- Filter by case types and supervision levels
    SELECT
        ctsl.state_code,
        ctsl.person_id,
        ctsl.start_date,
        ctsl.end_date,
        SAFE_CAST(NULL AS STRING) AS compartment_level_1,
        SAFE_CAST(NULL AS STRING) AS compartment_level_2,
        ctsl.case_type,
        ctsl.supervision_level,
    FROM `{{project_id}}.tasks_views.us_ix_case_type_supervision_level_spans_materialized` ctsl
        WHERE ctsl.case_type IN {case_types_str}
            AND ctsl.supervision_level IN {supervision_levels_str}
),

combine AS (
    SELECT *
    FROM active_supervision_population

    UNION ALL

    SELECT *
    FROM supervision_case_and_level
),

{create_sub_sessions_with_attributes(table_name="combine")}

SELECT
    state_code,
    person_id,
    start_date,
    end_date,
FROM sub_sessions_with_attributes
WHERE start_date != {nonnull_end_date_clause('end_date')}
GROUP BY 1,2,3,4
-- We only want spans where both criteria are met
HAVING COUNT(*) >= 2
"""

    return StateSpecificTaskCandidatePopulationBigQueryViewBuilder(
        state_code=StateCode.US_IX,
        population_name=population_name,
        population_spans_query_template=query_template,
        description=description,
    )


# Days after case assignment within which residence and employment must be verified.
US_IX_INITIAL_VERIFICATION_DAYS = 30

# Spans from each person's start of supervision to the end of their supervision super
# session, excluding Expanded CRC Furlough (XCRC) clients, whom policy exempts from the
# initial residence and employment verifications. The start is the first COURT_SENTENCE
# or RELEASE_FROM_INCARCERATION start in the super session, since those reasons mark a
# person beginning supervision rather than continuing it; without one, the super session
# start is used.
_US_IX_INITIAL_VERIFICATION_PERIOD_SPANS_QUERY = f"""
    WITH supervision_starts AS (
        SELECT
            sss.state_code,
            sss.person_id,
            MIN(IFNULL(cl1.start_date, sss.start_date)) AS start_date,
            sss.end_date_exclusive AS end_date,
        FROM `{{project_id}}.sessions.supervision_super_sessions_materialized` sss
        LEFT JOIN `{{project_id}}.sessions.compartment_level_1_super_sessions_materialized` cl1
            ON cl1.person_id = sss.person_id
            AND cl1.state_code = sss.state_code
            AND cl1.start_date BETWEEN sss.start_date AND {nonnull_end_date_clause('sss.end_date')}
            AND cl1.start_reason IN ('COURT_SENTENCE', 'RELEASE_FROM_INCARCERATION')
            AND cl1.compartment_level_1 = 'SUPERVISION'
        WHERE sss.state_code = 'US_IX'
        GROUP BY sss.state_code, sss.person_id, sss.supervision_super_session_id, sss.end_date_exclusive
    )
    SELECT
        ss.state_code,
        ss.person_id,
        ss.start_date,
        ss.end_date,
    FROM supervision_starts ss
    LEFT JOIN `{{project_id}}.tasks_views.us_ix_case_type_supervision_level_spans_materialized` xcrc
        ON xcrc.person_id = ss.person_id
        AND xcrc.state_code = ss.state_code
        AND xcrc.case_type = 'XCRC'
        AND ss.start_date BETWEEN xcrc.start_date AND {nonnull_end_date_clause('xcrc.end_date')}
    WHERE xcrc.person_id IS NULL
"""


def us_ix_initial_verification_trigger_view_builder(
    *,
    criteria_name: str,
    description: str,
    contact_type: ContactType,
    only_include_completed_contacts: bool,
    include_earliest_unmet_due_date: bool,
) -> StateSpecificTaskCriteriaBigQueryViewBuilder:
    """Returns a criterion view builder for a one-time verification due within
    US_IX_INITIAL_VERIFICATION_DAYS of the start of supervision. Each span is open
    until a contact of contact_type is logged. XCRC clients get no spans.

    Args:
        criteria_name (str): The name of the criterion view.
        description (str): A brief description of the criterion view.
        contact_type (ContactType): The contact type that satisfies the verification.
        only_include_completed_contacts (bool): Whether only COMPLETED contacts
            satisfy the verification.
        include_earliest_unmet_due_date (bool): Whether to emit an
            earliest_unmet_due_date reasons field, for criteria grouped with a
            fixed-cadence criterion that surfaces that field as the display due date.
    """
    return non_recurring_contact_compliance_builder(
        criteria_name=criteria_name,
        description=description,
        state_code=StateCode.US_IX,
        contact_events_query=get_contact_events_for_type(
            [contact_type],
            StateCode.US_IX,
            only_include_completed_contacts=only_include_completed_contacts,
        ),
        contact_types=[contact_type],
        contact_period_spans_query=_US_IX_INITIAL_VERIFICATION_PERIOD_SPANS_QUERY,
        time_period=US_IX_INITIAL_VERIFICATION_DAYS,
        only_include_completed_contacts=only_include_completed_contacts,
        include_earliest_unmet_due_date=include_earliest_unmet_due_date,
    )

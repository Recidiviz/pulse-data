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
"""Defines a criteria span view that shows spans of time during which someone has completed 12 months
on active supervision if serving a sentence for operating under the influence of liquor or operating while impaired
"""
from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = (
    "US_MI_IF_SERVING_AN_OUIL_OR_OWI_HAS_COMPLETED_12_MONTHS_ON_SUPERVISION"
)

_DESCRIPTION = """Defines a criteria span view that shows spans of time during which someone has completed 12 months
on active supervision if serving a sentence for operating under the influence of liquor or operating while impaired
"""
_QUERY_TEMPLATE = f"""
WITH owi_ouil_spans AS (
/* This CTE identifies sentence spans where an owi/ouil sentence is being served */
SELECT DISTINCT
    span.state_code,
    span.person_id,
    span.start_date,
    span.end_date,
    FROM `{{project_id}}.{{sessions_dataset}}.sentence_spans_materialized` span,
    UNNEST (sentences_preprocessed_id_array_actual_completion) AS sentences_preprocessed_id
    INNER JOIN `{{project_id}}.{{sessions_dataset}}.sentences_preprocessed_materialized` sent
      USING (state_code, person_id, sentences_preprocessed_id)
    INNER JOIN `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.RECIDIVIZ_REFERENCE_offense_exclusion_list_latest`e
        ON e.statute_code = sent.statute
    WHERE span.state_code = "US_MI"
    AND CAST(is_owi_ouil AS BOOL)
),
 owi_ouil_aggregated_spans AS (
 /* Sentence spans are aggregated here so that sub sessions later on can be identified as falling into owi/ouil
 sentence spans or not. Without aggregating spans, sub sessions that are split between two adjacent
owi/ouil sentence spans will be excluded */ 
{aggregate_adjacent_spans('owi_ouil_spans')}
),
owi_ouil_supervision_super_sessions AS (
/* Here, supervision super sessions are identified where a client is serving for an owi or ouil at any point in the
super session. Since any owi/ouil sentence spans that overlap with a supervison session are identified, 
the active_supervision_population_cumulative_day_spans starts accumulating days on supervision at the 
supervision session start, and not necessarily at the effective date of the owi/ouil sentence */
  SELECT DISTINCT
    ss.state_code,
    ss.person_id,
    ss.start_date,
    ss.end_date,
    ss.session_id_start,
    ss.session_id_end,
    ss.supervision_super_session_id,
    FROM owi_ouil_aggregated_spans span
    INNER JOIN `{{project_id}}.{{sessions_dataset}}.supervision_super_sessions_materialized` ss
        ON span.state_code = ss.state_code
        AND span.person_id = ss.person_id
        -- Use strictly less than for exclusive end_dates
        AND span.start_date < {nonnull_end_date_clause('ss.end_date_exclusive')}
        AND ss.start_date < {nonnull_end_date_clause('span.end_date')}
),
active_supervision_population_cumulative_day_spans AS
/* This CTE analyses supervision_super_sessions to create a cumulative_inactive_days variable that counts days spent
in inactive supervision, currently defined as compartment_level_2 = `BENCH_WARRANT`. This variable is used to NULL out
critical dates during inactive sub sessions as well as push out the critical date by the number of cumulative_inactive_days
once an active supervision sub session resumes. */
(
/*Join sub-sessions to super-sessions*/
  SELECT 
    sub.person_id,
    sub.sub_session_id,
    sub.session_id,
    sub.state_code,
    super.supervision_super_session_id AS super_session_id,
    sub.start_date,
    sub.end_date_exclusive AS end_date,
    sub.compartment_level_1,
    sub.compartment_level_2,
    sub.session_length_days,
    sub.inactive_session_days,
    SUM(sub.inactive_session_days) OVER(PARTITION BY sub.person_id, sub.state_code, super.supervision_super_session_id ORDER BY sub.start_date) AS cumulative_inactive_days,
    super.start_date AS super_session_start_date,
    super.end_date AS super_session_end_date,
  FROM
      (SELECT
          *,
          -- Here is where we define which sessions within a supervision super session should stop the clock
          IF(compartment_level_2 IN ('BENCH_WARRANT') OR 
             correctional_level IN ('ABSCONSION', 'IN_CUSTODY'), session_length_days, 0) AS inactive_session_days,
      FROM `{{project_id}}.{{sessions_dataset}}.compartment_sub_sessions_materialized` 
      ) sub
  JOIN owi_ouil_supervision_super_sessions super
    ON sub.person_id = super.person_id
    AND sub.state_code = super.state_code
    AND sub.session_id BETWEEN super.session_id_start AND super.session_id_end
),
critical_date_sub_session_spans AS (
/* This CTE sets a NULL critical date for inactive supervision sub sessions and otherwise sets the critical date as
12 months from the supervision super session start. In addition, for active sub sessions preceded by inactive
sub sessions, it bumps out the critical date by the number of days spent on inactive supervision. 

Additionally, it filters out sub sessions outside the date range of the owi/ouil sentence spans so that sub sessions
where a client is NOT serving for a owi/ouil are not included in eligibility calculations. Futhermore, it includes sub 
sessions that partially overlap with the owi/ouil span, and sets the end date of the sub session to the LEAST of the 
owi/ouil span and the sub session span. That way, the entire owi/ouil span is included in the crtiteria but no spans of 
time outside the owi/ouil spans are included. Since the meets_criteria default for this criteria is TRUE, 
eligibility is only considered when a client is serving for an owi/ouil.  */
    SELECT
    DISTINCT
    a.person_id,
    a.state_code,
    GREATEST(a.start_date, span.start_date) AS start_date,
    LEAST({nonnull_end_date_clause('a.end_date')}, {nonnull_end_date_clause('span.end_date')}) AS end_date,
    IF (inactive_session_days=0, DATE_ADD(DATE_ADD(super_session_start_date, INTERVAL 1 YEAR),
                                                        INTERVAL cumulative_inactive_days DAY), NULL) AS critical_date,
    a.super_session_start_date,
    a.super_session_end_date,
    FROM owi_ouil_aggregated_spans span
    INNER JOIN active_supervision_population_cumulative_day_spans a
        ON span.state_code = a.state_code
        AND span.person_id = a.person_id
        AND span.start_date < {nonnull_end_date_clause('a.end_date')}
        AND a.start_date < {nonnull_end_date_clause('span.end_date')}
),
critical_date_spans AS (
/* This CTE aggregates adjacent spans from critical_date_sub_session_spans based on the critical date. */
  SELECT 
    sp.state_code,
    sp.person_id,
    sp.start_date AS start_datetime,
    sp.end_date AS end_datetime,
    sp.critical_date,
   FROM ({aggregate_adjacent_spans(table_name='critical_date_sub_session_spans', 
                                  attribute=['critical_date', 'super_session_start_date', 'super_session_end_date'])}) sp
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
    cd.critical_date AS one_year_on_supervision_date,
FROM critical_date_has_passed_spans cd
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        state_code=StateCode.US_MI,
        sessions_dataset=SESSIONS_DATASET,
        raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
            state_code=StateCode.US_MI,
            instance=DirectIngestInstance.PRIMARY,
        ),
        meets_criteria_default=True,
        reasons_fields=[
            ReasonsField(
                name="one_year_on_supervision_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="#TODO(#29059): Add reasons field description",
            )
        ],
    )
)
if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()

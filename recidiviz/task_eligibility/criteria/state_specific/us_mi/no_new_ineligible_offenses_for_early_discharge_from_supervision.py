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
"""Defines a criteria span view that shows spans of time during which someone has no new
ineligible offenses on parole/dual or probation supervision
"""
from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import (
    NORMALIZED_STATE_DATASET,
    SESSIONS_DATASET,
)
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

_CRITERIA_NAME = "US_MI_NO_NEW_INELIGIBLE_OFFENSES_FOR_EARLY_DISCHARGE_FROM_SUPERVISION"

_DESCRIPTION = """Defines a criteria span view that shows spans of time during which
someone has no new ineligible offenses (below) on supervision
        - No involvement or suspected offenses during parole/probation requiring registry under the Sex Offender Registration Act
        - No new assaultive misdemeanors during parole/probation period
        - No new felony during parole/probation period
"""
_QUERY_TEMPLATE = f"""
WITH supervision_starts AS (
/* This CTE creates spans of parole or dual supervision sessions */
  SELECT
    state_code,
    person_id,
    start_date,
    end_date,
  FROM `{{project_id}}.{{sessions_dataset}}.compartment_sessions_materialized`
  WHERE state_code = "US_MI"
    AND 
    --ignore transitions from PAROLE to DUAL as parole starts
    ((compartment_level_2 IN ("PAROLE", "DUAL")  AND inflow_from_level_2 NOT IN ("PAROLE", "DUAL"))
   OR compartment_level_2 = 'PROBATION') 
),
sentences_and_violations AS (
/* This CTE groups by person and date imposed where ineligible offenses were sentenced */
  SELECT
      span.state_code,
      span.person_id,
      sent.date_imposed AS critical_date, 
  FROM `{{project_id}}.{{sessions_dataset}}.sentence_spans_materialized` span,
  UNNEST (sentences_preprocessed_id_array_actual_completion) AS sentences_preprocessed_id
  INNER JOIN `{{project_id}}.{{sessions_dataset}}.sentences_preprocessed_materialized` sent
    USING (state_code, person_id, sentences_preprocessed_id)
  WHERE state_code = "US_MI"
  AND (sent.statute IN (SELECT statute_code FROM `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.RECIDIVIZ_REFERENCE_offense_exclusion_list_latest`
            WHERE (CAST(requires_so_registration AS BOOL)
            OR CAST(is_assaultive_misdemeanor AS BOOL)))
    OR sent.classification_type = "FELONY")
  GROUP BY 1, 2, 3
  UNION ALL 
  SELECT
        v.state_code,
        v.person_id,
        v.violation_date AS critical_date,
    FROM `{{project_id}}.{{normalized_state_dataset}}.state_supervision_violation_type_entry` vt
    LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_supervision_violation` v
        ON vt.supervision_violation_id = v.supervision_violation_id
        AND vt.person_id = v.person_id
        AND vt.state_code = v.state_code
    WHERE vt.state_code = "US_MI"
    AND vt.violation_type = "FELONY"
    GROUP BY 1,2,3
),
critical_date_spans AS (
  SELECT
    p.state_code,
    p.person_id,
    p.start_date AS start_datetime,
    p.end_date AS end_datetime,
    s.critical_date,
  FROM supervision_starts p
  INNER JOIN sentences_and_violations s
    ON p.state_code = s.state_code
    AND p.person_id = s.person_id
    --only join supervision spans that have an ineligible offense date imposed during a supervision session
    AND s.critical_date > p.start_date 
    AND s.critical_date <= {nonnull_end_date_clause('p.end_date')}
),
{critical_date_has_passed_spans_cte()},
{create_sub_sessions_with_attributes('critical_date_has_passed_spans')}
SELECT
    cd.state_code,
    cd.person_id,
    cd.start_date,
    cd.end_date,
    --group by spans and if any ineligible offense date imposed has passed, set meets_criteria to FALSE
    NOT LOGICAL_OR(cd.critical_date_has_passed) AS meets_criteria,
    --only included violation dates where meets_criteria is FALSE
    TO_JSON(STRUCT(
      IF(LOGICAL_OR(cd.critical_date_has_passed), ARRAY_AGG(cd.critical_date ORDER BY cd.critical_date), NULL)
        AS latest_ineligible_convictions
    )) AS reason,
    IF(LOGICAL_OR(cd.critical_date_has_passed), ARRAY_AGG(cd.critical_date ORDER BY cd.critical_date), NULL)
        AS latest_ineligible_convictions,
FROM sub_sessions_with_attributes cd
GROUP BY 1,2,3,4

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
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        meets_criteria_default=True,
        reasons_fields=[
            ReasonsField(
                name="latest_ineligible_convictions",
                type=bigquery.enums.StandardSqlTypeNames.ARRAY,
                description="#TODO(#29059): Add reasons field description",
            )
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()

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
"""Idaho state-specific preprocessing for early discharge sessions."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    NORMALIZED_STATE_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_IX_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_NAME = (
    "us_ix_early_discharge_sessions_preprocessing"
)

US_IX_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_DESCRIPTION = (
    """Idaho state-specific preprocessing for early discharge sessions."""
)

DISCHARGE_SESSION_DIFF_DAYS = "7"

US_IX_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_QUERY_TEMPLATE = f"""
WITH us_ix_ed_probation AS (
  SELECT 
    sess.person_id,
    sess.end_date AS ed_date,
  FROM `{{project_id}}.{{sessions_dataset}}.supervision_projected_completion_date_spans_materialized` proj
  INNER JOIN `{{project_id}}.{{sessions_dataset}}.compartment_sessions_materialized` sess
    ON proj.state_code = sess.state_code
    AND proj.person_id = sess.person_id
    --only look at projected supervision completion date spans that overlap with the end of a probation session
    AND proj.start_date < {nonnull_end_date_exclusive_clause('sess.end_date_exclusive')}
    AND sess.end_date <= {nonnull_end_date_exclusive_clause('proj.end_date_exclusive')}
  WHERE proj.state_code = 'US_IX'
    AND sess.end_reason IN ('DISCHARGE') 
    AND compartment_level_2 IN ('PROBATION', 'INFORMAL_PROBATION')
    AND compartment_level_1  IN ('SUPERVISION', 'SUPERVISION_OUT_OF_STATE')
    -- only include sessions where the projected max completion date for that session is at least two weeks 
    -- after their discharge date
    AND DATE_DIFF(proj.projected_completion_date_max, sess.end_date_exclusive, DAY) > 14
),
us_ix_ed_parole AS (
  SELECT 
    DISTINCT
      person_id,
      decision_date AS ed_date
  FROM `{{project_id}}.{{normalized_state_dataset}}.state_early_discharge` 
  WHERE state_code = 'US_IX'
    AND decision = 'SENTENCE_TERMINATION_GRANTED'
    AND decision_status = 'DECIDED'
),
us_ix_ed_all AS (
  SELECT *
  FROM us_ix_ed_probation
  UNION ALL 
  SELECT *
  FROM us_ix_ed_parole
),

us_ix_supervision_sessions AS (
  SELECT 
    *
  FROM `{{project_id}}.{{sessions_dataset}}.compartment_sessions_materialized`
  WHERE compartment_level_1 IN ('SUPERVISION', 'SUPERVISION_OUT_OF_STATE')
    AND state_code = 'US_IX'
    AND end_date_exclusive IS NOT NULL
),

us_ix_ed_sessions AS (
  SELECT 
    ses.state_code,
    ses.person_id,
    ses.session_id,
    et.ed_date,
    ses.end_date,
    ABS(DATE_DIFF(et.ed_date, ses.end_date, DAY)) AS discharge_to_session_end_days,
    ses.outflow_to_level_1,
  FROM us_ix_ed_all et
  INNER JOIN us_ix_supervision_sessions ses
    ON ses.person_id = et.person_id
    AND DATE_SUB(ed_date, INTERVAL 1 DAY) 
      BETWEEN start_date AND {nonnull_end_date_exclusive_clause('ses.end_date_exclusive')}
  QUALIFY ROW_NUMBER() OVER(PARTITION BY ses.person_id, ses.session_id, ses.state_code
    ORDER BY et.ed_date DESC) = 1
)

SELECT 
  *
FROM us_ix_ed_sessions s
WHERE discharge_to_session_end_days <= CAST({{discharge_session_diff_days}} AS INT64)
"""

US_IX_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_IX_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_NAME,
    description=US_IX_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_DESCRIPTION,
    view_query_template=US_IX_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_QUERY_TEMPLATE,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    discharge_session_diff_days=DISCHARGE_SESSION_DIFF_DAYS,
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_IX_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_BUILDER.build_and_print()

# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Spans of time with the projected max completion date for clients under supervision
as indicated by the sentences that were active during that span."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.state.dataset_config import (
    NORMALIZED_STATE_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_latest_views_dataset_for_region,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_NAME = "supervision_projected_completion_date_spans"

_VIEW_DESCRIPTION = """
Spans of time with the projected max completion date for clients under supervision as
indicated by the sentences that were active during that span.
"""

STATES_WITH_NO_INCARCERATION_SENTENCES_ON_SUPERVISION = [StateCode.US_ND]

_QUERY_TEMPLATE = f"""
WITH all_states_spans AS (
    SELECT
        span.state_code,
        span.person_id,
        span.start_date,
        span.end_date_exclusive,
        MAX(sent.projected_completion_date_max) AS projected_completion_date_max,
    FROM `{{project_id}}.{{sessions_dataset}}.sentence_spans_materialized` span,
    UNNEST (sentences_preprocessed_id_array) AS sentences_preprocessed_id
    INNER JOIN `{{project_id}}.{{sessions_dataset}}.sentences_preprocessed_materialized` sent
      USING (state_code, person_id, sentences_preprocessed_id)
    INNER JOIN `{{project_id}}.{{sessions_dataset}}.compartment_sessions_materialized` sess
        ON span.state_code = sess.state_code
        AND span.person_id = sess.person_id
        -- Restrict to spans that overlap with supervision sessions
        AND sess.compartment_level_1 = "SUPERVISION"
        -- Use strictly less than for exclusive end_dates
        AND span.start_date < {nonnull_end_date_clause('sess.end_date_exclusive')}
        AND sess.start_date < {nonnull_end_date_clause('span.end_date_exclusive')}
    WHERE
        -- Exclude incarceration sentences for states that store all supervision
        -- sentence data (including parole)
        -- separately in supervision sentences
        (sent.state_code NOT IN ("{{excluded_incarceration_states}}") OR sent.sentence_type = "SUPERVISION")
    GROUP BY 1, 2, 3, 4
),
#TODO(#15410) remove piping of raw data once validation issues are addressed
id_max_date AS ( 
  SELECT 
  "US_ID" AS state_code,
  pei.person_id,
  SAFE_CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', sent_ftrd_dt) AS DATE) AS projected_completion_date_external,
FROM `{{project_id}}.{{raw_data_up_to_date_dataset}}.summary_ofndr_ftrd_dt_recidiviz_latest` ftrd
INNER JOIN `{{project_id}}.{{normalized_dataset}}.state_person_external_id` pei
  ON ftrd.ofndr_num = pei.external_id
  AND pei.state_code = 'US_ID'
)
SELECT 
    a.state_code,
    a.person_id,
    a.start_date,
    IF(MAX(COALESCE(a.end_date_exclusive,'9999-12-31')) OVER(PARTITION BY a.person_id) = COALESCE(a.end_date_exclusive,'9999-12-31'),
      NULL, a.end_date_exclusive) AS end_date_exclusive,
    IF(MAX(COALESCE(a.end_date_exclusive,'9999-12-31')) OVER(PARTITION BY a.person_id) = COALESCE(a.end_date_exclusive,'9999-12-31'),
      NULL, a.end_date_exclusive) AS end_date,
    COALESCE(id.projected_completion_date_external, a.projected_completion_date_max) AS projected_completion_date_max,
FROM all_states_spans a
LEFT JOIN id_max_date id
    ON id.person_id = a.person_id 
    AND id.state_code = a.state_code
    AND CURRENT_DATE('US/Pacific') <= end_date_exclusive 
"""

SUPERVISION_LATEST_PROJECTED_COMPLETION_DATE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=_VIEW_NAME,
    view_query_template=_QUERY_TEMPLATE,
    description=_VIEW_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    normalized_dataset=NORMALIZED_STATE_DATASET,
    raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_ID, instance=DirectIngestInstance.PRIMARY
    ),
    excluded_incarceration_states='", "'.join(
        [state.name for state in STATES_WITH_NO_INCARCERATION_SENTENCES_ON_SUPERVISION]
    ),
    should_materialize=True,
    clustering_fields=["state_code", "person_id"],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_LATEST_PROJECTED_COMPLETION_DATE_VIEW_BUILDER.build_and_print()

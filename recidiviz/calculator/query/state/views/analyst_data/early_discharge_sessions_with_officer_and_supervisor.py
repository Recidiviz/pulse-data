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
"""View of early_discharge_sessions with additional information related to 
1) the officer who supervised the individual during the early discharge, and
2) the supervisor who supervised the officer during the early discharge"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# States currently supported
SUPPORTED_STATES = ["US_ME"]

EARLY_DISCHARGE_SESSIONS_WITH_OFFICER_AND_SUPERVISOR_VIEW_NAME = (
    "early_discharge_sessions_with_officer_and_supervisor"
)

EARLY_DISCHARGE_SESSIONS_WITH_OFFICER_AND_SUPERVISOR_VIEW_DESCRIPTION = """
View of early_discharge_sessions with additional information related to 
1) the officer who supervised the individual during the early discharge, and
2) the supervisor who supervised the officer during the early discharge"""

EARLY_DISCHARGE_SESSIONS_WITH_OFFICER_AND_SUPERVISOR_QUERY_TEMPLATE = f"""
WITH ed_sessions AS (
    -- Early Discharge/Termination sessions
    SELECT 
        eds.*,
        sss.start_date AS probation_start_date,
        DATE_DIFF(eds.discharge_date, sss.start_date, MONTH) AS months_in_probation,
    FROM 
        `{{project_id}}.analyst_data.early_discharge_sessions_materialized` eds
    LEFT JOIN 
        `{{project_id}}.sessions.supervision_super_sessions_materialized` sss
    ON 
        eds.state_code = sss.state_code
        AND eds.person_id = sss.person_id
        AND eds.discharge_date BETWEEN sss.start_date AND IFNULL(sss.end_date_exclusive, '9999-12-31')
    WHERE eds.early_discharge = 1
        AND eds.state_code IN ('{{supported_states}}')
),

dv_sentences_spans AS (
    -- Domestic Violence sentences spans
    SELECT 
        span.state_code,
        span.person_id,
        span.start_date,
        span.end_date,
        TRUE AS is_domestic_violence,
    FROM 
        `{{project_id}}.sessions.sentence_spans_materialized` span,
    UNNEST (sentences_preprocessed_id_array_projected_completion) AS sentences_preprocessed_id
    INNER JOIN 
        `{{project_id}}.sessions.sentences_preprocessed_materialized` sent
    USING (state_code, person_id, sentences_preprocessed_id)
    WHERE sentence_type = 'SUPERVISION'
         #TODO(#25238): make this condition state-agnostic
        AND REGEXP_CONTAINS(description, r'DOMESTIC') -- ME-specific
    GROUP BY 1,2,3,4
)

SELECT 
    sos.state_code,
    sos.person_id,
    eds.discharge_date AS early_discharge_date,
    sos.supervising_officer_external_id AS officer_id,
    esm.officer_name, 
    esm.officer_email,
    esm.most_recent_supervisor_name AS supervisor_name,
    esm.most_recent_supervisor_staff_external_id AS supervisor_id,
    esm.most_recent_supervisor_email AS supervisor_email,
    eds.probation_start_date,
    eds.months_in_probation,
    IFNULL(dv.is_domestic_violence, False) AS is_domestic_violence,
FROM 
    ed_sessions eds
-- Map each ED with an officer
INNER JOIN 
    `{{project_id}}.sessions.supervision_officer_sessions_materialized` sos
ON 
    eds.person_id = sos.person_id
    AND eds.state_code = sos.state_code
    AND eds.discharge_date BETWEEN sos.start_date AND {nonnull_end_date_clause('end_date_exclusive')}
    AND sos.supervising_officer_external_id IS NOT NULL
-- Map each officer to his/her supervisor
LEFT JOIN 
    `{{project_id}}.reference_views.state_staff_and_most_recent_supervisor_with_names` esm
ON 
    esm.officer_staff_external_id = sos.supervising_officer_external_id
    AND eds.state_code = esm.state_code
LEFT JOIN 
    dv_sentences_spans dv
ON 
    eds.discharge_date BETWEEN dv.start_date AND 
        {nonnull_end_date_clause('dv.end_date')}
    AND eds.person_id = dv.person_id
    AND eds.state_code = dv.state_code"""

EARLY_DISCHARGE_SESSIONS_WITH_OFFICER_AND_SUPERVISOR_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=EARLY_DISCHARGE_SESSIONS_WITH_OFFICER_AND_SUPERVISOR_VIEW_NAME,
    description=EARLY_DISCHARGE_SESSIONS_WITH_OFFICER_AND_SUPERVISOR_VIEW_DESCRIPTION,
    view_query_template=EARLY_DISCHARGE_SESSIONS_WITH_OFFICER_AND_SUPERVISOR_QUERY_TEMPLATE,
    supported_states="', '".join(SUPPORTED_STATES),
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        EARLY_DISCHARGE_SESSIONS_WITH_OFFICER_AND_SUPERVISOR_VIEW_BUILDER.build_and_print()

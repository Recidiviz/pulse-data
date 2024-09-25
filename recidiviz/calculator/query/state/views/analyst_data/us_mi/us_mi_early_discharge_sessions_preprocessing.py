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
"""Michigan state-specific preprocessing for early discharge sessions"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_MI_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_NAME = (
    "us_mi_early_discharge_sessions_preprocessing"
)

US_MI_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_DESCRIPTION = (
    """Michigan state-specific preprocessing for early discharge sessions"""
)

DISCHARGE_SESSION_DIFF_DAYS = "7"

US_MI_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_QUERY_TEMPLATE = f"""
    -- Probation: state_supervision_period.termination_reason_raw_text = “125” (Early Discharge from Probation)
    WITH us_mi_ed_probation AS (
        SELECT DISTINCT
            person_id,
            termination_date AS discharge_date,
        FROM `{{project_id}}.{{normalized_state_dataset}}.state_supervision_period`
        WHERE state_code = 'US_MI'
            AND termination_reason_raw_text = '125'
    ),
    -- Parole: state_supervision_period.termination_reason_raw_text = “30” (Early Discharge from Parole)
    us_mi_ed_parole AS (
        SELECT DISTINCT
            person_id,
            termination_date AS discharge_date,
        FROM `{{project_id}}.{{normalized_state_dataset}}.state_supervision_period`
        WHERE state_code = 'US_MI'
            AND termination_reason_raw_text = '30'
    ),
    us_mi_ed_all AS (
        SELECT * FROM us_mi_ed_probation
        UNION DISTINCT
        SELECT * FROM us_mi_ed_parole
    ),
    -- Join to sessions based on state/person, and where discharge date lands between start/end date
    -- Due to differences in when sessions determines start/end dates, expand the interval by 1 day
    us_mi_ed_sessions AS (
        SELECT
            sessions.state_code,
            sessions.person_id,
            sessions.session_id,
            us_mi_ed_all.discharge_date AS ed_date,
            sessions.end_date_exclusive AS end_date,
            ABS(DATE_DIFF(us_mi_ed_all.discharge_date, sessions.end_date_exclusive, DAY)) AS discharge_to_session_end_days,
            sessions.outflow_to_level_1,
        FROM `{{project_id}}.{{sessions_dataset}}.compartment_sessions_materialized` sessions
        INNER JOIN us_mi_ed_all
            ON sessions.person_id = us_mi_ed_all.person_id
            AND DATE_SUB(us_mi_ed_all.discharge_date, INTERVAL 1 DAY) BETWEEN sessions.start_date 
                                                AND {nonnull_end_date_exclusive_clause('sessions.end_date_exclusive')}
        WHERE sessions.state_code = 'US_MI'
            AND sessions.end_date_exclusive IS NOT NULL
            AND compartment_level_1 IN ('SUPERVISION', 'SUPERVISION_OUT_OF_STATE')
        QUALIFY ROW_NUMBER() OVER(
            PARTITION BY sessions.person_id, sessions.session_id
            ORDER BY us_mi_ed_all.discharge_date DESC
        ) = 1
    )
    SELECT * FROM us_mi_ed_sessions
    WHERE discharge_to_session_end_days <= CAST({{discharge_session_diff_days}} AS INT64) 
"""

US_MI_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_MI_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_NAME,
    description=US_MI_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_DESCRIPTION,
    view_query_template=US_MI_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_QUERY_TEMPLATE,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    discharge_session_diff_days=DISCHARGE_SESSION_DIFF_DAYS,
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_MI_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_BUILDER.build_and_print()

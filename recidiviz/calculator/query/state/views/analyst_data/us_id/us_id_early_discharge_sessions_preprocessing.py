# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Idaho state-specific preprocessing for early discharge sessions"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SESSIONS_DATASET,
    STATE_BASE_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ID_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_NAME = (
    "us_id_early_discharge_sessions_preprocessing"
)

US_ID_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_DESCRIPTION = (
    """Idaho state-specific preprocessing for early discharge sessions"""
)

US_ID_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_QUERY_TEMPLATE = """
    -- Probation: SupervisionPeriod.termination_reason = 'DISCHARGE'
    WITH us_id_ed_probation AS (
        SELECT DISTINCT
            person_id,
            termination_date AS discharge_date,
        FROM `{project_id}.{state_dataset}.state_supervision_period`
        WHERE state_code = 'US_ID'
            AND termination_reason = 'DISCHARGE'
    ),
    -- Parole: StateIncarcerationSentence.status = 'COMPLETED' AND
    --         StateIncarcerationSentence.status_raw_text = 'F'
    us_id_ed_parole AS (
        SELECT DISTINCT
            person_id,
            completion_date AS discharge_date,
        FROM `{project_id}.{state_dataset}.state_incarceration_sentence`
        WHERE state_code = 'US_ID'
            AND status = 'COMPLETED'
            AND status_raw_text = 'F'
    ),
    us_id_ed_all AS (
        SELECT * FROM us_id_ed_probation
        UNION DISTINCT
        SELECT * FROM us_id_ed_parole
    ),
    -- Join to sessions based on state/person, and where discharge date lands between start/end date
    -- Due to differences in when sessions determines start/end dates, expand the interval by 1 day
    us_id_ed_sessions AS (
        SELECT
            sessions.state_code,
            sessions.person_id,
            sessions.session_id,
            us_id_ed_all.discharge_date AS ed_date,
            sessions.end_date,
            ABS(DATE_DIFF(us_id_ed_all.discharge_date, sessions.end_date, DAY)) AS discharge_to_session_end_days,
            sessions.outflow_to_level_1,
        FROM `{project_id}.{sessions_dataset}.compartment_sessions_materialized` sessions
        INNER JOIN us_id_ed_all
            ON sessions.person_id = us_id_ed_all.person_id
            AND DATE_SUB(us_id_ed_all.discharge_date, INTERVAL 1 DAY) BETWEEN sessions.start_date AND COALESCE(sessions.end_date, '9999-01-01')
        WHERE sessions.state_code = 'US_ID'
            AND sessions.end_date IS NOT NULL
            AND compartment_level_1 IN ('SUPERVISION', 'SUPERVISION_OUT_OF_STATE')
        QUALIFY ROW_NUMBER() OVER(
            PARTITION BY sessions.person_id, sessions.session_id
            ORDER BY us_id_ed_all.discharge_date DESC
        ) = 1
    )

    SELECT * FROM us_id_ed_sessions
"""

US_ID_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_ID_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_NAME,
    description=US_ID_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_DESCRIPTION,
    view_query_template=US_ID_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_QUERY_TEMPLATE,
    state_dataset=STATE_BASE_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ID_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_BUILDER.build_and_print()

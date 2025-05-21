# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Nebraska client metadata"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import (
    today_between_start_date_and_nullable_end_date_clause,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import WORKFLOWS_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_NE_CLIENT_METADATA_VIEW_NAME = "us_ne_client_metadata"

US_NE_CLIENT_METADATA_VIEW_DESCRIPTION = """
Nebraska client metadata
"""

US_NE_CLIENT_METADATA_VIEW_QUERY_TEMPLATE = f"""
-- The supervision session we use for the client record; there should be one row per person
WITH 

projected_dates AS (
    SELECT
        person_id,
        group_projected_parole_release_date,
    FROM `{{project_id}}.{{sentence_sessions_dataset}}.person_projected_date_sessions_materialized`
    WHERE state_code = "US_NE"
    AND start_date <= CURRENT_DATE("US/Eastern")
    QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY start_date DESC) = 1
)
,
last_four_oras_scores AS (
    SELECT
        person_id,
        TO_JSON(
            ARRAY_AGG(
                STRUCT(
                    assessment_level AS assessment_level,
                    assessment_date AS assessment_date
                )
                ORDER BY assessment_date DESC LIMIT 4
            )
        ) AS last_four_oras_scores,
    FROM `{{project_id}}.normalized_state.state_assessment`
    WHERE
        state_code = 'US_NE'
        AND assessment_type = 'ORAS_COMMUNITY_SUPERVISION_SCREENING'
    GROUP BY person_id
)
,
special_conditions AS (
    SELECT
        person_id,
        TO_JSON(
            ARRAY_AGG(
                STRUCT(
                    special_condition_type AS special_condition_type,
                    compliance AS compliance
                )
                ORDER BY special_condition_type ASC
            )
        ) AS special_conditions,
    FROM `{{project_id}}.sessions.us_ne_special_condition_compliance_sessions_materialized`
    WHERE {today_between_start_date_and_nullable_end_date_clause('start_date', 'end_date_exclusive')}
    GROUP BY person_id
)

SELECT
    pd.person_id,
    pd.group_projected_parole_release_date as parole_earned_discharge_date,
    oras.last_four_oras_scores,
    sc.special_conditions
FROM projected_dates pd
LEFT JOIN last_four_oras_scores oras USING(person_id)
LEFT JOIN special_conditions sc USING(person_id)
"""

US_NE_CLIENT_METADATA_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=WORKFLOWS_VIEWS_DATASET,
    view_id=US_NE_CLIENT_METADATA_VIEW_NAME,
    view_query_template=US_NE_CLIENT_METADATA_VIEW_QUERY_TEMPLATE,
    description=US_NE_CLIENT_METADATA_VIEW_DESCRIPTION,
    should_materialize=True,
    sentence_sessions_dataset=dataset_config.SENTENCE_SESSIONS_DATASET,
)


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_NE_CLIENT_METADATA_VIEW_BUILDER.build_and_print()

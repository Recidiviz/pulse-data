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
"""Computes Q4 (age) of Idaho's Reclassification of Security Level form at any point in time"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_IX_SLS_Q4_VIEW_NAME = "us_ix_sls_q4"

US_IX_SLS_Q4_VIEW_DESCRIPTION = """Computes Q4 (age) of Idaho's Reclassification of Security Level form at any point
in time. See details of the Reclassification Form here:
https://drive.google.com/file/d/1-Y3-RAqPEUrAKoeSdkNTDmB-V1YAEp1l/view
    """

US_IX_SLS_Q4_QUERY_TEMPLATE = f"""
    WITH age_spans AS (
        SELECT 
            state_code,
            person_id,
            start_date,
            end_date_exclusive AS end_date,
            age,
            CASE 
              WHEN age <=23 THEN 3
              WHEN age BETWEEN 24 AND 31 THEN 2
              WHEN age BETWEEN 32 AND 38 THEN 1
              WHEN age BETWEEN 39 AND 50 THEN 0
              WHEN age >= 51 THEN -1 
            ELSE NULL 
            END AS q4_score
        FROM `{{project_id}}.{{sessions_dataset}}.person_age_sessions`
        WHERE state_code = 'US_IX'
        
    )
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        q4_score
    FROM ({aggregate_adjacent_spans(table_name = 'age_spans', 
                                    attribute='q4_score')})
"""

US_IX_SLS_Q4_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_IX_SLS_Q4_VIEW_NAME,
    description=US_IX_SLS_Q4_VIEW_DESCRIPTION,
    view_query_template=US_IX_SLS_Q4_QUERY_TEMPLATE,
    sessions_dataset=SESSIONS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_IX_SLS_Q4_VIEW_BUILDER.build_and_print()

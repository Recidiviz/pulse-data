# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""North Dakota state-specific preprocessing for work-release sessions."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import (
    NORMALIZED_STATE_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.task_eligibility.utils.us_nd_query_fragments import (
    ATP_FACILITIES,
    MINIMUM_SECURITY_FACILITIES,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ND_WORK_RELEASE_SESSIONS_PREPROCESSING_VIEW_NAME = (
    "us_nd_work_release_sessions_preprocessing"
)

US_ND_WORK_RELEASE_SESSIONS_PREPROCESSING_VIEW_DESCRIPTION = """
North Dakota state-specific preprocessing for work_release sessions. Most folks are identified
as being on work-release based on their facility assignment, although sometimes we need
to use their program assignment too."""

US_ND_WORK_RELEASE_SESSIONS_PREPROCESSING_QUERY_TEMPLATE = f"""
WITH wr_facilities AS (
    -- All facilities that have work release programs
    SELECT  
      state_code,
      person_id,
      start_date,
      end_date_exclusive,
      facility,
      housing_unit,
    FROM `{{project_id}}.{{sessions_dataset}}.housing_unit_sessions_materialized`
    WHERE state_code = 'US_ND'
      AND facility IN {tuple(ATP_FACILITIES + MINIMUM_SECURITY_FACILITIES)}
),

wr_as_program AS (
    -- All work release programs registered in the program profiles table
    SELECT
      state_code,
      person_id,
      start_date,
      discharge_date AS end_date_exclusive,
      SPLIT(program_id, "@@")[SAFE_OFFSET(1)] AS DESCRIPTION,
    FROM `{{project_id}}.{{normalized_state_dataset}}.state_program_assignment`
    WHERE state_code = 'US_ND'
        AND SPLIT(program_id, "@@")[SAFE_OFFSET(1)] IN (
            'YCC INSTITUTIONAL WORK RELEASE',
            'WORK RELEASE',
            'JRMU WORK RELEASE'
        )
),

wr_sessions AS (
    -- In MINIMUM_SECURITY_FACILITIES, folks are only on WR if they are assigned explicitly to a WR program
    SELECT
        f.state_code,
        f.person_id,
        -- Given that someone could start at MINIMUM_SECURITY_FACILITIES without being in a WR program, we may 
        --      need to use the start_date of the WR program to determine when they 
        --      started WR.
        GREATEST(
            f.start_date,
            p.start_date
        ) AS start_date ,
        LEAST(
            f.end_date_exclusive,
            p.end_date_exclusive
        ) AS end_date,
        f.facility,
        f.housing_unit,
    FROM wr_facilities f
    INNER JOIN wr_as_program p
        ON f.person_id = p.person_id
        AND f.state_code = p.state_code
        AND f.start_date < {nonnull_end_date_exclusive_clause('p.end_date_exclusive')}
        AND p.start_date < {nonnull_end_date_exclusive_clause('f.end_date_exclusive')}
    WHERE f.facility IN {tuple(MINIMUM_SECURITY_FACILITIES)}
        -- HRCC does not have a work-release program
        AND f.facility != 'HRCC'

    UNION ALL 

    -- In BTC, folks are only on WR if they are not in the Women's Treatment 
    --    and Recovery Unit (WTRU)
    SELECT *
    FROM wr_facilities
    WHERE 
        -- Not in BTC's WTRU
        facility = 'BTC' AND NOT REGEXP_CONTAINS(housing_unit, r'WTRU')

    UNION ALL

    SELECT *
    FROM wr_facilities
    WHERE facility NOT IN {tuple(MINIMUM_SECURITY_FACILITIES + ['BTC'])}
),

{create_sub_sessions_with_attributes("wr_sessions")},

wr_sessions_deduped AS (
    -- Deduping overlapping sessions
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date AS end_date_exclusive,
    FROM sub_sessions_with_attributes
    GROUP BY 1,2,3,4
)

SELECT 
    state_code,
    person_id,
    start_date,
    end_date_exclusive,
FROM ({aggregate_adjacent_spans(table_name='wr_sessions_deduped',
                                end_date_field_name="end_date_exclusive")})
"""

US_ND_WORK_RELEASE_SESSIONS_PREPROCESSING_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=US_ND_WORK_RELEASE_SESSIONS_PREPROCESSING_VIEW_NAME,
    description=US_ND_WORK_RELEASE_SESSIONS_PREPROCESSING_VIEW_DESCRIPTION,
    view_query_template=US_ND_WORK_RELEASE_SESSIONS_PREPROCESSING_QUERY_TEMPLATE,
    sessions_dataset=SESSIONS_DATASET,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ND_WORK_RELEASE_SESSIONS_PREPROCESSING_VIEW_BUILDER.build_and_print()

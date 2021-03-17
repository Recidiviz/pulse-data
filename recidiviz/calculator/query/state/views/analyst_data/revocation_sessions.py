# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""A table of supervision start sessions with revocation session identifiers in cases where the person was revoked from that supervision term"""
# pylint: disable=trailing-whitespace
# pylint: disable=line-too-long
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

REVOCATION_SESSIONS_VIEW_NAME = "revocation_sessions"

REVOCATION_SESSIONS_VIEW_DESCRIPTION = """
    A table of supervision start sessions with revocation session identifiers in cases where the person was revoked from 
    that supervision term.
    """

REVOCATION_SESSIONS_QUERY_TEMPLATE = """
    /*{description}*/
    SELECT
        person_id,
        state_code,
        supervision_super_session_id,
        session_id_start AS supervision_session_id,
        sub_session_id_start AS supervision_sub_session_id,
        start_date AS supervision_start_date,
        DATE_DIFF(last_day_of_data, start_date, DAY) AS days_since_start,
        CAST(FLOOR(DATE_DIFF(last_day_of_data, start_date, DAY)/30) AS INT64) AS months_since_start,
        CAST(FLOOR(DATE_DIFF(last_day_of_data, start_date, DAY)/365.25) AS INT64) AS years_since_start,
        revocation_date,
        CASE WHEN revocation_date IS NOT NULL THEN 1 ELSE 0 END AS revocation,
        CASE WHEN revocation_date IS NOT NULL THEN session_id_end + 1 END AS revocation_session_id,
        CASE WHEN revocation_date IS NOT NULL THEN sub_session_id_end + 1 END AS revocation_sub_session_id,
        DATE_DIFF(revocation_date, start_date, DAY) AS supervision_start_to_revocation_days,
        CAST(CEILING(DATE_DIFF(revocation_date, start_date, DAY)/30) AS INT64) AS supervision_start_to_revocation_months,
        CAST(CEILING(DATE_DIFF(revocation_date, start_date, DAY)/365.25) AS INT64) AS supervision_start_to_revocation_years
    FROM
        (
        SELECT 
            *,
            CASE WHEN outflow_to_level_1 = 'INCARCERATION' THEN DATE_ADD(end_date, INTERVAL 1 DAY) END AS revocation_date
        FROM `{project_id}.{analyst_dataset}.supervision_super_sessions_materialized`
        )
    ORDER BY 1,2,3
    """

REVOCATION_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=REVOCATION_SESSIONS_VIEW_NAME,
    view_query_template=REVOCATION_SESSIONS_QUERY_TEMPLATE,
    description=REVOCATION_SESSIONS_VIEW_DESCRIPTION,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        REVOCATION_SESSIONS_VIEW_BUILDER.build_and_print()

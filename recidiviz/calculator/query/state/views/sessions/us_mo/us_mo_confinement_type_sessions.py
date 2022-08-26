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
"""Sessionization of housing stays to confinement type"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_clause,
    revert_nonnull_end_date_clause,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_MO_CONFINEMENT_TYPE_SESSIONS_VIEW_NAME = "us_mo_confinement_type_sessions"

US_MO_CONFINEMENT_TYPE_SESSIONS_VIEW_DESCRIPTION = (
    """Sessionization of housing stays to confinement type"""
)

US_MO_CONFINEMENT_TYPE_SESSIONS_QUERY_TEMPLATE = f"""
    /* {{description}} */
    SELECT
        person_id,
        state_code,
        confinement_type_session_id,
        MIN(start_date) AS start_date,
        {revert_nonnull_end_date_clause('MAX(end_date)')} AS end_date,
        confinement_type,
        FROM
            (
            SELECT 
                *,
                SUM(IF(date_gap OR attribute_change,1,0)) OVER(PARTITION BY person_id ORDER BY start_date, end_date) AS confinement_type_session_id
            FROM
                (
                SELECT
                    * EXCEPT(end_date),
                    {nonnull_end_date_clause('end_date')} AS end_date,
                    COALESCE(confinement_type != LAG(confinement_type) OVER w, TRUE) AS attribute_change,
                    COALESCE(LAG(end_date) OVER w != start_date, TRUE) AS date_gap
                FROM `{{project_id}}.{{sessions_dataset}}.us_mo_housing_stay_sessions_materialized`
                WINDOW w AS (PARTITION BY person_id ORDER BY start_date, {nonnull_end_date_clause('end_date')})
                )
            )
        GROUP BY 1,2,3,6
"""

US_MO_CONFINEMENT_TYPE_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    view_id=US_MO_CONFINEMENT_TYPE_SESSIONS_VIEW_NAME,
    description=US_MO_CONFINEMENT_TYPE_SESSIONS_VIEW_DESCRIPTION,
    view_query_template=US_MO_CONFINEMENT_TYPE_SESSIONS_QUERY_TEMPLATE,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_MO_CONFINEMENT_TYPE_SESSIONS_VIEW_BUILDER.build_and_print()

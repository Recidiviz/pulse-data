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
"""
View that is unique on person_id, release_session_id, and cohort_months. Each person and release from prison
has a record for each cohort that it is eligible to be a part of. Eligibility is determined based on having at least
that many months between the release date and the last day of data.

Additionally, the reincarceration flag is altered so that it represents not just a reincarceration,
but a reincarceration within that row's cohort month window.
"""
# pylint: disable=trailing-whitespace
# pylint: disable=line-too-long
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

REINCARCERATION_COHORT_SESSIONS_VIEW_NAME = "reincarceration_cohort_sessions"

REINCARCERATION_COHORT_SESSIONS_VIEW_DESCRIPTION = """
View that is unique on person_id, release_session_id, and cohort_months. Each person and release from prison
has a record for each cohort that it is eligible to be a part of. Eligibility is determined based on having at least 
that many months between the release date and the last day of data.

Additionally, the reincarceration flag is altered so that it represents not just a reincarceration,
but a reincarceration within that row's cohort month window.
"""

REINCARCERATION_COHORT_SESSIONS_QUERY_TEMPLATE = """
    /*{description}*/
    SELECT
        r.state_code,
        r.person_id,
        r.release_date,
        i.cohort_months,
        DATE_ADD(r.release_date, INTERVAL i.cohort_months MONTH) AS cohort_date,
        c.session_id AS cohort_session_id,
        r.release_session_id,
        CASE WHEN r.release_to_reincarceration_months <= i.cohort_months THEN 1 ELSE 0 END AS reincarceration,
        r.reincarceration_session_id,
        r.months_since_release,
        r.release_to_reincarceration_months,
        MAX(i.cohort_months) OVER(PARTITION BY r.person_id, r.release_date) AS max_cohort_months,
    FROM `{project_id}.{analyst_dataset}.reincarceration_sessions_from_sessions_materialized` r
    JOIN `{project_id}.{analyst_dataset}.cohort_month_index` i
        ON r.months_since_release>=i.cohort_months
    LEFT JOIN `{project_id}.{analyst_dataset}.compartment_sessions_materialized` c
        ON c.person_id = r.person_id
        AND DATE_ADD(r.release_date, INTERVAL i.cohort_months MONTH) BETWEEN c.start_date AND COALESCE(c.end_date, '9999-01-01')
    """

REINCARCERATION_COHORT_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=REINCARCERATION_COHORT_SESSIONS_VIEW_NAME,
    view_query_template=REINCARCERATION_COHORT_SESSIONS_QUERY_TEMPLATE,
    description=REINCARCERATION_COHORT_SESSIONS_VIEW_DESCRIPTION,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        REINCARCERATION_COHORT_SESSIONS_VIEW_BUILDER.build_and_print()

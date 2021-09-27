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
has a record for each eligible cohort. Eligibility is determined based on having at least
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
## Overview
View that is unique on `person_id`, `release_session_id`, and `cohort_months`. Each person and release from prison has a record for each eligible cohort. Eligibility is determined based on having at least that many months between the release date and the last day of data.

Additionally, the `reincarceration` flag is altered so that it represents not just a reincarceration, but a reincarceration within that row's cohort month window.

## Field Definitions
|	Field	|	Description	|
|	--------------------	|	--------------------	|
|	state_code	|	State	|
|	person_id	|	Unique person identifier	|
|	release_date	|	Date that a person is released. This is the cohort "start" date.	|
|	cohort_months	|	Month increment that each `person_id` and `release_session_id` are compared to. Used for calculating X month reincarceration rate rate. This value is incremented by 1-month for the first 18-months and then by 6-months up to 96 months. Each `person_id` and `release_session_id` will only be indexed for as many months as they have between the start date and the last day of data. For example, if a person was released 3-months and 5-days ago, they would have a record for months 1, 2, and 3.	|
|	cohort_date	|	The release date plus the cohort month index value. This is the date that each cohort is being evaluated at.	|
|	cohort_session_id	|	The compartment `session_id` of the session that the person is in as of the `cohort_date`. This is used to measure outcomes 6-months, 12-months, etc after a release.	|
|	release_session_id	|	The compartment session id of the release session	|
|	reincarceration	|	A 0/1 indicator of whether the release session was followed by a reincarceratin _as of the cohort month value_. If a person had a reincarceration 5-months and 10-days after the release, they would have a value of 0 for all records up to the 6-month value. All records 6-months onward would have a value of 1.	|
|	reincarceration_date	|	Reincarceration date (for releases followed by a reincarceration)	|
|	reincarceration_session_id	|	The compartment session id for the reincarceration session	|
|	months_since_release	|	Calendar count of full months between a person's release date and the last day of data. This field is pulled from `reincarceration_sessions` and is used to determine how many rows will exist within a given person and release session. If a person has a `months_since_release` value of 48 they will have `cohort_months` values up to 48 as well.	|
|	release_to_reincarceration_months	|	Calendar count of months between release and reincarceration. This is the field to determine for which records within a person and release session the `reincarceration` field takes on a value of 1.	|
|	max_cohort_months	|	Within a person and releaes session the max value of `cohort_months`. Used to determine the set of cohorts that a person / release session is eligible for.	|

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
        r.reincarceration_date,
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

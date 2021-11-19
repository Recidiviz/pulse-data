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
"""
View that is unique on person_id, supervision_super_session_id, and cohort_months. Each person and each super session
has a record for each cohort that it is eligible to be a part of. Eligibility is determined based on having at least
that many months between the supervision start and the last day of data.

Additionally, the revocation flag is altered so that it represents not just a super session ending in a revocation,
but a super session ending in a revocation within that row's cohort month window.
"""
# pylint: disable=trailing-whitespace
# pylint: disable=line-too-long
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

REVOCATION_COHORT_SESSIONS_VIEW_NAME = "revocation_cohort_sessions"

REVOCATION_COHORT_SESSIONS_VIEW_DESCRIPTION = """
## Overview

View that is unique on `person_id`, `supervision_super_session_id`, and `cohort_months`. Each person and each super session has a record for each cohort that it is eligible to be a part of. Eligibility is determined based on having at least that many months between the supervision start and the last day of data.

Additionally, the `revocation` flag is altered so that it represents not just a super session ending in a revocation, but a super session ending in a revocation within that row's cohort month window.

## Field Definitions

|	Field	|	Description	|
|	--------------------	|	--------------------	|
|	state_code	|	State	|
|	person_id	|	Unique person identifier	|
|	supervision_start_date	|	Start date of the supervision super-session	|
|	cohort_months	|	Month increment that each `person_id` and `supervision_super_session_id` are compared to. Used for calculating X month revocation rate. This value is incremented by 1-month for the first 18-months and then by 6-months up to 96 months. Each `person_id` and `supervision_super_session_id` will only be indexed for as many months as they have between the start date and the last day of data. For example, if a person started a supervision super-session 3-months and 5-days ago, they would have a record for months 1, 2, and 3.	|
|	cohort_date	|	The supervision start date plus the cohort month index value. This is the date that each cohort is being evaluated at.	|
|	cohort_session_id	|	The compartment `session_id` of the session that the person is in as of the `cohort_date`. This is used to measure outcomes 6-months, 12-months, etc after a supervision-start.	|
|	supervision_super_session_id	|	The supervision super-session id of the session that outcomes are evaluated relative to	|
|	supervision_session_id	|	The session id of the compartment session at the start of the supervision super-session	|
|	revocation	|	A 0/1 indicator of whether the super-session ended in a revocation _as of the cohort month value_. If a person had a revocation 5-months and 10-days after the super-session start, they would have a value of 0 for all records up the 6-month value. All records 6-months onward would have a value of 1.	|
|	revocation_date	|	Revocation date (for super-sessions that ended in a revocation)	|
|	revocation_session_id	|	The compartment session id for the revocation session	|
|	months_since_start	|	Calendar count of full months between a person's supervision start date and the last day of data. This field is pulled from `revocation_sessions` and is used to determine how many rows will exist within a given person and super-session. If a person has a `months_since_start` value of 48 they will have `cohort_months` values up to 48 as well.	|
|	supervision_start_to_revocation_months	|	Calendar count of months between supervision start and revocation. This is the field to determine for which records within a person and super-session the `revocation` field takes on a value of 1.	|
|	max_cohort_months	|	Within a person and super-session the max value of `cohort_months`. Used to determine the set of cohorts that a person / super-session is eligible for.	|

"""

REVOCATION_COHORT_SESSIONS_QUERY_TEMPLATE = """
    /*{description}*/
    SELECT
        r.state_code,
        r.person_id,
        r.supervision_start_date,
        i.cohort_months,
        DATE_ADD(r.supervision_start_date, INTERVAL i.cohort_months MONTH) AS cohort_date,
        c.session_id AS cohort_session_id,
        r.supervision_super_session_id,
        r.supervision_session_id,
        CASE WHEN r.supervision_start_to_revocation_months <= i.cohort_months THEN 1 ELSE 0 END AS revocation,
        r.revocation_date,
        r.revocation_session_id,
        r.months_since_start,
        r.supervision_start_to_revocation_months,
        MAX(i.cohort_months) OVER(PARTITION BY r.person_id, r.supervision_start_date) AS max_cohort_months,
    FROM `{project_id}.{sessions_dataset}.revocation_sessions_materialized` r
    JOIN `{project_id}.{sessions_dataset}.cohort_month_index` i
        ON r.months_since_start>=i.cohort_months
    LEFT JOIN `{project_id}.{sessions_dataset}.compartment_sessions_materialized` c
        ON c.person_id = r.person_id
        AND DATE_ADD(r.supervision_start_date, INTERVAL i.cohort_months MONTH) BETWEEN c.start_date AND COALESCE(c.end_date, '9999-01-01')
    """

REVOCATION_COHORT_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=REVOCATION_COHORT_SESSIONS_VIEW_NAME,
    view_query_template=REVOCATION_COHORT_SESSIONS_QUERY_TEMPLATE,
    description=REVOCATION_COHORT_SESSIONS_VIEW_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    clustering_fields=["state_code", "person_id"],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        REVOCATION_COHORT_SESSIONS_VIEW_BUILDER.build_and_print()

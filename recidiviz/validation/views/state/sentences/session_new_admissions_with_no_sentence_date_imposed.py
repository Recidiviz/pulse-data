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
"""A validation view that contains all of the new admission sessions that do not
have a sentence imposed on that start date."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

SESSION_NEW_ADMISSIONS_WITH_NO_SENTENCE_DATE_IMPOSED_VIEW_NAME = (
    "session_new_admissions_with_no_sentence_date_imposed"
)

SESSION_NEW_ADMISSIONS_WITH_NO_SENTENCE_DATE_IMPOSED_DESCRIPTION = """
Return all new admission sessions (inflowing from liberty or investigation) that
do not have a corresponding sentence imposed on the same date.
"""

SESSION_NEW_ADMISSIONS_WITH_NO_SENTENCE_DATE_IMPOSED_QUERY_TEMPLATE = """
WITH sessions_to_sentences AS (
    SELECT
        ses.state_code,
        ses.person_id,
        ses.session_id,
        ses.compartment_level_1,
        ses.start_date AS session_start_date,
        sen.imposed_date AS sentence_date_imposed,
        COALESCE(
            DATE_DIFF(ses.start_date, sen.imposed_date, DAY),
            9999
        ) AS sentence_to_session_offset_days,
    FROM `{project_id}.{sessions_dataset}.compartment_sessions_materialized` ses
    LEFT JOIN `{project_id}.sentence_sessions.sentences_and_charges_materialized` sen
        ON ses.state_code = sen.state_code
        AND ses.person_id = sen.person_id
        -- Only join sentences that were imposed before the session ended
        AND sen.imposed_date < COALESCE(DATE_ADD(ses.end_date, INTERVAL 1 DAY), CURRENT_DATE('US/Eastern'))
    WHERE inflow_from_level_1 IN ("LIBERTY", "INVESTIGATION")
        AND compartment_level_1 IN ("SUPERVISION", "INCARCERATION")
    -- Pick the closest sentence imposed for each session start
    QUALIFY ROW_NUMBER() OVER (PARTITION BY state_code, person_id, start_date
            ORDER BY ABS(DATE_DIFF(ses.start_date, sen.imposed_date, DAY)) ASC) = 1
)
SELECT
    state_code,
    state_code AS region_code,
    person_id,
    session_id,
    compartment_level_1,
    session_start_date,
    sentence_date_imposed,
FROM sessions_to_sentences
WHERE sentence_to_session_offset_days != 0
"""


SESSION_NEW_ADMISSIONS_WITH_NO_SENTENCE_DATE_IMPOSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=SESSION_NEW_ADMISSIONS_WITH_NO_SENTENCE_DATE_IMPOSED_VIEW_NAME,
    view_query_template=SESSION_NEW_ADMISSIONS_WITH_NO_SENTENCE_DATE_IMPOSED_QUERY_TEMPLATE,
    description=SESSION_NEW_ADMISSIONS_WITH_NO_SENTENCE_DATE_IMPOSED_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SESSION_NEW_ADMISSIONS_WITH_NO_SENTENCE_DATE_IMPOSED_VIEW_BUILDER.build_and_print()

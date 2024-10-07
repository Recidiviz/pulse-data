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
"""Flag all incarceration and supervision sessions that have no corresponding sentence imposed group"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

SESSIONS_MISSING_CLOSEST_SENTENCE_IMPOSED_GROUP_VIEW_NAME = (
    "sessions_missing_closest_sentence_imposed_group"
)

SESSIONS_MISSING_CLOSEST_SENTENCE_IMPOSED_GROUP_DESCRIPTION = """
Highlight any incarceration or supervision session without an overlapping sentence imposed group ingested. Requires
session start date before sentence completion/projected completion date and (if session has ended) sentence effective
date before session end date.
"""

SESSIONS_MISSING_CLOSEST_SENTENCE_IMPOSED_GROUP_QUERY_TEMPLATE = """
SELECT
    ses.state_code,
    ses.state_code AS region_code,
    ses.person_id,
    ses.session_id,
    ses.compartment_level_1,
    ses.start_date AS session_start_date,
    ses.end_date AS session_end_date,
FROM `{project_id}.{sessions_dataset}.compartment_sessions_materialized` ses
INNER JOIN `{project_id}.{sessions_dataset}.compartment_sessions_closest_sentence_imposed_group` sen
    USING (state_code, person_id, session_id)
-- All sessions that have started within the last 20 years should have an associated sentence imposed group
WHERE ses.compartment_level_1 IN ("INCARCERATION", "SUPERVISION")
    AND ses.compartment_level_2 != "INTERNAL_UNKNOWN"
    AND ses.start_date >= DATE_SUB(CURRENT_DATE("US/Eastern"), INTERVAL 20 YEAR)
    AND sen.sentence_imposed_group_id IS NULL
"""


SESSIONS_MISSING_CLOSEST_SENTENCE_IMPOSED_GROUP_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=SESSIONS_MISSING_CLOSEST_SENTENCE_IMPOSED_GROUP_VIEW_NAME,
    view_query_template=SESSIONS_MISSING_CLOSEST_SENTENCE_IMPOSED_GROUP_QUERY_TEMPLATE,
    description=SESSIONS_MISSING_CLOSEST_SENTENCE_IMPOSED_GROUP_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SESSIONS_MISSING_CLOSEST_SENTENCE_IMPOSED_GROUP_VIEW_BUILDER.build_and_print()

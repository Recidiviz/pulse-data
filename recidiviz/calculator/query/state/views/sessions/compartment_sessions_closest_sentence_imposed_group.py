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
"""View that reduces sentence groups to 1 record and summarizes information about that sentence group"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

COMPARTMENT_SESSIONS_CLOSEST_SENTENCE_IMPOSED_GROUP_VIEW_NAME = (
    "compartment_sessions_closest_sentence_imposed_group"
)

COMPARTMENT_SESSIONS_CLOSEST_SENTENCE_IMPOSED_GROUP_VIEW_DESCRIPTION = """
    View that associates each compartment session start with the closest sentence imposed group,
    based on difference between session start date and sentence imposed date.
    """

COMPARTMENT_SESSIONS_CLOSEST_SENTENCE_IMPOSED_GROUP_QUERY_TEMPLATE = """
    /*{description}*/
    SELECT
        state_code,
        person_id,
        a.session_id,
        a.start_date,
        b.sentence_imposed_group_id,
        b.date_imposed,
    FROM 
        `{project_id}.{sessions_dataset}.compartment_sessions_materialized` a
    LEFT JOIN 
        `{project_id}.{sessions_dataset}.sentence_imposed_group_summary_materialized` b
    USING 
        (state_code, person_id)
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY a.person_id, a.session_id
        ORDER BY ABS(COALESCE(DATE_DIFF(b.date_imposed, a.start_date, DAY), 999999999))
    ) = 1
"""

COMPARTMENT_SESSIONS_CLOSEST_SENTENCE_IMPOSED_GROUP_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    view_id=COMPARTMENT_SESSIONS_CLOSEST_SENTENCE_IMPOSED_GROUP_VIEW_NAME,
    view_query_template=COMPARTMENT_SESSIONS_CLOSEST_SENTENCE_IMPOSED_GROUP_QUERY_TEMPLATE,
    description=COMPARTMENT_SESSIONS_CLOSEST_SENTENCE_IMPOSED_GROUP_VIEW_DESCRIPTION,
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        COMPARTMENT_SESSIONS_CLOSEST_SENTENCE_IMPOSED_GROUP_VIEW_BUILDER.build_and_print()

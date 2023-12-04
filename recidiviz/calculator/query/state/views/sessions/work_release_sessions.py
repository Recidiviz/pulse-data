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
"""
View of compartment sessions subsetted to represent all periods of incarceration
in which a person is also permitted to go out on a work-release: a program that
allows incarcerated people to leave the facility to work in the community and
return to the facility at the end of the work day."""


from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import (
    create_intersection_spans,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# States currently supported
SUPPORTED_STATES = ["US_ME"]

WORK_RELEASE_SESSIONS_VIEW_NAME = "work_release_sessions"

WORK_RELEASE_SESSIONS_VIEW_DESCRIPTION = """
View of compartment sessions subsetted to represent all periods of incarceration
in which a person is also permitted to go out on a work-release: a program that
allows incarcerated people to leave the facility to work in the community and
return to the facility at the end of the work day."""

WORK_RELEASE_SESSIONS_QUERY_TEMPLATE = f"""
-- Join separate states datasets
WITH all_wr_sessions AS (
    # TODO(#24698): Add IX
    SELECT * FROM `{{project_id}}.{{sessions_dataset}}.us_me_work_release_sessions_preprocessing`
    ),
comp_ses AS (
    SELECT *
    FROM `{{project_id}}.{{sessions_dataset}}.compartment_sessions_materialized`
)

{create_intersection_spans(table_1_name = 'all_wr_sessions', 
                           table_2_name = 'comp_ses',
                           index_columns= ['person_id', 'state_code'],
                           table_2_columns = ['session_id'])}
WHERE comp_ses.compartment_level_1 IN ('INCARCERATION','INCARCERATION_OUT_OF_STATE')
    AND comp_ses.state_code IN ('{{supported_states}}')
"""

WORK_RELEASE_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=WORK_RELEASE_SESSIONS_VIEW_NAME,
    description=WORK_RELEASE_SESSIONS_VIEW_DESCRIPTION,
    view_query_template=WORK_RELEASE_SESSIONS_QUERY_TEMPLATE,
    sessions_dataset=SESSIONS_DATASET,
    supported_states="', '".join(SUPPORTED_STATES),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        WORK_RELEASE_SESSIONS_VIEW_BUILDER.build_and_print()

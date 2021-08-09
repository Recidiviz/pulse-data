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

"""A view which provides a comparison of annual sessions supervision out of state population to dataflow."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

SESSION_SUPERVISION_OUT_OF_STATE_POPULATION_TO_DATAFLOW_VIEW_NAME = (
    "session_supervision_out_of_state_population_to_dataflow"
)

SESSION_SUPERVISION_OUT_OF_STATE_POPULATION_TO_DATAFLOW_DESCRIPTION = """
    A view which provides a comparison of annual supervision population counts in sessions vs dataflow
    """

SESSION_SUPERVISION_OUT_OF_STATE_POPULATION_TO_DATAFLOW_QUERY_TEMPLATE = """
    /*{description}*/
    SELECT
        state_code AS region_code,
        population_date,
        SUM(in_dataflow) AS dataflow_population,
        SUM(in_sessions) AS session_population
    FROM `{project_id}.{validation_views_dataset}.session_supervision_out_of_state_population_to_dataflow_disaggregated`
    GROUP BY 1,2 order by 1,2
    """

SESSION_SUPERVISION_OUT_OF_STATE_POPULATION_TO_DATAFLOW_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=SESSION_SUPERVISION_OUT_OF_STATE_POPULATION_TO_DATAFLOW_VIEW_NAME,
    view_query_template=SESSION_SUPERVISION_OUT_OF_STATE_POPULATION_TO_DATAFLOW_QUERY_TEMPLATE,
    description=SESSION_SUPERVISION_OUT_OF_STATE_POPULATION_TO_DATAFLOW_DESCRIPTION,
    validation_views_dataset=dataset_config.VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SESSION_SUPERVISION_OUT_OF_STATE_POPULATION_TO_DATAFLOW_VIEW_BUILDER.build_and_print()

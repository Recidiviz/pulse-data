# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""View of milestones congratulated another way.

python -m recidiviz.calculator.query.state.views.workflows.clients_milestones_congratulated_another_way
"""


from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.workflows.user_event_template import (
    user_event_template,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

CLIENTS_MILESTONES_CONGRATULATED_ANOTHER_WAY_VIEW_NAME = (
    "clients_milestones_congratulated_another_way"
)

CLIENTS_MILESTONES_CONGRATULATED_ANOTHER_WAY_DESCRIPTION = """
    View of milestones congratulated another way logged from UI
"""

CLIENTS_MILESTONES_CONGRATULATED_ANOTHER_WAY_QUERY_TEMPLATE = f"""
    {user_event_template("frontend_milestones_congratulated_another_way", should_check_client_id=False )}
"""

CLIENTS_MILESTONES_CONGRATULATED_ANOTHER_WAY_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=CLIENTS_MILESTONES_CONGRATULATED_ANOTHER_WAY_VIEW_NAME,
    view_query_template=CLIENTS_MILESTONES_CONGRATULATED_ANOTHER_WAY_QUERY_TEMPLATE,
    description=CLIENTS_MILESTONES_CONGRATULATED_ANOTHER_WAY_DESCRIPTION,
    workflows_views_dataset=dataset_config.WORKFLOWS_VIEWS_DATASET,
    segment_dataset=dataset_config.PULSE_DASHBOARD_SEGMENT_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CLIENTS_MILESTONES_CONGRATULATED_ANOTHER_WAY_VIEW_BUILDER.build_and_print()

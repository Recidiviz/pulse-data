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
"""View of client opportunity snoozed events logged from UI

python -m recidiviz.calculator.query.state.views.workflows.clients_opportunity_snoozed
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.workflows.user_event_template import (
    user_event_template,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

CLIENTS_OPPORTUNITY_SNOOZED_VIEW_NAME = "clients_opportunity_snoozed"

CLIENTS_OPPORTUNITY_SNOOZED_DESCRIPTION = """
    View of client opportunity snoozed events logged from UI
    """

CLIENTS_OPPORTUNITY_SNOOZED_QUERY_TEMPLATE = f"""
    {user_event_template(
        "frontend_opportunity_snoozed", should_check_client_id=False, add_columns=["opportunity_type", "snooze_for_days", "snooze_until", "reasons"]
    )}
"""

CLIENTS_OPPORTUNITY_SNOOZED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=CLIENTS_OPPORTUNITY_SNOOZED_VIEW_NAME,
    view_query_template=CLIENTS_OPPORTUNITY_SNOOZED_QUERY_TEMPLATE,
    description=CLIENTS_OPPORTUNITY_SNOOZED_DESCRIPTION,
    workflows_views_dataset=dataset_config.WORKFLOWS_VIEWS_DATASET,
    segment_dataset=dataset_config.PULSE_DASHBOARD_SEGMENT_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CLIENTS_OPPORTUNITY_SNOOZED_VIEW_BUILDER.build_and_print()

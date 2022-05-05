# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""View of client surfaced events logged from UI

python -m recidiviz.calculator.query.state.views.practices.clients_surfaced
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.practices.user_event_template import (
    user_event_template,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

CLIENTS_SURFACED_VIEW_NAME = "clients_surfaced"

CLIENTS_SURFACED_DESCRIPTION = """
    View of client surfaced events logged from UI
    """

CLIENTS_SURFACED_QUERY_TEMPLATE = f"""
    /*{{description}}*/
    {user_event_template(
        "frontend_surfaced_in_list", add_columns=["opportunity_type"]
    )}
"""

CLIENTS_SURFACED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.PRACTICES_VIEWS_DATASET,
    view_id=CLIENTS_SURFACED_VIEW_NAME,
    view_query_template=CLIENTS_SURFACED_QUERY_TEMPLATE,
    description=CLIENTS_SURFACED_DESCRIPTION,
    practices_views_dataset=dataset_config.PRACTICES_VIEWS_DATASET,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    segment_dataset=dataset_config.PULSE_DASHBOARD_SEGMENT_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CLIENTS_SURFACED_VIEW_BUILDER.build_and_print()

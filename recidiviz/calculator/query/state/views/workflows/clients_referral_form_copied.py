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
"""View of client form copied events logged from UI

python -m recidiviz.calculator.query.state.views.workflows.clients_referral_form_copied
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.workflows.user_event_template import (
    user_event_template,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

CLIENTS_REFERRAL_FORM_COPIED_VIEW_NAME = "clients_referral_form_copied"

CLIENTS_REFERRAL_FORM_COPIED_DESCRIPTION = """
    View of form copied events logged from UI
"""

CLIENTS_REFERRAL_FORM_COPIED_QUERY_TEMPLATE = f"""
    {user_event_template("frontend_referral_form_copied_to_clipboard", add_columns=["opportunity_type"])}
"""

CLIENTS_REFERRAL_FORM_COPIED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=CLIENTS_REFERRAL_FORM_COPIED_VIEW_NAME,
    view_query_template=CLIENTS_REFERRAL_FORM_COPIED_QUERY_TEMPLATE,
    description=CLIENTS_REFERRAL_FORM_COPIED_DESCRIPTION,
    workflows_views_dataset=dataset_config.WORKFLOWS_VIEWS_DATASET,
    segment_dataset=dataset_config.PULSE_DASHBOARD_SEGMENT_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CLIENTS_REFERRAL_FORM_COPIED_VIEW_BUILDER.build_and_print()

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
"""View of client referral form completed events logged from UI

python -m recidiviz.calculator.query.state.views.workflows.clients_referral_completed
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

CLIENTS_REFERRAL_COMPLETED_VIEW_NAME = "clients_referral_completed"

CLIENTS_REFERRAL_COMPLETED_DESCRIPTION = """
    View of client referral form completed events logged from UI
    """


CLIENTS_REFERRAL_COMPLETED_QUERY_TEMPLATE = """
    /*{description}*/
    SELECT
        person_id,
        state_code,
        person_external_id,
        timestamp,
        opportunity_type,
    FROM `{project_id}.{workflows_views_dataset}.clients_latest_referral_status`
    WHERE status = "COMPLETED"
"""

CLIENTS_REFERRAL_COMPLETED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=CLIENTS_REFERRAL_COMPLETED_VIEW_NAME,
    view_query_template=CLIENTS_REFERRAL_COMPLETED_QUERY_TEMPLATE,
    description=CLIENTS_REFERRAL_COMPLETED_DESCRIPTION,
    workflows_views_dataset=dataset_config.WORKFLOWS_VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CLIENTS_REFERRAL_COMPLETED_VIEW_BUILDER.build_and_print()

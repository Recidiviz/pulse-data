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
"""A view revealing when state supervision periods have termination reasons but no date."""

# pylint: disable=trailing-whitespace
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

SUPERVISION_TERMINATION_REASON_NO_DATE_VIEW_NAME = (
    "supervision_termination_reason_no_date"
)

SUPERVISION_TERMINATION_REASON_NO_DATE_DESCRIPTION = (
    """Supervision periods with termination reasons but no termination date."""
)

SUPERVISION_TERMINATION_REASON_NO_DATE_QUERY_TEMPLATE = """
    /*{description}*/
    SELECT *, state_code as region_code
    FROM `{project_id}.{state_dataset}.state_supervision_period`
    WHERE termination_date IS NULL
    AND termination_reason IS NOT NULL
    ORDER BY region_code, termination_reason, external_id
"""

SUPERVISION_TERMINATION_REASON_NO_DATE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=SUPERVISION_TERMINATION_REASON_NO_DATE_VIEW_NAME,
    view_query_template=SUPERVISION_TERMINATION_REASON_NO_DATE_QUERY_TEMPLATE,
    description=SUPERVISION_TERMINATION_REASON_NO_DATE_DESCRIPTION,
    state_dataset=state_dataset_config.STATE_BASE_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_TERMINATION_REASON_NO_DATE_VIEW_BUILDER.build_and_print()

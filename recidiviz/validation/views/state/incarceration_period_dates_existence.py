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
"""A view revealing when incarceration periods do not have either start nor termination dates."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

INCARCERATION_PERIOD_DATES_EXISTENCE_VIEW_NAME = "incarceration_period_dates_existence"

INCARCERATION_PERIOD_DATES_EXISTENCE_DESCRIPTION = (
    """Incarceration periods that do not have start nor termination dates."""
)

INCARCERATION_PERIOD_DATES_EXISTENCE_QUERY_TEMPLATE = """
    /*{description}*/
    SELECT *, state_code as region_code
    FROM `{project_id}.{state_dataset}.state_incarceration_period`
    WHERE admission_date IS NULL
    AND release_date IS NULL
    AND external_id IS NOT NULL
    ORDER BY region_code, external_id
"""

INCARCERATION_PERIOD_DATES_EXISTENCE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=INCARCERATION_PERIOD_DATES_EXISTENCE_VIEW_NAME,
    view_query_template=INCARCERATION_PERIOD_DATES_EXISTENCE_QUERY_TEMPLATE,
    description=INCARCERATION_PERIOD_DATES_EXISTENCE_DESCRIPTION,
    state_dataset=state_dataset_config.STATE_BASE_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        INCARCERATION_PERIOD_DATES_EXISTENCE_VIEW_BUILDER.build_and_print()

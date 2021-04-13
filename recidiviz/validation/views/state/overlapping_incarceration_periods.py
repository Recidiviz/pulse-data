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

"""A view revealing when incarceration periods for a given person overlap.

Note: for some states it may be expected that incarceration periods overlap. In this case, we should add this validation
to the `exclusions` section of that state's validation config.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config
from recidiviz.validation.views.state.overlapping_periods_template import (
    overlapping_periods_query,
)

OVERLAPPING_INCARCERATION_PERIODS_VIEW_NAME = "overlapping_incarceration_periods"

OVERLAPPING_INCARCERATION_PERIODS_DESCRIPTION = """ Incarceration periods with another incarceration
period with overlapping dates """

OVERLAPPING_INCARCERATION_PERIODS_QUERY_TEMPLATE = f"""
  /*{{description}}*/
  {overlapping_periods_query(StateIncarcerationPeriod)}
"""

OVERLAPPING_INCARCERATION_PERIODS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=OVERLAPPING_INCARCERATION_PERIODS_VIEW_NAME,
    view_query_template=OVERLAPPING_INCARCERATION_PERIODS_QUERY_TEMPLATE,
    description=OVERLAPPING_INCARCERATION_PERIODS_DESCRIPTION,
    state_dataset=state_dataset_config.STATE_BASE_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        OVERLAPPING_INCARCERATION_PERIODS_VIEW_BUILDER.build_and_print()

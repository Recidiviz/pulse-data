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

"""A view revealing when sequences of state incarceration periods for a given person exhibit that an "open"
incarceration period, i.e. one which has no release date yet, is followed by another incarceration period with an
admission date."""

# pylint: disable=trailing-whitespace
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

INCARCERATION_ADMISSION_AFTER_OPEN_PERIOD_VIEW_NAME = (
    "incarceration_admission_after_open_period"
)

INCARCERATION_ADMISSION_AFTER_OPEN_PERIOD_DESCRIPTION = (
    """ Incarceration admissions after open periods """
)

INCARCERATION_ADMISSION_AFTER_OPEN_PERIOD_QUERY_TEMPLATE = """
    /*{description}*/
    WITH 
    non_zero_day_periods AS (
        -- Zero-day periods get largely filtered / collapsed out of our calc
        -- pipelines.
        SELECT *
        FROM `{project_id}.{state_dataset}.state_incarceration_period`
        WHERE admission_date != release_date OR release_date IS NULL
    ),
    periods_with_next_admission AS (
      SELECT 
        state_code as region_code, 
        external_id,
        person_id,
        admission_date,
        release_date,
        LEAD(external_id) OVER next_period AS next_external_id,
        LEAD(admission_date) OVER next_period AS next_admission_date,
        LEAD(release_date) OVER next_period AS next_release_date,
      FROM non_zero_day_periods
      WHERE admission_date IS NOT NULL AND external_id IS NOT NULL
      WINDOW next_period AS (
        PARTITION BY state_code, person_id order by admission_date ASC, COALESCE(release_date, DATE(3000, 01, 01)) ASC
      )
    )
    SELECT * FROM periods_with_next_admission
    WHERE release_date IS NULL AND next_admission_date IS NOT NULL
"""

INCARCERATION_ADMISSION_AFTER_OPEN_PERIOD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=INCARCERATION_ADMISSION_AFTER_OPEN_PERIOD_VIEW_NAME,
    view_query_template=INCARCERATION_ADMISSION_AFTER_OPEN_PERIOD_QUERY_TEMPLATE,
    description=INCARCERATION_ADMISSION_AFTER_OPEN_PERIOD_DESCRIPTION,
    state_dataset=state_dataset_config.STATE_BASE_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        INCARCERATION_ADMISSION_AFTER_OPEN_PERIOD_VIEW_BUILDER.build_and_print()

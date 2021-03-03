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

"""A view revealing when state incarceration periods have nonnull release reasons but no release date."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

INCARCERATION_RELEASE_REASON_NO_RELEASE_DATE_VIEW_NAME = (
    "incarceration_release_reason_no_release_date"
)

INCARCERATION_RELEASE_REASON_NO_RELEASE_DATE_DESCRIPTION = (
    """ Incarceration release reasons with no release date """
)

INCARCERATION_RELEASE_REASON_NO_RELEASE_DATE_QUERY_TEMPLATE = """
    /*{description}*/
    WITH non_placeholder_unterminated_incarceration_periods AS (
      SELECT state_code, release_date, release_reason, release_reason_raw_text
      FROM `{project_id}.state.state_incarceration_period`
      WHERE external_id IS NOT NULL AND release_date IS NULL
    )
    SELECT
        state_code, state_code AS region_code, release_reason, release_reason_raw_text,
        COUNT(*) AS periods_with_no_release_date_and_release_reason
    FROM non_placeholder_unterminated_incarceration_periods
    WHERE release_reason IS NOT NULL OR release_reason_raw_text IS NOT NULL
    GROUP BY state_code, release_reason, release_reason_raw_text
"""

INCARCERATION_RELEASE_REASON_NO_RELEASE_DATE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=INCARCERATION_RELEASE_REASON_NO_RELEASE_DATE_VIEW_NAME,
    view_query_template=INCARCERATION_RELEASE_REASON_NO_RELEASE_DATE_QUERY_TEMPLATE,
    description=INCARCERATION_RELEASE_REASON_NO_RELEASE_DATE_DESCRIPTION,
    state_dataset=state_dataset_config.STATE_BASE_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        INCARCERATION_RELEASE_REASON_NO_RELEASE_DATE_VIEW_BUILDER.build_and_print()

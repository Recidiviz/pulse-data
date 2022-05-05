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
"""Creates a view for collapsing raw ID employment data into contiguous periods of employment or unemployment overlapping with a supervision super session"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

EMPLOYMENT_PERIODS_PREPROCESSED_VIEW_NAME = "employment_periods_preprocessed"

EMPLOYMENT_PERIODS_PREPROCESSED_VIEW_DESCRIPTION = """View of preprocessed employment information across states. Currently supports Idaho."""

EMPLOYMENT_PERIODS_PREPROCESSED_QUERY_TEMPLATE = """
    /* {description} */
    #TODO(#12548): Deprecate state preprocessing views once employment data exists in state schema
    SELECT * FROM `{project_id}.{sessions_dataset}.us_id_employment_periods_preprocessed`
"""

EMPLOYMENT_PERIODS_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=EMPLOYMENT_PERIODS_PREPROCESSED_VIEW_NAME,
    description=EMPLOYMENT_PERIODS_PREPROCESSED_VIEW_DESCRIPTION,
    view_query_template=EMPLOYMENT_PERIODS_PREPROCESSED_QUERY_TEMPLATE,
    sessions_dataset=SESSIONS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        EMPLOYMENT_PERIODS_PREPROCESSED_VIEW_BUILDER.build_and_print()

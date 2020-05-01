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

"""A view revealing when state supervision periods have termination dates prior to start dates."""

# pylint: disable=trailing-whitespace
from recidiviz.calculator.query import bqview, export_config

from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
BASE_DATASET = export_config.STATE_BASE_TABLES_BQ_DATASET

SUPERVISION_TERMINATION_PRIOR_TO_START_VIEW_NAME = 'supervision_termination_prior_to_start'

SUPERVISION_TERMINATION_PRIOR_TO_START_DESCRIPTION = """ Supervision termination dates prior to start dates """

SUPERVISION_TERMINATION_PRIOR_TO_START_QUERY = \
    """
    /*{description}*/
    SELECT *, state_code as region_code
    FROM `{project_id}.{state_dataset}.state_supervision_period`
    WHERE termination_date IS NOT NULL
    AND termination_date < start_date
    AND external_id IS NOT NULL
    ORDER BY start_date, region_code, external_id
""".format(
        description=SUPERVISION_TERMINATION_PRIOR_TO_START_DESCRIPTION,
        project_id=PROJECT_ID,
        state_dataset=BASE_DATASET,
    )

SUPERVISION_TERMINATION_PRIOR_TO_START_VIEW = bqview.BigQueryView(
    view_id=SUPERVISION_TERMINATION_PRIOR_TO_START_VIEW_NAME,
    view_query=SUPERVISION_TERMINATION_PRIOR_TO_START_QUERY
)

if __name__ == '__main__':
    print(SUPERVISION_TERMINATION_PRIOR_TO_START_VIEW.view_id)
    print(SUPERVISION_TERMINATION_PRIOR_TO_START_VIEW.view_query)

# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""All persons with their age in years."""
# pylint: disable=trailing-whitespace

from recidiviz.calculator.query import export_config, bqview

from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
BASE_DATASET = export_config.STATE_BASE_TABLES_BQ_DATASET

PERSONS_WITH_AGE_VIEW_NAME = 'persons_with_age'

PERSONS_WITH_AGE_DESCRIPTION = \
    """All persons with their age in years."""

PERSONS_WITH_AGE_QUERY = \
    """
/*{description}*/
    SELECT person_id,
    CASE
      WHEN birthdate IS NULL THEN NULL
      ELSE CAST(TRUNC(DATE_DIFF(CURRENT_DATE(), birthdate, DAY) / 365.25) AS INT64)
    END AS age
    FROM `{project_id}.{base_dataset}.state_person`
""".format(
        description=PERSONS_WITH_AGE_DESCRIPTION,
        project_id=PROJECT_ID,
        base_dataset=BASE_DATASET,
    )

PERSONS_WITH_AGE_VIEW = bqview.BigQueryView(
    view_id=PERSONS_WITH_AGE_VIEW_NAME,
    view_query=PERSONS_WITH_AGE_QUERY
)

if __name__ == '__main__':
    print(PERSONS_WITH_AGE_VIEW.view_id)
    print(PERSONS_WITH_AGE_VIEW.view_query)

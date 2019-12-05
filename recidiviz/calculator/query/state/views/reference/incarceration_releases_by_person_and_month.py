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
# pylint: disable=line-too-long, trailing-whitespace
"""Incarceration releases de-duped by person by month."""
from recidiviz.calculator.query import bqview
from recidiviz.calculator.query.state import view_config
from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
VIEWS_DATASET = view_config.DASHBOARD_VIEWS_DATASET

INCARCERATION_RELEASES_BY_PERSON_AND_MONTH_VIEW_NAME = \
    'incarceration_releases_by_person_and_month'

INCARCERATION_RELEASES_BY_PERSON_AND_MONTH_DESCRIPTION = \
    """Incarceration releases de-duped by person by month.

    In the very rare case that someone has more than one new release in a given
    month, the earliest release of that month is chosen.
    """

INCARCERATION_RELEASES_BY_PERSON_AND_MONTH_QUERY = \
    """
/*{description}*/

SELECT * EXCEPT (rownum) FROM
(SELECT *, row_number() OVER (PARTITION BY person_id, release_year, release_month ORDER BY release_date ASC, external_id DESC) AS rownum
FROM
(SELECT *, EXTRACT(YEAR from release_date) AS release_year, EXTRACT(MONTH from release_date) AS release_month
FROM `{project_id}.{views_dataset}.incarceration_releases_deduped`)
ORDER BY person_id, release_year, release_month)
where rownum = 1

""".format(
        description=INCARCERATION_RELEASES_BY_PERSON_AND_MONTH_DESCRIPTION,
        project_id=PROJECT_ID,
        views_dataset=VIEWS_DATASET,
    )

INCARCERATION_RELEASES_BY_PERSON_AND_MONTH_VIEW = bqview.BigQueryView(
    view_id=INCARCERATION_RELEASES_BY_PERSON_AND_MONTH_VIEW_NAME,
    view_query=INCARCERATION_RELEASES_BY_PERSON_AND_MONTH_QUERY
)

if __name__ == '__main__':
    print(INCARCERATION_RELEASES_BY_PERSON_AND_MONTH_VIEW.view_id)
    print(INCARCERATION_RELEASES_BY_PERSON_AND_MONTH_VIEW.view_query)

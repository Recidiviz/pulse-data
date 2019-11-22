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
"""Revocations by supervision type by month."""
# pylint: disable=line-too-long, trailing-whitespace
from recidiviz.calculator.query import bqview
from recidiviz.calculator.query.state import view_config

from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
VIEWS_DATASET = view_config.DASHBOARD_VIEWS_DATASET


REVOCATIONS_BY_SUPERVISION_TYPE_BY_MONTH_VIEW_NAME = \
    'revocations_by_supervision_type_by_month'

REVOCATIONS_BY_SUPERVISION_TYPE_BY_MONTH_DESCRIPTION = \
    """ Revocations by supervision type by month """

REVOCATIONS_BY_SUPERVISION_TYPE_BY_MONTH_QUERY = \
    """
    /*{description}*/
    
    SELECT state_code, year, month, COUNTIF(admission_reason = 'PROBATION_REVOCATION') as probation_count, COUNTIF(admission_reason = 'PAROLE_REVOCATION') as parole_count
    FROM
    (SELECT state_code, person_id, EXTRACT(YEAR FROM admission_date) as year, EXTRACT(MONTH FROM admission_date) as month, admission_reason
    FROM `{project_id}.{views_dataset}.incarceration_admissions_by_person_and_month`
    WHERE admission_reason in ('PROBATION_REVOCATION', 'PAROLE_REVOCATION'))
    GROUP BY state_code, year, month having year > EXTRACT(YEAR FROM DATE_ADD(CURRENT_DATE(), INTERVAL -3 YEAR))
    ORDER BY year, month ASC
    """.format(
        description=REVOCATIONS_BY_SUPERVISION_TYPE_BY_MONTH_DESCRIPTION,
        project_id=PROJECT_ID,
        views_dataset=VIEWS_DATASET,
        )

REVOCATIONS_BY_SUPERVISION_TYPE_BY_MONTH_VIEW = bqview.BigQueryView(
    view_id=REVOCATIONS_BY_SUPERVISION_TYPE_BY_MONTH_VIEW_NAME,
    view_query=REVOCATIONS_BY_SUPERVISION_TYPE_BY_MONTH_QUERY
)

if __name__ == '__main__':
    print(REVOCATIONS_BY_SUPERVISION_TYPE_BY_MONTH_VIEW.view_id)
    print(REVOCATIONS_BY_SUPERVISION_TYPE_BY_MONTH_VIEW.view_query)

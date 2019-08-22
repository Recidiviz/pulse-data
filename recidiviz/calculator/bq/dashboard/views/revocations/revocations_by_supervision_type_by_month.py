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
from recidiviz.calculator.bq import bqview, export_config

from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
BASE_DATASET = export_config.STATE_BASE_TABLES_BQ_DATASET

REVOCATIONS_BY_SUPERVISION_TYPE_BY_MONTH_VIEW_NAME = \
    'revocations_by_supervision_type_by_month'

REVOCATIONS_BY_SUPERVISION_TYPE_BY_MONTH_DESCRIPTION = \
    """ Revocations by supervision type by month """

REVOCATIONS_BY_SUPERVISION_TYPE_BY_MONTH_QUERY = \
    """
    /*{description}*/
    
    SELECT IFNULL(prob.state_code, parole.state_code) as state_code, IFNULL(prob.year, parole.year) as year,  IFNULL(prob.month, parole.month) as month, IFNULL(probation_count, 0) as probation_count, IFNULL(parole_count, 0) as parole_count
    FROM
    (SELECT state_code, EXTRACT(YEAR FROM admission_date) as year, EXTRACT(MONTH FROM admission_date) as month, count(*) as probation_count
    FROM `{project_id}.{base_dataset}.state_incarceration_period`
    WHERE admission_reason in ('PROBATION_REVOCATION')
    GROUP BY state_code, year, month, admission_reason having year > EXTRACT(YEAR FROM DATE_ADD(CURRENT_DATE(), INTERVAL -3 YEAR))) prob
    FULL OUTER JOIN
    (SELECT state_code, EXTRACT(YEAR FROM admission_date) as year, EXTRACT(MONTH FROM admission_date) as month, count(*) as parole_count
    FROM `{project_id}.{base_dataset}.state_incarceration_period`
    WHERE admission_reason in ('PAROLE_REVOCATION')
    GROUP BY state_code, year, month, admission_reason having year > EXTRACT(YEAR FROM DATE_ADD(CURRENT_DATE(), INTERVAL -3 YEAR))) parole
    ON prob.state_code = parole.state_code AND prob.year = parole.year and prob.month = parole.month
    ORDER BY year, month ASC
    """.format(
        description=REVOCATIONS_BY_SUPERVISION_TYPE_BY_MONTH_DESCRIPTION,
        project_id=PROJECT_ID,
        base_dataset=BASE_DATASET,
        )

REVOCATIONS_BY_SUPERVISION_TYPE_BY_MONTH_VIEW = bqview.BigQueryView(
    view_id=REVOCATIONS_BY_SUPERVISION_TYPE_BY_MONTH_VIEW_NAME,
    view_query=REVOCATIONS_BY_SUPERVISION_TYPE_BY_MONTH_QUERY
)

if __name__ == '__main__':
    print(REVOCATIONS_BY_SUPERVISION_TYPE_BY_MONTH_VIEW.view_id)
    print(REVOCATIONS_BY_SUPERVISION_TYPE_BY_MONTH_VIEW.view_query)

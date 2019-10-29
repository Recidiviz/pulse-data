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
"""All supervision periods that have terminated in the last 10 years."""
# pylint: disable=line-too-long, trailing-whitespace
from recidiviz.calculator.bq import bqview, export_config
from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
BASE_DATASET = export_config.STATE_BASE_TABLES_BQ_DATASET

SUPERVISION_TERMINATIONS_10_YEARS = 'supervision_terminations_10_years'

SUPERVISION_TERMINATIONS_10_YEARS_DESCRIPTION = \
    """ All supervision periods that have terminated in the last 10 years.
    """

SUPERVISION_TERMINATIONS_10_YEARS_QUERY = \
    """
    /*{description}*/
    
    SELECT state_code, person_id, supervision_period_id, start_date, EXTRACT(YEAR FROM start_date) as start_year, termination_date,
    EXTRACT(YEAR FROM termination_date) as termination_year, EXTRACT(MONTH from termination_date) as termination_month
    FROM
    `{project_id}.{base_dataset}.state_supervision_period`  
    WHERE termination_date IS NOT NULL
    AND EXTRACT(YEAR FROM termination_date) > EXTRACT(YEAR FROM DATE_ADD(CURRENT_DATE(), INTERVAL -11 YEAR)) 
    ORDER BY person_id, start_date ASC

""".format(
        description=SUPERVISION_TERMINATIONS_10_YEARS_DESCRIPTION,
        project_id=PROJECT_ID,
        base_dataset=BASE_DATASET,
    )

SUPERVISION_TERMINATIONS_10_YEARS_VIEW = bqview.BigQueryView(
    view_id=SUPERVISION_TERMINATIONS_10_YEARS,
    view_query=SUPERVISION_TERMINATIONS_10_YEARS_QUERY
)

if __name__ == '__main__':
    print(SUPERVISION_TERMINATIONS_10_YEARS_VIEW.view_id)
    print(SUPERVISION_TERMINATIONS_10_YEARS_VIEW.view_query)

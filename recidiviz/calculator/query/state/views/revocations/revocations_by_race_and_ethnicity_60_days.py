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
"""Revocations by race in the last 60 days."""
# pylint: disable=line-too-long, trailing-whitespace
from recidiviz.calculator.query import export_config, bqview
from recidiviz.calculator.query.state import view_config
from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
VIEWS_DATASET = view_config.DASHBOARD_VIEWS_DATASET
BASE_DATASET = export_config.STATE_BASE_TABLES_BQ_DATASET

REVOCATIONS_BY_RACE_AND_ETHNICITY_60_DAYS_VIEW_NAME = \
    'revocations_by_race_and_ethnicity_60_days'

REVOCATIONS_BY_RACE_AND_ETHNICITY_60_DAYS_DESCRIPTION = \
    """ Revocations by race and ethnicity in last 60 days """

REVOCATIONS_BY_RACE_AND_ETHNICITY_60_DAYS_QUERY = \
    """
    /*{description}*/

    SELECT state_code, race_or_ethnicity, count(*) as revocation_count
    FROM
    (SELECT sip.state_code, spre.race_or_ethnicity
    FROM
    (SELECT state_code, person_id FROM 
    `{project_id}.{views_dataset}.incarceration_admissions_by_person_60_days` 
    WHERE admission_reason in ('PROBATION_REVOCATION', 'PAROLE_REVOCATION')
    GROUP BY state_code, person_id) sip
    JOIN `{project_id}.{views_dataset}.state_person_race_and_ethnicity` spre
    ON spre.person_id = sip.person_id)
    GROUP BY state_code, race_or_ethnicity
    ORDER BY race_or_ethnicity ASC

    """.format(
        description=REVOCATIONS_BY_RACE_AND_ETHNICITY_60_DAYS_DESCRIPTION,
        project_id=PROJECT_ID,
        views_dataset=VIEWS_DATASET,
    )

REVOCATIONS_BY_RACE_AND_ETHNICITY_60_DAYS_VIEW = bqview.BigQueryView(
    view_id=REVOCATIONS_BY_RACE_AND_ETHNICITY_60_DAYS_VIEW_NAME,
    view_query=REVOCATIONS_BY_RACE_AND_ETHNICITY_60_DAYS_QUERY
)

if __name__ == '__main__':
    print(REVOCATIONS_BY_RACE_AND_ETHNICITY_60_DAYS_VIEW.view_id)
    print(REVOCATIONS_BY_RACE_AND_ETHNICITY_60_DAYS_VIEW.view_query)

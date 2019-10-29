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
"""All individuals who have been on active supervision within the last
60 days, with their county of last known residence, and whether or not
they have been admitted to prison for a revocation in the last 60 days.
"""
# pylint: disable=trailing-whitespace, line-too-long

from recidiviz.calculator.bq import bqview
from recidiviz.calculator.bq.dashboard.views import view_config

from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
VIEWS_DATASET = view_config.DASHBOARD_VIEWS_DATASET

PERSONS_ON_SUPERVISION_60_DAYS_WITH_COUNTY_AND_REVOCATION_VIEW_NAME = \
    'persons_on_supervision_60_days_with_county_and_revocation'

PERSONS_ON_SUPERVISION_60_DAYS_WITH_COUNTY_AND_REVOCATION_DESCRIPTION = \
    """All individuals who have been on active supervision within the last
    60 days, with their county of last known residence, and whether or not
    they have been admitted to prison for a revocation in the last 60 days."""

# TODO(2227): Use county on state_person instead of last known address, which
#  will remove state-specific reference to us_nd_zipcode_county_map
PERSONS_ON_SUPERVISION_60_DAYS_WITH_COUNTY_AND_REVOCATION_QUERY = \
    """
    /*{description}*/
    SELECT state_code, pop.person_id, IFNULL(county, 'UNKNOWN') as county_code, revoked
    FROM
    (SELECT IFNULL(sup.state_code, inc.state_code) as state_code, IFNULL(sup.person_id, inc.person_id) as person_id, IFNULL(revoked, FALSE) as revoked
    FROM 
    (SELECT state_code, person_id
    FROM
    `{project_id}.{views_dataset}.persons_on_supervision_60_days`) sup
    FULL OUTER JOIN
    (SELECT state_code, person_id, TRUE as revoked
    FROM `{project_id}.{views_dataset}.incarceration_admissions_60_days` 
    WHERE admission_reason in ('PROBATION_REVOCATION', 'PAROLE_REVOCATION')
    GROUP BY state_code, person_id) inc
    ON sup.state_code = inc.state_code AND sup.person_id = inc.person_id) pop
    LEFT JOIN
    (SELECT state_person.person_id,
    CONCAT('US_ND_', UPPER(substr(zipcode_county_map.county, 0, LENGTH(zipcode_county_map.county) - 7))) as county
    FROM `{project_id}.{views_dataset}.persons_with_last_known_address` as state_person
    LEFT JOIN 
    `{project_id}.{views_dataset}.us_nd_zipcode_county_map` as zipcode_county_map
    ON substr(state_person.last_known_address, -5) = zipcode_county_map.zip_code
    WHERE state_person.last_known_address IS NOT NULL) count
    ON pop.person_id = count.person_id
    """.format(
        description=PERSONS_ON_SUPERVISION_60_DAYS_WITH_COUNTY_AND_REVOCATION_DESCRIPTION,
        project_id=PROJECT_ID,
        views_dataset=VIEWS_DATASET,
    )

PERSONS_ON_SUPERVISION_60_DAYS_WITH_COUNTY_AND_REVOCATION_VIEW = bqview.BigQueryView(
    view_id=PERSONS_ON_SUPERVISION_60_DAYS_WITH_COUNTY_AND_REVOCATION_VIEW_NAME,
    view_query=PERSONS_ON_SUPERVISION_60_DAYS_WITH_COUNTY_AND_REVOCATION_QUERY
)

if __name__ == '__main__':
    print(PERSONS_ON_SUPERVISION_60_DAYS_WITH_COUNTY_AND_REVOCATION_VIEW.view_id)
    print(PERSONS_ON_SUPERVISION_60_DAYS_WITH_COUNTY_AND_REVOCATION_VIEW.view_query)

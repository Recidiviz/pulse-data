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
"""Persons to their most recent county of residence."""
# pylint: disable=trailing-whitespace

from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.calculator.query.state import dataset_config

PERSONS_TO_RECENT_COUNTY_OF_RESIDENCE_VIEW_NAME = \
    'persons_to_recent_county_of_residence'

PERSONS_TO_RECENT_COUNTY_OF_RESIDENCE_DESCRIPTION = \
    """Persons to their most recent county of residence."""

PERSONS_TO_RECENT_COUNTY_OF_RESIDENCE_QUERY_TEMPLATE = \
    """
/*{description}*/
    SELECT 
      person_id,
      CONCAT('US_', UPPER(state_code), '_', UPPER(county)) AS county_of_residence
    FROM (
      SELECT 
        person_id, 
        SUBSTR(last_known_address, -9, 2) AS state_code,
        SUBSTR(county, 0, LENGTH(county) -7) AS county
      FROM 
        `{project_id}.{reference_tables_dataset}.persons_with_last_known_address` as persons_with_address
      JOIN
        `{project_id}.{reference_tables_dataset}.zipcode_county_map` zipcode_county_map
      ON 
        substr(persons_with_address.last_known_address, -5) = zipcode_county_map.zip_code
      WHERE 
        persons_with_address.last_known_address IS NOT NULL
    )

    WHERE 
      state_code IN ('ND', 'MO')
"""

PERSONS_TO_RECENT_COUNTY_OF_RESIDENCE_VIEW = BigQueryView(
    dataset_id=dataset_config.REFERENCE_TABLES_DATASET,
    view_id=PERSONS_TO_RECENT_COUNTY_OF_RESIDENCE_VIEW_NAME,
    view_query_template=PERSONS_TO_RECENT_COUNTY_OF_RESIDENCE_QUERY_TEMPLATE,
    description=PERSONS_TO_RECENT_COUNTY_OF_RESIDENCE_DESCRIPTION,
    reference_tables_dataset=dataset_config.REFERENCE_TABLES_DATASET,
)

if __name__ == '__main__':
    print(PERSONS_TO_RECENT_COUNTY_OF_RESIDENCE_VIEW.view_id)
    print(PERSONS_TO_RECENT_COUNTY_OF_RESIDENCE_VIEW.view_query)

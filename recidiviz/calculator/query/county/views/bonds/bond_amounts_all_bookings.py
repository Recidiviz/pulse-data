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
"""For every Booking, total bond amounts and UNKNOWN or DENIED."""
# pylint: disable=line-too-long

from recidiviz.calculator.query import export_config, bqview
from recidiviz.calculator.query.county import view_config
from recidiviz.calculator.query.county.views.bonds.bond_amounts_by_booking import BOND_AMOUNTS_BY_BOOKING_VIEW
from recidiviz.calculator.query.county.views.vera.county_names import COUNTY_NAMES_VIEW

from recidiviz.common.constants.enum_canonical_strings import bond_type_denied

from recidiviz.persistence.database.schema.county.schema import Booking, Person

from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
BASE_DATASET = export_config.COUNTY_BASE_TABLES_BQ_DATASET
VIEWS_DATASET = view_config.VIEWS_DATASET

BOND_AMOUNTS_ALL_BOOKINGS_VIEW_NAME = 'bond_amounts_all_bookings'

BOND_AMOUNTS_ALL_BOOKINGS_DESCRIPTION = \
"""
A complete table of total bond amounts,
and whether the bonds are UNKNOWN or {bond_type_denied}, for every Booking.

Joins {bond_amounts_by_booking_view} with all of the Bookings,
and all of those bookings' People.

If no bonds are present for a Booking in {bond_amounts_by_booking_view},
`total_bond_dollars` is NULL, `denied` is False, and `unknown` is True.

Constraints:
If a Booking has `total_bond_dollars`, it cannot be {bond_type_denied} or UNKNOWN.
If a Booking's `total_bond_dollars` is NULL, it must be one of {bond_type_denied} or UNKNOWN.
If a Booking is {bond_type_denied} or UNKNOWN, its `total_bond_dollars` must be NULL.

Assertions:
The total number of distinct `booking_id`s in this table should be equal to
the total number of distinct  `booking_id`s in Booking.

The number of UNKNOWN Bookings must be equal to the sum of
UNKNOWN Bookings in `{bond_amounts_by_booking_view}`,
plus all the Bookings whose booking_id is not in Bonds.
""".format(
    bond_type_denied=bond_type_denied,
    bond_amounts_by_booking_view=BOND_AMOUNTS_BY_BOOKING_VIEW.view_id
)

BOND_AMOUNTS_ALL_BOOKINGS_QUERY = \
"""
/*{description}*/
SELECT
  CountyNames.fips,
  CountyNames.state,
  CountyNames.county_name,
  Person.person_id,
  Booking.booking_id,
  Booking.admission_date,
  Booking.release_date,
  -- If total_bond_dollars is NULL, it remains NULL.
  BondAmounts.total_bond_dollars,
  -- If either BondAmounts.denied or BondAmounts.unknown are NULL,
  -- it is because no Bonds were found for the Booking.
  -- Set BondAmounts.denied to False if no Bonds were found.
  COALESCE(BondAmounts.denied, False) AS denied,
  -- Set BondAmounts.unknown to True if no Bonds were found.
  COALESCE(BondAmounts.unknown, True) AS unknown
FROM
  `{project_id}.{views_dataset}.{bond_amounts_by_booking_view}` BondAmounts
-- RIGHT JOIN so that we have a row for every Booking
-- whether or not it has a record in BondAmounts.
RIGHT JOIN
  `{project_id}.{base_dataset}.{booking_table}` Booking
ON
  BondAmounts.booking_id = Booking.booking_id
-- LEFT JOIN so that we have a Person attached to every Booking above.
LEFT JOIN
  `{project_id}.{base_dataset}.{person_table}` Person
ON
  Booking.person_id = Person.person_id
JOIN
  `{project_id}.{views_dataset}.{county_names_view}` CountyNames
ON
  SUBSTR(Person.jurisdiction_id, 0, 5) = CountyNames.fips
""".format(
    description=BOND_AMOUNTS_ALL_BOOKINGS_DESCRIPTION,
    project_id=PROJECT_ID,
    views_dataset=VIEWS_DATASET,
    bond_amounts_by_booking_view=BOND_AMOUNTS_BY_BOOKING_VIEW.view_id,
    base_dataset=BASE_DATASET,
    booking_table=Booking.__tablename__,
    person_table=Person.__tablename__,
    county_names_view=COUNTY_NAMES_VIEW.view_id
)

BOND_AMOUNTS_ALL_BOOKINGS_VIEW = bqview.BigQueryView(
    view_id=BOND_AMOUNTS_ALL_BOOKINGS_VIEW_NAME,
    view_query=BOND_AMOUNTS_ALL_BOOKINGS_QUERY
)

if __name__ == '__main__':
    print(BOND_AMOUNTS_ALL_BOOKINGS_VIEW.view_id)
    print(BOND_AMOUNTS_ALL_BOOKINGS_VIEW.view_query)

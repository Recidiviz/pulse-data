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
"""A complete table of total charge class and levels, for every Booking."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.county import dataset_config
from recidiviz.calculator.query.county.views.charges.charge_info_by_booking import (
    CHARGE_INFO_BY_BOOKING_VIEW_BUILDER,
)
from recidiviz.calculator.query.county.views.vera.county_names import (
    COUNTY_NAMES_VIEW_BUILDER,
)
from recidiviz.persistence.database.schema.county.schema import Booking, Person
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

CHARGE_INFO_ALL_BOOKINGS_VIEW_NAME = "charge_info_all_bookings"

CHARGE_INFO_ALL_BOOKINGS_DESCRIPTION = """
A complete table of total charge class and levels, for every Booking.

Joins charge_info_by_booking with all of the Bookings,
and all of those bookings' People.

Assertions:
The total number of distinct `booking_id`s in this table should be equal to
the total number of distinct  `booking_id`s in Booking.
"""

CHARGE_INFO_ALL_BOOKINGS_QUERY_TEMPLATE = """
SELECT
  CountyNames.fips,
  CountyNames.state,
  CountyNames.county_name,
  Person.person_id,
  Booking.booking_id,
  Booking.admission_date,
  Booking.release_date,
  ChargeInfo.charge_class,
  ChargeInfo.charge_level
FROM
  `{project_id}.{views_dataset}.{charge_info_by_booking_view}` ChargeInfo
-- RIGHT JOIN so that we have a row for every Booking
-- whether or not it has a record in ChargeClassAndLevel.
RIGHT JOIN
  `{project_id}.{base_dataset}.{booking_table}` Booking
ON
  ChargeInfo.booking_id = Booking.booking_id
-- LEFT JOIN so that we have a Person attached to every Booking above.
LEFT JOIN
  `{project_id}.{base_dataset}.{person_table}` Person
ON
  Booking.person_id = Person.person_id
JOIN
  `{project_id}.{views_dataset}.{county_names_view}` CountyNames
ON
  SUBSTR(Person.jurisdiction_id, 0, 5) = CountyNames.fips
"""

CHARGE_INFO_ALL_BOOKINGS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=CHARGE_INFO_ALL_BOOKINGS_VIEW_NAME,
    view_query_template=CHARGE_INFO_ALL_BOOKINGS_QUERY_TEMPLATE,
    description=CHARGE_INFO_ALL_BOOKINGS_DESCRIPTION,
    base_dataset=dataset_config.COUNTY_BASE_DATASET,
    views_dataset=dataset_config.VIEWS_DATASET,
    charge_info_by_booking_view=CHARGE_INFO_BY_BOOKING_VIEW_BUILDER.view_id,
    county_names_view=COUNTY_NAMES_VIEW_BUILDER.view_id,
    booking_table=Booking.__tablename__,
    person_table=Person.__tablename__,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CHARGE_INFO_ALL_BOOKINGS_VIEW_BUILDER.build_and_print()

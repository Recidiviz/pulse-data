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
"""Booking counts by day, fips, and most_severe_charge."""


from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.county import dataset_config
from recidiviz.calculator.query.county.views.charges.charge_severity_all_bookings import (
    CHARGE_SEVERITY_ALL_BOOKINGS_VIEW_BUILDER,
)
from recidiviz.calculator.query.county.views.vera.county_names import (
    COUNTY_NAMES_VIEW_BUILDER,
)
from recidiviz.persistence.database.schema.county.schema import Booking, Person
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

CHARGE_SEVERITY_COUNTS_ALL_BOOKINGS_VIEW_NAME = "charge_severity_counts_all_bookings"

CHARGE_SEVERITY_COUNTS_ALL_BOOKINGS_DESCRIPTION = """
Counts the total number of active, admitted, and released Bookings
on each day and FIPS by their `most_severe_charge`.
"""

CHARGE_SEVERITY_COUNTS_ALL_BOOKINGS_QUERY_TEMPLATE = """
/*{description}*/
WITH

Days AS (
  SELECT * FROM UNNEST(GENERATE_DATE_ARRAY('1900-01-01', CURRENT_DATE('America/New_York'))) AS day
),

BookingDates AS (
  SELECT
    Person.person_id,
    SUBSTR(Person.jurisdiction_id, 0, 5) AS fips,
    Booking.booking_id,
    Booking.admission_date,
    Booking.release_date,
    BookingSeverity.most_severe_charge
  FROM `{project_id}.{base_dataset}.{booking_table}` Booking
  JOIN `{project_id}.{base_dataset}.{person_table}` Person
  ON Person.person_id = Booking.person_id
  AND Booking.admission_date IS NOT NULL
  LEFT JOIN `{project_id}.{views_dataset}.{charge_severity_all_bookings_view}` BookingSeverity
  ON Booking.booking_id = BookingSeverity.booking_id
),

AdmittedTable AS (
  SELECT day, fips, most_severe_charge, COUNT(DISTINCT(booking_id)) AS admitted
  FROM BookingDates
  JOIN Days
  ON day = BookingDates.admission_date
  GROUP BY day, fips, most_severe_charge
),

ReleasedTable AS (
  SELECT day, fips, most_severe_charge, COUNT(DISTINCT(booking_id)) AS released
  FROM BookingDates
  JOIN Days
  ON day = BookingDates.release_date
  GROUP BY day, fips, most_severe_charge
),

BookingCountTable AS (
  SELECT day, fips, most_severe_charge, COUNT(DISTINCT(booking_id)) AS booking_count
  FROM BookingDates
  JOIN Days
  ON day BETWEEN BookingDates.admission_date AND COALESCE(BookingDates.release_date, CURRENT_DATE('America/New_York'))
  GROUP BY day, fips, most_severe_charge
)

SELECT BookingCountTable.day, BookingCountTable.fips, BookingCountTable.most_severe_charge, booking_count, admitted, released, CountyNames.state, CountyNames.county_name
FROM BookingCountTable
FULL JOIN AdmittedTable
ON BookingCountTable.day = AdmittedTable.day AND BookingCountTable.fips = AdmittedTable.fips AND BookingCountTable.most_severe_charge = AdmittedTable.most_severe_charge
FULL JOIN ReleasedTable
ON BookingCountTable.day = ReleasedTable.day AND BookingCountTable.fips = ReleasedTable.fips AND BookingCountTable.most_severe_charge = ReleasedTable.most_severe_charge
JOIN
  `{project_id}.{views_dataset}.{county_names_view}` CountyNames
ON
  BookingCountTable.fips = CountyNames.fips
ORDER BY day DESC, fips
"""

CHARGE_SEVERITY_COUNTS_ALL_BOOKINGS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=CHARGE_SEVERITY_COUNTS_ALL_BOOKINGS_VIEW_NAME,
    view_query_template=CHARGE_SEVERITY_COUNTS_ALL_BOOKINGS_QUERY_TEMPLATE,
    description=CHARGE_SEVERITY_COUNTS_ALL_BOOKINGS_DESCRIPTION,
    base_dataset=dataset_config.COUNTY_BASE_DATASET,
    views_dataset=dataset_config.VIEWS_DATASET,
    booking_table=Booking.__tablename__,
    person_table=Person.__tablename__,
    charge_severity_all_bookings_view=CHARGE_SEVERITY_ALL_BOOKINGS_VIEW_BUILDER.view_id,
    county_names_view=COUNTY_NAMES_VIEW_BUILDER.view_id,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CHARGE_SEVERITY_COUNTS_ALL_BOOKINGS_VIEW_BUILDER.build_and_print()

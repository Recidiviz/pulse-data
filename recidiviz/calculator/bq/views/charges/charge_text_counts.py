# Recidiviz - a platform for tracking granular recidivism metrics in real time
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
# pylint: disable=line-too-long

from recidiviz.calculator.bq import export_config
from recidiviz.calculator.bq.views import bqview
from recidiviz.calculator.bq.views import view_config
from recidiviz.calculator.bq.views.vera.county_names import COUNTY_NAMES_VIEW

from recidiviz.persistence.database.schema import Booking
from recidiviz.persistence.database.schema import Charge
from recidiviz.persistence.database.schema import Person

from recidiviz.utils import metadata


PROJECT_ID = metadata.project_id()
BASE_DATASET = export_config.BASE_TABLES_BQ_DATASET
VIEWS_DATASET = view_config.VIEWS_DATASET

CHARGE_TEXT_COUNTS_VIEW_NAME = 'charge_text_counts'

CHARGE_TEXT_COUNTS_DESCRIPTION = \
"""
Counts the number of bookings associated with each charge text
for every day-fips combination.
"""

CHARGE_TEXT_COUNTS_QUERY = \
"""
/*{description}*/
WITH

Days AS (
  SELECT * FROM UNNEST(GENERATE_DATE_ARRAY('1900-01-01', CURRENT_DATE('America/New_York'))) AS day
),

ChargeDates AS (
  SELECT
    Person.person_id,
    SUBSTR(Person.jurisdiction_id, 0, 5) AS fips,
    Booking.booking_id,
    Booking.admission_date,
    Booking.release_date,
    Charge.charge_id,
    Charge.charge_text
  FROM (
    SELECT charge_id, booking_id, name AS charge_text
    FROM
      `{project_id}.{base_dataset}.{charge_table}`
  ) Charge
  LEFT JOIN
    `{project_id}.{base_dataset}.{booking_table}` Booking
  ON
    Charge.booking_id = Booking.booking_id
  LEFT JOIN
    `{project_id}.{base_dataset}.{person_table}` Person
  ON
    Booking.person_id = Person.person_id
),

AdmittedTable AS (
  SELECT day, fips, charge_text, COUNT(DISTINCT(booking_id)) AS admitted
  FROM ChargeDates
  JOIN Days
  ON day = ChargeDates.admission_date
  GROUP BY day, fips, charge_text
),

ReleasedTable AS (
  SELECT day, fips, charge_text, COUNT(DISTINCT(booking_id)) AS released
  FROM ChargeDates
  JOIN Days
  ON day = ChargeDates.release_date
  GROUP BY day, fips, charge_text
),

PersonCountTable AS (
  SELECT day, fips, charge_text, COUNT(DISTINCT(booking_id)) AS person_count
  FROM ChargeDates
  JOIN Days
  ON day BETWEEN ChargeDates.admission_date AND COALESCE(ChargeDates.release_date, CURRENT_DATE('America/New_York'))
  GROUP BY day, fips, charge_text
)

SELECT PersonCountTable.day, PersonCountTable.fips, PersonCountTable.charge_text, person_count, admitted, released, CountyNames.state, CountyNames.county_name
FROM PersonCountTable
FULL JOIN AdmittedTable
ON PersonCountTable.day = AdmittedTable.day AND PersonCountTable.fips = AdmittedTable.fips AND PersonCountTable.charge_text = AdmittedTable.charge_text
FULL JOIN ReleasedTable
ON PersonCountTable.day = ReleasedTable.day AND PersonCountTable.fips = ReleasedTable.fips AND PersonCountTable.charge_text = ReleasedTable.charge_text
JOIN
  `{project_id}.{views_dataset}.{county_names_view}` CountyNames
ON
  PersonCountTable.fips = CountyNames.fips
ORDER BY day DESC, fips, person_count DESC
""".format(
    description=CHARGE_TEXT_COUNTS_DESCRIPTION,
    project_id=PROJECT_ID,
    base_dataset=BASE_DATASET,
    views_dataset=VIEWS_DATASET,
    charge_table=Charge.__tablename__,
    booking_table=Booking.__tablename__,
    person_table=Person.__tablename__,
    county_names_view=COUNTY_NAMES_VIEW.view_id
)

CHARGE_TEXT_COUNTS_VIEW = bqview.BigQueryView(
    view_id=CHARGE_TEXT_COUNTS_VIEW_NAME,
    view_query=CHARGE_TEXT_COUNTS_QUERY
)

if __name__ == '__main__':
    print(CHARGE_TEXT_COUNTS_VIEW.view_id)
    print(CHARGE_TEXT_COUNTS_VIEW.view_query)

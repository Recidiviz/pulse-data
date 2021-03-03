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
"""Total population, admissions, releases by race, gender and day-fips."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.county import dataset_config

from recidiviz.calculator.query.county.views.vera.county_names import (
    COUNTY_NAMES_VIEW_BUILDER,
)
from recidiviz.persistence.database.schema.county.schema import Booking, Person
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

POPULATION_ADMISSIONS_RELEASES_RACE_GENDER_VIEW_NAME = (
    "population_admissions_releases_race_gender"
)

POPULATION_ADMISSIONS_RELEASES_RACE_GENDER_DESCRIPTION = """
For each day-fips combination,
compute the total population, admissions, and releases by race and gender.
"""

POPULATION_ADMISSIONS_RELEASES_RACE_GENDER_QUERY_TEMPLATE = """
/*{description}*/
WITH

Days AS (
  SELECT * FROM UNNEST(GENERATE_DATE_ARRAY('1900-01-01', CURRENT_DATE('America/New_York'))) AS day
),

BookingDates AS (
  SELECT
    Person.person_id,
    SUBSTR(Person.jurisdiction_id, 0, 5) AS fips,
    COALESCE(COALESCE(Person.race, Person.ethnicity), 'EXTERNAL_UNKNOWN') AS race,
    COALESCE(Person.gender, 'EXTERNAL_UNKNOWN') AS gender,
    Booking.booking_id,
    Booking.admission_date,
    Booking.release_date
  FROM `{project_id}.{base_dataset}.{booking_table}` Booking
  JOIN `{project_id}.{base_dataset}.{person_table}` Person
  ON Person.person_id = Booking.person_id
  AND Booking.admission_date IS NOT NULL
),

AdmittedTable AS (
  SELECT day, fips, race, gender, COUNT(DISTINCT(booking_id)) AS admitted
  FROM BookingDates
  JOIN Days
  ON day = BookingDates.admission_date
  GROUP BY day, fips, race, gender
),

ReleasedTable AS (
  SELECT day, fips, race, gender, COUNT(DISTINCT(booking_id)) AS released
  FROM BookingDates
  JOIN Days
  ON day = BookingDates.release_date
  GROUP BY day, fips, race, gender
),

PersonCountTable AS (
  SELECT day, fips, race, gender, COUNT(DISTINCT(booking_id)) AS person_count
  FROM BookingDates
  JOIN Days
  ON day BETWEEN BookingDates.admission_date AND COALESCE(BookingDates.release_date, CURRENT_DATE('America/New_York'))
  GROUP BY day, fips, race, gender
)

SELECT
  PersonCountTable.day,
  PersonCountTable.fips,
  PersonCountTable.race,
  PersonCountTable.gender,
  PersonCountTable.person_count - COALESCE(ReleasedTable.released, 0) AS person_count,
  AdmittedTable.admitted,
  ReleasedTable.released,
  CountyNames.county_name,
  CountyNames.state
FROM PersonCountTable
FULL JOIN AdmittedTable
ON PersonCountTable.day = AdmittedTable.day
  AND PersonCountTable.fips = AdmittedTable.fips
  AND PersonCountTable.race = AdmittedTable.race
  AND PersonCountTable.gender = AdmittedTable.gender
FULL JOIN ReleasedTable
ON PersonCountTable.day = ReleasedTable.day
  AND PersonCountTable.fips = ReleasedTable.fips
  AND PersonCountTable.race = ReleasedTable.race
  AND PersonCountTable.gender = ReleasedTable.gender
JOIN
  `{project_id}.{views_dataset}.{county_names_view}` CountyNames
ON
  PersonCountTable.fips = CountyNames.fips
ORDER BY day DESC, fips, race, gender
"""

POPULATION_ADMISSIONS_RELEASES_RACE_GENDER_VIEW_BUILDER: SimpleBigQueryViewBuilder = (
    SimpleBigQueryViewBuilder(
        dataset_id=dataset_config.VIEWS_DATASET,
        view_id=POPULATION_ADMISSIONS_RELEASES_RACE_GENDER_VIEW_NAME,
        view_query_template=POPULATION_ADMISSIONS_RELEASES_RACE_GENDER_QUERY_TEMPLATE,
        description=POPULATION_ADMISSIONS_RELEASES_RACE_GENDER_DESCRIPTION,
        base_dataset=dataset_config.COUNTY_BASE_DATASET,
        views_dataset=dataset_config.VIEWS_DATASET,
        county_names_view=COUNTY_NAMES_VIEW_BUILDER.view_id,
        booking_table=Booking.__tablename__,
        person_table=Person.__tablename__,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        POPULATION_ADMISSIONS_RELEASES_RACE_GENDER_VIEW_BUILDER.build_and_print()

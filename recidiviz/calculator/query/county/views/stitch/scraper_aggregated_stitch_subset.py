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
"""Scraper data used for stitch"""


from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.county import dataset_config
from recidiviz.calculator.query.county.views.population import (
    population_admissions_releases,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_DESCRIPTION = """
Aggregate individual scraper data by summing person_count for each condition
"""

_QUERY_TEMPLATE = """
/*{description}*/

SELECT
  fips,
  day AS valid_from,
  DATE_ADD(day, INTERVAL 1 DAY) AS valid_to,
  'scraped' AS data_source,
  SUM(person_count) AS population,
  SUM(IF(gender = 'MALE', person_count, null)) AS male,
  SUM(IF(gender = 'FEMALE', person_count, null)) AS female,
  SUM(IF(gender = 'EXTERNAL_UNKNOWN', person_count, null)) AS unknown_gender,
  SUM(IF(race = 'ASIAN', person_count, null)) AS asian,
  SUM(IF(race = 'BLACK', person_count, null)) AS black,
  SUM(IF(race = 'AMERICAN_INDIAN_ALASKAN_NATIVE', person_count, null)) AS native_american,
  SUM(IF(race = 'HISPANIC', person_count, null)) AS latino,
  SUM(IF(race = 'WHITE', person_count, null)) AS white,
  SUM(IF(race = 'OTHER', person_count, null)) AS other,
  SUM(IF(race = 'EXTERNAL_UNKNOWN', person_count, null)) AS unknown_race,
  SUM(IF(gender = 'MALE' AND race = 'ASIAN', person_count, null)) AS male_asian,
  SUM(IF(gender = 'MALE' AND race = 'BLACK', person_count, null)) AS male_black,
  SUM(IF(gender = 'MALE' AND race = 'AMERICAN_INDIAN_ALASKAN_NATIVE', person_count, null)) AS male_native_american,
  SUM(IF(gender = 'MALE' AND race = 'HISPANIC', person_count, null)) AS male_latino,
  SUM(IF(gender = 'MALE' AND race = 'WHITE', person_count, null)) AS male_white,
  SUM(IF(gender = 'MALE' AND race = 'OTHER', person_count, null)) AS male_other,
  SUM(IF(gender = 'MALE' AND race = 'EXTERNAL_UNKNOWN', person_count, null)) AS male_unknown_race,
  SUM(IF(gender = 'FEMALE' AND race = 'ASIAN', person_count, null)) AS female_asian,
  SUM(IF(gender = 'FEMALE' AND race = 'BLACK', person_count, null)) AS female_black,
  SUM(IF(gender = 'FEMALE' AND race = 'AMERICAN_INDIAN_ALASKAN_NATIVE', person_count, null)) AS female_native_american,
  SUM(IF(gender = 'FEMALE' AND race = 'HISPANIC', person_count, null)) AS female_latino,
  SUM(IF(gender = 'FEMALE' AND race = 'WHITE', person_count, null)) AS female_white,
  SUM(IF(gender = 'FEMALE' AND race = 'OTHER', person_count, null)) AS female_other,
  SUM(IF(gender = 'FEMALE' AND race = 'EXTERNAL_UNKNOWN', person_count, null)) AS female_unknown_race,
  SUM(IF(gender = 'EXTERNAL_UNKNOWN' AND race = 'ASIAN', person_count, null)) AS unknown_gender_asian,
  SUM(IF(gender = 'EXTERNAL_UNKNOWN' AND race = 'BLACK', person_count, null)) AS unknown_gender_black,
  SUM(IF(gender = 'EXTERNAL_UNKNOWN' AND race = 'AMERICAN_INDIAN_ALASKAN_NATIVE', person_count, null)) AS unknown_gender_native_american,
  SUM(IF(gender = 'EXTERNAL_UNKNOWN' AND race = 'HISPANIC', person_count, null)) AS unknown_gender_latino,
  SUM(IF(gender = 'EXTERNAL_UNKNOWN' AND race = 'WHITE', person_count, null)) AS unknown_gender_white,
  SUM(IF(gender = 'EXTERNAL_UNKNOWN' AND race = 'OTHER', person_count, null)) AS unknown_gender_other,
  SUM(IF(gender = 'EXTERNAL_UNKNOWN' AND race = 'EXTERNAL_UNKNOWN', person_count, null)) AS unknown_gender_unknown_race
FROM `{project_id}.{views_dataset}.{population_admissions_releases_race_gender}` RaceGender
GROUP BY fips, RaceGender.day
"""

SCRAPER_AGGREGATED_STITCH_SUBSET_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.UNMANAGED_VIEWS_DATASET,
    view_id="scraper_aggregated_stitch_subset",
    view_query_template=_QUERY_TEMPLATE,
    views_dataset=dataset_config.UNMANAGED_VIEWS_DATASET,
    population_admissions_releases_race_gender=population_admissions_releases.POPULATION_ADMISSIONS_RELEASES_RACE_GENDER_VIEW_BUILDER.view_id,
    description=_DESCRIPTION,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SCRAPER_AGGREGATED_STITCH_SUBSET_VIEW_BUILDER.build_and_print()

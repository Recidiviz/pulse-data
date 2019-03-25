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
"""Define views for combining scraper & state-reports."""
# pylint:disable=line-too-long

from recidiviz.calculator.bq.views import view_config
from recidiviz.calculator.bq.views.bqview import BigQueryView
from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
VIEWS_DATASET = view_config.VIEWS_DATASET

_INTERPOLATED_STATE_AGGREGATE_QUERY = """
WITH

Days AS (
  SELECT * FROM UNNEST(GENERATE_DATE_ARRAY('1900-01-01', CURRENT_DATE('America/New_York'))) AS day
)

SELECT
  StateAggregatesWithValidDates.fips,
  Days.day,
  'state_aggregates' AS data_source,
  StateAggregatesWithValidDates.population,
  StateAggregatesWithValidDates.male,
  StateAggregatesWithValidDates.female,
  NULL AS unknown_gender,
  NULL AS asian,
  NULL AS black,
  NULL AS native_american,
  NULL AS latino,
  NULL AS white,
  NULL AS other,
  NULL AS unknown_race,
  NULL AS male_asian,
  NULL AS male_black,
  NULL AS male_native_american,
  NULL AS male_latino,
  NULL AS male_white,
  NULL AS male_other,
  NULL AS male_unknown_race,
  NULL AS female_asian,
  NULL AS female_black,
  NULL AS female_native_american,
  NULL AS female_latino,
  NULL AS female_white,
  NULL AS female_other,
  NULL AS female_unknown_race,
  NULL AS unknown_gender_asian,
  NULL AS unknown_gender_asian_black,
  NULL AS unknown_gender_asian_native_american,
  NULL AS unknown_gender_asian_latino,
  NULL AS unknown_gender_asian_white,
  NULL AS unknown_gender_asian_other,  
  NULL AS unknown_gender_unknown_race
  
  FROM (
      SELECT
        StateAggregates.report_date AS valid_to,
        StateAggregates.fips AS fips,
        StateAggregates.custodial AS population,
        StateAggregates.male AS male,
        StateAggregates.female AS female,
        CASE StateAggregates.aggregation_window
          WHEN 'DAILY' THEN DATE_SUB(report_date, INTERVAL 1 DAY)
          WHEN 'WEEKLY' THEN DATE_SUB(report_date, INTERVAL 1 WEEK)
          WHEN 'MONTHLY' THEN DATE_SUB(report_date, INTERVAL 1 MONTH)
          WHEN 'QUARTERLY' THEN DATE_SUB(report_date, INTERVAL 1 QUARTER)
          WHEN 'YEARLY' THEN DATE_SUB(report_date, INTERVAL 1 YEAR)
        END AS valid_from
      FROM
        `{project_id}.{views_dataset}.combined_state_aggregates` StateAggregates
    ) StateAggregatesWithValidDates
JOIN
  Days
ON
  Days.day BETWEEN StateAggregatesWithValidDates.valid_from AND StateAggregatesWithValidDates.valid_to
ORDER BY
  day DESC
""".format(project_id=PROJECT_ID, views_dataset=VIEWS_DATASET)

INTERPOLATED_STATE_AGGREGATE_VIEW = BigQueryView(
    view_id='interpolated_state_aggregate',
    view_query=_INTERPOLATED_STATE_AGGREGATE_QUERY
)

_SCRAPER_DATA_AGGREGATED_QUERY = """
SELECT
  fips,
  day,
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
  SUM(IF(gender = 'EXTERNAL_UNKNOWN' AND race = 'BLACK', person_count, null)) AS unknown_gender_asian_black,
  SUM(IF(gender = 'EXTERNAL_UNKNOWN' AND race = 'AMERICAN_INDIAN_ALASKAN_NATIVE', person_count, null)) AS unknown_gender_asian_native_american,
  SUM(IF(gender = 'EXTERNAL_UNKNOWN' AND race = 'HISPANIC', person_count, null)) AS unknown_gender_asian_latino,
  SUM(IF(gender = 'EXTERNAL_UNKNOWN' AND race = 'WHITE', person_count, null)) AS unknown_gender_asian_white,
  SUM(IF(gender = 'EXTERNAL_UNKNOWN' AND race = 'OTHER', person_count, null)) AS unknown_gender_asian_other,  
  SUM(IF(gender = 'EXTERNAL_UNKNOWN' AND race = 'EXTERNAL_UNKNOWN', person_count, null)) AS unknown_gender_unknown_race
FROM `{project_id}.{views_dataset}.population_admissions_releases_race_gender`
GROUP BY fips, day
""".format(project_id=PROJECT_ID, views_dataset=VIEWS_DATASET)

SCRAPER_DATA_AGGREGATED = BigQueryView(
    view_id='scraper_data_aggregated',
    view_query=_SCRAPER_DATA_AGGREGATED_QUERY
)

_SCRAPER_AND_STATE_COMBINED_QUERY = """
SELECT * FROM `{project_id}.{views_dataset}.interpolated_state_aggregate`
UNION ALL
SELECT * FROM `{project_id}.{views_dataset}.scraper_data_aggregated`
""".format(project_id=PROJECT_ID, views_dataset=VIEWS_DATASET)

SCRAPER_AND_STATE_COMBINED = BigQueryView(
    view_id='scraper_and_state_combined',
    view_query=_SCRAPER_AND_STATE_COMBINED_QUERY
)

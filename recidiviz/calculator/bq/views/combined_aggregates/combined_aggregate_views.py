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
from recidiviz.calculator.bq.views.population import \
    population_admissions_releases
from recidiviz.calculator.bq.views.state_aggregates import state_aggregate_views
from recidiviz.utils import metadata
from recidiviz.calculator.bq.views.vera import vera_view_constants

PROJECT_ID = metadata.project_id()
VIEWS_DATASET = view_config.VIEWS_DATASET
VERA_DATASET = vera_view_constants.VERA_DATASET
INCARCERATION_TRENDS_TABLE = vera_view_constants.INCARCERATION_TRENDS_TABLE

_INTERPOLATED_STATE_AGGREGATE_DESCRIPTION = """
First select combined aggregate data then interpolate it over the given
aggregation_window for each row. We SELECT NULL for unmapped columns to ensure
we SELECT the same num of columns.
"""
_INTERPOLATED_STATE_AGGREGATE_QUERY = """
/*{description}*/

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
        `{project_id}.{views_dataset}.{combined_state_aggregates}` StateAggregates
    ) StateAggregatesWithValidDates
JOIN
  Days
ON
  Days.day BETWEEN StateAggregatesWithValidDates.valid_from AND StateAggregatesWithValidDates.valid_to
ORDER BY
  day DESC
""".format(project_id=PROJECT_ID, views_dataset=VIEWS_DATASET,
           combined_state_aggregates=state_aggregate_views.STATE_AGGREGATE_VIEW.view_id,
           description=_INTERPOLATED_STATE_AGGREGATE_DESCRIPTION)
_INTERPOLATED_STATE_AGGREGATE_VIEW = BigQueryView(
    view_id='interpolated_state_aggregate',
    view_query=_INTERPOLATED_STATE_AGGREGATE_QUERY
)

_SCRAPER_DATA_AGGREGATED_DESCRIPTION = """
Aggregate individual scraper data by summing person_count for each condition
"""
_SCRAPER_DATA_AGGREGATED_QUERY = """
/*{description}*/

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
FROM `{project_id}.{views_dataset}.{population_admissions_releases_race_gender}`
GROUP BY fips, day
""".format(project_id=PROJECT_ID, views_dataset=VIEWS_DATASET,
           population_admissions_releases_race_gender=population_admissions_releases.POPULATION_ADMISSIONS_RELEASES_RACE_GENDER_VIEW.view_id,
           description=_SCRAPER_DATA_AGGREGATED_DESCRIPTION)
_SCRAPER_DATA_AGGREGATED = BigQueryView(
    view_id='scraper_data_aggregated',
    view_query=_SCRAPER_DATA_AGGREGATED_QUERY
)

_INCARCERATION_TRENDS_DESCRIPTION = """
Select ITP data in a format that can be combined with other aggregate data.
SELECT NULL for unmapped columns to ensure we SELECT the same num of columns.
"""
_INCARCERATION_TRENDS_QUERY = """
/*{description}*/

SELECT
  SUBSTR(CAST(yfips AS STRING), 5) AS fips,
  DATE(year, 1, 1) AS day,
  'incarceration_trends' AS data_source,

  total_jail_pop AS population,

  male_jail_pop AS male,
  female_jail_pop AS female,
  NULL AS unknown_gender,

  asian_jail_pop AS asian,
  black_jail_pop AS black,
  native_jail_pop AS native_american,
  latino_jail_pop AS latino,
  white_jail_pop AS white,
  NULL AS other,
  NuLL AS unknown_race,

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
FROM
  `{project_id}.{vera_dataset}.{incarceration_trends}`
""".format(project_id=PROJECT_ID, vera_dataset=VERA_DATASET,
           incarceration_trends=INCARCERATION_TRENDS_TABLE,
           description=_INCARCERATION_TRENDS_DESCRIPTION)
_INCARCERATION_TRENDS_AGGREGATE = BigQueryView(
    view_id='incarceration_trends_aggregate',
    view_query=_INCARCERATION_TRENDS_QUERY
)

_SCRAPER_AND_STATE_AND_ITP_COMBINED_DESCRIPTION = """
Combine {interpolated_state_aggregate}, {scraper_data_aggregated},
{incarceration_trends_aggregate} into one unified view
""".format(interpolated_state_aggregate=_INTERPOLATED_STATE_AGGREGATE_VIEW.view_id,
           scraper_data_aggregated=_SCRAPER_DATA_AGGREGATED.view_id,
           incarceration_trends_aggregate=_INCARCERATION_TRENDS_AGGREGATE.view_id)
_SCRAPER_AND_STATE_AND_ITP_COMBINED_QUERY = """
/*{description}*/

SELECT * FROM `{project_id}.{views_dataset}.{interpolated_state_aggregate}`
UNION ALL
SELECT * FROM `{project_id}.{views_dataset}.{scraper_data_aggregated}`
UNION ALL
SELECT * FROM `{project_id}.{views_dataset}.{incarceration_trends_aggregate}`
""".format(project_id=PROJECT_ID, views_dataset=VIEWS_DATASET,
           interpolated_state_aggregate=_INTERPOLATED_STATE_AGGREGATE_VIEW.view_id,
           scraper_data_aggregated=_SCRAPER_DATA_AGGREGATED.view_id,
           incarceration_trends_aggregate=_INCARCERATION_TRENDS_AGGREGATE.view_id,
           description=_SCRAPER_AND_STATE_AND_ITP_COMBINED_DESCRIPTION)
_SCRAPER_AND_STATE_AND_ITP_COMBINED = BigQueryView(
    view_id='scraper_and_state_and_itp_combined',
    view_query=_SCRAPER_AND_STATE_AND_ITP_COMBINED_QUERY
)


COMBINED_AGGREGATE_VIEWS = [
    _INTERPOLATED_STATE_AGGREGATE_VIEW,
    _SCRAPER_DATA_AGGREGATED,
    _INCARCERATION_TRENDS_AGGREGATE,
    _SCRAPER_AND_STATE_AND_ITP_COMBINED
]

if __name__ == '__main__':
    print(_INTERPOLATED_STATE_AGGREGATE_VIEW.view_query)
    print(_SCRAPER_DATA_AGGREGATED.view_query)
    print(_INCARCERATION_TRENDS_AGGREGATE.view_query)
    print(_SCRAPER_AND_STATE_AND_ITP_COMBINED.view_query)

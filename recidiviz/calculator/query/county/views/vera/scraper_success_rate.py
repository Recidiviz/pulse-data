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
"""scraper success rate"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.county import dataset_config
from recidiviz.calculator.query.county.views.vera.region_fips_map import (
    REGION_FIPS_MAP_VIEW_BUILDER,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SCRAPER_SUCCESS_RATE_VIEW_NAME = "scraper_success_rate"

SCRAPER_SUCCESS_RATE_VIEW_DESCRIPTION = """
scraper success rate
"""

SCRAPER_SUCCESS_RATE_VIEW_QUERY_TEMPLATE = """
SELECT
  region,
  ss.jid,
  SUBSTR(ss.jid, 0, 5) as fips,
  DATE_DIFF(CURRENT_DATE(), MIN(CASE WHEN (ss.date < sc.date AND ss.date IS NOT NULL) OR (sc.date IS NULL) THEN ss.date ELSE sc.date END), DAY) AS days_in_production,
  MIN(ss.date) AS first_success,
  MIN(sc.date) AS first_single_count,
  MAX(ss.date) AS most_recent_success,
  MAX(sc.date) AS most_recent_single_count,
  COUNT(DISTINCT ss.date) AS total_successes, 
  COUNT(DISTINCT sc.date) AS total_single_counts,
  DATE_DIFF(CURRENT_DATE(), MAX(ss.date), DAY) AS days_since_success,
  DATE_DIFF(CURRENT_DATE(), MAX(sc.date), DAY) AS days_since_single_count,
  IFNULL(COUNT(DISTINCT ss.date)/date_diff(CURRENT_DATE(), MIN(ss.date), DAY), 0) AS overall_success_rate,
  COUNT(DISTINCT (CASE WHEN date_diff(CURRENT_DATE(), ss.date, DAY) < 7 THEN ss.date END))/7 AS prev_week_success_rate,
  COUNT(DISTINCT (CASE WHEN date_diff(CURRENT_DATE(), ss.date, DAY) < 30 THEN ss.date END))/30 AS prev_month_success_rate,
  COUNT(DISTINCT (CASE WHEN date_diff(CURRENT_DATE(), ss.date, DAY) < 60 THEN ss.date END))/60 AS prev_2month_success_rate,
  IFNULL(COUNT(DISTINCT sc.date)/date_diff(CURRENT_DATE(), MIN(sc.date), DAY), 0) AS overall_single_count_rate,
  COUNT(DISTINCT (CASE WHEN date_diff(CURRENT_DATE(), sc.date, DAY) < 7 THEN sc.date END))/7 AS prev_week_single_count_rate,
  COUNT(DISTINCT (CASE WHEN date_diff(CURRENT_DATE(), sc.date, DAY) < 30 THEN sc.date END))/30 AS prev_month_single_count_rate,
  COUNT(DISTINCT (CASE WHEN date_diff(CURRENT_DATE(), sc.date, DAY) < 60 THEN sc.date END))/60 AS prev_2month_single_count_rate
FROM `{project_id}.{base_dataset}.{scraper_success_table}` ss
  FULL JOIN `{project_id}.{base_dataset}.{single_count_aggregate_table}` sc ON ss.jid = sc.jid
  JOIN `{project_id}.{views_dataset}.{region_fips_map_view}` fips_map ON SUBSTR(ss.jid, 0, 5) = fips_map.fips
GROUP BY region, jid
ORDER BY region;
"""

SCRAPER_SUCCESS_RATE_VIEW_BUILDER: SimpleBigQueryViewBuilder = (
    SimpleBigQueryViewBuilder(
        dataset_id=dataset_config.VIEWS_DATASET,
        view_id=SCRAPER_SUCCESS_RATE_VIEW_NAME,
        view_query_template=SCRAPER_SUCCESS_RATE_VIEW_QUERY_TEMPLATE,
        description=SCRAPER_SUCCESS_RATE_VIEW_DESCRIPTION,
        scraper_success_table="scraper_success",
        single_count_aggregate_table="single_count_aggregate",
        region_fips_map_view=REGION_FIPS_MAP_VIEW_BUILDER.view_id,
        base_dataset=dataset_config.COUNTY_BASE_DATASET,
        views_dataset=dataset_config.VIEWS_DATASET,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SCRAPER_SUCCESS_RATE_VIEW_BUILDER.build_and_print()

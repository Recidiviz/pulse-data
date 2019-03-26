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
"""State Aggregate data used for stitch"""
# pylint:disable=line-too-long

from recidiviz.calculator.bq.views.bqview import BigQueryView
from recidiviz.calculator.bq.views.state_aggregates import state_aggregate_views
from recidiviz.calculator.bq.views.view_config import VIEWS_DATASET
from recidiviz.utils import metadata

PROJECT_ID: str = metadata.project_id()

_DESCRIPTION = """
First select combined aggregate data then interpolate it over the given
aggregation_window for each row. We SELECT NULL for unmapped columns to ensure
we SELECT the same num of columns.
"""

_QUERY = """
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
           description=_DESCRIPTION)

STATE_AGGREGATE_STITCH_SUBSET_VIEW = BigQueryView(
    view_id='state_aggregate_stitch_subset',
    view_query=_QUERY
)

if __name__ == '__main__':
    print(STATE_AGGREGATE_STITCH_SUBSET_VIEW.view_query)

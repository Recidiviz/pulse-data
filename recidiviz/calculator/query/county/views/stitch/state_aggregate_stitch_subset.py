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
"""State Aggregate data used for stitch"""


from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.county import dataset_config
from recidiviz.calculator.query.county.views.state_aggregates import (
    state_aggregate_collapsed_to_fips,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_DESCRIPTION = """
First select combined aggregate data then interpolate it over the given
aggregation_window for each row. We SELECT NULL for unmapped columns to ensure
we SELECT the same num of columns.
"""

_QUERY_TEMPLATE = """
/*{description}*/

SELECT
  fips,
  CASE aggregation_window
    WHEN 'DAILY' THEN DATE_SUB(report_date, INTERVAL 1 DAY)
    WHEN 'WEEKLY' THEN DATE_SUB(report_date, INTERVAL 1 WEEK)
    WHEN 'MONTHLY' THEN DATE_SUB(report_date, INTERVAL 1 MONTH)
    WHEN 'QUARTERLY' THEN DATE_SUB(report_date, INTERVAL 1 QUARTER)
    WHEN 'YEARLY' THEN DATE_SUB(report_date, INTERVAL 1 YEAR)
  END AS valid_from,
  report_date AS valid_to,
  'state_aggregates' AS data_source,
  custodial AS population,
  male,
  female,
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
  NULL AS unknown_gender_black,
  NULL AS unknown_gender_native_american,
  NULL AS unknown_gender_latino,
  NULL AS unknown_gender_white,
  NULL AS unknown_gender_other,  
  NULL AS unknown_gender_unknown_race
FROM
  `{project_id}.{views_dataset}.{combined_state_aggregates}`
"""

STATE_AGGREGATE_STITCH_SUBSET_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id="state_aggregate_stitch_subset",
    view_query_template=_QUERY_TEMPLATE,
    views_dataset=dataset_config.VIEWS_DATASET,
    combined_state_aggregates=state_aggregate_collapsed_to_fips.STATE_AGGREGATES_COLLAPSED_TO_FIPS_BUILDER.view_id,
    description=_DESCRIPTION,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        STATE_AGGREGATE_STITCH_SUBSET_VIEW_BUILDER.build_and_print()

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
"""Define views for combining scraper & state-reports & ITP."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.county import dataset_config
from recidiviz.calculator.query.county.views.stitch import combined_stitch
from recidiviz.calculator.query.county.views.stitch.incarceration_trends_stitch_subset import (
    INCARCERATION_TRENDS_STITCH_SUBSET_VIEW_BUILDER,
)
from recidiviz.calculator.query.county.views.stitch.scraper_aggregated_stitch_subset import (
    SCRAPER_AGGREGATED_STITCH_SUBSET_VIEW_BUILDER,
)
from recidiviz.calculator.query.county.views.stitch.state_aggregate_stitch_subset import (
    STATE_AGGREGATE_STITCH_SUBSET_VIEW_BUILDER,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.string import StrictStringFormatter

_DESCRIPTION = StrictStringFormatter().format(
    """
Combine {itp}, {state}, {scraper} into one unified view. When overlapping data
exists, we select {state} data first. We then select any {itp} data that exists
before {state} data. We then select and {scraper} data that exists after {state}
data.

Note: Use we use valid_from to check cutoffs, instead of checking valid_from and
valid_to (eg: {itp}.valid_to < {state}.valid_from). This is because all data
points are plotted using valid_from.
""",
    state=INCARCERATION_TRENDS_STITCH_SUBSET_VIEW_BUILDER.view_id,
    scraper=SCRAPER_AGGREGATED_STITCH_SUBSET_VIEW_BUILDER.view_id,
    itp=STATE_AGGREGATE_STITCH_SUBSET_VIEW_BUILDER.view_id,
)

_QUERY_TEMPLATE = """
/*{description}*/

WITH StateCutoffs AS (
  SELECT
    fips,
    MIN(valid_from) AS min_valid_from
  FROM
    `{project_id}.{views_dataset}.{combined_stitch}`
  WHERE
    data_source = 'state_aggregates'
  GROUP BY
    fips
),

ScrapedCutoffs AS (
  SELECT
    fips,
    MIN(valid_from) AS min_valid_from
  FROM
    `{project_id}.{views_dataset}.{combined_stitch}`
  WHERE
    data_source = 'scraped'
  GROUP BY
    fips
)

SELECT
  Data.fips,
  Data.valid_from AS day,
  Data.data_source,
  population,
  male,
  female,
  unknown_gender,
  asian,
  black,
  native_american,
  latino,
  white,
  other,
  unknown_race,
  male_asian,
  male_black,
  male_native_american,
  male_latino,
  male_white,
  male_other,
  male_unknown_race,
  female_asian,
  female_black,
  female_native_american,
  female_latino,
  female_white,
  female_other,
  female_unknown_race,
  unknown_gender_asian,
  unknown_gender_black,
  unknown_gender_native_american,
  unknown_gender_latino,
  unknown_gender_white,
  unknown_gender_other,  
  unknown_gender_unknown_race
FROM
  `{project_id}.{views_dataset}.{combined_stitch}` AS Data
FULL JOIN
  StateCutoffs
ON
  Data.fips = StateCutoffs.fips
FULL JOIN
  ScrapedCutoffs
ON
  Data.fips = ScrapedCutoffs.fips
WHERE
  # We only have itp data
  (StateCutoffs.fips IS NULL AND ScrapedCutoffs.fips IS NULL) OR
  
  # We have itp and scraped
  (StateCutoffs.fips IS NULL AND (
      Data.data_source = 'scraped' OR
      (Data.data_source = 'incarceration_trends' AND Data.valid_from < ScrapedCutoffs.min_valid_from)
  )) OR

  # We have itp, state_aggregate and scraped
  Data.data_source = 'state_aggregates' OR
  (Data.data_source = 'incarceration_trends' AND Data.valid_from < StateCutoffs.min_valid_from) OR
  (Data.data_source = 'scraped' AND StateCutoffs.min_valid_from < Data.valid_from)
"""

COMBINED_STITCH_DROP_OVERLAPPING_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.UNMANAGED_VIEWS_DATASET,
    view_id="combined_stitch_drop_overlapping",
    view_query_template=_QUERY_TEMPLATE,
    views_dataset=dataset_config.VIEWS_DATASET,
    combined_stitch=combined_stitch.COMBINED_STITCH_VIEW_BUILDER.view_id,
    description=_DESCRIPTION,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        COMBINED_STITCH_DROP_OVERLAPPING_VIEW_BUILDER.build_and_print()

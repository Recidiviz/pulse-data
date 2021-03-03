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
"""ITP data used for stitch"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.county import dataset_config
from recidiviz.calculator.query.county.views.vera.vera_view_constants import (
    VERA_DATASET,
    INCARCERATION_TRENDS_TABLE,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_DESCRIPTION = """
Select ITP data in a format that can be combined with other aggregate data.
SELECT NULL for unmapped columns to ensure we SELECT the same num of columns.
"""

_QUERY_TEMPLATE = """
/*{description}*/

SELECT
  SUBSTR(CAST(yfips AS STRING), 5) AS fips,
  DATE(year, 1, 1) AS valid_from,
  DATE_ADD(DATE(year, 1, 1), INTERVAL 1 YEAR) AS valid_to,
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
  NULL AS unknown_gender_black,
  NULL AS unknown_gender_native_american,
  NULL AS unknown_gender_latino,
  NULL AS unknown_gender_white,
  NULL AS unknown_gender_other,
  NULL AS unknown_gender_unknown_race
FROM
  `{project_id}.{vera_dataset}.{incarceration_trends}`
"""

INCARCERATION_TRENDS_STITCH_SUBSET_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id="incarceration_trends_stitch_subset",
    view_query_template=_QUERY_TEMPLATE,
    vera_dataset=VERA_DATASET,
    incarceration_trends=INCARCERATION_TRENDS_TABLE,
    description=_DESCRIPTION,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        INCARCERATION_TRENDS_STITCH_SUBSET_VIEW_BUILDER.build_and_print()

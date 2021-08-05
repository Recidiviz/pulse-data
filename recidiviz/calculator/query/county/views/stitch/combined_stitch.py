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
from recidiviz.calculator.query.county.views.stitch.incarceration_trends_stitch_subset import (
    INCARCERATION_TRENDS_STITCH_SUBSET_VIEW_BUILDER,
)
from recidiviz.calculator.query.county.views.stitch.scraper_aggregated_stitch_subset import (
    SCRAPER_AGGREGATED_STITCH_SUBSET_VIEW_BUILDER,
)
from recidiviz.calculator.query.county.views.stitch.single_count_stitch_subset import (
    SINGLE_COUNT_STITCH_SUBSET_VIEW_BUILDER,
)
from recidiviz.calculator.query.county.views.stitch.state_aggregate_stitch_subset import (
    STATE_AGGREGATE_STITCH_SUBSET_VIEW_BUILDER,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_DESCRIPTION = """
Combine {interpolated_state_aggregate}, {scraper_data_aggregated},
{incarceration_trends_aggregate}, and {single_count} into one unified view
""".format(
    interpolated_state_aggregate=STATE_AGGREGATE_STITCH_SUBSET_VIEW_BUILDER.view_id,
    scraper_data_aggregated=SCRAPER_AGGREGATED_STITCH_SUBSET_VIEW_BUILDER.view_id,
    single_count=SINGLE_COUNT_STITCH_SUBSET_VIEW_BUILDER.view_id,
    incarceration_trends_aggregate=INCARCERATION_TRENDS_STITCH_SUBSET_VIEW_BUILDER.view_id,
)

_QUERY_TEMPLATE = """
/*{description}*/

SELECT * FROM `{project_id}.{views_dataset}.{interpolated_state_aggregate}`
UNION ALL
SELECT * FROM `{project_id}.{views_dataset}.{single_count_aggregate}`
UNION ALL
SELECT * FROM `{project_id}.{views_dataset}.{scraper_data_aggregated}`
UNION ALL
SELECT * FROM `{project_id}.{views_dataset}.{incarceration_trends_aggregate}`
"""

COMBINED_STITCH_VIEW_BUILDER: SimpleBigQueryViewBuilder = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.UNMANAGED_VIEWS_DATASET,
    view_id="combined_stitch",
    view_query_template=_QUERY_TEMPLATE,
    views_dataset=dataset_config.UNMANAGED_VIEWS_DATASET,
    interpolated_state_aggregate=STATE_AGGREGATE_STITCH_SUBSET_VIEW_BUILDER.view_id,
    single_count_aggregate=SINGLE_COUNT_STITCH_SUBSET_VIEW_BUILDER.view_id,
    scraper_data_aggregated=SCRAPER_AGGREGATED_STITCH_SUBSET_VIEW_BUILDER.view_id,
    incarceration_trends_aggregate=INCARCERATION_TRENDS_STITCH_SUBSET_VIEW_BUILDER.view_id,
    description=_DESCRIPTION,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        COMBINED_STITCH_VIEW_BUILDER.build_and_print()

# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""State configurations and views for stale raw data validations."""
from typing import List

import attr

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_tables_dataset_for_region
from recidiviz.ingest.direct.direct_ingest_regions import get_direct_ingest_region
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_import_manager import (
    get_region_raw_file_config,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRegionRawFileConfig,
)
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_direct_ingest_states_existing_in_env,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.string import StrictStringFormatter
from recidiviz.validation.views import dataset_config

_VIEW_ID_TEMPLATE = "_stale_raw_data_tables"
_DESCRIPTION_TEMPLATE = (
    "Validation view that shows one row per stale raw data table in "
)

# TODO(#28239) replace this query (expensive, full db scan) with one that just looks
# at the import sessions table as we will be recording the number of rows imported there
_SUB_QUERY_TEMPLATE = """
  SELECT file_tag, most_recent_rows_datetime, hours_stale FROM (
    SELECT 
      "{file_tag}" as file_tag, 
      MAX(update_datetime) as most_recent_rows_datetime, 
      DATETIME_DIFF(CURRENT_DATETIME('US/Eastern'), MAX(update_datetime), HOUR) AS hours_stale
    FROM `{{project_id}}.{us_xx_raw_data_dataset}.{file_tag}`
  )
  WHERE hours_stale > {max_hours_stale}
"""

_QUERY_TEMPLATE = """
SELECT
  file_tag,
  most_recent_rows_datetime,
  ROUND(SAFE_DIVIDE(hours_stale, 24), 1) as days_stale,
  "{region_code}" as region_code
FROM ({all_sub_queries})
"""


@attr.define
class StaleRawDataQueryBuilder:
    """Query builder for stale raw data validations. Given a region config, this
    class will build stale data queries for each raw data config that is "regularly
    updated" (i.e. has a non-irregular cadence and is not a code file)

    If you want to build queries for files that are not "regularly updated" or exclude
    files that are non-irregular and not a code a code file, please consider adding a
    new candence type or creating a declarative way (e.g. in the raw yamls) to define
    how often we would expect to see each file.
    """

    region_config: DirectIngestRegionRawFileConfig

    def _generate_stale_data_sub_queries_for_files(self) -> str:
        return "\n  UNION ALL \n".join(
            [
                StrictStringFormatter().format(
                    _SUB_QUERY_TEMPLATE,
                    file_tag=config.file_tag,
                    us_xx_raw_data_dataset=raw_tables_dataset_for_region(
                        StateCode[self.region_config.region_code.upper()],
                        DirectIngestInstance.PRIMARY,
                    ),
                    max_hours_stale=str(config.max_hours_before_stale()),
                )
                for config in self.region_config.get_configs_with_regularly_updated_data()
            ]
        )

    def generate_query_template(self) -> str:
        return StrictStringFormatter().format(
            _QUERY_TEMPLATE,
            region_code=self.region_config.region_code.upper(),
            all_sub_queries=self._generate_stale_data_sub_queries_for_files(),
        )


def collect_stale_raw_data_view_builders() -> List[SimpleBigQueryViewBuilder]:
    view_builders = []
    # TODO(#28896) deprecate this pattern
    for state_code in get_direct_ingest_states_existing_in_env():

        # skip playground states
        if get_direct_ingest_region(state_code.value).playground:
            continue

        region_config = get_region_raw_file_config(state_code.value)

        if any(region_config.get_configs_with_regularly_updated_data()):
            view_builder = SimpleBigQueryViewBuilder(
                dataset_id=dataset_config.VIEWS_DATASET,
                view_id=f"{state_code.value.lower()}{_VIEW_ID_TEMPLATE}",
                description=f"{_DESCRIPTION_TEMPLATE}{state_code.value}",
                view_query_template=StaleRawDataQueryBuilder(
                    region_config
                ).generate_query_template(),
                should_materialize=True,
                projects_to_deploy={GCP_PROJECT_PRODUCTION},
            )
            view_builders.append(view_builder)

    return view_builders


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        for builder in collect_stale_raw_data_view_builders():
            builder.build_and_print()

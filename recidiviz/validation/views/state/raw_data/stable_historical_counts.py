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
"""State configurations and views for historical stable row counts"""

from typing import List

import attr

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_tables_dataset_for_region
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_import_manager import (
    get_region_raw_file_config,
)
from recidiviz.ingest.direct.raw_data.raw_file_config_utils import (
    raw_file_tags_referenced_downstream,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRawFileConfig,
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

_VIEW_ID_TEMPLATE = "_stable_historical_raw_data_counts"
_DESCRIPTION_TEMPLATE = (
    "Validation view that compares historical counts of raw data in "
)

_DATE_FILTER_TEMPLATE = "WHERE update_datetime > DATETIME_SUB(CURRENT_DATETIME('US/Eastern'), INTERVAL {interval} DAY)"

# TODO(#28417) update to use * query that filter by file tag, will be simpler
# TODO(#28239) replace this query (expensive, full db scan) with one that just looks
# at the import sessions table as we will be recording the number of rows imported there
# and maybe (??) make this query look back over all time so we have better coverage
_SUB_QUERY_TEMPLATE = """
  SELECT
    "{file_tag}" AS file_tag,
    file_id,
    row_count,
    update_datetime,
    LAG(row_count) OVER prev_row AS prev_row_count,
    LAG(file_id) OVER prev_row AS prev_file_id,
    LAG(update_datetime) OVER prev_row AS prev_update_datetime,
  FROM (
    SELECT
      file_id,
      update_datetime,
      count(file_id) AS row_count
    FROM `{{project_id}}.{us_xx_raw_data_dataset}.{file_tag}`
    {date_filter}
    GROUP BY file_id, update_datetime
  )
  --- the prev_row_count will always be null for the oldest value in the window,
  --- so let's filter it out
  QUALIFY prev_row_count IS NOT NULL
  WINDOW prev_row AS (ORDER BY update_datetime ASC)
"""

_QUERY_TEMPLATE = """
SELECT *, "{region_code}" AS region_code
FROM (
    {all_sub_queries}
)
"""


@attr.define
class StableHistoricalRawDataCountsQueryBuilder:
    """Query builder for stable historical raw data count validations.

    Given a region config, this class will build stable historical raw data count
    queries for each raw data config that is receives historical exports and is
    used downstream.
    """

    region_config: DirectIngestRegionRawFileConfig

    def has_used_historical_files(self) -> bool:
        return any(self._get_used_historical_files())

    def _get_used_historical_files(self) -> List[DirectIngestRawFileConfig]:
        """Collects all raw data files that are:
        - always_historical_export: True
        - used downstream
        """
        file_tags_with_downstream_reference = raw_file_tags_referenced_downstream(
            self.region_config.region_code,
            region_module_override=self.region_config.region_module,
        )
        return [
            file_config
            for file_tag, file_config in self.region_config.raw_file_configs.items()
            if file_config.always_historical_export
            and file_tag in file_tags_with_downstream_reference
        ]

    @staticmethod
    def _build_date_filter_for_config(config: DirectIngestRawFileConfig) -> str:
        """Builds a lookback date filter for the raw file. If there is no update
        cadence, let's look back to all time; if there is one, let's just look at
        all files
        """
        if not config.has_regularly_updated_data():
            return ""

        return StrictStringFormatter().format(
            _DATE_FILTER_TEMPLATE,
            interval=config.get_update_interval() * 7,  # look back to the past 7 files
        )

    def _generate_stable_historical_counts_for_files(self) -> str:
        return "\n UNION ALL \n".join(
            [
                StrictStringFormatter().format(
                    _SUB_QUERY_TEMPLATE,
                    file_tag=config.file_tag,
                    us_xx_raw_data_dataset=raw_tables_dataset_for_region(
                        StateCode[self.region_config.region_code.upper()],
                        DirectIngestInstance.PRIMARY,
                    ),
                    date_filter=self._build_date_filter_for_config(config),
                )
                for config in self._get_used_historical_files()
            ]
        )

    def generate_query_template(self) -> str:
        return StrictStringFormatter().format(
            _QUERY_TEMPLATE,
            region_code=self.region_config.region_code.upper(),
            all_sub_queries=self._generate_stable_historical_counts_for_files(),
        )


def get_stable_historical_counts_view_builders() -> List[SimpleBigQueryViewBuilder]:
    view_builders = []
    for state_code in get_direct_ingest_states_existing_in_env():

        region_config = get_region_raw_file_config(state_code.value)
        query_builder = StableHistoricalRawDataCountsQueryBuilder(region_config)

        if query_builder.has_used_historical_files():
            view_builder = SimpleBigQueryViewBuilder(
                dataset_id=dataset_config.VIEWS_DATASET,
                view_id=f"{state_code.value.lower()}{_VIEW_ID_TEMPLATE}",
                description=f"{_DESCRIPTION_TEMPLATE}{state_code.value}",
                view_query_template=query_builder.generate_query_template(),
                should_materialize=True,
                projects_to_deploy={GCP_PROJECT_PRODUCTION},
            )
            view_builders.append(view_builder)

    return view_builders


ALL_STALBLE_HISTORICAL_COUNTS_VIEW_BUILDER = (
    get_stable_historical_counts_view_builders()
)


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        for builder in ALL_STALBLE_HISTORICAL_COUNTS_VIEW_BUILDER:
            builder.build_and_print()

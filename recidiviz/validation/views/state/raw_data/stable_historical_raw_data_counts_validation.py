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
"""Views builders and configurations for stable historical raw data counts"""
import datetime
import json
from collections import defaultdict
from functools import cache
from typing import Dict, List, Set

import attr
import yaml
from jsonschema.validators import validate

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.common.constants.states import StateCode
from recidiviz.common.local_file_paths import filepath_relative_to_caller
from recidiviz.ingest.direct.dataset_config import raw_tables_dataset_for_region
from recidiviz.ingest.direct.direct_ingest_regions import get_direct_ingest_region
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRawFileConfig,
    DirectIngestRegionRawFileConfig,
    get_region_raw_file_config,
)
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_direct_ingest_states_existing_in_env,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.string import StrictStringFormatter
from recidiviz.validation.views import dataset_config

_STABLE_HISTORICAL_RAW_DATA_COUNTS_VALIDATION_CONFIG_YAML = (
    "stable_historical_raw_data_counts_validation_config.yaml"
)
_STABLE_HISTORICAL_RAW_DATA_COUNTS_VALIDATION_CONFIG_SPEC = (
    "stable_historical_raw_data_counts_validation_config_spec.json"
)
_VIEW_ID_TEMPLATE = "_stable_historical_raw_data_counts"
_DESCRIPTION_TEMPLATE = (
    f"Validation view that compares historical counts of raw data in {{state_code}}. All "
    f"raw data tables that are (1) marked as always_historical_export=True and (2) not "
    f"excluded in {_STABLE_HISTORICAL_RAW_DATA_COUNTS_VALIDATION_CONFIG_YAML} are "
    f"included in this validation."
)

_DATE_FILTER_TEMPLATE = "update_datetime > DATETIME_SUB(CURRENT_DATETIME('US/Eastern'), INTERVAL {interval} DAY)"
_DATE_EXCLUSION_TEMPLATE = "update_datetime NOT BETWEEN PARSE_DATETIME('%FT%T', '{datetime_start_inclusive}') AND PARSE_DATETIME('%FT%T', '{datetime_end_exclusive}')"
_PERMANENT_EXCLUSION_KEY = "PERMANENT"
_DATE_RANGE_EXCLUSION_KEY = "DATE_RANGE"


# TODO(#28239) replace this query (expensive, full db scan) with one that just looks
# at the file import table as we will be recording the number of rows imported there
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
SELECT 
    *,
    "{region_code}" AS state_code,
    "{region_code}" AS region_code
FROM (
    {all_sub_queries}
)
"""


@attr.define
class StableCountsDateRangeExclusion:
    file_tag: str
    datetime_start_inclusive: datetime.datetime
    datetime_end_exclusive: datetime.datetime

    @staticmethod
    def format_for_query(dt: datetime.datetime) -> str:
        return dt.strftime("%Y-%m-%dT%H:%M:%S")

    @classmethod
    def from_exclusion(
        cls, exclusion: Dict[str, str]
    ) -> "StableCountsDateRangeExclusion":
        return StableCountsDateRangeExclusion(
            file_tag=exclusion["file_tag"],
            datetime_start_inclusive=datetime.datetime.fromisoformat(
                exclusion["datetime_start_inclusive"]
            ),
            datetime_end_exclusive=(
                datetime.datetime.fromisoformat(exclusion["datetime_end_exclusive"])
                if exclusion.get("datetime_end_exclusive")
                else datetime.datetime.now(tz=datetime.UTC)
            ),
        )


@cache
def _load_stable_historical_raw_data_counts_validation_config() -> Dict:
    """Loads and validates a config yaml using the provided |config_name| and |spec_name|"""
    config_path = filepath_relative_to_caller(
        _STABLE_HISTORICAL_RAW_DATA_COUNTS_VALIDATION_CONFIG_YAML, "configs"
    )
    spec_path = filepath_relative_to_caller(
        _STABLE_HISTORICAL_RAW_DATA_COUNTS_VALIDATION_CONFIG_SPEC, "configs"
    )

    with open(config_path, "r", encoding="utf-8") as f:
        loaded_config = yaml.safe_load(f)
    with open(spec_path, "r", encoding="utf-8") as f:
        loaded_spec = json.load(f)

    validate(loaded_config, loaded_spec)
    return loaded_config


@attr.define
class StableHistoricalRawDataCountsQueryBuilder:
    """Query builder for stable historical raw data count validations.

    Given a region config, this class will build stable historical raw data count
    queries for each raw data config that is receives historical exports and is
    used downstream.
    """

    region_config: DirectIngestRegionRawFileConfig

    def _get_date_range_exclusions(
        self,
    ) -> Dict[str, List[StableCountsDateRangeExclusion]]:
        exclusions_for_region = (
            _load_stable_historical_raw_data_counts_validation_config()[
                _DATE_RANGE_EXCLUSION_KEY
            ].get(self.region_config.region_code.upper(), [])
        )

        exclusions_by_file_tag = defaultdict(list)

        for exclusion in exclusions_for_region:
            exclusions_by_file_tag[exclusion["file_tag"]].append(
                StableCountsDateRangeExclusion.from_exclusion(exclusion)
            )

        return exclusions_by_file_tag

    def _get_permanent_exclusions(self) -> Set[str]:
        exclusions_for_region = (
            _load_stable_historical_raw_data_counts_validation_config()[
                _PERMANENT_EXCLUSION_KEY
            ].get(self.region_config.region_code.upper(), [])
        )
        return {exclusion["file_tag"] for exclusion in exclusions_for_region}

    def has_historical_files(self) -> bool:
        """Indicates whether or not this region has any historical raw data configs
        that would be included in the validation view.
        """
        return any(self._get_historical_files())

    def _get_historical_files(self) -> List[DirectIngestRawFileConfig]:
        """Collects all raw data files that are `always_historical_export: True` and
        not excluded by a PERMANENT validation exclusion
        """
        excluded_file_tags = self._get_permanent_exclusions()

        return [
            file_config
            for file_tag, file_config in self.region_config.raw_file_configs.items()
            if file_config.always_historical_export
            and file_tag not in excluded_file_tags
        ]

    @staticmethod
    def _build_date_filter_for_config(
        config: DirectIngestRawFileConfig,
        date_range_exclusions: List[StableCountsDateRangeExclusion],
    ) -> str:
        """Builds a lookback date filter for the raw file. If there is regularly updated
        data, will only look back for a max of 7 windows. If there are any date range
        exlcusions, will exclude all file_ids within those date ranges from
        consideration.
        """
        date_filter_clauses = []

        if config.has_regularly_updated_data():
            date_filter_clauses.append(
                StrictStringFormatter().format(
                    _DATE_FILTER_TEMPLATE,
                    interval=config.get_update_interval_in_days() * 7,  # past 7 files
                )
            )

        if date_range_exclusions:
            for exclusion in date_range_exclusions:
                date_filter_clauses.append(
                    StrictStringFormatter().format(
                        _DATE_EXCLUSION_TEMPLATE,
                        datetime_start_inclusive=exclusion.format_for_query(
                            exclusion.datetime_start_inclusive
                        ),
                        datetime_end_exclusive=exclusion.format_for_query(
                            exclusion.datetime_end_exclusive
                        ),
                    )
                )

        if not date_filter_clauses:
            return ""

        return "WHERE " + " AND \n".join(date_filter_clauses)

    def _generate_stable_historical_counts_for_files(self) -> str:
        exclusions_by_file_tag = self._get_date_range_exclusions()
        return "\n UNION ALL \n".join(
            [
                StrictStringFormatter().format(
                    _SUB_QUERY_TEMPLATE,
                    file_tag=config.file_tag,
                    us_xx_raw_data_dataset=raw_tables_dataset_for_region(
                        StateCode[self.region_config.region_code.upper()],
                        DirectIngestInstance.PRIMARY,
                    ),
                    date_filter=self._build_date_filter_for_config(
                        config, exclusions_by_file_tag.get(config.file_tag, [])
                    ),
                )
                for config in self._get_historical_files()
            ]
        )

    def generate_query_template(self) -> str:
        return StrictStringFormatter().format(
            _QUERY_TEMPLATE,
            region_code=self.region_config.region_code.upper(),
            all_sub_queries=self._generate_stable_historical_counts_for_files(),
        )


def collect_stable_historical_raw_data_counts_view_builders() -> (
    List[SimpleBigQueryViewBuilder]
):
    view_builders = []
    # TODO(#28896) deprecate this pattern
    for state_code in get_direct_ingest_states_existing_in_env():

        # skip playground states
        if get_direct_ingest_region(state_code.value).playground:
            continue

        region_config = get_region_raw_file_config(state_code.value)
        query_builder = StableHistoricalRawDataCountsQueryBuilder(region_config)

        if query_builder.has_historical_files():
            view_builder = SimpleBigQueryViewBuilder(
                dataset_id=dataset_config.VIEWS_DATASET,
                view_id=f"{state_code.value.lower()}{_VIEW_ID_TEMPLATE}",
                description=StrictStringFormatter().format(
                    _DESCRIPTION_TEMPLATE, state_code=state_code.value
                ),
                view_query_template=query_builder.generate_query_template(),
                should_materialize=True,
                projects_to_deploy={GCP_PROJECT_PRODUCTION},
            )
            view_builders.append(view_builder)

    return view_builders


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        for builder in collect_stable_historical_raw_data_counts_view_builders():
            builder.build_and_print()

# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
""" Helper file for collecting and consolidating raw ingest file/ingest view configurations """
import csv
import os
from typing import List
from typing import Optional

import attr

import recidiviz

from recidiviz.ingest.direct.controllers.direct_ingest_raw_file_import_manager import (
    DirectIngestRegionRawFileConfig,
)
from recidiviz.ingest.direct.controllers.direct_ingest_view_collector import (
    DirectIngestPreProcessedIngestViewCollector,
)
from recidiviz.utils.regions import get_region


@attr.s
class DataDiscoveryStandardizedFileConfig:
    """ Shared interface describing both raw file / ingest view metadata"""

    file_tag: str = attr.ib()
    columns: List[str] = attr.ib()
    description: Optional[str] = attr.ib(default=None)
    primary_keys: List[str] = attr.ib(factory=list)
    separator: str = attr.ib(default=",")
    encoding: str = attr.ib(default="UTF-8")
    quoting: int = attr.ib(default=csv.QUOTE_MINIMAL)


def get_raw_data_configs(region_code: str) -> List[DataDiscoveryStandardizedFileConfig]:
    """Collect raw file configs for region; translate to our StandardizedFileConfig type"""
    region_config = DirectIngestRegionRawFileConfig(region_code)

    configs = []
    for config in region_config.raw_file_configs.values():
        standardized_config = DataDiscoveryStandardizedFileConfig(
            file_tag=config.file_tag,
            primary_keys=config.primary_key_cols,
            columns=[column.name for column in config.columns],
            separator=config.separator,
            encoding=config.encoding,
            quoting=csv.QUOTE_NONE if config.ignore_quotes else csv.QUOTE_MINIMAL,
        )

        configs.append(standardized_config)

    return configs


def get_ingest_view_configs(
    region_code: str,
) -> List[DataDiscoveryStandardizedFileConfig]:
    """Collect ingest views for region; reads columns from their corresponding fixture csv"""
    views = DirectIngestPreProcessedIngestViewCollector(
        get_region(region_code, True), []
    ).collect_view_builders()

    configs = []
    for view in views:
        try:
            # TODO(#6925) Infer columns from the mapping file rather than the fixture csv
            fixture_path = os.path.join(
                os.path.dirname(recidiviz.__file__),
                f"tests/ingest/direct/direct_ingest_fixtures/{region_code}/{view.ingest_view_name}.csv",
            )

            with open(fixture_path, "r") as f:
                columns = f.readline().split(",")
        except FileNotFoundError:
            continue

        standardized_config = DataDiscoveryStandardizedFileConfig(
            file_tag=view.ingest_view_name,
            columns=columns,
        )

        configs.append(standardized_config)

    return configs

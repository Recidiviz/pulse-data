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
""" Miscellaneous types / configuration for data discovery """
from typing import Dict, List

import pandas as pd

from recidiviz.admin_panel.data_discovery.file_configs import (
    DataDiscoveryStandardizedFileConfig,
)
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.ingest.direct.gcs.file_type import GcsfsDirectIngestFileType

ConfigsByFileType = Dict[
    GcsfsDirectIngestFileType, Dict[str, DataDiscoveryStandardizedFileConfig]
]
FilesByFileType = Dict[GcsfsDirectIngestFileType, List[GcsfsFilePath]]
DataFramesByFileType = Dict[GcsfsDirectIngestFileType, Dict[str, pd.DataFrame]]


class DataDiscoveryTTL:
    DATA_DISCOVERY_ARGS = 60 * 60  # 1 hour
    STATE_FILES = 60 * 20  # 20 minutes
    PARQUET_FILES = 60 * 60 * 24  # 1 day

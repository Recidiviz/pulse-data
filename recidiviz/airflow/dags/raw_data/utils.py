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
"""Utils for the raw data import dag"""
import logging
from types import ModuleType
from typing import Callable, Iterable, List, Optional, Tuple, TypeVar

from more_itertools import partition

from recidiviz.ingest.direct import regions as direct_ingest_regions_module
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRegionRawFileConfig,
)

logger = logging.getLogger("raw_data")
T = TypeVar("T")


def get_direct_ingest_region_raw_config(
    region_code: str, region_module_override: Optional[ModuleType] = None
) -> DirectIngestRegionRawFileConfig:
    region_code = region_code.lower()

    if region_module_override:
        region_module = region_module_override
    else:
        region_module = direct_ingest_regions_module

    return DirectIngestRegionRawFileConfig(
        region_code=region_code,
        region_module=region_module,
    )


def partition_as_list(
    pred: Optional[Callable[[T], object]], iterable: Iterable[T]
) -> Tuple[List[T], List[T]]:
    """Wrapper around more_itertools.partition that, instead of returning iterators
    will return lists
    """
    falses, trues = partition(pred, iterable)
    return list(falses), list(trues)

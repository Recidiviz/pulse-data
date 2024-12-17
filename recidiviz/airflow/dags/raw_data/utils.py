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
import heapq
import logging
from types import ModuleType
from typing import Callable, Iterable, List, Optional, Tuple, TypeVar

from more_itertools import distribute, partition

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


def n_evenly_weighted_buckets(
    items_and_weight: List[Tuple[T, int]], n: int
) -> List[List[T]]:
    """Constructs at most |n| approximately even weighted buckets from
    |items_and_weight|
    """
    if n <= 0:
        raise ValueError(f"Expected n to be greater than or equal to 0; got {n}")

    sorted_items = list(sorted(items_and_weight, key=lambda x: x[1], reverse=True))
    num_buckets = min(len(sorted_items), n)
    buckets: List[List[T]] = [[] for _ in range(num_buckets)]
    heap = [(0, bucket_index) for bucket_index in range(num_buckets)]
    heapq.heapify(heap)

    for item, weight in sorted_items:
        bucket_size, bucket_index = heapq.heappop(heap)
        buckets[bucket_index].append(item)
        heapq.heappush(heap, (bucket_size + weight, bucket_index))

    return buckets


def evenly_weighted_buckets_with_max(
    items_and_weight: List[Tuple[T, int]],
    *,
    target_n: int,
    max_weight_per_bucket: int,
) -> List[List[T]]:
    """Tries to construct |target_n| buckets from |items_and_weight|. If there is more
    than |max_weight_per_bucket| weight per bucket, will construct an arbitrarily large
    number of buckets with max |max_weight_per_bucket| or max weight from items_and_weight.
    """
    if target_n <= 0:
        raise ValueError(
            f"Expected target_n to be greater than or equal to 0; got {target_n}"
        )

    total_weight = sum(item_and_weight[1] for item_and_weight in items_and_weight)
    num_buckets = (
        min(len(items_and_weight), target_n)
        if target_n * max_weight_per_bucket > total_weight
        else min(len(items_and_weight), (total_weight // max_weight_per_bucket) + 1)
    )

    sorted_items = list(sorted(items_and_weight, key=lambda x: x[1], reverse=True))
    buckets: List[List[T]] = [[] for _ in range(num_buckets)]
    heap = [(0, bucket_index) for bucket_index in range(num_buckets)]
    heapq.heapify(heap)

    for item, weight in sorted_items:
        bucket_size, bucket_index = heapq.heappop(heap)
        buckets[bucket_index].append(item)
        heapq.heappush(heap, (bucket_size + weight, bucket_index))

    return buckets


def max_number_of_buckets_with_target(
    items: List[T], max_per_bucket: int, target_number_of_buckets: int
) -> List[List[T]]:
    """Tries to construct |target_number_of_buckets| buckets from |items|. If there are
    more than |max_per_bucket| items per bucket, will construct an arbitrarily large
    number of buckets with |max_per_bucket| items.
    """
    if len(items) == 0:
        return []

    num_buckets = (
        min(len(items), target_number_of_buckets)
        if target_number_of_buckets * max_per_bucket > len(items)
        else (len(items) // max_per_bucket) + 1
    )

    return [list(batch) for batch in distribute(num_buckets, items)]

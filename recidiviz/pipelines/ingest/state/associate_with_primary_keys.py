# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""A PTransform to associate root entities with their primary keys."""
from collections import defaultdict
from typing import Dict, Iterable, Tuple, Union, cast

import apache_beam as beam
from more_itertools import one

from recidiviz.persistence.entity.base_entity import RootEntity
from recidiviz.pipelines.ingest.state.constants import (
    ExternalIdKey,
    PrimaryKey,
    UpperBoundDate,
)
from recidiviz.utils.types import assert_type_list

PRIMARY_KEYS = "primary_keys"
MERGED_ROOT_ENTITIES_WITH_DATES = "merged_root_entities_with_dates"


class AssociateRootEntitiesWithPrimaryKeys(beam.PTransform):
    """A PTransform to associate root entities with their primary keys.

    The input to this PTransform is a Dict[str, beam.PCollection] where the keys are
        {
            "primary_keys": beam.PCollection[Tuple[ExternalIdKey, PrimaryKey]],
            "merged_root_entities_with_dates: beam.PCollection[Tuple[ExternalIdKey, Tuple[UpperBoundDate, RootEntity]]]
        }
    After CoGroupByKey, we have a PCollection that is in the form of:
        (ExternalIdKey, {"primary_keys": Iterable[PrimaryKey], "merged_root_entities_with_dates": Iterable[Tuple[UpperBoundDate, RootEntity]]})
    After Map(transform_joined_data), we have a PCollection that is:
        (PrimaryKey, Dict[UpperBoundDate: Iterable[RootEntity]]) but there may be multiple instances of PrimaryKey
    After GroupByKey, we have a PCollection that is:
        (PrimaryKey, Iterable[Dict[UpperBoundDate: Iterable[RootEntity]]]) where PrimaryKey is now unique
    After Map(merge_date_to_root_entity_dictionary_values), we have a PCollection that is:
        (PrimaryKey, Dict[UpperBoundDate: Iterable[RootEntity]]) where PrimaryKey is now unique and
        the values are merged.
    """

    def expand(
        self, input_or_inputs: Dict[str, beam.PCollection]
    ) -> beam.PCollection[
        Tuple[PrimaryKey, Dict[UpperBoundDate, Iterable[RootEntity]]]
    ]:
        return (
            input_or_inputs
            | "CoGroup the PCollections by Key" >> beam.CoGroupByKey()
            | "Transform joined data to be keyed by primary key"
            >> beam.Map(self.transform_joined_data)
            | "GroupBy primary key" >> beam.GroupByKey()
            | "Merge entities by date"
            >> beam.Map(self.merge_date_to_root_entity_dictionary_values)
        )

    def transform_joined_data(
        self,
        element: Tuple[
            ExternalIdKey,
            Dict[
                str,
                Union[
                    Iterable[Tuple[UpperBoundDate, RootEntity]], Iterable[PrimaryKey]
                ],
            ],
        ],
    ) -> Tuple[PrimaryKey, Dict[UpperBoundDate, Iterable[RootEntity]]]:
        _, values = element
        primary_key: PrimaryKey = one(
            assert_type_list(values[PRIMARY_KEYS], PrimaryKey)
        )
        entities_with_dates: Iterable[Tuple[UpperBoundDate, RootEntity]] = cast(
            Iterable[Tuple[UpperBoundDate, RootEntity]],
            values[MERGED_ROOT_ENTITIES_WITH_DATES],
        )
        result: Dict[UpperBoundDate, Iterable[RootEntity]] = defaultdict(list)
        for date, entity in entities_with_dates:
            result[date] = [*result[date], entity]
        return (primary_key, result)

    def merge_date_to_root_entity_dictionary_values(
        self,
        element: Tuple[
            PrimaryKey, Iterable[Dict[UpperBoundDate, Iterable[RootEntity]]]
        ],
    ) -> Tuple[PrimaryKey, Dict[UpperBoundDate, Iterable[RootEntity]]]:
        primary_key, values = element
        result: Dict[UpperBoundDate, Iterable[RootEntity]] = defaultdict(list)
        for value in values:
            for date, entities in value.items():
                result[date] = [*result[date], *entities]
        return (primary_key, result)

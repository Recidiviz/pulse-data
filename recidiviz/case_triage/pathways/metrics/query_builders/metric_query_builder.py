# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
""" Contains functionality to map metrics to our relational models inside the query builders"""
import abc
from typing import Any, Dict, Generic, List, Tuple, Type, TypeVar, Union

import attr
from sqlalchemy.orm import Query

from recidiviz.case_triage.pathways.dimensions.dimension import Dimension
from recidiviz.case_triage.pathways.dimensions.dimension_mapping import (
    DimensionMapping,
    DimensionMappingCollection,
    DimensionOperation,
)
from recidiviz.persistence.database.schema.pathways.schema import (
    MetricMetadata,
    PathwaysBase,
)


@attr.s(auto_attribs=True)
class FetchMetricParams:
    filters: Dict[Dimension, List[str]] = attr.field(factory=dict)

    @property
    def sorted_filters(self) -> List[Tuple[str, List[str]]]:
        return sorted(
            (dimension.value, sorted(values))
            for dimension, values in self.filters.items()
        )

    @property
    def cache_fragment(self) -> str:
        return f"filters={repr(self.sorted_filters)}"


ParamsType = TypeVar("ParamsType", bound=FetchMetricParams)
MetricConfigOptionsType = Dict[str, Union[str, List[str]]]


@attr.s(auto_attribs=True)
class MetricQueryBuilder(Generic[ParamsType]):
    """Builds Postgres queries for Pathways metrics"""

    name: str
    model: PathwaysBase
    dimension_mappings: List[DimensionMapping]

    def __attrs_post_init__(self) -> None:
        self.dimension_mapping_collection = DimensionMappingCollection(
            self.dimension_mappings
        )

    def build_filter_conditions(self, params: ParamsType) -> List[str]:
        conditions = [
            self.dimension_mapping_collection.columns_for_dimension_operation(
                DimensionOperation.FILTER,
                dimension,
            ).in_(value)
            for dimension, value in params.filters.items()
        ]

        return conditions

    @property
    def cache_fragment(self) -> str:
        return self.name

    @abc.abstractmethod
    def build_query(self, params: ParamsType) -> Query:
        ...

    def build_metadata_query(self) -> Query:
        return Query(MetricMetadata).filter(
            MetricMetadata.metric == self.model.__name__
        )

    @classmethod
    def get_params_class(cls) -> Type[FetchMetricParams]:
        return FetchMetricParams

    @classmethod
    def build_params(cls, schema: Dict) -> FetchMetricParams:
        return cls.get_params_class()(**schema)

    @classmethod
    def adapt_config_options(
        cls, _model: PathwaysBase, _options: Dict[str, Union[str, List[str]]]
    ) -> Dict:
        return {}

    @classmethod
    def has_valid_option(
        cls, options: MetricConfigOptionsType, name: str, instance: Any
    ) -> bool:
        if name in options:
            if isinstance(options[name], instance):
                return True

            raise TypeError(
                f"{cls} option `{name}` must be an instance of `{instance}`"
            )

        return False

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
""" Contains all available metrics """
import os
from typing import Dict, List, Type, TypeVar, Union

import yaml
from sqlalchemy import Column

from recidiviz.case_triage.pathways.dimensions.dimension import Dimension
from recidiviz.case_triage.pathways.dimensions.dimension_mapping import (
    DimensionMapping,
    DimensionOperation,
)
from recidiviz.case_triage.pathways.metrics.query_builders.all_metric_query_builders import (
    ALL_METRIC_QUERY_BUILDERS,
)
from recidiviz.case_triage.pathways.metrics.query_builders.metric_query_builder import (
    MetricQueryBuilder,
)
from recidiviz.persistence.database.schema.pathways.schema import PathwaysBase
from recidiviz.persistence.database.schema_utils import get_pathways_database_entities

config_path = os.path.join(os.path.dirname(__file__), "config.yaml")


FindByNameElement = TypeVar("FindByNameElement")


def find_by_name(name: str, objects: List[FindByNameElement]) -> FindByNameElement:
    try:
        element: FindByNameElement = next(
            obj for obj in objects if getattr(obj, "__name__") == name
        )

        return element
    except StopIteration as e:
        raise ValueError(
            f"Could not find element {name}. "
            f"Must be one of {[getattr(obj, '__name__') for obj in objects]}"
        ) from e


class MetricQueryBuilderConfigParser:
    """Parser for config.yaml"""

    all_metrics_by_name: Dict[str, MetricQueryBuilder]

    def __init__(self) -> None:
        self.all_metrics_by_name = {}

    def parse(self) -> Dict[str, MetricQueryBuilder]:
        """Transforms the config YAML to MetricQueryBuilders"""
        with open(config_path, "r", encoding="utf-8") as config_file:
            config = yaml.safe_load(config_file)
            pathways_database_entities = get_pathways_database_entities()

            for metric_name, metric_config in config.items():
                QueryBuilderClass: Type[MetricQueryBuilder] = find_by_name(
                    metric_config["query_builder_class"],
                    ALL_METRIC_QUERY_BUILDERS,
                )

                database_entity = find_by_name(
                    metric_config["database_entity"],
                    pathways_database_entities,
                )

                dimension_mappings = self.parse_dimension_mappings(
                    database_entity,
                    metric_config["dimension_mappings"],
                )
                metric_config_options = metric_config.get("query_builder_options", {})
                adapted_options = QueryBuilderClass.adapt_config_options(
                    database_entity, metric_config_options
                )

                try:
                    self.all_metrics_by_name[metric_name] = QueryBuilderClass(
                        name=metric_name,
                        model=database_entity,
                        dimension_mappings=dimension_mappings,
                        **adapted_options,
                    )
                except TypeError as e:
                    raise TypeError(
                        f"Could not instantiate {QueryBuilderClass} with adapted options {adapted_options}."
                        f"Options were adapted from {metric_config_options}. Check the adapt_config_options class "
                        f"method of {QueryBuilderClass}"
                    ) from e

        return self.all_metrics_by_name

    @staticmethod
    def parse_column(database_entity: PathwaysBase, column_name: str) -> Column:
        return getattr(database_entity, column_name)

    @staticmethod
    def parse_dimension_operation(dimension_operation: str) -> DimensionOperation:
        return getattr(DimensionOperation, dimension_operation)

    @staticmethod
    def parse_dimension(dimension_name: str) -> Dimension:
        return Dimension(dimension_name)

    def parse_dimension_mapping(
        self,
        database_entity: PathwaysBase,
        dimension_name: str,
        dimension_mapping_config: Union[str, Dict[str, str]],
    ) -> DimensionMapping:
        dimension = self.parse_dimension(dimension_name)

        if isinstance(dimension_mapping_config, str):
            return DimensionMapping(
                dimension=dimension,
                columns=[self.parse_column(database_entity, dimension_name)],
                operations=self.parse_dimension_operation(dimension_mapping_config),
            )

        operations = self.parse_dimension_operation(
            dimension_mapping_config["operations"]
        )
        column_name = dimension_mapping_config["column"]
        column_label = dimension_mapping_config.get("column_label", None)
        column = self.parse_column(database_entity, column_name)
        if column_label:
            column = column.label(column_label)

        return DimensionMapping(
            dimension=dimension,
            columns=[column],
            operations=operations,
        )

    def parse_dimension_mappings(
        self,
        database_entity: PathwaysBase,
        dimension_mappings: Dict[str, Union[str, Dict[str, str]]],
    ) -> List[DimensionMapping]:
        return [
            self.parse_dimension_mapping(
                database_entity, dimension_name, dimension_mapping
            )
            for dimension_name, dimension_mapping in dimension_mappings.items()
        ]


config_parser = MetricQueryBuilderConfigParser()
ALL_METRICS_BY_NAME = config_parser.parse()
ALL_METRICS = list(ALL_METRICS_BY_NAME.values())

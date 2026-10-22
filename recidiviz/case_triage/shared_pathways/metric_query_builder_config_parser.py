# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Config parser for building MetricQueryBuilder instances from YAML config files."""

from typing import Any, Dict, List, Type, TypeVar, Union

import yaml
from sqlalchemy import Column

from recidiviz.case_triage.shared_pathways.dimensions.dimension import Dimension
from recidiviz.case_triage.shared_pathways.dimensions.dimension_mapping import (
    DimensionMapping,
    DimensionOperation,
)
from recidiviz.case_triage.shared_pathways.query_builders.all_metric_query_builders import (
    ALL_METRIC_QUERY_BUILDERS,
)
from recidiviz.case_triage.shared_pathways.query_builders.metric_query_builder import (
    MetricQueryBuilder,
)
from recidiviz.persistence.database.schema.pathways.schema import PathwaysBase
from recidiviz.persistence.database.schema.public_pathways.schema import (
    PublicPathwaysBase,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.schema_utils import (
    get_pathways_database_entities,
    get_public_pathways_database_entities,
)

FindByNameElement = TypeVar("FindByNameElement")

ColumnConfig = Union[str, Dict[str, str]]
# One entry of a dimension_mappings block. The dict form holds operations, and
# either column (with an optional column_label) or filter_columns, plus optional
# group_columns and applies_to.
DimensionMappingConfig = Union[str, Dict[str, Any]]


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


def _get_database_entities_for_schema(schema_type: SchemaType) -> list:
    """Returns the database entities for the given schema type."""
    if schema_type == SchemaType.PATHWAYS:
        return get_pathways_database_entities()
    if schema_type == SchemaType.PUBLIC_PATHWAYS:
        return get_public_pathways_database_entities()
    raise ValueError(f"Unsupported schema type: {schema_type}")


class MetricQueryBuilderConfigParser:
    """Parser for config.yaml files that builds MetricQueryBuilder instances."""

    ALL_PATHWAYS_METRICS_BY_NAME: Dict[str, MetricQueryBuilder]

    def __init__(self, config_path: str, schema_type: SchemaType) -> None:
        self.config_path = config_path
        self.schema_type = schema_type
        self.ALL_PATHWAYS_METRICS_BY_NAME = {}

    def parse(self) -> Dict[str, MetricQueryBuilder]:
        """Transforms the config YAML to MetricQueryBuilders"""
        with open(self.config_path, "r", encoding="utf-8") as config_file:
            config = yaml.safe_load(config_file)
            database_entities = _get_database_entities_for_schema(self.schema_type)

            for metric_name, metric_config in config.items():
                QueryBuilderClass: Type[MetricQueryBuilder] = find_by_name(
                    metric_config["query_builder_class"],
                    ALL_METRIC_QUERY_BUILDERS,
                )

                database_entity = find_by_name(
                    metric_config["database_entity"],
                    database_entities,
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
                    self.ALL_PATHWAYS_METRICS_BY_NAME[metric_name] = QueryBuilderClass(
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

        return self.ALL_PATHWAYS_METRICS_BY_NAME

    @staticmethod
    def parse_column(
        database_entity: PathwaysBase | PublicPathwaysBase, column_name: str
    ) -> Column:
        return getattr(database_entity, column_name)

    @staticmethod
    def parse_dimension_operation(dimension_operation: str) -> DimensionOperation:
        return getattr(DimensionOperation, dimension_operation)

    @staticmethod
    def parse_dimension(dimension_name: str) -> Dimension:
        return Dimension(dimension_name)

    def parse_labeled_column(
        self,
        database_entity: PathwaysBase | PublicPathwaysBase,
        column_config: ColumnConfig,
    ) -> Column:
        """Returns one column, named either by a bare column name or by a
        column/column_label pair."""
        if isinstance(column_config, str):
            return self.parse_column(database_entity, column_config)

        column = self.parse_column(database_entity, column_config["column"])
        column_label = column_config.get("column_label", None)
        return column.label(column_label) if column_label else column

    def parse_dimension_mapping(
        self,
        database_entity: PathwaysBase | PublicPathwaysBase,
        dimension_name: str,
        dimension_mapping_config: DimensionMappingConfig,
    ) -> List[DimensionMapping]:
        """Returns the mappings one dimension_mappings entry declares.

        A dict entry can set three optional keys:

        - group_columns makes the dimension composite: grouping by it selects
          every column listed. Filtering still reads the filter column(s), and
          one DimensionMapping cannot hold two column lists, so the entry
          produces a FILTER mapping and a GROUP mapping.
        - filter_columns makes each filter value compound: one value per
          column, joined by PATHWAYS_COMPOUND_FILTER_VALUE_DELIMITER. It
          replaces column for filtering.
        - applies_to maps column names to values, and names the rows the
          dimension describes, e.g. "movement_category: admission" for an
          admission type. Grouping counts only those rows, and filtering
          narrows only those rows.
        """
        dimension = self.parse_dimension(dimension_name)

        if isinstance(dimension_mapping_config, str):
            return [
                DimensionMapping(
                    dimension=dimension,
                    columns=[self.parse_column(database_entity, dimension_name)],
                    operations=self.parse_dimension_operation(dimension_mapping_config),
                )
            ]

        operations = self.parse_dimension_operation(
            dimension_mapping_config["operations"]
        )
        filter_columns_config = dimension_mapping_config.get("filter_columns", None)
        group_columns_config = dimension_mapping_config.get("group_columns", None)
        applicable_row_conditions = [
            self.parse_column(database_entity, column_name) == value
            for column_name, value in (
                dimension_mapping_config.get("applies_to", None) or {}
            ).items()
        ]

        def parse_filter_columns() -> List[Column]:
            if filter_columns_config:
                return [
                    self.parse_column(database_entity, column_name)
                    for column_name in filter_columns_config
                ]
            return [
                self.parse_labeled_column(database_entity, dimension_mapping_config)
            ]

        if not group_columns_config:
            if filter_columns_config and DimensionOperation.GROUP in operations:
                raise ValueError(
                    f"Dimension [{dimension_name}] sets filter_columns and allows "
                    f"GROUP, but sets no group_columns. A grouping cannot read "
                    f"compound filter columns, so add group_columns to the entry or "
                    f"change its operations to FILTER."
                )
            return [
                DimensionMapping(
                    dimension=dimension,
                    columns=parse_filter_columns(),
                    operations=operations,
                    applicable_row_conditions=applicable_row_conditions,
                )
            ]

        mappings: List[DimensionMapping] = []
        if DimensionOperation.FILTER in operations:
            mappings.append(
                DimensionMapping(
                    dimension=dimension,
                    columns=parse_filter_columns(),
                    operations=DimensionOperation.FILTER,
                    applicable_row_conditions=applicable_row_conditions,
                )
            )
        if DimensionOperation.GROUP in operations:
            mappings.append(
                DimensionMapping(
                    dimension=dimension,
                    columns=[
                        self.parse_labeled_column(database_entity, column_config)
                        for column_config in group_columns_config
                    ],
                    operations=DimensionOperation.GROUP,
                    applicable_row_conditions=applicable_row_conditions,
                )
            )
        return mappings

    def parse_dimension_mappings(
        self,
        database_entity: PathwaysBase | PublicPathwaysBase,
        dimension_mappings: Dict[str, DimensionMappingConfig],
    ) -> List[DimensionMapping]:
        return [
            mapping
            for dimension_name, dimension_mapping in dimension_mappings.items()
            for mapping in self.parse_dimension_mapping(
                database_entity, dimension_name, dimension_mapping
            )
        ]

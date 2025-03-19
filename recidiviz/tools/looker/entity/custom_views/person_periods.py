# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Class for creating a LookML view for person periods."""
import abc

import attr
from google.cloud import bigquery

from recidiviz.common import attr_validators
from recidiviz.looker.lookml_bq_utils import lookml_view_field_for_schema_field
from recidiviz.looker.lookml_view import LookMLView
from recidiviz.looker.lookml_view_field import DimensionLookMLViewField
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.utils.string import StrictStringFormatter


@attr.define
class _ColumnSelectClause:
    """
    Abstract base class representing a column select clause to be used in an SQL query.

    Attributes:
        alias (str): The name to select the column as.
    """

    alias: str = attr.ib(validator=attr_validators.is_non_empty_str)

    @abc.abstractmethod
    def build(self) -> str:
        """Abstract method to build the column's SQL representation."""

    @property
    @abc.abstractmethod
    def referenced_field(self) -> str | None:
        """Abstract property to get the referenced field of the column, if any."""


@attr.define
class _LiteralStringColumnSelectClause(_ColumnSelectClause):
    """
    A select clause for a literal string, ex: `'incarceration_period' AS period_type`

    Attributes:
        value (str): The string literal to select.
    """

    value: str = attr.ib(validator=attr_validators.is_non_empty_str)

    def build(self) -> str:
        return f"'{self.value}' AS {self.alias}"

    @property
    def referenced_field(self) -> None:
        return None


@attr.define
class _DataColumnSelectClause(_ColumnSelectClause):
    """
    A select clause for a data column, with an optional expression for transforming the field.
    ex: `IFNULL(release_date, CURRENT_DATE('US/Eastern')) AS end_date`

    Attributes:
        field (str): The name of the field.
        expr (str | None): An optional expression for transforming the field. Must contain
            the string literal '{field}' to be replaced with the field name. Defaults to None.
    """

    field: str = attr.ib(validator=attr_validators.is_non_empty_str)
    expr: str | None = attr.ib(default=None, validator=attr_validators.is_opt_str)

    def __attrs_post_init__(self) -> None:
        # pylint doesn't believe that self.expr is definitely not None
        # in the second clause if you check that it's not None in the first
        if (
            self.expr is not None
            and "{field}"
            not in self.expr  # pylint: disable=unsupported-membership-test
        ):
            raise ValueError(
                f"Expression [{self.expr}] must contain string '{{field}}' for field reference"
            )

    def build(self) -> str:
        return (
            f"{StrictStringFormatter().format(self.expr, field=self.field)} AS {self.alias}"
            if self.expr
            else f"{self.field} AS {self.alias}"
        )

    @property
    def referenced_field(self) -> str:
        return self.field


def person_periods_view_name_for_dataset(dataset_id: str) -> str:
    return f"{dataset_id}_person_periods"


@attr.define
class PersonPeriodsLookMLViewBuilder:
    """LookML view builder for person periods. person_periods is a derived table that combines
    data from incarceration and supervision periods into a single view."""

    incarceration_period_table: str
    supervision_period_table: str
    incarceration_period_schema: list[bigquery.SchemaField]
    supervision_period_schema: list[bigquery.SchemaField]
    dataset_id: str

    primary_key_fields: tuple[str, str] = ("period_id", "period_type")

    incarceration_period_columns: list[_ColumnSelectClause] = [
        _DataColumnSelectClause(alias="period_id", field="external_id"),
        _LiteralStringColumnSelectClause(
            alias="period_type", value="incarceration_period"
        ),
        _DataColumnSelectClause(alias="person_id", field="person_id"),
        _DataColumnSelectClause(alias="start_date", field="admission_date"),
        _DataColumnSelectClause(alias="start_reason", field="admission_reason"),
        _DataColumnSelectClause(
            alias="end_date",
            field="release_date",
            expr="IFNULL({field}, CURRENT_DATE('US/Eastern'))",
        ),
        _DataColumnSelectClause(alias="end_reason", field="release_reason"),
    ]
    supervision_period_columns: list[_ColumnSelectClause] = [
        _DataColumnSelectClause(alias="period_id", field="external_id"),
        _LiteralStringColumnSelectClause(
            alias="period_type", value="supervision_period"
        ),
        _DataColumnSelectClause(alias="person_id", field="person_id"),
        _DataColumnSelectClause(alias="start_date", field="start_date"),
        _DataColumnSelectClause(alias="start_reason", field="admission_reason"),
        _DataColumnSelectClause(
            alias="end_date",
            field="termination_date",
            expr="IFNULL({field}, CURRENT_DATE('US/Eastern'))",
        ),
        _DataColumnSelectClause(alias="end_reason", field="termination_reason"),
    ]

    @classmethod
    def from_schema(
        cls, dataset_id: str, bq_schema: dict[str, list[bigquery.SchemaField]]
    ) -> "PersonPeriodsLookMLViewBuilder":
        """Creates a PersonPeriodsLookMLView instance using a dictionary of BQ table name to schema fields."""
        incarceration_period_table = (
            state_entities.StateIncarcerationPeriod.get_table_id()
        )
        supervision_period_table = state_entities.StateSupervisionPeriod.get_table_id()

        return cls(
            dataset_id=dataset_id,
            incarceration_period_table=incarceration_period_table,
            supervision_period_table=supervision_period_table,
            incarceration_period_schema=bq_schema[incarceration_period_table],
            supervision_period_schema=bq_schema[supervision_period_table],
        )

    def _build_schema(self) -> set[bigquery.SchemaField]:
        """Builds the schema for the person periods view based on the referenced fields
        in the incarceration and supervision period schemas."""

        person_periods_schema = set()

        def build_schema_for_columns(
            columns: list[_ColumnSelectClause],
            schema_field_name_map: dict[str, bigquery.SchemaField],
        ) -> None:
            for column in columns:
                referenced_field = column.referenced_field
                if not referenced_field:
                    person_periods_schema.add(
                        bigquery.SchemaField(column.alias, "STRING")
                    )
                    continue
                if referenced_field not in schema_field_name_map:
                    raise ValueError(
                        f"Referenced field [{referenced_field}] not found in schema fields {list(schema_field_name_map.keys())}"
                    )
                person_periods_schema.add(
                    bigquery.SchemaField(
                        column.alias, schema_field_name_map[referenced_field].field_type
                    )
                )

        build_schema_for_columns(
            self.incarceration_period_columns,
            schema_field_name_map={
                field.name: field for field in self.incarceration_period_schema
            },
        )
        build_schema_for_columns(
            self.supervision_period_columns,
            schema_field_name_map={
                field.name: field for field in self.supervision_period_schema
            },
        )

        return person_periods_schema

    def _build_query(self) -> str:
        query_template = """
    SELECT
        {incarceration_period_columns}
      FROM {dataset_id}.{incarceration_period_table}
      UNION ALL
      SELECT
        {supervision_period_columns}
      FROM {dataset_id}.{supervision_period_table}
"""
        return StrictStringFormatter().format(
            query_template,
            incarceration_period_columns=",\n\t".join(
                [c.build() for c in self.incarceration_period_columns]
            ),
            supervision_period_columns=",\n\t".join(
                [c.build() for c in self.supervision_period_columns]
            ),
            incarceration_period_table=self.incarceration_period_table,
            supervision_period_table=self.supervision_period_table,
            dataset_id=self.dataset_id,
        )

    def build(self) -> LookMLView:
        schema_fields = self._build_schema()
        dimension_fields = [
            DimensionLookMLViewField.for_compound_primary_key(
                self.primary_key_fields[0], self.primary_key_fields[1]
            )
        ] + [lookml_view_field_for_schema_field(f) for f in schema_fields]

        return LookMLView.for_derived_table(
            view_name=person_periods_view_name_for_dataset(self.dataset_id),
            derived_table_query=self._build_query(),
            fields=sorted(dimension_fields, key=lambda f: f.field_name),
        )

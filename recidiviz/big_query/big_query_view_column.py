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
"""Classes for defining columns used in view schemas."""

import abc
from typing import Literal, Sequence, get_args

import attrs
from google.cloud import bigquery

from recidiviz.common import attr_validators
from recidiviz.utils.string import is_meaningful_docstring

SqlFieldMode = Literal["NULLABLE", "REQUIRED", "REPEATED"]


@attrs.define(kw_only=True)
class BigQueryViewColumn(abc.ABC):
    """Represents a column in BigQuery, with additional metadata and constraints that
    will not be reflected in the bigquery.SchemaField representation of this column.
    """

    name: str = attrs.field(validator=attr_validators.is_non_empty_str)
    # TODO(#18306) v1 -- enforce that the documentation is meaningfully filled in for
    # column -- like raw data configs, no placeholders
    description: str = attrs.field(validator=attr_validators.is_non_empty_str)
    field_type: bigquery.SqlTypeNames
    mode: SqlFieldMode = attrs.field(
        validator=attrs.validators.in_(get_args(SqlFieldMode))
    )

    def as_schema_field(self) -> bigquery.SchemaField:
        """Returns a BigQueryColumn as a bigquery.SchemaField."""
        return bigquery.SchemaField(
            name=self.name,
            description=self.description,
            field_type=self.field_type.value,
            mode=self.mode,
        )

    def matches_bq_field(self, schema_field: bigquery.SchemaField) -> bool:
        """Returns True if this column matches a native BigQuery schema field.

        Comparison treats REQUIRED and NULLABLE modes as equivalent since BigQuery
        views always produce NULLABLE columns even when the underlying data is REQUIRED.
        """
        self_mode = "NULLABLE" if self.mode == "REQUIRED" else self.mode
        other_mode = (
            "NULLABLE" if schema_field.mode == "REQUIRED" else schema_field.mode
        )
        # TODO(#54941) compare descriptions after they are deployed with views
        return (self.field_type.value, self.name, self_mode,) == (
            schema_field.field_type,
            schema_field.name,
            other_mode,
        )

    @property
    def is_documented(self) -> bool:
        """Boolean value for whether this column has meaningful documentation"""
        return is_meaningful_docstring(self.description)


@attrs.define(kw_only=True)
class String(BigQueryViewColumn):
    """A BigQueryViewColumn representing a STRING."""

    field_type: bigquery.SqlTypeNames = bigquery.SqlTypeNames.STRING


@attrs.define(kw_only=True)
class Integer(BigQueryViewColumn):
    """A BigQueryViewColumn representing an INT64."""

    field_type: bigquery.SqlTypeNames = bigquery.SqlTypeNames.INTEGER


@attrs.define(kw_only=True)
class Date(BigQueryViewColumn):
    """A BigQueryViewColumn representing a DATE."""

    field_type: bigquery.SqlTypeNames = bigquery.SqlTypeNames.DATE


def diff_declared_schema_to_bq_schema(
    declared_schema: Sequence[BigQueryViewColumn],
    deployed_schema: Sequence[bigquery.SchemaField],
) -> list[tuple[str, bigquery.SchemaField]]:
    """Compare a declared view schema (list of BigQueryViewColumns) against a native
    BigQuery schema (list of bigquery.SchemaFields).

    Returns:
        A list of (indicator, field) tuples sorted alphabetically by field name.
        The indicator is "+" for deployed fields missing from declared schema, and
        "-" for declared fields missing from deployed schema.
    """
    declared_by_name = {f.name: f for f in declared_schema}
    deployed_by_name = {f.name: f for f in deployed_schema}

    field_names = sorted(set(declared_by_name.keys()) | set(deployed_by_name.keys()))

    diff: list[tuple[str, bigquery.SchemaField]] = []
    for name in field_names:
        declared_field = declared_by_name.get(name)
        deployed_field = deployed_by_name.get(name)

        if deployed_field and not declared_field:
            diff.append(("+", deployed_field))
        elif declared_field and not deployed_field:
            diff.append(("-", declared_field.as_schema_field()))
        elif declared_field and deployed_field:
            if not declared_field.matches_bq_field(deployed_field):
                diff.append(("-", declared_field.as_schema_field()))
                diff.append(("+", deployed_field))

    return diff

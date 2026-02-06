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
from typing import Literal, get_args

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
    field_type: bigquery.SqlTypeNames | bigquery.StandardSqlTypeNames
    mode: SqlFieldMode = attrs.field(
        validator=attrs.validators.in_(get_args(SqlFieldMode))
    )

    def as_schema_field(self) -> bigquery.SchemaField:
        """Returns a BigQueryColumn as a bigquery.SchemaField."""
        return bigquery.SchemaField(
            name=self.name,
            description=self.description,
            field_type=self.field_type.name,
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


@attrs.define(kw_only=True)
class Float(BigQueryViewColumn):
    """A BigQueryViewColumn representing a FLOAT64."""

    field_type: bigquery.SqlTypeNames = bigquery.SqlTypeNames.FLOAT


@attrs.define(kw_only=True)
class Bool(BigQueryViewColumn):
    """A BigQueryViewColumn representing a BOOL."""

    field_type: bigquery.SqlTypeNames = bigquery.SqlTypeNames.BOOLEAN


@attrs.define(kw_only=True)
class Timestamp(BigQueryViewColumn):
    """A BigQueryViewColumn representing a TIMESTAMP."""

    field_type: bigquery.SqlTypeNames = bigquery.SqlTypeNames.TIMESTAMP


@attrs.define(kw_only=True)
class Time(BigQueryViewColumn):
    """A BigQueryViewColumn representing a TIME."""

    field_type: bigquery.SqlTypeNames = bigquery.SqlTypeNames.TIME


@attrs.define(kw_only=True)
class DateTime(BigQueryViewColumn):
    """A BigQueryViewColumn representing a DATETIME."""

    field_type: bigquery.SqlTypeNames = bigquery.SqlTypeNames.DATETIME


@attrs.define(kw_only=True)
class Json(BigQueryViewColumn):
    """A BigQueryViewColumn representing a JSON."""

    field_type: bigquery.StandardSqlTypeNames = bigquery.StandardSqlTypeNames.JSON


@attrs.define(kw_only=True)
class Record(BigQueryViewColumn):
    """A BigQueryViewColumn representing a RECORD (STRUCT) with subfields."""

    field_type: bigquery.SqlTypeNames = bigquery.SqlTypeNames.RECORD
    fields: list[BigQueryViewColumn] = attrs.field(
        validator=attr_validators.is_list_of(BigQueryViewColumn)
    )

    def as_schema_field(self) -> bigquery.SchemaField:
        return bigquery.SchemaField(
            name=self.name,
            description=self.description,
            field_type=self.field_type.name,
            mode=self.mode,
            fields=[f.as_schema_field() for f in self.fields],
        )

    def matches_bq_field(self, schema_field: bigquery.SchemaField) -> bool:
        if not super().matches_bq_field(schema_field):
            return False
        declared_by_name = {f.name: f for f in self.fields}
        deployed_by_name = {f.name: f for f in schema_field.fields}
        if declared_by_name.keys() != deployed_by_name.keys():
            return False
        return all(
            declared_by_name[name].matches_bq_field(deployed_by_name[name])
            for name in declared_by_name
        )

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
"""Utilities for comparing declared view schemas against deployed BigQuery schemas."""

from typing import Sequence

from google.cloud import bigquery

from recidiviz.big_query.big_query_view_column import BigQueryViewColumn, Record


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
            if (
                isinstance(declared_field, Record)
                and deployed_field.field_type == bigquery.SqlTypeNames.RECORD
            ):
                # Record subfield diff entries have names of the form
                # `top_level_field.subfield_name`
                subdiff = diff_declared_schema_to_bq_schema(
                    declared_field.fields, deployed_field.fields
                )
                for line in subdiff:
                    sub_field = line[1]
                    sub_field = bigquery.SchemaField(
                        declared_field.name + "." + sub_field.name,
                        sub_field.field_type,
                    )
                    diff.append((line[0], sub_field))
            elif not declared_field.matches_bq_field(deployed_field):
                diff.append(("-", declared_field.as_schema_field()))
                diff.append(("+", deployed_field))

    return diff


def format_schema_diffs(
    diffs_by_address: dict[str, list[tuple[str, bigquery.SchemaField]]],
) -> str:
    """Format a dictionary of schema diffs (keyed by view address)."""
    parts = []
    for address, schema_diff in diffs_by_address.items():
        # TODO(#54941) include description when it is loaded
        diff_lines = "\n".join(
            f"  {indicator} {field.name} ({field.mode} {field.field_type})"
            for indicator, field in schema_diff
        )
        parts.append(f"{address}:\n{diff_lines}")
    return "\n".join(parts)

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
"""A script for generating one LookML view for each state schema table
Used within state_dataset_lookml_writer
"""
import os
from typing import List

import sqlalchemy
from google.cloud import bigquery
from sqlalchemy import Table

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_utils import (
    bq_schema_column_type_for_sqlalchemy_column,
)
from recidiviz.ingest.views.dataset_config import STATE_BASE_DATASET
from recidiviz.looker.lookml_view import LookMLView
from recidiviz.looker.lookml_view_field import (
    DimensionGroupLookMLViewField,
    DimensionLookMLViewField,
    LookMLFieldParameter,
    LookMLViewField,
    MeasureLookMLViewField,
)
from recidiviz.looker.lookml_view_field_parameter import (
    LookMLFieldDatatype,
    LookMLFieldType,
    LookMLTimeframesOption,
)
from recidiviz.looker.lookml_view_source_table import LookMLViewSourceTable
from recidiviz.persistence.database.schema.state import schema as state_schema
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.schema_utils import (
    get_all_table_classes_in_schema,
    get_primary_key_column_name,
)
from recidiviz.tools.looker.script_helpers import remove_lookml_files_from


def _field_type_for_column(column: sqlalchemy.Column) -> LookMLFieldType:
    """
    Convert the provided column's field type into a LookMLFieldType.
    """
    column_type = bq_schema_column_type_for_sqlalchemy_column(column)
    if column_type in (
        bigquery.enums.SqlTypeNames.DATE,
        bigquery.enums.SqlTypeNames.DATETIME,
    ):
        return LookMLFieldType.TIME
    if column_type == bigquery.enums.SqlTypeNames.STRING:
        return LookMLFieldType.STRING
    if column_type == bigquery.enums.SqlTypeNames.BOOLEAN:
        return LookMLFieldType.YESNO
    if column_type == bigquery.enums.SqlTypeNames.INTEGER:
        return LookMLFieldType.NUMBER

    raise NotImplementedError(
        f"Could not convert {column.name} into Looker: LookML data type equivalent to {column_type} has not been implemented"
    )


def _build_time_column_dimension_group(
    column_name: str,
) -> DimensionGroupLookMLViewField:
    """
    Return a DimensionGroupLookMLField corresponding to the given column,
    which should be of type TIME. Dates and timestamps can be represented
    in Looker using a dimension group of type: time.
    """
    field_name = column_name
    return DimensionGroupLookMLViewField(
        field_name=field_name,
        parameters=[
            LookMLFieldParameter.type(LookMLFieldType.TIME),
            LookMLFieldParameter.timeframes(
                [
                    LookMLTimeframesOption.RAW,
                    LookMLTimeframesOption.DATE,
                    LookMLTimeframesOption.WEEK,
                    LookMLTimeframesOption.MONTH,
                    LookMLTimeframesOption.QUARTER,
                    LookMLTimeframesOption.YEAR,
                ]
            ),
            LookMLFieldParameter.convert_tz(False),
            LookMLFieldParameter.datatype(LookMLFieldDatatype.DATE),
            LookMLFieldParameter.sql(f"${{TABLE}}.{column_name}"),
        ],
    )


def _build_column_dimension(
    table_name: str, column_name: str, field_type: LookMLFieldType
) -> DimensionLookMLViewField:
    """
    Return a DimensionLookMLField corresponding to the given column, which should not be a TIME Type
    """
    primary_key_col_name = get_primary_key_column_name(state_schema, table_name)
    custom_params = []
    if column_name == primary_key_col_name:
        custom_params.append(LookMLFieldParameter.primary_key(True))
    elif column_name in ("person_id", "staff_id"):
        custom_params.extend(
            [
                LookMLFieldParameter.hidden(True),
                LookMLFieldParameter.value_format("0"),
            ]
        )
    return DimensionLookMLViewField.for_column(
        column_name, field_type=field_type, custom_params=custom_params
    )


def get_lookml_view_table(
    table: Table,
) -> LookMLView:
    """
    Returns a LookML View object corresponding to the given Table
    """
    table_name = table.name

    fields: List[LookMLViewField] = []
    for column in sorted(table.columns, key=lambda c: c.name):
        field_type = _field_type_for_column(column)
        if field_type == LookMLFieldType.TIME:
            fields.append(_build_time_column_dimension_group(column.name))
        else:
            fields.append(_build_column_dimension(table.name, column.name, field_type))

    fields.append(
        MeasureLookMLViewField(
            field_name="count",
            parameters=[
                LookMLFieldParameter.type(LookMLFieldType.COUNT),
                LookMLFieldParameter.drill_fields(
                    []
                    # TODO(#23292): Drilled fields may not be empty, depending on the view
                ),
            ],
        )
    )

    return LookMLView(
        view_name=table_name,
        table=LookMLViewSourceTable.sql_table_address(
            BigQueryAddress(dataset_id=STATE_BASE_DATASET, table_id=table_name)
        ),
        fields=fields,
    )


def generate_state_views(looker_dir: str) -> None:
    """Produce LookML View files for the state dataset, writing up-to-date
    .view.lkml files in looker_dir/views/state/

    looker_dir: Local path to root directory of the Looker repo
    """
    state_dir = os.path.join(looker_dir, "views", "state")
    remove_lookml_files_from(state_dir)
    # TODO(#23292): Either auto-generate or don't delete the person_periods view

    for table in get_all_table_classes_in_schema(SchemaType.STATE):
        lookml_view = get_lookml_view_table(table=table)
        lookml_view.write(state_dir, source_script_path=__file__)
    # TODO(#23292): Add `actions` dimension to state_person view
    # TODO(#23292): Custom formatting for open (null end date) supervision / incarceration periods

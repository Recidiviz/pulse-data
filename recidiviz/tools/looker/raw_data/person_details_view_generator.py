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
"""Code for building LookML views for raw data tables and writing them to a file.
Used inside person_details_lookml_writer
"""

import os

from recidiviz.looker.lookml_view import LookMLView
from recidiviz.looker.lookml_view_field import (
    DimensionGroupLookMLViewField,
    DimensionLookMLViewField,
    LookMLFieldDatatype,
    LookMLFieldParameter,
    ParameterLookMLViewField,
)
from recidiviz.looker.lookml_view_field_parameter import (
    LookMLFieldType,
    LookMLTimeframesOption,
)
from recidiviz.looker.parameterized_value import ParameterizedValue

RAW_DATA_OPTION = "raw_data"
RAW_DATA_UP_TO_DATE_VIEWS_OPTION = "raw_data_up_to_date_views"


def generate_shared_fields_view() -> LookMLView:
    """Produce state_raw_data_shared_fields.view.lkml,
    which contains configuration/dimensions shared among all raw data views

    Return: The LookMLView object corresponding to the file
    """

    view_type_param = ParameterLookMLViewField(
        field_name="view_type",
        parameters=[
            LookMLFieldParameter.type(LookMLFieldType.UNQUOTED),
            LookMLFieldParameter.description(
                "Used to select whether the view has the most recent version (raw data up to date views) or all data"
            ),
            LookMLFieldParameter.allowed_value("Raw Data", RAW_DATA_OPTION),
            LookMLFieldParameter.allowed_value(
                "Raw Data Up To Date Views", RAW_DATA_UP_TO_DATE_VIEWS_OPTION
            ),
            LookMLFieldParameter.default_value(RAW_DATA_UP_TO_DATE_VIEWS_OPTION),
        ],
    )

    # Dimensions
    file_id_sql = ParameterizedValue(
        parameter_name="view_type",
        parameter_options=[RAW_DATA_OPTION, RAW_DATA_UP_TO_DATE_VIEWS_OPTION],
        value_builder=lambda s: "${TABLE}.file_id" if s == RAW_DATA_OPTION else "NULL",
        indentation_level=3,
    )
    file_id_dimension = DimensionLookMLViewField(
        field_name="file_id",
        parameters=[
            LookMLFieldParameter.description(
                "Ingest file ID, NULL for raw up to date views"
            ),
            LookMLFieldParameter.type(LookMLFieldType.NUMBER),
            LookMLFieldParameter.sql(file_id_sql),
        ],
    )

    is_deleted_sql = ParameterizedValue(
        parameter_name="view_type",
        parameter_options=[RAW_DATA_OPTION, RAW_DATA_UP_TO_DATE_VIEWS_OPTION],
        value_builder=lambda s: "${TABLE}.is_deleted"
        if s == RAW_DATA_OPTION
        else "NULL",
        indentation_level=3,
    )
    is_deleted_dimension = DimensionLookMLViewField(
        field_name="is_deleted",
        parameters=[
            LookMLFieldParameter.description(
                "Whether table is deleted, NULL for raw up to date views"
            ),
            LookMLFieldParameter.type(LookMLFieldType.YESNO),
            LookMLFieldParameter.sql(is_deleted_sql),
        ],
    )

    update_datetime_sql = ParameterizedValue(
        parameter_name="view_type",
        parameter_options=[RAW_DATA_OPTION, RAW_DATA_UP_TO_DATE_VIEWS_OPTION],
        value_builder=lambda s: "${TABLE}.update_datetime"
        if s == RAW_DATA_OPTION
        else "NULL",
        indentation_level=3,
    )
    update_datetime_dimension_group = DimensionGroupLookMLViewField(
        field_name="update_datetime",
        parameters=[
            LookMLFieldParameter.description(
                "Time of most recent update, NULL for raw up to date views"
            ),
            LookMLFieldParameter.type(LookMLFieldType.TIME),
            LookMLFieldParameter.timeframes(
                [
                    LookMLTimeframesOption.RAW,
                    LookMLTimeframesOption.TIME,
                    LookMLTimeframesOption.DATE,
                    LookMLTimeframesOption.WEEK,
                    LookMLTimeframesOption.MONTH,
                    LookMLTimeframesOption.QUARTER,
                    LookMLTimeframesOption.YEAR,
                ]
            ),
            LookMLFieldParameter.datatype(LookMLFieldDatatype.DATETIME),
            LookMLFieldParameter.sql(update_datetime_sql),
        ],
    )

    view = LookMLView(
        view_name="state_raw_data_shared_fields",
        extension_required=True,
        fields=[
            view_type_param,
            file_id_dimension,
            is_deleted_dimension,
            update_datetime_dimension_group,
        ],
    )
    return view


def generate_lookml_views(looker_dir: str) -> None:
    """Produce LookML View files for a given state, writing to looker_dir/views/raw_data/

    looker_dir: Local path to root directory of the Looker repo"""

    view_dir = os.path.join(looker_dir, "views", "raw_data")

    shared_fields_view = generate_shared_fields_view()
    shared_fields_view.write(view_dir, source_script_path=__file__)

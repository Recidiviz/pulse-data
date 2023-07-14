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
from typing import Dict, List

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import (
    raw_latest_views_dataset_for_region,
    raw_tables_dataset_for_region,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRawFileConfig,
    DirectIngestRegionRawFileConfig,
    RawTableColumnInfo,
)
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_existing_direct_ingest_states,
)
from recidiviz.ingest.direct.types.direct_ingest_constants import FILE_ID_COL_NAME
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.looker.lookml_view import LookMLView
from recidiviz.looker.lookml_view_field import (
    DimensionGroupLookMLViewField,
    DimensionLookMLViewField,
    LookMLFieldDatatype,
    LookMLFieldParameter,
    LookMLViewField,
    MeasureLookMLViewField,
    ParameterLookMLViewField,
)
from recidiviz.looker.lookml_view_field_parameter import (
    LookMLFieldType,
    LookMLTimeframesOption,
)
from recidiviz.looker.lookml_view_source_table import LookMLViewSourceTable
from recidiviz.looker.parameterized_value import ParameterizedValue
from recidiviz.utils.string import StrictStringFormatter

RAW_DATA_OPTION = "raw_data"
RAW_DATA_UP_TO_DATE_VIEWS_OPTION = "raw_data_up_to_date_views"
SHARED_FIELDS_NAME = "state_raw_data_shared_fields"


def _generate_shared_fields_view() -> LookMLView:
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
        field_name=FILE_ID_COL_NAME,
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
        view_name=SHARED_FIELDS_NAME,
        extension_required=True,
        fields=[
            view_type_param,
            file_id_dimension,
            is_deleted_dimension,
            update_datetime_dimension_group,
        ],
    )
    return view


def _get_dimensions_for_raw_file_view(
    config: DirectIngestRawFileConfig, primary_key_sql: str
) -> List[LookMLViewField]:
    """
    Returns a list of dimensions/dimension groups representing the columns
    from the provided raw file config, including:
    - One primary key dimension depending on the provided string
    - One dimension per column of the table
    - One additional dimension group for each datetime column, using the provided
      SQL parsers to parse the provided string into a time
    """

    dimensions: List[LookMLViewField] = []

    dimensions.append(
        DimensionLookMLViewField(
            field_name="primary_key",
            parameters=[
                LookMLFieldParameter.primary_key(True),
                LookMLFieldParameter.hidden(True),
                LookMLFieldParameter.type(LookMLFieldType.STRING),
                LookMLFieldParameter.sql(primary_key_sql),
            ],
        )
    )

    # Add one dimension per column
    for column in config.columns:
        # Add an additional dimension group for datetime columns
        if column.is_datetime:
            dimensions.append(_get_datetime_dimension_group(column))

        col_params = [
            LookMLFieldParameter.label(_label_name_for_column(column)),
            LookMLFieldParameter.type(LookMLFieldType.STRING),
            LookMLFieldParameter.sql("${TABLE}." + column.name),
        ]

        if column.description:
            col_params.append(LookMLFieldParameter.description(column.description))

        if column.name in config.primary_key_cols:
            col_params.append(LookMLFieldParameter.group_label("Primary Key"))

        dimensions.append(
            DimensionLookMLViewField(
                field_name=_label_name_for_column(column),
                parameters=col_params,
            )
        )

    return dimensions


def _label_name_for_column(column: RawTableColumnInfo) -> str:
    """
    Return the label to use for this column
    """
    return column.name if not column.is_datetime else f"{column.name}__raw"


def _get_datetime_dimension_group(col: RawTableColumnInfo) -> LookMLViewField:
    """
    Given a RawTableColumnInfo with type datetime, create a dimension group
    with information parsed from the column
    """
    # Parse the original column
    # If there are no parsers, set to NULL
    sql_text = "NULL"
    if col.datetime_sql_parsers:
        formatter = StrictStringFormatter()
        parsers = [
            formatter.format(parser, col_name="${TABLE}." + col.name)
            for parser in col.datetime_sql_parsers
        ]

        # If there are multiple parsers, use COALESCE to fall back in order
        sql_text = "COALESCE(" + ", ".join(parsers) + ")"

    return DimensionGroupLookMLViewField(
        field_name=col.name,
        parameters=[
            LookMLFieldParameter.description(
                f"[DATE PARSED FROM {_label_name_for_column(col)}]{col.description or ''}"
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
            LookMLFieldParameter.sql(sql_text),
        ],
    )


def _generate_raw_file_view(
    state_code: StateCode, config: DirectIngestRawFileConfig
) -> LookMLView:
    """
    Return a single LookMLView object representing the provided raw file config.
    """
    # Include SQL table address
    ingest_instance = DirectIngestInstance.PRIMARY

    def get_table_id(option: str) -> str:
        """
        Returns the table dataset id according to the raw data or up to date views data
        """
        if option == RAW_DATA_OPTION:
            dataset_id = raw_tables_dataset_for_region(state_code, ingest_instance)
            return BigQueryAddress(
                dataset_id=dataset_id, table_id=config.file_tag
            ).to_str()
        if option == RAW_DATA_UP_TO_DATE_VIEWS_OPTION:
            dataset_id = raw_latest_views_dataset_for_region(
                state_code, ingest_instance
            )
            return BigQueryAddress(
                dataset_id=dataset_id, table_id=config.file_tag + "_latest"
            ).to_str()
        raise ValueError(f"Unexpected raw data table type: {option}")

    parameterized_table_value = ParameterizedValue(
        parameter_name="view_type",
        parameter_options=[RAW_DATA_OPTION, RAW_DATA_UP_TO_DATE_VIEWS_OPTION],
        value_builder=get_table_id,
        indentation_level=2,
    )
    source_table = LookMLViewSourceTable.sql_table_address(parameterized_table_value)

    # Add a primary key dimension concatenating all the primary key columns with file id,
    # or all the columns if there aren't any primary key columns
    cols_to_concat = config.primary_key_cols or [col.name for col in config.columns]
    all_primary_keys = [FILE_ID_COL_NAME] + cols_to_concat
    primary_key_string = ", ".join([f"${{{col}}}" for col in all_primary_keys])
    primary_key_sql = f"CONCAT({primary_key_string})"

    view = LookMLView(
        view_name=config.file_tag,
        table=source_table,
        included_paths=[f"../{SHARED_FIELDS_NAME}.view"],
        extended_views=[SHARED_FIELDS_NAME],
        fields=[
            *_get_dimensions_for_raw_file_view(config, primary_key_sql),
            MeasureLookMLViewField(
                field_name="count",
                parameters=[
                    LookMLFieldParameter.type(LookMLFieldType.COUNT),
                    LookMLFieldParameter.drill_fields(
                        [FILE_ID_COL_NAME, *config.primary_key_cols]
                    ),
                ],
            ),
        ],
    )

    return view


def _generate_state_raw_data_views() -> Dict[StateCode, List[LookMLView]]:
    """
    Return a dictionary mapping all of the StateCodes for states with raw data
    to a list of LookMLViews corresponding to every raw data file in that State
    """
    views: Dict[StateCode, List[LookMLView]] = {}
    for state_code in get_existing_direct_ingest_states():
        views[state_code] = []
        region_config = DirectIngestRegionRawFileConfig(region_code=state_code.value)
        for raw_file_config in region_config.raw_file_configs.values():
            if raw_file_config.columns:
                views[state_code].append(
                    _generate_raw_file_view(state_code, raw_file_config)
                )
    return views


def generate_lookml_views(looker_dir: str) -> None:
    """Produce LookML View files for a given state, writing to looker_dir/views/raw_data/

    looker_dir: Local path to root directory of the Looker repo"""

    view_dir = os.path.join(looker_dir, "views", "raw_data")

    shared_fields_view = _generate_shared_fields_view()
    shared_fields_view.write(view_dir, source_script_path=__file__)

    for state_code, raw_file_views in _generate_state_raw_data_views().items():
        state_dir = os.path.join(view_dir, state_code.value.lower())
        for view in raw_file_views:
            view.write(state_dir, source_script_path=__file__)

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

import functools
import itertools
import os
from collections import defaultdict
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
from recidiviz.ingest.direct.types.direct_ingest_constants import (
    FILE_ID_COL_DESCRIPTION,
    FILE_ID_COL_NAME,
    IS_DELETED_COL_DESCRIPTION,
    IS_DELETED_COL_NAME,
    UPDATE_DATETIME_COL_DESCRIPTION,
    UPDATE_DATETIME_COL_NAME,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.looker.lookml_view import LookMLView
from recidiviz.looker.lookml_view_field import (
    DimensionLookMLViewField,
    LookMLFieldDatatype,
    LookMLFieldParameter,
    LookMLViewField,
    MeasureLookMLViewField,
    ParameterLookMLViewField,
    TimeDimensionGroupLookMLViewField,
)
from recidiviz.looker.lookml_view_field_parameter import (
    LookMLFieldType,
    LookMLTimeframesOption,
)
from recidiviz.looker.lookml_view_source_table import LookMLViewSourceTable
from recidiviz.looker.parameterized_value import ParameterizedValue
from recidiviz.tools.looker.raw_data.person_details_explore_generator import (
    get_table_relationship_edges,
)
from recidiviz.tools.looker.script_helpers import remove_lookml_files_from
from recidiviz.utils.string import StrictStringFormatter
from recidiviz.utils.types import assert_type

RAW_DATA_OPTION = "raw_data"
RAW_DATA_UP_TO_DATE_VIEWS_OPTION = "raw_data_up_to_date_views"
SHARED_FIELDS_NAME = "raw_data_shared_fields"
VIEW_TYPE_PARAM_NAME = "view_type"


def _view_name(state_code: StateCode, file_tag: str) -> str:
    """Return the view name corresponding to the given state code and file.
    The view name is prefixed with the state code to avoid conflicts when
    the same file tag is shared between different states."""
    state_code_abbrev = state_code.value.lower()
    return f"{state_code_abbrev}_{file_tag}"


def _generate_shared_fields_view(
    state_code: StateCode, primary_person_view_name: str
) -> LookMLView:
    """Produce us_XX_raw_data_shared_fields.view.lkml,
    which contains configuration/dimensions shared among all raw data views
    for the provided state

    Return: The LookMLView object corresponding to the file
    """
    # Get the name of the primary table for this state
    parameter_name = f"{primary_person_view_name}.{VIEW_TYPE_PARAM_NAME}"

    # Dimensions
    file_id_sql_options = {
        RAW_DATA_OPTION: "${TABLE}." + FILE_ID_COL_NAME,
        RAW_DATA_UP_TO_DATE_VIEWS_OPTION: "NULL",
    }
    file_id_sql = ParameterizedValue(
        parameter_name=parameter_name,
        parameter_options=list(file_id_sql_options.keys()),
        value_builder=lambda s: file_id_sql_options[s],
        indentation_level=3,
    )
    file_id_dimension = DimensionLookMLViewField(
        field_name=FILE_ID_COL_NAME,
        parameters=[
            LookMLFieldParameter.description(FILE_ID_COL_DESCRIPTION),
            LookMLFieldParameter.type(LookMLFieldType.NUMBER),
            LookMLFieldParameter.sql(file_id_sql),
        ],
    )

    is_deleted_sql_options = {
        RAW_DATA_OPTION: "${TABLE}." + IS_DELETED_COL_NAME,
        RAW_DATA_UP_TO_DATE_VIEWS_OPTION: "NULL",
    }
    is_deleted_sql = ParameterizedValue(
        parameter_name=parameter_name,
        parameter_options=list(is_deleted_sql_options.keys()),
        value_builder=lambda s: is_deleted_sql_options[s],
        indentation_level=3,
    )
    is_deleted_dimension = DimensionLookMLViewField(
        field_name=IS_DELETED_COL_NAME,
        parameters=[
            LookMLFieldParameter.description(IS_DELETED_COL_DESCRIPTION),
            LookMLFieldParameter.type(LookMLFieldType.YESNO),
            LookMLFieldParameter.sql(is_deleted_sql),
        ],
    )

    update_datetime_options = {
        RAW_DATA_OPTION: "${TABLE}." + UPDATE_DATETIME_COL_NAME,
        RAW_DATA_UP_TO_DATE_VIEWS_OPTION: "NULL",
    }
    update_datetime_sql = ParameterizedValue(
        parameter_name=parameter_name,
        parameter_options=list(is_deleted_sql_options.keys()),
        value_builder=lambda s: update_datetime_options[s],
        indentation_level=3,
    )
    update_datetime_dimension_group = TimeDimensionGroupLookMLViewField(
        field_name=UPDATE_DATETIME_COL_NAME,
        parameters=[
            LookMLFieldParameter.description(UPDATE_DATETIME_COL_DESCRIPTION),
            LookMLFieldParameter.type(LookMLFieldType.TIME),
            LookMLFieldParameter.timeframes(
                [
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
        view_name=_view_name(state_code, SHARED_FIELDS_NAME),
        extension_required=True,
        fields=[
            file_id_dimension,
            is_deleted_dimension,
            update_datetime_dimension_group,
        ],
    )
    return view


def _get_dimensions_for_raw_file_view(
    config: DirectIngestRawFileConfig,
    primary_key_sql: str,
    primary_person_view_name: str,
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
    for column in config.current_columns:
        # Add an additional dimension group for datetime columns
        if column.is_datetime:
            dimensions.append(_get_datetime_dimension_group(column))

        col_params = [
            LookMLFieldParameter.label(raw_field_name_for_column(column)),
            LookMLFieldParameter.type(LookMLFieldType.STRING),
        ]

        if column.is_documented:
            col_params.append(
                LookMLFieldParameter.description(assert_type(column.description, str))
            )
            col_params.append(LookMLFieldParameter.sql("${TABLE}." + column.name))
        else:
            # Columns with no description aren't available in up to date views
            def no_description_value_builder(s: str, *, col: RawTableColumnInfo) -> str:
                if s == RAW_DATA_OPTION:
                    return "${TABLE}." + col.name
                if s == RAW_DATA_UP_TO_DATE_VIEWS_OPTION:
                    return "NULL"
                raise ValueError(f"Unexpected raw data table type: {s}")

            col_params.append(
                LookMLFieldParameter.sql(
                    ParameterizedValue(
                        parameter_name=f"{primary_person_view_name}.{VIEW_TYPE_PARAM_NAME}",
                        parameter_options=[
                            RAW_DATA_OPTION,
                            RAW_DATA_UP_TO_DATE_VIEWS_OPTION,
                        ],
                        value_builder=functools.partial(
                            no_description_value_builder, col=column
                        ),
                        indentation_level=3,
                    )
                )
            )

        if column.name in config.primary_key_cols:
            col_params.append(LookMLFieldParameter.group_label("Primary Key"))

        if column.is_primary_for_external_id_type:
            # We need to include a 'full suggestions' parameter on these dimensions
            # to ensure that suggestions show up on the dashboard filter
            col_params.append(LookMLFieldParameter.full_suggestions(True))

        dimensions.append(
            DimensionLookMLViewField(
                field_name=raw_field_name_for_column(column),
                parameters=col_params,
            )
        )

    return dimensions


def raw_field_name_for_column(column: RawTableColumnInfo) -> str:
    """
    Return the raw (unparsed) field name to use for this column
    For datetime columns, the datetimes might be in a format that Looker can't parse,
    so we need to create a separate field for the raw string value
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

    return TimeDimensionGroupLookMLViewField(
        field_name=col.name,
        parameters=[
            LookMLFieldParameter.description(
                f"[DATE PARSED FROM {raw_field_name_for_column(col)}]{f' {col.description}' if col.description else ''}"
            ),
            LookMLFieldParameter.type(LookMLFieldType.TIME),
            LookMLFieldParameter.timeframes(
                [
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
    state_code: StateCode,
    config: DirectIngestRawFileConfig,
    primary_person_view_name: str,
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
        parameter_name=f"{primary_person_view_name}.{VIEW_TYPE_PARAM_NAME}",
        parameter_options=[RAW_DATA_OPTION, RAW_DATA_UP_TO_DATE_VIEWS_OPTION],
        value_builder=get_table_id,
        indentation_level=2,
    )
    source_table = LookMLViewSourceTable.sql_table_address(parameterized_table_value)

    # Add a primary key dimension concatenating all the primary key columns with file id,
    # or all the columns if there aren't any primary key columns
    if config.primary_key_cols:
        cols_to_concat = [
            raw_field_name_for_column(col)
            for col in config.current_columns
            if col.name in config.primary_key_cols
        ]
        drill_fields_cols = cols_to_concat
    else:
        cols_to_concat = [
            raw_field_name_for_column(col) for col in config.current_columns
        ]
        drill_fields_cols = []
    all_primary_keys = [FILE_ID_COL_NAME] + cols_to_concat
    primary_key_string = ", ".join(
        [f'IFNULL(CAST(${{{col}}} AS STRING), "")' for col in all_primary_keys]
    )
    primary_key_sql = f"CONCAT({primary_key_string})"

    shared_fields_name = _view_name(state_code, SHARED_FIELDS_NAME)
    view = LookMLView(
        view_name=_view_name(state_code, config.file_tag),
        table=source_table,
        included_paths=[f"{shared_fields_name}.view"],
        extended_views=[shared_fields_name],
        fields=[
            *_get_dimensions_for_raw_file_view(
                config, primary_key_sql, primary_person_view_name
            ),
            MeasureLookMLViewField(
                field_name="count",
                parameters=[
                    LookMLFieldParameter.type(LookMLFieldType.COUNT),
                    LookMLFieldParameter.drill_fields(
                        [FILE_ID_COL_NAME, *drill_fields_cols]
                    ),
                ],
            ),
        ],
    )

    return view


def _generate_view_type_param() -> LookMLViewField:
    """
    Return a 'view_type' parameter for use in the LookMLView corresponding
    to the primary person table for a certain state.
    """
    return ParameterLookMLViewField(
        field_name=VIEW_TYPE_PARAM_NAME,
        parameters=[
            LookMLFieldParameter.type(LookMLFieldType.UNQUOTED),
            LookMLFieldParameter.description(
                "Used to select whether the view has the most recent version (raw data up to date views) or all data"
            ),
            LookMLFieldParameter.view_label("Cross Table Filters"),
            LookMLFieldParameter.allowed_value("Raw Data", RAW_DATA_OPTION),
            LookMLFieldParameter.allowed_value(
                "Raw Data Up To Date Views", RAW_DATA_UP_TO_DATE_VIEWS_OPTION
            ),
            LookMLFieldParameter.default_value(RAW_DATA_UP_TO_DATE_VIEWS_OPTION),
        ],
    )


def _generate_state_raw_data_views() -> Dict[StateCode, Dict[str, LookMLView]]:
    """
    Return a dictionary mapping all of the StateCodes for states with raw data
    to a dictionary from file tags to LookMLViews corresponding to those
    raw data files in that State, one view for every file that will be included
    in the raw data explore for that State
    """
    views: Dict[StateCode, Dict[str, LookMLView]] = defaultdict(dict)
    for state_code in get_existing_direct_ingest_states():
        region_config = DirectIngestRegionRawFileConfig(region_code=state_code.value)
        if primary_person_table := region_config.get_primary_person_table():
            primary_person_view_name = _view_name(
                state_code, primary_person_table.file_tag
            )

            # Generate the shared view for this state
            shared_fields_view = _generate_shared_fields_view(
                state_code, primary_person_view_name
            )
            views[state_code][shared_fields_view.view_name] = shared_fields_view

            # Generate a view for every raw file that will be included in the explore
            all_tables = region_config.raw_file_configs
            explore_edges = get_table_relationship_edges(
                primary_person_table, all_tables
            )
            files_in_explore = set(
                itertools.chain.from_iterable(
                    (edge.foreign_table, edge.file_tag) for edge in explore_edges
                )
            )
            for file_tag in files_in_explore:
                raw_file_config = all_tables[file_tag]
                # Skip if the file doesn't have enough info to be used in ingest views
                if raw_file_config.is_undocumented:
                    continue
                view = _generate_raw_file_view(
                    state_code, raw_file_config, primary_person_view_name
                )
                # Place the view type parameter in the primary person table's view
                if file_tag == primary_person_table.file_tag:
                    view.fields.append(_generate_view_type_param())
                views[state_code][file_tag] = view
    return views


def generate_lookml_views(looker_dir: str) -> Dict[StateCode, Dict[str, LookMLView]]:
    """Produce LookML View files for a given state, writing up-to-date
    .view.lkml files in looker_dir/views/raw_data/

    looker_dir: Local path to root directory of the Looker repo
    """

    view_dir = os.path.join(looker_dir, "views", "raw_data")
    remove_lookml_files_from(view_dir)

    all_state_views = _generate_state_raw_data_views()
    for state_code, views_by_file_tag in _generate_state_raw_data_views().items():
        state_dir = os.path.join(view_dir, state_code.value.lower())
        for view in views_by_file_tag.values():
            view.write(state_dir, source_script_path=__file__)

    return all_state_views

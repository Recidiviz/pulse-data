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
"""Implements helper methods for tests involving ingesting spreadsheets."""

import os
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd

from recidiviz.justice_counts.metricfile import MetricFile
from recidiviz.justice_counts.metricfiles.metricfile_registry import (
    SYSTEM_TO_FILENAME_TO_METRICFILE,
)
from recidiviz.persistence.database.schema.justice_counts import schema

TEST_EXCEL_FILE = os.path.join(
    os.path.dirname(__file__),
    "bulk_upload/bulk_upload_fixtures/bulk_upload_test.xlsx",
)
TEST_CSV_FILE = os.path.join(
    os.path.dirname(__file__),
    "bulk_upload/bulk_upload_fixtures/bulk_upload_test.csv",
)


def _get_dimension_columns(
    month_list: List[str],
    dimension_list: List[str],
    vary_values: bool,
    null_data: Optional[bool] = False,
    child_agencies: Optional[List[schema.Agency]] = None,
) -> Tuple[List[int], List[str], List[int], List[int], List[str]]:
    """Helper function that creates the columns for the data frame based upon the reporting
    frequency and breakdowns of the MetricFile.
    """
    year_col = [2021, 2022, 2023]
    if null_data is False:
        value_col = [12, 45, 30] if vary_values else [10, 20, 30]
    else:
        value_col = [None, None, None]  # type: ignore[list-item]
    month_col, dimension_col, agency_col = ([], [], [])

    if len(dimension_list) > 0:
        # # [Person, Property, .. Unknown, Other] -> [Person, ... Unknown, Person ... Unknown]
        dimension_col = dimension_list * len(value_col)
        # [2021, 2022, 2023] -> [2021, 2021, 2022, 2022, 2023 ...]
        year_col = [ele for ele in year_col for _ in range(len(dimension_list))]
        # [10, 20, 30] -> [10, 0, 0, 0, 0, 20, 0, 0, 0, 0, 30 ,0 ,0 ,0 ,0] (if there are 5 breakdowns)
        value_col_copy = value_col.copy()
        value_col = []
        for value in value_col_copy:
            value_col.append(value)
            if null_data is False:
                value_col += [0] * (len(dimension_list) - 1)
            else:
                value_col += [None] * (len(dimension_list) - 1)  # type: ignore[list-item]

    if len(month_list) > 0:
        month_col = month_list * len(
            value_col
        )  # [January, February] -> [January, February,... January, February ...]
        year_col = [
            val for val in year_col for _ in (0, 1)
        ]  # [2021, 2022, 2023] -> [2021, 2021, 2022, 2022, 2023 ...]
        value_col = [
            val for val in value_col for _ in (0, 1)
        ]  # [10, 20, 30] -> [10, 10, 20, 20, 30, 30]
        if len(dimension_col) > 0:
            dimension_col = [
                val for val in dimension_col for _ in (0, 1)
            ]  # [Person, Property, .. Unknown, Other] -> [Person, Person, Property, Property...]

    if child_agencies is not None:
        agency_names = [a.name for a in child_agencies]
        agency_col = agency_names * len(
            value_col
        )  # [Agency A, Agency B] -> [Agency A, Agency B,... Agency A, Agency B ...]
        value_col = [
            val for val in value_col for _ in (0, len(agency_names))
        ]  # [10, 20, 30] -> [10, 10, 20, 20, 30, 30]
        month_col = [
            mon for mon in month_col for _ in (0, len(agency_names))
        ]  # [January, February] -> [January, January, February, February]
        if len(dimension_col) > 0:
            dimension_col = [
                val for val in dimension_col for _ in (0, len(agency_names))
            ]  # [Person, Property, .. Unknown, Other] -> [Person, Person, Property, Property...]
        year_col = [
            year for year in year_col for _ in (0, len(agency_names))
        ]  # [2021, 2022, 2023] -> [2021, 2021, 2022, 2022, 2023 ...]

    return (
        year_col,
        month_col,
        agency_col,
        value_col,
        dimension_col,
    )


def _create_dataframe_dict(
    metricfile: MetricFile,
    reporting_frequency: schema.ReportingFrequency,
    child_agencies: Optional[List[schema.Agency]] = None,
    invalid_month: bool = False,
    vary_values: bool = False,
    missing_column: Optional[bool] = False,
    invalid_value_type: Optional[bool] = False,
    too_many_rows: Optional[bool] = False,
    unexpected_month: Optional[bool] = False,
    unexpected_column: Optional[bool] = False,
    unexpected_disaggregation: Optional[bool] = False,
    null_data: Optional[bool] = False,
) -> Dict[str, Any]:
    """Helper function that creates a dictionary, which is later converted into a
    dataframe and exported as an excel file for testing purposes.
    """
    dimension_list = (
        [d.value for d in list(metricfile.disaggregation)]  # type: ignore[call-overload]
        if metricfile.disaggregation is not None
        else []
    )

    month_list = (
        ["January", "February"]
        if reporting_frequency == schema.ReportingFrequency.ANNUAL
        and unexpected_month is True
        or reporting_frequency == schema.ReportingFrequency.MONTHLY
        else []
    )

    year_col, month_col, agency_col, value_col, dimension_col, = _get_dimension_columns(
        month_list=month_list,
        dimension_list=dimension_list,
        vary_values=vary_values,
        null_data=null_data,
        child_agencies=child_agencies,
    )

    if invalid_value_type is True:
        value_col[0] = "wrong value type - string"  # type: ignore[call-overload]

    if too_many_rows is True:
        year_col[1] = year_col[0]
        if len(month_col) > 0:
            month_col[1] = month_col[0]  # type: ignore[index]
        if len(dimension_col) > 0:
            dimension_col[1] = dimension_col[0]  # type: ignore[index]

    if invalid_month:
        month_col[0] = "Marchuary"  # type: ignore[index]

    # Spreadsheet columns have to be added in to the dict in the order
    # to which they will appear.As a result we will add year, month, value,
    # disaggregation, and system in that order.
    dataframe_dict = {"year": year_col}

    if len(month_list) > 0:
        dataframe_dict["month"] = month_col  # type: ignore[assignment]

    if len(agency_col) > 0:
        dataframe_dict["agency"] = agency_col  # type: ignore[assignment]

    if missing_column is False:
        dataframe_dict["value"] = value_col

    if metricfile.disaggregation is not None:
        dataframe_dict[metricfile.disaggregation_column_name] = dimension_col  # type: ignore[index, assignment]

    if unexpected_disaggregation is True:
        dataframe_dict["bloop_type"] = range(0, len(value_col))  # type: ignore[assignment]

    if unexpected_column is True:
        dataframe_dict["bloop"] = range(0, len(value_col))  # type: ignore[assignment]

    return dataframe_dict


def create_excel_file(
    system: schema.System,
    file_name: str = TEST_EXCEL_FILE,
    child_agencies: Optional[list] = None,
    metric_key_to_subsystems: Optional[Dict[str, List[schema.System]]] = None,
    invalid_month_sheet_name: Optional[str] = None,
    add_invalid_sheet_name: Optional[bool] = False,
    missing_column_sheet_name: Optional[str] = None,
    invalid_value_type_sheet_name: Optional[str] = None,
    too_many_rows_filename: Optional[str] = None,
    sheet_names_to_skip: Optional[Set[str]] = None,
    sheet_names_to_vary_values: Optional[Set[str]] = None,
    unexpected_month_sheet_name: Optional[str] = None,
    unexpected_column_sheet_name: Optional[str] = None,
    unexpected_system_sheet_name: Optional[str] = None,
    unexpected_disaggregation_sheet_name: Optional[str] = None,
    sheetnames_with_null_data: Optional[Set[str]] = None,
) -> None:
    """Populates bulk_upload_test.xlsx with fake data to test functions that ingest spreadsheets"""
    filename_to_metricfile = SYSTEM_TO_FILENAME_TO_METRICFILE[system.value]
    with pd.ExcelWriter(  # pylint: disable=abstract-class-instantiated
        file_name
    ) as writer:
        for filename, metricfile in filename_to_metricfile.items():
            if sheet_names_to_skip is not None and filename in sheet_names_to_skip:
                continue
            if (
                sheetnames_with_null_data is not None
                and filename in sheetnames_with_null_data
            ):
                dataframe_dict = _create_dataframe_dict(
                    metricfile=metricfile,
                    child_agencies=child_agencies,
                    reporting_frequency=metricfile.definition.reporting_frequency,
                    invalid_month=filename == invalid_month_sheet_name,
                    invalid_value_type=filename == invalid_value_type_sheet_name,
                    missing_column=missing_column_sheet_name == filename,
                    too_many_rows=too_many_rows_filename == filename,
                    vary_values=sheet_names_to_vary_values is not None
                    and filename in sheet_names_to_vary_values,
                    unexpected_month=filename == unexpected_month_sheet_name,
                    unexpected_column=filename == unexpected_column_sheet_name,
                    unexpected_disaggregation=filename
                    == unexpected_disaggregation_sheet_name,
                    null_data=True,
                )
            else:
                dataframe_dict = _create_dataframe_dict(
                    metricfile=metricfile,
                    child_agencies=child_agencies,
                    reporting_frequency=metricfile.definition.reporting_frequency,
                    invalid_month=filename == invalid_month_sheet_name,
                    invalid_value_type=filename == invalid_value_type_sheet_name,
                    missing_column=missing_column_sheet_name == filename,
                    too_many_rows=too_many_rows_filename == filename,
                    vary_values=sheet_names_to_vary_values is not None
                    and filename in sheet_names_to_vary_values,
                    unexpected_month=filename == unexpected_month_sheet_name,
                    unexpected_column=filename == unexpected_column_sheet_name,
                    unexpected_disaggregation=filename
                    == unexpected_disaggregation_sheet_name,
                    null_data=False,
                )

            # If the metric is for the supervision system or the sheet contains
            # an unexpected system error, add a system column.
            if (
                system == schema.System.SUPERVISION
                or unexpected_system_sheet_name == filename
            ):
                if "value" in dataframe_dict:
                    # There will be no value column if the sheet being generated
                    # has a missing_metric error.
                    dataframe_dict["system"] = ["all"] * len(dataframe_dict["value"])

                if metric_key_to_subsystems is not None:
                    subsystems = metric_key_to_subsystems.get(
                        metricfile.definition.key, []
                    )
                    if len(subsystems) > 1:
                        temp_dataframe_dict = {
                            key: [val for val in values for _ in (0, len(subsystems))]
                            for key, values in dataframe_dict.items()
                        }

                        temp_dataframe_dict["system"] = [
                            subsystems[x % len(subsystems)].value
                            for x in range(0, len(temp_dataframe_dict["value"]))
                        ]
                        dataframe_dict = temp_dataframe_dict

            df = pd.DataFrame(dataframe_dict)
            df.to_excel(writer, sheet_name=filename)
        if add_invalid_sheet_name:
            df = pd.DataFrame({})
            df.to_excel(writer, sheet_name="gender")


def create_csv_file(
    system: schema.System,
    metric: str,
    file_name: str = TEST_CSV_FILE,
    metric_key_to_subsystems: Optional[Dict[str, List[schema.System]]] = None,
    too_many_rows_filename: Optional[str] = None,
) -> None:
    """Populates bulk_upload_test.csv with fake data to test functions that ingest spreadsheets"""
    filename_to_metricfile = SYSTEM_TO_FILENAME_TO_METRICFILE[system.value]

    dataframe_dict = {}
    for filename, metricfile in filename_to_metricfile.items():
        if filename == metric:
            new_dataframe_dict = _create_dataframe_dict(
                metricfile=metricfile,
                reporting_frequency=metricfile.definition.reporting_frequency,
                too_many_rows=too_many_rows_filename == filename,
                null_data=False,
            )
            dataframe_dict.update(new_dataframe_dict)

            # If the metric is for the supervision system add a system column
            if system == schema.System.SUPERVISION:
                if "value" in dataframe_dict:
                    # There will be no value column if the sheet being generated
                    # has a missing_metric error.
                    dataframe_dict["system"] = ["all"] * len(dataframe_dict["value"])

                if metric_key_to_subsystems is not None:
                    subsystems = metric_key_to_subsystems.get(
                        metricfile.definition.key, []
                    )
                    if len(subsystems) > 1:
                        temp_dataframe_dict = {
                            key: [val for val in values for _ in (0, len(subsystems))]
                            for key, values in dataframe_dict.items()
                        }

                        temp_dataframe_dict["system"] = [
                            subsystems[x % len(subsystems)].value
                            for x in range(0, len(temp_dataframe_dict["value"]))
                        ]
                        dataframe_dict.update(temp_dataframe_dict)

    df = pd.DataFrame(dataframe_dict)
    df.to_csv(file_name)

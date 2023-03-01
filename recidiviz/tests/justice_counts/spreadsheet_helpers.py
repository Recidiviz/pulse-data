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
from typing import Any, Dict, List, Optional, Set

import pandas as pd

from recidiviz.justice_counts.metricfiles.metricfile_registry import (
    SYSTEM_TO_FILENAME_TO_METRICFILE,
)
from recidiviz.persistence.database.schema.justice_counts import schema

TEST_EXCEL_FILE = os.path.join(
    os.path.dirname(__file__),
    "bulk_upload/bulk_upload_fixtures/bulk_upload_test.xlsx",
)


def _create_dataframe_dict(
    reporting_frequency: schema.ReportingFrequency,
    invalid_month: bool = False,
    vary_values: bool = False,
) -> Dict[str, Any]:
    year_col = [2021, 2022, 2023]
    value_col = [12, 45, 30] if vary_values else [10, 20, 30]

    if reporting_frequency == schema.ReportingFrequency.ANNUAL:
        return {"year": year_col, "value": value_col}

    # If the metric is reported monthly, add a month column for
    # January and February of each year.
    dataframe_dict = {
        "month": ["January", "February"] * len(value_col),
        "year": [val for val in year_col for _ in (0, 1)],
        "value": [val for val in value_col for _ in (0, 1)],
    }

    if invalid_month:
        dataframe_dict["month"][0] = "Marchuary"  # type: ignore[index]

    return dataframe_dict


def create_excel_file(
    system: schema.System,
    metric_key_to_subsystems: Optional[Dict[str, List[schema.System]]] = None,
    invalid_month_sheetname: Optional[str] = None,
    add_invalid_sheetname: Optional[bool] = False,
    sheetnames_to_skip: Optional[Set[str]] = None,
    sheetnames_to_vary_values: Optional[Set[str]] = None,
) -> None:
    """Populates bulk_upload_test.xlsx with fake data to test functions that ingest spreadsheets"""
    filename_to_metricfile = SYSTEM_TO_FILENAME_TO_METRICFILE[system.value]
    with pd.ExcelWriter(  # pylint: disable=abstract-class-instantiated
        TEST_EXCEL_FILE
    ) as writer:
        for filename, metricfile in filename_to_metricfile.items():
            if sheetnames_to_skip is not None and filename in sheetnames_to_skip:
                continue
            # For every metric, add a value for 2021, 2022, and 2023
            dataframe_dict = _create_dataframe_dict(
                reporting_frequency=metricfile.definition.reporting_frequency,
                invalid_month=filename == invalid_month_sheetname,
                vary_values=sheetnames_to_vary_values is not None
                and filename in sheetnames_to_vary_values,
            )

            if metricfile.disaggregation_column_name is not None:
                # If the metric is has a breakdown, add a column for the breakdown
                dimension_list = list(metricfile.disaggregation)  # type: ignore[call-overload, arg-type]
                dataframe_dict[metricfile.disaggregation_column_name] = [
                    dimension_list[x % len(dimension_list)].value
                    for x in range(0, len(dataframe_dict["value"]))
                ]

            # If the metric is for the supervision system, add a system column.
            if system == schema.System.SUPERVISION:
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
        if add_invalid_sheetname:
            df = pd.DataFrame({})
            df.to_excel(writer, sheet_name="gender")

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
"""Functionality for bulk upload of a spreadsheet into the Justice Counts database."""

import datetime
from collections import defaultdict
from itertools import groupby
from typing import Any, Dict, List, Optional, Set, Tuple

from sqlalchemy.orm import Session

from recidiviz.common.text_analysis import TextAnalyzer
from recidiviz.justice_counts.bulk_upload.bulk_upload_helpers import get_column_value
from recidiviz.justice_counts.bulk_upload.time_range_uploader import TimeRangeUploader
from recidiviz.justice_counts.datapoint import DatapointUniqueKey
from recidiviz.justice_counts.exceptions import (
    BulkUploadMessageType,
    JusticeCountsBulkUploadException,
)
from recidiviz.justice_counts.metricfile import MetricFile
from recidiviz.justice_counts.metricfiles.metricfile_registry import (
    SYSTEM_TO_FILENAME_TO_METRICFILE,
    get_metricfile_by_sheet_name,
)
from recidiviz.justice_counts.metrics.metric_definition import MetricDefinition
from recidiviz.justice_counts.report import ReportInterface
from recidiviz.justice_counts.types import DatapointJson
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.schema.justice_counts.schema import (
    ReportingFrequency,
)


class SpreadsheetUploader:
    """Functionality for bulk upload of a spreadsheet into the Justice Counts database."""

    def __init__(
        self,
        text_analyzer: TextAnalyzer,
        system: schema.System,
        agency_id: int,
        metric_key_to_agency_datapoints: Dict[str, List[schema.Datapoint]],
        sheet_name: str,
        column_names: List[str],
        reports_by_time_range: Dict[tuple[datetime.date, datetime.date], Any],
        existing_datapoints_dict: Dict[DatapointUniqueKey, schema.Datapoint],
        metric_key_to_timerange_to_total_value: Dict[
            str, Dict[Tuple[datetime.date, datetime.date], Optional[int]]
        ],
        user_account: Optional[schema.UserAccount] = None,
    ) -> None:
        self.text_analyzer = text_analyzer
        self.system = system
        self.agency_id = agency_id
        self.user_account = user_account
        self.metric_key_to_agency_datapoints = metric_key_to_agency_datapoints
        self.sheet_name = sheet_name
        self.column_names = column_names
        self.reports_by_time_range = reports_by_time_range
        self.existing_datapoints_dict = existing_datapoints_dict
        self.metric_key_to_timerange_to_total_value = (
            metric_key_to_timerange_to_total_value
        )

    def upload_sheet(
        self,
        session: Session,
        rows: List[Dict[str, Any]],
        invalid_sheet_names: List[str],
        metric_key_to_datapoint_jsons: Dict[str, List[DatapointJson]],
        metric_key_to_errors: Dict[
            Optional[str], List[JusticeCountsBulkUploadException]
        ],
    ) -> Tuple[
        Dict[str, List[DatapointJson]],
        Dict[Optional[str], List[JusticeCountsBulkUploadException]],
    ]:
        """Generally, a file will only contain metrics for one system. In the case
        of supervision, the file could contain metrics for supervision, parole, or
        probation. This is indicated by the `system` column. In this case, we break
        up the rows by system, and then ingest one system at a time."""
        system_to_rows = self._get_system_to_rows(
            system=self.system,
            rows=rows,
            metric_key_to_errors=metric_key_to_errors,
        )
        for current_system, current_rows in system_to_rows.items():

            # Redefine this here to properly handle sheets that contain
            # rows for multiple systems (e.g. a Supervision sheet can
            # contain rows for Parole and Probation)
            sheet_name_to_metricfile = SYSTEM_TO_FILENAME_TO_METRICFILE[
                current_system.value
            ]

            # Based on the system and the name of the CSV file, determine which
            # Justice Counts metric this file contains data for
            metricfile = get_metricfile_by_sheet_name(
                sheet_name=self.sheet_name, system=current_system
            )
            if not metricfile:
                invalid_sheet_names.append(self.sheet_name)
                return metric_key_to_datapoint_jsons, metric_key_to_errors

            existing_datapoint_json_list = metric_key_to_datapoint_jsons[
                metricfile.definition.key
            ]

            try:
                new_datapoint_json_list = self._upload_rows_for_metricfile(
                    session=session,
                    rows=current_rows,
                    metricfile=metricfile,
                    metric_key_to_errors=metric_key_to_errors,
                )
            except Exception as e:
                new_datapoint_json_list = []
                curr_metricfile = sheet_name_to_metricfile[self.sheet_name]
                metric_key_to_errors[curr_metricfile.definition.key].append(
                    self._handle_error(
                        e=e,
                        sheet_name=self.sheet_name,
                    )
                )

            metric_key_to_datapoint_jsons[metricfile.definition.key] = (
                existing_datapoint_json_list + new_datapoint_json_list
            )

        return metric_key_to_datapoint_jsons, metric_key_to_errors

    def _upload_rows_for_metricfile(
        self,
        session: Session,
        rows: List[Dict[str, Any]],
        metricfile: MetricFile,
        metric_key_to_errors: Dict[
            Optional[str], List[JusticeCountsBulkUploadException]
        ],
    ) -> List[DatapointJson]:
        """Takes as input a set of rows (originating from a CSV or Excel spreadsheet tab)
        in the format of a list of dictionaries, i.e. [{"column_name": <column_value>} ... ].
        The rows should be formatted according to the technical specification, and contain
        data for a particular metric across multiple time periods.
        Uploads this data into the Justice Counts database by breaking it up into Report objects,
        and either updating existing reports or creating new ones.
        A simplified version of the expected format:
        year | month | value | offense_type
        ===================================
        2021 | 1     | 100   | All
        2021 | 1     | 50    | Felony
        2021 | 2     | 110   | All
        This data would be used to either update or create two reports, one for January
        2021 and one for February 2021.
        The filename is assumed to be of the format "metric_name.csv", where metric_name
        corresponds to one of the MetricFile objects in bulk_upload_helpers.py.
        """
        metric_definition = metricfile.definition
        (
            reporting_frequency,
            custom_starting_month,
        ) = metric_definition.get_reporting_frequency_to_use(
            agency_datapoints=self.metric_key_to_agency_datapoints.get(
                metric_definition.key, []
            ),
        )
        # Step 1: Warn if there are unexpected columns in the file
        # actual_columns is a set of all of the column names that have been uploaded by the user
        # we are filtering out 'Unnamed: 0' because this is the column name of the index column
        # the index column is produced when the excel file is converted to a pandas df
        actual_columns = {
            col.lower() for col in self.column_names if col != "Unnamed: 0"
        }
        metric_key_to_errors = self._check_expected_columns(
            metricfile=metricfile,
            actual_columns=actual_columns,
            metric_key_to_errors=metric_key_to_errors,
            metric_definition=metric_definition,
            reporting_frequency=reporting_frequency,
        )

        # Step 2: Group the rows in this file by time range.
        (rows_by_time_range, time_range_to_year_month,) = self._get_rows_by_time_range(
            rows=rows,
            reporting_frequency=reporting_frequency,
            custom_starting_month=custom_starting_month,
        )
        # Step 3: For each time range represented in the file, convert the
        # reported data into a MetricInterface object. If a report already
        # exists for this time range, update it with the MetricInterface.
        # Else, create a new report and add the MetricInterface.
        datapoint_jsons_list = []
        for time_range, rows_for_this_time_range in rows_by_time_range.items():

            try:
                time_range_uploader = TimeRangeUploader(
                    time_range=time_range,
                    agency_id=self.agency_id,
                    rows_for_this_time_range=rows_for_this_time_range,
                    user_account=self.user_account,
                    # TODO(#15499) Infer aggregate value only if total sheet was not provided.
                    existing_datapoints_dict=self.existing_datapoints_dict,
                    text_analyzer=self.text_analyzer,
                    metricfile=metricfile,
                    metric_key_to_timerange_to_total_value=self.metric_key_to_timerange_to_total_value,
                )
                existing_report = self.reports_by_time_range.get(time_range)
                (
                    report,
                    datapoint_json_list_for_time_range,
                ) = time_range_uploader.upload_time_range(
                    session=session,
                    time_range_to_year_month=time_range_to_year_month,
                    existing_report=existing_report,
                    reporting_frequency=reporting_frequency,
                )  # TODO(#15499) Infer aggregate value only if total sheet was not provided
                self.reports_by_time_range[time_range] = [report]
                datapoint_jsons_list += datapoint_json_list_for_time_range
            except Exception as e:
                metric_key_to_errors[metricfile.definition.key].append(
                    self._handle_error(
                        e=e,
                        sheet_name=metricfile.canonical_filename,
                    )
                )

        return datapoint_jsons_list

    def _check_expected_columns(
        self,
        metricfile: MetricFile,
        actual_columns: Set[str],
        metric_key_to_errors: Dict[
            Optional[str], List[JusticeCountsBulkUploadException]
        ],
        metric_definition: MetricDefinition,
        reporting_frequency: ReportingFrequency,
    ) -> Dict[Optional[str], List[JusticeCountsBulkUploadException]]:
        """This function throws Missing Column errors if a given sheet is missing required columns.
        Additionally, this function adds Unexpected Columns warnings to the metric_key_to_errors
        dictionary if there are unexpected column names for a given metric in a given sheet.
        """
        # First, handle missing columns
        if "value" not in actual_columns:
            description = (
                f'We expected to see a column named "value". '
                f"Only the following columns were found in the sheet: "
                f"{', '.join(actual_columns)}."
            )
            raise JusticeCountsBulkUploadException(
                title="Missing Value Column",
                description=description,
                message_type=BulkUploadMessageType.ERROR,
            )
        if "year" not in actual_columns:
            description = (
                f'We expected to see a column named "year". '
                f"Only the following columns were found in the sheet: "
                f"{', '.join(actual_columns)}."
            )
            raise JusticeCountsBulkUploadException(
                title="Missing Year Column",
                description=description,
                message_type=BulkUploadMessageType.ERROR,
            )
        if (
            metric_definition.system.value == "SUPERVISION"
            and "system" not in actual_columns
        ):
            description = (
                f'We expected to see a column named "system". '
                f"Only the following columns were found in the sheet: "
                f"{', '.join(actual_columns)}."
            )
            raise JusticeCountsBulkUploadException(
                title="Missing System Column",
                description=description,
                message_type=BulkUploadMessageType.ERROR,
            )
        if (
            metricfile.disaggregation_column_name is not None
            and metricfile.disaggregation_column_name not in actual_columns
        ):
            description = (
                f'We expected to see a column named "{metricfile.disaggregation_column_name}". '
                f"Only the following columns were found in the sheet: "
                f"{', '.join(actual_columns)}."
            )
            raise JusticeCountsBulkUploadException(
                title="Missing Breakdown Column",
                description=description,
                message_type=BulkUploadMessageType.ERROR,
            )
        if reporting_frequency.value == "MONTHLY" and "month" not in actual_columns:
            warning_title = "Missing Month Column"
            warning_description = (
                f"Your uploaded data has been saved. "
                f"The {metric_definition.display_name} metric is configured to be reported monthly, however the {metricfile.canonical_filename} sheet does not contain a month column. "
                f"To update the reporting frequency of this metric, please visit the Metric Configuration page."
            )
            missing_column_warning = JusticeCountsBulkUploadException(
                title=warning_title,
                message_type=BulkUploadMessageType.WARNING,
                description=warning_description,
            )
            metric_key_to_errors[metric_definition.key].append(missing_column_warning)

        # Next, handle unexpected (extra) columns
        expected_columns = {"value", "year"}
        if reporting_frequency.value == "MONTHLY":
            expected_columns.add("month")
        if metricfile.disaggregation_column_name is not None:
            expected_columns.add(
                metricfile.disaggregation_column_name  # type: ignore[arg-type]
            )
        if metric_definition.system.value == "SUPERVISION":
            expected_columns.add("system")
        unexpected_columns = actual_columns.difference(expected_columns)
        for unexpected_col in unexpected_columns:
            if unexpected_col == "month":
                warning_title = "Unexpected Month Column"
                warning_description = (
                    f"Your uploaded data has been saved. "
                    f"The {metric_definition.display_name} metric is configured to be reported annually, however the {metricfile.canonical_filename} sheet contains a month column. "
                    f"To update the reporting frequency of this metric, please visit the Metric Configuration page."
                )
            elif unexpected_col == "system":
                warning_title = "Unexpected System Column"
                warning_description = (
                    f"The {metricfile.canonical_filename} sheet contained the following unexpected column: system. "
                    f"The {metric_definition.system.value} system does not have subsystems and does not require a system column."
                )
            elif (
                "_type" in unexpected_col
                and metricfile.disaggregation_column_name is None
            ):
                warning_title = "Unexpected Breakdown Column"
                warning_description = f"Breakdown data ({unexpected_col} column) was provided in the {metricfile.canonical_filename} sheet, but this sheet should only contain aggregate data."
            else:
                warning_title = "Unexpected Column"
                warning_description = f"The {metricfile.canonical_filename} sheet contained the following unexpected column: {unexpected_col}. The {unexpected_col} column is not aligned with the Technical Implementation Guides."
            unexpected_column_warning = JusticeCountsBulkUploadException(
                title=warning_title,
                message_type=BulkUploadMessageType.WARNING,
                description=warning_description,
            )
            metric_key_to_errors[metric_definition.key].append(
                unexpected_column_warning
            )
        return metric_key_to_errors

    def _get_rows_by_time_range(
        self,
        rows: List[Dict[str, Any]],
        reporting_frequency: ReportingFrequency,
        custom_starting_month: Optional[int],
    ) -> Tuple[
        Dict[Tuple[datetime.date, datetime.date], List[Dict[str, Any]]],
        Dict[Tuple[datetime.date, datetime.date], Tuple[int, int]],
    ]:
        """Given the rows from a particular sheet, this method returns the rows
        organized by time range and by year and month."""
        rows_by_time_range = defaultdict(list)
        time_range_to_year_month = {}
        for row in rows:
            # remove whitespace from column headers
            row = {k.strip(): v for k, v in row.items() if k is not None}
            year = get_column_value(
                row=row,
                column_name="year",
                column_type=int,
                analyzer=self.text_analyzer,
            )
            if (
                reporting_frequency == ReportingFrequency.MONTHLY
                and row.get("month") is None
            ):
                # We will be in this case if annual data is provided for monthly metrics
                # warning will be added in _check_expected_columns()
                month = (
                    custom_starting_month or 1
                )  # if no custom starting month specified, assume calendar year
            elif reporting_frequency == ReportingFrequency.MONTHLY:
                month = get_column_value(
                    row=row,
                    column_name="month",
                    column_type=int,
                    analyzer=self.text_analyzer,
                )
            elif (
                reporting_frequency == ReportingFrequency.ANNUAL
                and row.get("month") is not None
            ):
                # We will be in this case if monthly data is provided for annual metrics
                # warning will be added in _check_expected_columns()
                month = get_column_value(
                    analyzer=self.text_analyzer,
                    row=row,
                    column_name="month",
                    column_type=int,
                )
            elif reporting_frequency == ReportingFrequency.ANNUAL:
                month = (
                    custom_starting_month or 1
                )  # if no custom starting month specified, assume calendar year

            date_range_start, date_range_end = ReportInterface.get_date_range(
                year=year, month=month, frequency=reporting_frequency.value
            )
            time_range_to_year_month[(date_range_start, date_range_end)] = (year, month)
            rows_by_time_range[(date_range_start, date_range_end)].append(row)
        return rows_by_time_range, time_range_to_year_month

    def _get_system_to_rows(
        self,
        system: schema.System,
        rows: List[Dict[str, Any]],
        metric_key_to_errors: Dict[
            Optional[str], List[JusticeCountsBulkUploadException]
        ],
    ) -> Dict[schema.System, List[Dict[str, Any]]]:
        """Groups the rows in the file by the value of the `system` column.
        Returns a dictionary mapping each system to its list of rows."""
        system_to_rows = {}
        if system == schema.System.SUPERVISION:
            system_value_to_rows = {
                k: list(v)
                for k, v in groupby(
                    sorted(rows, key=lambda x: x.get("system", "both")),
                    lambda x: x.get("system", "both"),
                )
            }
            normalized_system_value_to_system = {
                "supervision": schema.System.SUPERVISION,
                "all": schema.System.SUPERVISION,
                "parole": schema.System.PAROLE,
                "probation": schema.System.PROBATION,
                "other supervision": schema.System.OTHER_SUPERVISION,
                "pretrial supervision": schema.System.PRETRIAL_SUPERVISION,
            }
            for system_value, system_rows in system_value_to_rows.items():
                normalized_system_value = (
                    system_value.lower().strip().replace("-", " ").replace("_", " ")
                )
                if normalized_system_value not in normalized_system_value_to_system:
                    metric_key_to_errors[None].append(
                        JusticeCountsBulkUploadException(
                            title="System Not Recognized",
                            description=(
                                f'"{system_value}" is not a valid value for the System column. '
                                f"The valid values for this column are {', '.join(v.value for v in normalized_system_value_to_system.values())}."
                            ),
                            message_type=BulkUploadMessageType.ERROR,
                            sheet_name=self.sheet_name,
                        )
                    )
                    continue
                mapped_system = normalized_system_value_to_system[
                    normalized_system_value
                ]
                system_to_rows[mapped_system] = system_rows
        else:
            system_to_rows[system] = rows
        return system_to_rows

    def _handle_error(
        self, e: Exception, sheet_name: str
    ) -> JusticeCountsBulkUploadException:
        if not isinstance(e, JusticeCountsBulkUploadException):
            # If an error is not a JusticeCountsBulkUploadException, wrap it
            # in a JusticeCountsBulkUploadException and label it unexpected.
            return JusticeCountsBulkUploadException(
                title="Unexpected Error",
                message_type=BulkUploadMessageType.ERROR,
                sheet_name=sheet_name,
                description=e.message  # type: ignore[attr-defined]
                if hasattr(e, "message")
                else "",
            )
        e.sheet_name = sheet_name
        return e

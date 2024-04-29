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
import math
from collections import defaultdict
from itertools import groupby
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd
from sqlalchemy.orm import Session

from recidiviz.common.text_analysis import TextAnalyzer
from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.justice_counts.bulk_upload.bulk_upload_helpers import get_column_value
from recidiviz.justice_counts.bulk_upload.time_range_uploader import TimeRangeUploader
from recidiviz.justice_counts.datapoint import DatapointUniqueKey
from recidiviz.justice_counts.exceptions import (
    BulkUploadMessageType,
    JusticeCountsBulkUploadException,
)
from recidiviz.justice_counts.metric_setting import MetricSettingInterface
from recidiviz.justice_counts.metricfile import MetricFile
from recidiviz.justice_counts.metricfiles.metricfile_registry import (
    SYSTEM_TO_FILENAME_TO_METRICFILE,
    get_metricfile_by_sheet_name,
)
from recidiviz.justice_counts.metrics.metric_definition import MetricDefinition
from recidiviz.justice_counts.metrics.metric_interface import MetricInterface
from recidiviz.justice_counts.report import ReportInterface
from recidiviz.justice_counts.types import DatapointJson
from recidiviz.justice_counts.utils.constants import (
    INVALID_CHILD_AGENCY,
    UNEXPECTED_ERROR,
    UploadMethod,
)
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
        agency: schema.Agency,
        metric_key_to_metric_interface: Dict[str, MetricInterface],
        sheet_name: str,
        agency_id_to_time_range_to_reports: Dict[
            int, Dict[Tuple[Any, Any], List[schema.Report]]
        ],
        existing_datapoints_dict: Dict[DatapointUniqueKey, schema.Datapoint],
        agency_name_to_metric_key_to_timerange_to_total_value: Dict[
            str,
            Dict[str, Dict[Tuple[datetime.date, datetime.date], Optional[int]]],
        ],
        child_agency_name_to_agency: Dict[str, schema.Agency],
        user_account: Optional[schema.UserAccount] = None,
    ) -> None:
        self.text_analyzer = text_analyzer
        self.system = system
        self.agency = agency
        self.user_account = user_account
        self.metric_key_to_metric_interface = metric_key_to_metric_interface
        self.sheet_name = sheet_name
        self.agency_id_to_time_range_to_reports = agency_id_to_time_range_to_reports
        self.existing_datapoints_dict = existing_datapoints_dict
        self.agency_name_to_metric_key_to_timerange_to_total_value = (
            agency_name_to_metric_key_to_timerange_to_total_value
        )
        self.child_agency_name_to_agency = child_agency_name_to_agency

    def upload_sheet(
        self,
        session: Session,
        inserts: List[schema.Datapoint],
        updates: List[schema.Datapoint],
        histories: List[schema.DatapointHistory],
        rows: List[Dict[str, Any]],
        invalid_sheet_names: List[str],
        metric_key_to_datapoint_jsons: Dict[str, List[DatapointJson]],
        metric_key_to_errors: Dict[
            Optional[str], List[JusticeCountsBulkUploadException]
        ],
        uploaded_reports: Set[schema.Report],
        upload_method: UploadMethod,
    ) -> Tuple[
        Dict[str, List[DatapointJson]],
        Dict[Optional[str], List[JusticeCountsBulkUploadException]],
    ]:
        """Uploads the rows of a sheet by type. Generally, a file will only
        contain metrics for one system. In the case of supervision,
        the sheet could contain metrics for supervision, parole, or probation.
        This is indicated by the `system` column. In this case, we break up
        the rows by system, and then ingest one system at a time."""

        if (
            len(self.child_agency_name_to_agency) > 0
            and self.system != schema.System.SUPERAGENCY
        ):
            agency_name_to_rows = self._get_agency_name_to_rows(
                rows=rows,
                metric_key_to_errors=metric_key_to_errors,
            )
            self._upload_super_agency_sheet(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
                agency_name_to_rows=agency_name_to_rows,
                invalid_sheet_names=invalid_sheet_names,
                metric_key_to_datapoint_jsons=metric_key_to_datapoint_jsons,
                metric_key_to_errors=metric_key_to_errors,
                uploaded_reports=uploaded_reports,
                upload_method=upload_method,
            )
        elif self.system == schema.System.SUPERVISION:
            system_to_rows = self._get_system_to_rows(
                rows=rows,
                metric_key_to_errors=metric_key_to_errors,
            )
            self._upload_supervision_sheet(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
                system_to_rows=system_to_rows,
                invalid_sheet_names=invalid_sheet_names,
                metric_key_to_datapoint_jsons=metric_key_to_datapoint_jsons,
                metric_key_to_errors=metric_key_to_errors,
                uploaded_reports=uploaded_reports,
                upload_method=upload_method,
            )
        else:
            self._upload_rows(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
                rows=rows,
                system=self.system,
                invalid_sheet_names=invalid_sheet_names,
                metric_key_to_datapoint_jsons=metric_key_to_datapoint_jsons,
                metric_key_to_errors=metric_key_to_errors,
                uploaded_reports=uploaded_reports,
                upload_method=upload_method,
            )
        return metric_key_to_datapoint_jsons, metric_key_to_errors

    def _upload_super_agency_sheet(
        self,
        session: Session,
        inserts: List[schema.Datapoint],
        updates: List[schema.Datapoint],
        histories: List[schema.DatapointHistory],
        agency_name_to_rows: Dict[str, List[Dict[str, Any]]],
        invalid_sheet_names: List[str],
        metric_key_to_datapoint_jsons: Dict[str, List[DatapointJson]],
        metric_key_to_errors: Dict[
            Optional[str], List[JusticeCountsBulkUploadException]
        ],
        uploaded_reports: Set[schema.Report],
        upload_method: UploadMethod,
    ) -> None:
        """Uploads agency rows one agency at a time. If the agency is a
        supervision agency, the rows from each agency will be uploaded one system
        at a time."""

        child_agencies = [
            self.child_agency_name_to_agency[child_agency_name]
            for child_agency_name in agency_name_to_rows.keys()
            if child_agency_name != INVALID_CHILD_AGENCY
            # When the agency column does not have a value or has the name of a
            # child agency that does not exist, the child_agency_name is saved as "".
            # We do not want to ingest data for those columns so we  skip them.
            # An "Agency Name Not Recognized" error has already been added to the
            # metric_key_to_errors dictionary in _get_agency_name_to_rows.
        ]
        # get agency_datapoints specific to child agency
        child_agency_id_to_metric_key_to_metric_interface = (
            MetricSettingInterface.get_agency_id_to_metric_key_to_metric_interface(
                session=session, agencies=child_agencies
            )
        )

        for curr_agency_name, current_rows in agency_name_to_rows.items():
            # For child agencies, update current_rows to not include columns with NaN values
            # since different child agencies can have different metric configs. For example,
            # Child Agency 1 can report data monthly while Child Agency 2 can report data
            # annually. So the same sheet may have a 'month' column filled out for some
            # child agencies, and not for others.

            if curr_agency_name == INVALID_CHILD_AGENCY:
                continue

            current_rows = (
                pd.DataFrame(current_rows).dropna(axis=1).to_dict(orient="records")
            )
            if self.system == schema.System.SUPERVISION:
                system_to_rows = self._get_system_to_rows(
                    rows=current_rows,
                    metric_key_to_errors=metric_key_to_errors,
                )
                self._upload_supervision_sheet(
                    session=session,
                    inserts=inserts,
                    updates=updates,
                    histories=histories,
                    system_to_rows=system_to_rows,
                    invalid_sheet_names=invalid_sheet_names,
                    metric_key_to_datapoint_jsons=metric_key_to_datapoint_jsons,
                    metric_key_to_errors=metric_key_to_errors,
                    child_agency_name=curr_agency_name,
                    uploaded_reports=uploaded_reports,
                    upload_method=upload_method,
                    child_agency_id_to_metric_key_to_metric_interface=child_agency_id_to_metric_key_to_metric_interface,
                )
            else:
                self._upload_rows(
                    session=session,
                    inserts=inserts,
                    updates=updates,
                    histories=histories,
                    rows=current_rows,
                    system=self.system,
                    invalid_sheet_names=invalid_sheet_names,
                    metric_key_to_datapoint_jsons=metric_key_to_datapoint_jsons,
                    metric_key_to_errors=metric_key_to_errors,
                    child_agency_name=curr_agency_name,
                    uploaded_reports=uploaded_reports,
                    upload_method=upload_method,
                    child_agency_id_to_metric_key_to_metric_interface=child_agency_id_to_metric_key_to_metric_interface,
                )

    def _upload_supervision_sheet(
        self,
        session: Session,
        inserts: List[schema.Datapoint],
        updates: List[schema.Datapoint],
        histories: List[schema.DatapointHistory],
        system_to_rows: Dict[schema.System, List[Dict[str, Any]]],
        invalid_sheet_names: List[str],
        metric_key_to_datapoint_jsons: Dict[str, List[DatapointJson]],
        metric_key_to_errors: Dict[
            Optional[str], List[JusticeCountsBulkUploadException]
        ],
        uploaded_reports: Set[schema.Report],
        upload_method: UploadMethod,
        child_agency_name: Optional[str] = None,
        child_agency_id_to_metric_key_to_metric_interface: Optional[
            Dict[int, Dict[str, MetricInterface]]
        ] = None,
    ) -> None:
        """Uploads supervision rows one system at a time."""
        for current_system, current_rows in system_to_rows.items():
            self._upload_rows(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
                rows=current_rows,
                system=current_system,
                invalid_sheet_names=invalid_sheet_names,
                metric_key_to_datapoint_jsons=metric_key_to_datapoint_jsons,
                metric_key_to_errors=metric_key_to_errors,
                uploaded_reports=uploaded_reports,
                child_agency_name=child_agency_name,
                upload_method=upload_method,
                child_agency_id_to_metric_key_to_metric_interface=child_agency_id_to_metric_key_to_metric_interface,
            )

    def _upload_rows(
        self,
        session: Session,
        inserts: List[schema.Datapoint],
        updates: List[schema.Datapoint],
        histories: List[schema.DatapointHistory],
        rows: List[Dict[str, Any]],
        system: schema.System,
        invalid_sheet_names: List[str],
        metric_key_to_datapoint_jsons: Dict[str, List[DatapointJson]],
        metric_key_to_errors: Dict[
            Optional[str], List[JusticeCountsBulkUploadException]
        ],
        uploaded_reports: Set[schema.Report],
        upload_method: UploadMethod,
        child_agency_name: Optional[str] = None,
        child_agency_id_to_metric_key_to_metric_interface: Optional[
            Dict[int, Dict[str, MetricInterface]]
        ] = None,
    ) -> None:
        """Uploads rows for a given system and child agency. If child_agency_id
        if None, then the rows are uploaded for the agency specified as self.agency."""
        # Redefine this here to properly handle sheets that contain
        # rows for multiple systems (e.g. a Supervision sheet can
        # contain rows for Parole and Probation)
        sheet_name_to_metricfile = SYSTEM_TO_FILENAME_TO_METRICFILE[system.value]

        # Based on the system and the name of the CSV file, determine which
        # Justice Counts metric this file contains data for
        metricfile = get_metricfile_by_sheet_name(
            sheet_name=self.sheet_name, system=system
        )
        if not metricfile:
            if self.sheet_name not in invalid_sheet_names:
                invalid_sheet_names.append(self.sheet_name)
            return

        existing_datapoint_json_list = metric_key_to_datapoint_jsons[
            metricfile.definition.key
        ]
        try:
            new_datapoint_json_list = self._upload_rows_for_metricfile(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
                rows=rows,
                metricfile=metricfile,
                metric_key_to_errors=metric_key_to_errors,
                uploaded_reports=uploaded_reports,
                child_agency_name=child_agency_name,
                upload_method=upload_method,
                child_agency_id_to_metric_key_to_metric_interface=child_agency_id_to_metric_key_to_metric_interface,
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

    def _upload_rows_for_metricfile(
        self,
        session: Session,
        inserts: List[schema.Datapoint],
        updates: List[schema.Datapoint],
        histories: List[schema.DatapointHistory],
        rows: List[Dict[str, Any]],
        metricfile: MetricFile,
        metric_key_to_errors: Dict[
            Optional[str], List[JusticeCountsBulkUploadException]
        ],
        uploaded_reports: Set[schema.Report],
        upload_method: UploadMethod,
        child_agency_name: Optional[str] = None,
        child_agency_id_to_metric_key_to_metric_interface: Optional[
            Dict[int, Dict[str, MetricInterface]]
        ] = None,
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

        child_agency_metric_interface = None
        if (
            child_agency_name is not None
            and child_agency_id_to_metric_key_to_metric_interface is not None
        ):
            child_agency = self.child_agency_name_to_agency.get(child_agency_name)
            if child_agency is not None:
                child_agency_id = child_agency.id
                metric_key_to_child_agency_metric_interface: Dict[
                    str, MetricInterface
                ] = child_agency_id_to_metric_key_to_metric_interface.get(
                    child_agency_id, {}
                )
                child_agency_metric_interface = (
                    metric_key_to_child_agency_metric_interface.get(
                        metric_definition.key
                    )
                )

        metric_interface = (
            child_agency_metric_interface
            or self.metric_key_to_metric_interface.get(
                metric_definition.key, MetricInterface(key=metric_definition.key)
            )
        )
        (
            reporting_frequency,
            custom_starting_month,
        ) = metric_interface.get_reporting_frequency_to_use()

        # Step 1: Warn if there are unexpected columns in the file
        # actual_columns is a set of all of the column names that have been uploaded by the user
        # we are filtering out 'Unnamed: 0' because this is the column name of the index column
        # the index column is produced when the excel file is converted to a pandas df
        column_names = pd.DataFrame(rows).dropna(axis=1).columns
        actual_columns = {col for col in column_names if col != "unnamed: 0"}
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
            metric_key_to_errors=metric_key_to_errors,
            metric_key=metricfile.definition.key,
        )
        # Step 3: For each time range represented in the file, convert the
        # reported data into a MetricInterface object. If a report already
        # exists for this time range, update it with the MetricInterface.
        # Else, create a new report and add the MetricInterface.
        datapoint_jsons_list = []
        curr_agency = (
            self.agency
            if child_agency_name is None or len(child_agency_name) == 0
            else self.child_agency_name_to_agency[child_agency_name]
        )
        for time_range, rows_for_this_time_range in rows_by_time_range.items():
            try:
                time_range_uploader = TimeRangeUploader(
                    time_range=time_range,
                    agency=curr_agency,
                    rows_for_this_time_range=rows_for_this_time_range,
                    user_account=self.user_account,
                    existing_datapoints_dict=self.existing_datapoints_dict,
                    text_analyzer=self.text_analyzer,
                    metricfile=metricfile,
                    agency_name_to_metric_key_to_timerange_to_total_value=self.agency_name_to_metric_key_to_timerange_to_total_value,
                )
                existing_report = self.agency_id_to_time_range_to_reports.get(
                    curr_agency.id, {}
                ).get(time_range)
                if existing_report is not None and existing_report[0].id is not None:
                    uploaded_reports.add(existing_report[0])
                (
                    report,
                    datapoint_json_list_for_time_range,
                ) = time_range_uploader.upload_time_range(
                    session=session,
                    inserts=inserts,
                    updates=updates,
                    histories=histories,
                    time_range_to_year_month=time_range_to_year_month,
                    existing_report=existing_report,
                    metric_key_to_errors=metric_key_to_errors,
                    metric_key=metricfile.definition.key,
                    upload_method=upload_method,
                )
                self.agency_id_to_time_range_to_reports[curr_agency.id][time_range] = [
                    report
                ]
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
        # The "system" column is expected for supervision agencies that have supervision subsystems,
        # but not for a supervision agency that does not have supervision subsystems.
        if (
            AgencyInterface.does_supervision_agency_report_for_subsystems(
                agency=self.agency
            )
            and metric_definition.is_metric_for_supervision_or_subsystem
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

        # Expect a system column if the agency has supervision subsystems AND
        # we are uploading to a supervision system or subsystem. The "system" column
        # is not expected for an agency that is ONLY a supervision agency and has no subsystems?
        if (
            AgencyInterface.does_supervision_agency_report_for_subsystems(
                agency=self.agency
            )
            and metric_definition.is_metric_for_supervision_or_subsystem
        ):
            expected_columns.add("system")
        if (
            len(self.child_agency_name_to_agency) > 0
            and self.system != schema.System.SUPERAGENCY
        ):
            expected_columns.add("agency")
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
            elif unexpected_col == "agency":
                warning_title = "Unexpected Agency Column"
                warning_description = f"Agency data ({unexpected_col} column) was provided in the {metricfile.canonical_filename} sheet, but your agency does not have the permissions to upload data for any other agency."
            else:
                warning_title = "Unexpected Column"
                warning_description = f"The {metricfile.canonical_filename} sheet contained the following unexpected column: {unexpected_col}. The {unexpected_col} column is not aligned with the Technical Implementation Guides."
            unexpected_column_warning = JusticeCountsBulkUploadException(
                title=warning_title,
                message_type=(
                    BulkUploadMessageType.WARNING
                    if unexpected_col != "agency"
                    else BulkUploadMessageType.ERROR
                ),
                description=warning_description,
            )
            if unexpected_col == "agency":
                # Stop ingest if unexpected_col is "agency! If an agency has no child agencies,
                # it will skip the logic to ingest the rows one agency at a time.
                # Ingest needs to be stopped on the sheet so that we can avoid rows being
                # assigned incorrectly.
                raise unexpected_column_warning
            metric_key_to_errors[metric_definition.key].append(
                unexpected_column_warning
            )
        return metric_key_to_errors

    def _get_rows_by_time_range(
        self,
        rows: List[Dict[str, Any]],
        reporting_frequency: ReportingFrequency,
        custom_starting_month: Optional[int],
        metric_key_to_errors: Dict[
            Optional[str], List[JusticeCountsBulkUploadException]
        ],
        metric_key: str,
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
                metric_key_to_errors=metric_key_to_errors,
                metric_key=metric_key,
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
                assumed_frequency = ReportingFrequency.ANNUAL
            elif reporting_frequency == ReportingFrequency.MONTHLY:
                month = get_column_value(
                    row=row,
                    column_name="month",
                    column_type=int,
                    analyzer=self.text_analyzer,
                    metric_key_to_errors=metric_key_to_errors,
                    metric_key=metric_key,
                )
                assumed_frequency = ReportingFrequency.MONTHLY
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
                    metric_key_to_errors=metric_key_to_errors,
                    metric_key=metric_key,
                )
                assumed_frequency = ReportingFrequency.MONTHLY
            elif reporting_frequency == ReportingFrequency.ANNUAL:
                month = (
                    custom_starting_month or 1
                )  # if no custom starting month specified, assume calendar year
                assumed_frequency = ReportingFrequency.ANNUAL

            date_range_start, date_range_end = ReportInterface.get_date_range(
                year=year, month=month, frequency=assumed_frequency.value
            )
            time_range_to_year_month[(date_range_start, date_range_end)] = (year, month)
            rows_by_time_range[(date_range_start, date_range_end)].append(row)
        return rows_by_time_range, time_range_to_year_month

    def _get_agency_name_to_rows(
        self,
        rows: List[Dict[str, Any]],
        metric_key_to_errors: Dict[
            Optional[str], List[JusticeCountsBulkUploadException]
        ],
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Groups the rows in the file by the value of the `agency` column.
        Returns a dictionary mapping each agency to its list of rows."""
        agency_name_to_rows: Dict[str, List[Dict[str, Any]]] = {}

        def get_agency_name(row: Dict[str, Any]) -> str:
            agency_name = row.get("agency")
            system = row.get("system")
            metric_file = get_metricfile_by_sheet_name(
                sheet_name=self.sheet_name,
                system=system if system is not None else self.system,
            )
            if agency_name is None:
                actual_columns = {col for col in row.keys() if col != "unnamed: 0"}
                description = (
                    f'We expected to see a column named "agency". '
                    f"Only the following columns were found in the sheet: "
                    f"{', '.join(actual_columns)}."
                )
                raise JusticeCountsBulkUploadException(
                    title="Missing Agency Column",
                    description=description,
                    message_type=BulkUploadMessageType.ERROR,
                )
            if isinstance(agency_name, float) and math.isnan(agency_name):
                # When there is an agency column but there is a missing
                # agency value for the row, then the agency_name in the row is nan
                if metric_file is not None and "Missing Agency Data" not in {
                    e.title for e in metric_key_to_errors[metric_file.definition.key]
                }:
                    # Don't stop ingest if only a row is missing an agency name
                    metric_key_to_errors[metric_file.definition.key].append(
                        JusticeCountsBulkUploadException(
                            title="Missing Agency Data",
                            description="We expected to see an agency name provided in the agency column for all rows.",
                            message_type=BulkUploadMessageType.ERROR,
                        )
                    )
                return INVALID_CHILD_AGENCY
            normalized_agency_name = agency_name.strip().lower()
            if (
                normalized_agency_name not in self.child_agency_name_to_agency
                and normalized_agency_name != self.agency.name.lower()
            ):
                description = f"Failed to upload data for {agency_name}. Either this agency does not exist in our database, or your agency does not have permission to upload for this agency."
                if metric_file is not None and description not in {
                    e.description
                    for e in metric_key_to_errors[metric_file.definition.key]
                }:
                    # Don't stop ingest if the agency name is not recognized
                    metric_key_to_errors[metric_file.definition.key].append(
                        JusticeCountsBulkUploadException(
                            title="Agency Not Recognized",
                            description=description,
                            message_type=BulkUploadMessageType.ERROR,
                        )
                    )
                return INVALID_CHILD_AGENCY

            return normalized_agency_name

        agency_name_to_rows = {}
        try:
            # This try/except block is meant to catch errors thrown in the get_agency_name method.

            agency_name_to_rows = {
                k: list(v)
                for k, v in groupby(
                    sorted(
                        rows,
                        key=get_agency_name,
                    ),
                    get_agency_name,
                )
            }
        except Exception as e:
            metric_file = get_metricfile_by_sheet_name(
                sheet_name=self.sheet_name,
                system=self.system,
            )
            if metric_file:
                metric_key_to_errors[metric_file.definition.key].append(
                    self._handle_error(
                        e=e,
                        sheet_name=self.sheet_name,
                    )
                )

        for agency_name, agency_rows in agency_name_to_rows.items():
            if len(agency_name) > 0:
                # Agency Name will be "" if the row is missing an agency.
                # The rows associated with no agency name will not be ingested.
                agency_name_to_rows[agency_name] = agency_rows

        return agency_name_to_rows

    def _get_system_to_rows(
        self,
        rows: List[Dict[str, Any]],
        metric_key_to_errors: Dict[
            Optional[str], List[JusticeCountsBulkUploadException]
        ],
    ) -> Dict[schema.System, List[Dict[str, Any]]]:
        """Groups the rows in the file by the value of the `system` column.
        Returns a dictionary mapping each system to its list of rows."""
        system_to_rows = {}
        system_value_to_rows = {
            k: list(v)
            for k, v in groupby(
                sorted(rows, key=lambda x: x.get("system", "all")),
                lambda x: x.get("system", "all"),
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
            mapped_system = normalized_system_value_to_system[normalized_system_value]
            system_to_rows[mapped_system] = system_rows
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
                description=(
                    e.message  # type: ignore[attr-defined]
                    if hasattr(e, "message")
                    else UNEXPECTED_ERROR
                ),
            )
        e.sheet_name = sheet_name
        return e

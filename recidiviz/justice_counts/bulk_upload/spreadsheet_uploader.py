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
import logging
import math
from collections import defaultdict
from itertools import groupby
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd

from recidiviz.justice_counts.bulk_upload.bulk_upload_helpers import get_column_value
from recidiviz.justice_counts.bulk_upload.bulk_upload_metadata import BulkUploadMetadata
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
from recidiviz.justice_counts.metrics.metric_interface import MetricInterface
from recidiviz.justice_counts.report import ReportInterface
from recidiviz.justice_counts.types import DatapointJson
from recidiviz.justice_counts.utils.constants import (
    INVALID_CHILD_AGENCY,
    UNEXPECTED_ERROR,
)
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.schema.justice_counts.schema import (
    ReportingFrequency,
)


class SpreadsheetUploader:
    """Functionality for bulk upload of a spreadsheet into the Justice Counts database."""

    def __init__(
        self,
        metadata: BulkUploadMetadata,
        sheet_name: str,
        existing_datapoints_dict: Dict[DatapointUniqueKey, schema.Datapoint],
    ) -> None:
        self.metadata = metadata
        self.sheet_name = sheet_name
        self.existing_datapoints_dict = existing_datapoints_dict

    def upload_sheet(
        self,
        inserts: List[schema.Datapoint],
        updates: List[schema.Datapoint],
        histories: List[schema.DatapointHistory],
        rows: List[Dict[str, Any]],
        uploaded_reports: Set[schema.Report],
    ) -> None:
        """Uploads the rows of a sheet by type. Generally, a file will only
        contain metrics for one system. In the case of supervision,
        the sheet could contain metrics for supervision, parole, or probation.
        This is indicated by the `system` column. In this case, we break up
        the rows by system, and then ingest one system at a time."""
        if (
            len(self.metadata.child_agency_name_to_agency) > 0
            and self.metadata.system != schema.System.SUPERAGENCY
        ):
            agency_name_to_rows = self._get_agency_name_to_rows(
                rows=rows,
                metric_key_to_errors=self.metadata.metric_key_to_errors,
            )
            self._upload_super_agency_sheet(
                inserts=inserts,
                updates=updates,
                histories=histories,
                agency_name_to_rows=agency_name_to_rows,
                uploaded_reports=uploaded_reports,
            )
        elif self.metadata.system == schema.System.SUPERVISION:
            system_to_rows = self._get_system_to_rows(
                rows=rows,
            )
            self._upload_supervision_sheet(
                inserts=inserts,
                updates=updates,
                histories=histories,
                system_to_rows=system_to_rows,
                uploaded_reports=uploaded_reports,
            )
        else:
            self._upload_rows(
                inserts=inserts,
                updates=updates,
                histories=histories,
                rows=rows,
                system=self.metadata.system,
                uploaded_reports=uploaded_reports,
            )

    def _upload_super_agency_sheet(
        self,
        inserts: List[schema.Datapoint],
        updates: List[schema.Datapoint],
        histories: List[schema.DatapointHistory],
        agency_name_to_rows: Dict[str, List[Dict[str, Any]]],
        uploaded_reports: Set[schema.Report],
    ) -> None:
        """Uploads agency rows one agency at a time. If the agency is a
        supervision agency, the rows from each agency will be uploaded one system
        at a time."""

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
            if self.metadata.system == schema.System.SUPERVISION:
                system_to_rows = self._get_system_to_rows(
                    rows=current_rows,
                )
                self._upload_supervision_sheet(
                    inserts=inserts,
                    updates=updates,
                    histories=histories,
                    system_to_rows=system_to_rows,
                    child_agency_name=curr_agency_name,
                    uploaded_reports=uploaded_reports,
                )
            else:
                self._upload_rows(
                    inserts=inserts,
                    updates=updates,
                    histories=histories,
                    rows=current_rows,
                    system=self.metadata.system,
                    child_agency_name=curr_agency_name,
                    uploaded_reports=uploaded_reports,
                )

    def _upload_supervision_sheet(
        self,
        inserts: List[schema.Datapoint],
        updates: List[schema.Datapoint],
        histories: List[schema.DatapointHistory],
        system_to_rows: Dict[schema.System, List[Dict[str, Any]]],
        uploaded_reports: Set[schema.Report],
        child_agency_name: Optional[str] = None,
    ) -> None:
        """Uploads supervision rows one system at a time."""
        for current_system, current_rows in system_to_rows.items():
            self._upload_rows(
                inserts=inserts,
                updates=updates,
                histories=histories,
                rows=current_rows,
                system=current_system,
                uploaded_reports=uploaded_reports,
                child_agency_name=child_agency_name,
            )

    def _upload_rows(
        self,
        inserts: List[schema.Datapoint],
        updates: List[schema.Datapoint],
        histories: List[schema.DatapointHistory],
        rows: List[Dict[str, Any]],
        system: schema.System,
        uploaded_reports: Set[schema.Report],
        child_agency_name: Optional[str] = None,
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
            return

        existing_datapoint_json_list = self.metadata.metric_key_to_datapoint_jsons[
            metricfile.definition.key
        ]
        try:
            new_datapoint_json_list = self._upload_rows_for_metricfile(
                inserts=inserts,
                updates=updates,
                histories=histories,
                rows=rows,
                metricfile=metricfile,
                uploaded_reports=uploaded_reports,
                child_agency_name=child_agency_name,
            )
        except Exception as e:
            new_datapoint_json_list = []
            curr_metricfile = sheet_name_to_metricfile[self.sheet_name]
            self.metadata.metric_key_to_errors[curr_metricfile.definition.key].append(
                self._handle_error(
                    e=e,
                    sheet_name=self.sheet_name,
                )
            )

        self.metadata.metric_key_to_datapoint_jsons[metricfile.definition.key] = (
            existing_datapoint_json_list + new_datapoint_json_list
        )

    def _upload_rows_for_metricfile(
        self,
        inserts: List[schema.Datapoint],
        updates: List[schema.Datapoint],
        histories: List[schema.DatapointHistory],
        rows: List[Dict[str, Any]],
        metricfile: MetricFile,
        uploaded_reports: Set[schema.Report],
        child_agency_name: Optional[str] = None,
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
        if child_agency_name is not None:
            child_agency_id = self.metadata.child_agency_name_to_agency[
                child_agency_name
            ].id

            child_agency_metric_interface = (
                self.metadata.child_agency_id_to_metric_key_to_metric_interface[
                    child_agency_id
                ].get(metric_definition.key, MetricInterface(key=metric_definition.key))
            )

        metric_interface = (
            child_agency_metric_interface
            or self.metadata.metric_key_to_metric_interface.get(
                metric_definition.key, MetricInterface(key=metric_definition.key)
            )
        )

        (
            reporting_frequency,
            custom_starting_month,
        ) = metric_interface.get_reporting_frequency_to_use()

        # Step 1: Group the rows in this file by time range.
        (rows_by_time_range, time_range_to_year_month,) = self._get_rows_by_time_range(
            rows=rows,
            reporting_frequency=reporting_frequency,
            custom_starting_month=custom_starting_month,
            metric_key=metricfile.definition.key,
        )
        # Step 2: For each time range represented in the file, convert the
        # reported data into a MetricInterface object. If a report already
        # exists for this time range, update it with the MetricInterface.
        # Else, create a new report and add the MetricInterface.
        datapoint_jsons_list = []
        curr_agency = (
            self.metadata.agency
            if child_agency_name is None or len(child_agency_name) == 0
            else self.metadata.child_agency_name_to_agency[child_agency_name]
        )
        for time_range, rows_for_this_time_range in rows_by_time_range.items():
            try:
                time_range_uploader = TimeRangeUploader(
                    time_range=time_range,
                    agency=curr_agency,
                    metadata=self.metadata,
                    rows_for_this_time_range=rows_for_this_time_range,
                    existing_datapoints_dict=self.existing_datapoints_dict,
                    metricfile=metricfile,
                )
                existing_report = self.metadata.agency_id_to_time_range_to_reports.get(
                    curr_agency.id, {}
                ).get(time_range)
                if existing_report is not None and existing_report[0].id is not None:
                    uploaded_reports.add(existing_report[0])
                (
                    report,
                    datapoint_json_list_for_time_range,
                ) = time_range_uploader.upload_time_range(
                    session=self.metadata.session,
                    inserts=inserts,
                    updates=updates,
                    histories=histories,
                    time_range_to_year_month=time_range_to_year_month,
                    existing_report=existing_report,
                    metric_key=metricfile.definition.key,
                )
                self.metadata.agency_id_to_time_range_to_reports[curr_agency.id][
                    time_range
                ] = [report]
                datapoint_jsons_list += datapoint_json_list_for_time_range
            except Exception as e:
                self.metadata.metric_key_to_errors[metricfile.definition.key].append(
                    self._handle_error(
                        e=e,
                        sheet_name=metricfile.canonical_filename,
                    )
                )

        return datapoint_jsons_list

    def _get_rows_by_time_range(
        self,
        rows: List[Dict[str, Any]],
        reporting_frequency: ReportingFrequency,
        custom_starting_month: Optional[int],
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
                analyzer=self.metadata.text_analyzer,
                metric_key_to_errors=self.metadata.metric_key_to_errors,
                metric_key=metric_key,
            )
            if (
                reporting_frequency == ReportingFrequency.MONTHLY
                and row.get("month") is None
            ):
                # We will be in this case if annual data is provided for monthly metrics
                month = (
                    custom_starting_month or 1
                )  # if no custom starting month specified, assume calendar year
                assumed_frequency = ReportingFrequency.ANNUAL
            elif reporting_frequency == ReportingFrequency.MONTHLY:
                month = get_column_value(
                    row=row,
                    column_name="month",
                    column_type=int,
                    analyzer=self.metadata.text_analyzer,
                    metric_key_to_errors=self.metadata.metric_key_to_errors,
                    metric_key=metric_key,
                )
                assumed_frequency = ReportingFrequency.MONTHLY
            elif (
                reporting_frequency == ReportingFrequency.ANNUAL
                and row.get("month") is not None
            ):
                # We will be in this case if monthly data is provided for annual metrics
                month = get_column_value(
                    analyzer=self.metadata.text_analyzer,
                    row=row,
                    column_name="month",
                    column_type=int,
                    metric_key_to_errors=self.metadata.metric_key_to_errors,
                    metric_key=metric_key,
                )
                assumed_frequency = ReportingFrequency.MONTHLY
            elif reporting_frequency == ReportingFrequency.ANNUAL:
                month = (
                    custom_starting_month or 1
                )  # if no custom starting month specified, assume calendar year
                assumed_frequency = ReportingFrequency.ANNUAL
            else:
                raise JusticeCountsBulkUploadException(
                    title="Reporting Frequency Not Recognized",
                    description=(
                        f"Unexpected reporting frequency: {reporting_frequency} and "
                        f'month {row.get("month")}'
                    ),
                    message_type=BulkUploadMessageType.ERROR,
                    time_range=None,
                )

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
            agency_name = row["agency"]
            system = row.get("system")
            metric_file = get_metricfile_by_sheet_name(
                sheet_name=self.sheet_name,
                system=(
                    schema.System[system]
                    if system is not None
                    else self.metadata.system
                ),
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
                normalized_agency_name not in self.metadata.child_agency_name_to_agency
                and normalized_agency_name != self.metadata.agency.name.lower()
            ):
                description = f"Failed to upload data for {agency_name}. Either this agency does not exist in our database, or your agency does not have permission to upload for this agency."
                if metric_file is not None and description not in {
                    e.description
                    for e in self.metadata.metric_key_to_errors[
                        metric_file.definition.key
                    ]
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
                system=self.metadata.system,
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
                self.metadata.metric_key_to_errors[None].append(
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
            logging.error(  # Log Unexpected Errors to Sentry
                "[Bulk Upload] %s experienced an unexpected server error during upload. Error: %s",
                self.metadata.agency.name,
                e,
            )
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

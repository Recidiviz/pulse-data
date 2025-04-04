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
from collections import defaultdict
from itertools import groupby
from typing import Any, Dict, List, Optional, Set, Tuple

from recidiviz.justice_counts.bulk_upload.bulk_upload_metadata import BulkUploadMetadata
from recidiviz.justice_counts.bulk_upload.time_range_uploader import TimeRangeUploader
from recidiviz.justice_counts.datapoint import DatapointInterface, DatapointUniqueKey
from recidiviz.justice_counts.exceptions import (
    BulkUploadMessageType,
    JusticeCountsBulkUploadException,
)
from recidiviz.justice_counts.metricfile import MetricFile
from recidiviz.justice_counts.metricfiles.metricfile_registry import (
    get_metricfile_by_sheet_name,
)
from recidiviz.justice_counts.metrics.metric_interface import MetricInterface
from recidiviz.justice_counts.report import ReportInterface
from recidiviz.justice_counts.types import DatapointJson
from recidiviz.justice_counts.utils.constants import UNEXPECTED_ERROR
from recidiviz.justice_counts.utils.system_filename_breakdown_to_metricfile import (
    SYSTEM_METRIC_BREAKDOWN_PAIR_TO_METRICFILE,
)
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.schema.justice_counts.schema import (
    ReportingFrequency,
)
from recidiviz.utils.types import assert_type


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
        self.agency_name_to_metric_key_canonical_file_name_to_timerange_to_inferred_value: Dict[
            str,
            Dict[
                str,
                Dict[
                    str,
                    Dict[Tuple[datetime.date, datetime.date], Any],
                ],
            ],
        ] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
        )

    def upload_dataframe_rows(
        self,
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
            agency_name_to_rows = {
                k: list(v)
                for k, v in groupby(
                    sorted(
                        rows,
                        key=lambda row: row[
                            "agency"
                        ],  # Use lambda to extract "agency" from each row
                    ),
                    key=lambda row: row["agency"],  # Group by "agency" after sorting
                )
            }
            self._upload_super_agency_sheet(
                agency_name_to_rows=agency_name_to_rows,
                uploaded_reports=uploaded_reports,
            )
        elif self.metadata.system == schema.System.SUPERVISION:
            system_to_rows = self._get_system_to_rows(
                rows=rows,
            )
            self._upload_supervision_sheet(
                system_to_rows=system_to_rows,
                uploaded_reports=uploaded_reports,
            )
        else:
            self._upload_rows(
                rows=rows,
                system=self.metadata.system,
                uploaded_reports=uploaded_reports,
            )

    def compare_total_and_inferred_values(self) -> None:
        """
        Compares inferred values from the breakdown sheets with total values from the aggregate sheets.

        This method iterates over agencies and their associated metrics to check whether the inferred
        sum of values in the breakdown sheets matches the total value provided in the aggregate sheets.
        If a mismatch greater than 1 (allowing for small floating-point differences) is detected, a
        warning is raised.

        If no total value is found in the aggregate sheet for a given metric, the inferred value is
        added as a new data point.

        Finally, all report datapoints are flushed to the database.
        """

        for (
            agency_name,
            metric_data,
        ) in (
            self.agency_name_to_metric_key_canonical_file_name_to_timerange_to_inferred_value.items()
        ):
            for metric_key, file_name_data in metric_data.items():
                # Combine inferred values across multiple metric files for the same metric key
                for timerange_to_inferred_value in file_name_data.values():
                    for (
                        time_range,
                        inferred_value,
                    ) in timerange_to_inferred_value.items():
                        total_value = (
                            self.metadata.agency_name_to_metric_key_to_timerange_to_total_value.get(
                                agency_name, {}
                            )
                            .get(metric_key, {})
                            .get(time_range)
                        )

                        agency = self.metadata.child_agency_name_to_agency.get(
                            agency_name, self.metadata.agency
                        )

                        if (
                            total_value is not None
                            and abs(float(total_value) - float(inferred_value)) > 1
                        ):
                            self._handle_mismatch_warning(
                                agency=agency,
                                metric_key=metric_key,
                                inferred_value=inferred_value,
                                total_value=total_value,
                                time_range=time_range,
                            )
                        elif total_value is None:
                            self._handle_missing_total_value(
                                agency=agency,
                                metric_key=metric_key,
                                time_range=time_range,
                                inferred_value=inferred_value,
                            )

        # Commit all datapoints to the database
        DatapointInterface.flush_report_datapoints(
            session=self.metadata.session,
            inserts=self.metadata.inserts,
            updates=self.metadata.updates,
            histories=self.metadata.histories,
        )

    def _handle_mismatch_warning(
        self,
        agency: schema.Agency,
        metric_key: str,
        inferred_value: float,
        total_value: float,
        time_range: Tuple[datetime.date, datetime.date],
    ) -> None:
        """Raises a warning when inferred and total values do not match.

        - If multiple child agencies are present, the agency name is included in the warning description.
        - For single-page uploads, references the metric key instead of the sheet name.
        - Uses 'aggregate rows' instead of 'aggregate sheet'.
        """

        agency_info = (
            f" for agency {agency.name}"
            if len(self.metadata.child_agency_name_to_agency) > 1
            else ""
        )

        reference = (
            f"the '{metric_key.lower()}' metric"
            if self.metadata.is_single_page_upload
            else f"the {self.sheet_name} sheet"
        )

        total = (
            f" row ({total_value})"
            if self.metadata.is_single_page_upload
            else f"sheet ({total_value})"
        )

        description = (
            f"The sum of all values ({inferred_value}) in {reference}{agency_info} for "
            f"{time_range[0].strftime('%m/%d/%Y')}-{time_range[1].strftime('%m/%d/%Y')} "
            f"does not equal the total value provided in the aggregate {total}."
        )

        warning = JusticeCountsBulkUploadException(
            title="Breakdown Total Warning",
            message_type=BulkUploadMessageType.WARNING,
            description=description,
        )
        self.metadata.metric_key_to_errors[metric_key].append(warning)

    def _handle_missing_total_value(
        self,
        agency: schema.Agency,
        metric_key: str,
        inferred_value: float,
        time_range: Tuple[datetime.date, datetime.date],
    ) -> None:
        """Handles cases where no total value exists by adding a new inferred value as a data point."""
        existing_datapoints = self.metadata.metric_key_to_datapoint_jsons.get(
            metric_key, []
        )

        new_datapoints = ReportInterface.add_or_update_metric(
            session=self.metadata.session,
            inserts=self.metadata.inserts,
            updates=self.metadata.updates,
            histories=self.metadata.histories,
            report=self.metadata.agency_id_to_time_range_to_reports[agency.id][
                time_range
            ][0],
            report_metric=MetricInterface(key=metric_key, value=inferred_value),
            user_account=self.metadata.user_account,
            uploaded_via_breakdown_sheet=True,
            existing_datapoints_dict=self.existing_datapoints_dict,
            agency=agency,
            upload_method=self.metadata.upload_method,
        )

        self.metadata.metric_key_to_datapoint_jsons[metric_key] = (
            existing_datapoints + new_datapoints
        )

    def _upload_super_agency_sheet(
        self,
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

            if self.metadata.system == schema.System.SUPERVISION:
                system_to_rows = self._get_system_to_rows(
                    rows=current_rows,
                )
                self._upload_supervision_sheet(
                    system_to_rows=system_to_rows,
                    child_agency_name=curr_agency_name,
                    uploaded_reports=uploaded_reports,
                )
            else:
                self._upload_rows(
                    rows=current_rows,
                    system=self.metadata.system,
                    child_agency_name=curr_agency_name,
                    uploaded_reports=uploaded_reports,
                )

    def _upload_supervision_sheet(
        self,
        system_to_rows: Dict[schema.System, List[Dict[str, Any]]],
        uploaded_reports: Set[schema.Report],
        child_agency_name: Optional[str] = None,
    ) -> None:
        """Uploads supervision rows one system at a time."""
        for current_system, current_rows in system_to_rows.items():
            self._upload_rows(
                rows=current_rows,
                system=current_system,
                uploaded_reports=uploaded_reports,
                child_agency_name=child_agency_name,
            )

    def _upload_rows(
        self,
        rows: List[Dict[str, Any]],
        system: schema.System,
        uploaded_reports: Set[schema.Report],
        child_agency_name: Optional[str] = None,
    ) -> None:
        """
        Uploads metric rows for a given system and child agency by organizing and
        mapping the rows to the appropriate metric files.

        For single-page uploads, all rows are assigned to the metric file directly
        based on the sheet name and system. For multi-page uploads, rows are
        sorted and grouped by 'breakdown' and 'breakdown category', and then
        mapped to the corresponding metric file.

        Parameters:
        records for tracking changes.
        rows (List[Dict[str, Any]]): List of rows containing metric data.
        system (schema.System): The system being processed (e.g., Parole,
        Probation).
        uploaded_reports (Set[schema.Report]): Set of reports that have been
        successfully uploaded.
        child_agency_name (Optional[str]): Name of the child agency if rows are
        uploaded for a sub-agency (default is None, meaning it uploads for
        the main agency).

        Raises:
        ValueError: If a corresponding metric file cannot be found for a given
        breakdown and breakdown category.
        """
        if self.metadata.is_single_page_upload is False:
            metricfile = get_metricfile_by_sheet_name(
                sheet_name=self.sheet_name, system=system
            )
            metricfile_to_rows = {}
            metricfile_to_rows[assert_type(metricfile, MetricFile)] = rows
        else:
            metricfile_to_rows = defaultdict(list)
            for row in rows:
                metricfile_key = (
                    system,
                    str(row["metric"]),
                    (
                        str(row.get("breakdown_category"))
                        if row.get("breakdown_category") is not None
                        else None
                    ),
                )

                try:
                    metricfile = SYSTEM_METRIC_BREAKDOWN_PAIR_TO_METRICFILE[
                        metricfile_key
                    ]
                    metricfile_to_rows[metricfile].append(row)
                except KeyError as e:
                    raise ValueError(
                        f"Metric file not found for key: {metricfile_key}"
                    ) from e
        for metricfile, grouped_rows in metricfile_to_rows.items():
            existing_datapoint_json_list = (
                self.metadata.metric_key_to_datapoint_jsons.get(
                    metricfile.definition.key, []
                )
            )
            try:
                new_datapoint_json_list = self._upload_rows_for_metricfile(
                    rows=grouped_rows,
                    metricfile=metricfile,
                    uploaded_reports=uploaded_reports,
                    child_agency_name=child_agency_name,
                )
            except Exception as e:
                new_datapoint_json_list = []
                if metricfile is not None:
                    self.metadata.metric_key_to_errors[
                        metricfile.definition.key
                    ].append(self._handle_error(e=e, sheet_name=self.sheet_name))

            self.metadata.metric_key_to_datapoint_jsons[metricfile.definition.key] = (
                existing_datapoint_json_list + new_datapoint_json_list
            )

    def _upload_rows_for_metricfile(
        self,
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
                    sheet_name=self.sheet_name,
                )
                existing_report_list = (
                    self.metadata.agency_id_to_time_range_to_reports.get(
                        curr_agency.id, {}
                    ).get(time_range, [])
                )
                existing_report = (
                    existing_report_list[0] if len(existing_report_list) > 0 else None
                )
                if existing_report is not None and existing_report.id is not None:
                    uploaded_reports.add(existing_report)
                (
                    report,
                    datapoint_json_list_for_time_range,
                ) = time_range_uploader.upload_time_range(
                    time_range_to_year_month=time_range_to_year_month,
                    existing_report=existing_report,
                    agency_name_to_metric_key_canonical_file_name_to_timerange_to_inferred_value=self.agency_name_to_metric_key_canonical_file_name_to_timerange_to_inferred_value,
                )
                if existing_report is None:
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
            year = row["year"]
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
                month = row["month"]
                assumed_frequency = ReportingFrequency.MONTHLY
            elif (
                reporting_frequency == ReportingFrequency.ANNUAL
                and row.get("month") is not None
            ):
                # We will be in this case if monthly data is provided for annual metrics
                month = row["month"]
                assumed_frequency = ReportingFrequency.MONTHLY
            else:
                month = (
                    custom_starting_month or 1
                )  # if no custom starting month specified, assume calendar year
                assumed_frequency = ReportingFrequency.ANNUAL
            date_range_start, date_range_end = ReportInterface.get_date_range(
                year=year, month=int(month), frequency=assumed_frequency.value
            )
            time_range_to_year_month[(date_range_start, date_range_end)] = (
                year,
                int(month),
            )
            rows_by_time_range[(date_range_start, date_range_end)].append(row)
        return rows_by_time_range, time_range_to_year_month

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
                sorted(rows, key=lambda x: x.get("system") or "all"),
                lambda x: x.get("system") or "all",
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

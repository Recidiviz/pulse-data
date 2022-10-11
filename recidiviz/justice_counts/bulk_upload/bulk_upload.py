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
"""Functionality for bulk upload of data into the Justice Counts database."""


import calendar
import datetime
import itertools
import logging
from collections import defaultdict
from itertools import groupby
from typing import Any, Dict, List, Optional, Set, Tuple, Type

import pandas as pd
from sqlalchemy.orm import Session

from recidiviz.common.text_analysis import TextAnalyzer, TextMatchingConfiguration
from recidiviz.justice_counts.bulk_upload.bulk_upload_helpers import (
    fuzzy_match_against_options,
)
from recidiviz.justice_counts.datapoint import DatapointInterface, DatapointUniqueKey
from recidiviz.justice_counts.dimensions.base import DimensionBase
from recidiviz.justice_counts.exceptions import (
    BulkUploadMessageType,
    JusticeCountsBulkUploadException,
)
from recidiviz.justice_counts.metricfile import MetricFile
from recidiviz.justice_counts.metricfiles.metricfile_registry import (
    SYSTEM_TO_FILENAME_TO_METRICFILE,
)
from recidiviz.justice_counts.metrics.metric_definition import MetricDefinition
from recidiviz.justice_counts.metrics.metric_interface import (
    MetricAggregatedDimensionData,
    MetricInterface,
)
from recidiviz.justice_counts.metrics.metric_registry import (
    METRIC_KEY_TO_METRIC,
    METRICS_BY_SYSTEM,
)
from recidiviz.justice_counts.report import ReportInterface
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.schema.justice_counts.schema import (
    ReportingFrequency,
    ReportStatus,
)

MONTH_NAMES = list(calendar.month_name)
PYTHON_TYPE_TO_READABLE_NAME = {"int": "a number", "float": "a number", "str": "text"}


class BulkUploader:
    """Functionality for bulk upload of data into the Justice Counts database."""

    def __init__(self) -> None:
        self.text_analyzer = TextAnalyzer(
            configuration=TextMatchingConfiguration(
                # We don't want to treat "other" as a stop word,
                # because it's a valid breakdown category
                stop_words_to_remove={"other"}
            )
        )

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

    def upload_excel(
        self,
        session: Session,
        xls: pd.ExcelFile,
        agency_id: int,
        system: schema.System,
        user_account: schema.UserAccount,
        metric_key_to_agency_datapoints: Dict[str, List[schema.Datapoint]],
    ) -> Tuple[
        Dict[str, List[Dict[str, Any]]],
        Dict[Optional[str], List[JusticeCountsBulkUploadException]],
    ]:
        """Iterate through all tabs in an Excel spreadsheet and upload them
        to the Justice Counts database using the `upload_rows` method defined below.
        If an error is encountered in a particular sheet, log it and continue.
        """
        # 1. Fetch existing reports and datapoints for this agency, so that
        # we know what objects to update vs. what new objects to create.
        reports = ReportInterface.get_reports_by_agency_id(
            session, agency_id=agency_id, include_datapoints=True
        )
        reports_sorted_by_time_range = sorted(
            reports, key=lambda x: (x.date_range_start, x.date_range_end)
        )
        reports_by_time_range = {
            k: list(v)
            for k, v in groupby(
                reports_sorted_by_time_range,
                key=lambda x: (x.date_range_start, x.date_range_end),
            )
        }
        existing_datapoints_dict = ReportInterface.get_existing_datapoints_dict(
            reports=reports
        )

        metric_key_to_datapoint_jsons: Dict[str, List[Dict[str, Any]]] = defaultdict(
            list
        )
        metric_key_to_errors: Dict[
            Optional[str], List[JusticeCountsBulkUploadException]
        ] = defaultdict(list)
        sheet_name_to_metricfile = SYSTEM_TO_FILENAME_TO_METRICFILE[system.value]

        expected_breakdown_sheet_names: Set[str] = {
            metricfile.canonical_filename
            for metricfile in sheet_name_to_metricfile.values()
            if metricfile.disaggregation_column_name is not None
        }

        # 2. Sort sheet_names so that we process by aggregate sheets first
        # e.g. caseloads before caseloads_by_gender. This ensures we don't
        # infer an aggregate value when one is explicitly given.
        # Note that the regular sorting will work for this case, since
        # foobar will always come before foobar_by_xxx alphabetically.
        actual_sheet_names = sorted(xls.sheet_names)

        # 3. Now run through all sheets and process each in turn.
        invalid_sheetnames: List[str] = []
        sheet_name_to_df = pd.read_excel(xls, sheet_name=None)
        for sheet_name in actual_sheet_names:
            logging.info("Uploading %s", sheet_name)
            df = sheet_name_to_df[sheet_name]
            # Drop any rows that contain any NaN values
            df = df.dropna(axis=0, how="any", subset="value")
            # Convert dataframe to a list of dictionaries
            rows = df.to_dict("records")
            metric_key_to_datapoint_jsons, metric_key_to_errors = self._upload_rows(
                session=session,
                system=system,
                rows=rows,
                sheet_name=sheet_name,
                agency_id=agency_id,
                user_account=user_account,
                reports_by_time_range=reports_by_time_range,
                existing_datapoints_dict=existing_datapoints_dict,
                metric_key_to_datapoint_jsons=metric_key_to_datapoint_jsons,
                metric_key_to_errors=metric_key_to_errors,
                invalid_sheetnames=invalid_sheetnames,
                metric_key_to_agency_datapoints=metric_key_to_agency_datapoints,
            )

        metric_key_to_errors = self._add_invalid_sheet_name_error(
            invalid_sheet_names=invalid_sheetnames,
            metric_key_to_errors=metric_key_to_errors,
            sheet_name_to_metricfile=sheet_name_to_metricfile,
        )

        # 5. For any report that was updated, set its status to DRAFT
        report: schema.Report  # make mypy happy
        for report in itertools.chain(*reports_by_time_range.values()):
            ReportInterface.update_report_metadata(
                report=report,
                editor_id=user_account.id,
                status=ReportStatus.DRAFT.value,
            )

        metric_key_to_errors = self._add_missing_metric_errors(
            metric_key_to_datapoint_jsons=metric_key_to_datapoint_jsons,
            metric_key_to_errors=metric_key_to_errors,
            metric_definitions=METRICS_BY_SYSTEM[system.value],
            actual_sheet_names=actual_sheet_names,
            sheet_name_to_metricfile=sheet_name_to_metricfile,
            metric_key_to_agency_datapoints=metric_key_to_agency_datapoints,
        )

        actual_breakdown_sheet_names: Set[str] = {
            s for s in actual_sheet_names if s in expected_breakdown_sheet_names
        }

        metric_key_to_errors = self._add_missing_breakdowns_errors(
            metric_key_to_errors=metric_key_to_errors,
            metric_key_to_datapoint_jsons=metric_key_to_datapoint_jsons,
            expected_breakdown_sheet_names=expected_breakdown_sheet_names,
            actual_breakdown_sheet_names=actual_breakdown_sheet_names,
            sheet_name_to_metricfile=sheet_name_to_metricfile,
            metric_key_to_agency_datapoints=metric_key_to_agency_datapoints,
        )

        return metric_key_to_datapoint_jsons, metric_key_to_errors

    def _add_invalid_sheet_name_error(
        self,
        invalid_sheet_names: List[str],
        metric_key_to_errors: Dict[
            Optional[str], List[JusticeCountsBulkUploadException]
        ],
        sheet_name_to_metricfile: Dict[str, MetricFile],
    ) -> Dict[Optional[str], List[JusticeCountsBulkUploadException]]:
        """This function adds an Invalid Sheet Names error to the metric_key_to_errors
        dictionary if the user has included sheet names in their Excel workbook
        that do not correspond to the metrics that are specified for their agency."""

        if len(invalid_sheet_names) > 0:
            description = (
                f"The following sheet names do not correspond to a metric for "
                f"your agency: {', '.join(invalid_sheet_names)}. "
                f"Valid options include {', '.join(sheet_name_to_metricfile.keys())}."
            )
            invalid_sheet_name_error = JusticeCountsBulkUploadException(
                title="Invalid Sheet Names",
                message_type=BulkUploadMessageType.ERROR,
                description=description,
            )
            metric_key_to_errors[None].append(invalid_sheet_name_error)
        return metric_key_to_errors

    def _add_missing_metric_errors(
        self,
        actual_sheet_names: List[str],
        sheet_name_to_metricfile: Dict[str, MetricFile],
        metric_key_to_datapoint_jsons: Dict[str, List[Dict[str, Any]]],
        metric_key_to_errors: Dict[
            Optional[str], List[JusticeCountsBulkUploadException]
        ],
        metric_definitions: List[MetricDefinition],
        metric_key_to_agency_datapoints: Dict[str, List[schema.Datapoint]],
    ) -> Dict[Optional[str], List[JusticeCountsBulkUploadException]]:
        """This function adds an Missing Metric error to the metric_key_to_errors
        dictionary if the user has not included rows in their Excel workbook
        for a metric that is required for their agency."""
        for metric_definition in metric_definitions:
            # If no datapoints were ingested for a metric and no errors are associated with the
            # metric (i.e there was no error in the sheet that prevented ingest), then the
            # metric is missing.
            if (
                len(metric_key_to_datapoint_jsons.get(metric_definition.key, [])) == 0
                and len(metric_key_to_errors.get(metric_definition.key, [])) == 0
                and metric_definition.disabled is not True
                and not DatapointInterface.is_metric_disabled(
                    metric_key_to_agency_datapoints=metric_key_to_agency_datapoints,
                    metric_key=metric_definition.key,
                )
            ):
                files_without_rows = [
                    sheet_name
                    for sheet_name, metricfile in sheet_name_to_metricfile.items()
                    if sheet_name in actual_sheet_names
                    and metricfile.definition.key == metric_definition.key
                ]
                totals_filename = [
                    sheet_name
                    for sheet_name, metricfile in sheet_name_to_metricfile.items()
                    if metricfile.definition.key == metric_definition.key
                    and metricfile.disaggregation is None
                ].pop()
                description_suffix = (
                    f"Please provide data in a sheet named {totals_filename}."
                )
                if len(files_without_rows) > 0:
                    description_suffix = f"The following sheets were empty: {', '.join(files_without_rows)}."
                missing_metric_warning = JusticeCountsBulkUploadException(
                    title="Missing Metric",
                    message_type=BulkUploadMessageType.ERROR,
                    description=(
                        f"No data for the {METRIC_KEY_TO_METRIC[metric_definition.key].display_name} metric was provided. "
                        + description_suffix
                    ),
                )
                metric_key_to_errors[metric_definition.key].append(
                    missing_metric_warning
                )

        return metric_key_to_errors

    def _add_missing_breakdowns_errors(
        self,
        actual_breakdown_sheet_names: Set[str],
        expected_breakdown_sheet_names: Set[str],
        metric_key_to_datapoint_jsons: Dict[str, List[Dict[str, Any]]],
        metric_key_to_errors: Dict[
            Optional[str], List[JusticeCountsBulkUploadException]
        ],
        sheet_name_to_metricfile: Dict[str, MetricFile],
        metric_key_to_agency_datapoints: Dict[str, List[schema.Datapoint]],
    ) -> Dict[Optional[str], List[JusticeCountsBulkUploadException]]:
        """This function adds an Missing Breakdown Sheet error to the metric_key_to_errors
        dictionary if the user has not included a sheet in their Excel workbook
        for a metric breakdown.
        """
        actual_breakdown_canonical_filenames = {
            sheet_name_to_metricfile.get(
                s
            ).canonical_filename  # type: ignore[union-attr]
            for s in actual_breakdown_sheet_names
            if sheet_name_to_metricfile.get(s) is not None
        }
        for missing_sheet in (
            expected_breakdown_sheet_names - actual_breakdown_sheet_names
        ):
            metricfile = sheet_name_to_metricfile.get(missing_sheet)
            if (
                metricfile is not None
                and metricfile.canonical_filename
                not in actual_breakdown_canonical_filenames
                and len(
                    metric_key_to_datapoint_jsons.get(metricfile.definition.key, [])
                )
                > 0
            ):
                # Only add missing breakdown warning if the metric is not missing.
                self._add_missing_breakdowns_error(
                    sheet_name=missing_sheet,
                    metric_key_to_errors=metric_key_to_errors,
                    metric_definition=metricfile.definition,
                    is_sheet_provided=False,
                    # we know metricfile.disaggreation will be non-None, so it's
                    # safe to silence mypy
                    metric_key_to_agency_datapoints=metric_key_to_agency_datapoints,
                    disaggregation=metricfile.disaggregation,  # type: ignore[arg-type]
                )
        return metric_key_to_errors

    def _add_missing_breakdowns_error(
        self,
        sheet_name: str,
        metric_key_to_errors: Dict[
            Optional[str], List[JusticeCountsBulkUploadException]
        ],
        metric_definition: MetricDefinition,
        is_sheet_provided: bool,
        disaggregation: Type[DimensionBase],
        metric_key_to_agency_datapoints: Dict[str, List[schema.Datapoint]],
    ) -> Dict[Optional[str], List[JusticeCountsBulkUploadException]]:
        if DatapointInterface.is_metric_disabled(
            metric_key_to_agency_datapoints=metric_key_to_agency_datapoints,
            metric_key=metric_definition.key,
            dimension_id=disaggregation.dimension_identifier(),
        ):
            return metric_key_to_errors
        description_suffix = f"Please provide data in a sheet named {sheet_name}."
        if is_sheet_provided is True:
            description_suffix = f"The sheet named {sheet_name} was empty."
        missing_sheet_error = JusticeCountsBulkUploadException(
            title="Missing Breakdown Sheet",
            message_type=BulkUploadMessageType.WARNING,
            description=f"No data for the {disaggregation.human_readable_name()} breakdown was provided. "
            + description_suffix,
            sheet_name=sheet_name,
        )
        metric_key_to_errors[metric_definition.key].append(missing_sheet_error)
        return metric_key_to_errors

    def _add_missing_total_warning(
        self,
        metric_definition: MetricDefinition,
        sheet_name: str,
        metric_key_to_errors: Dict[
            Optional[str], List[JusticeCountsBulkUploadException]
        ],
        metric_key_to_agency_datapoints: Dict[str, List[schema.Datapoint]],
    ) -> Dict[Optional[str], List[JusticeCountsBulkUploadException]]:
        # Add a warning for missing total value only if datapoints
        # were successfully ingested from the breakdown sheet.
        if DatapointInterface.is_metric_disabled(
            metric_key_to_agency_datapoints=metric_key_to_agency_datapoints,
            metric_key=metric_definition.key,
        ):
            return metric_key_to_errors

        missing_total_error = JusticeCountsBulkUploadException(
            title="Missing Total Value",
            message_type=BulkUploadMessageType.WARNING,
            sheet_name=sheet_name,
            description=(
                f"No total values were provided for this metric. The total values will be assumed "
                f"to be equal to the sum of the breakdown values provided in {sheet_name}."
            ),
        )
        metric_key_to_errors[metric_definition.key].append(missing_total_error)
        return metric_key_to_errors

    def _upload_rows(
        self,
        session: Session,
        system: schema.System,
        rows: List[Dict[str, Any]],
        sheet_name: str,
        agency_id: int,
        metric_key_to_datapoint_jsons: Dict[str, List[Dict[str, Any]]],
        metric_key_to_errors: Dict[
            Optional[str], List[JusticeCountsBulkUploadException]
        ],
        user_account: schema.UserAccount,
        reports_by_time_range: Dict,
        existing_datapoints_dict: Dict[DatapointUniqueKey, schema.Datapoint],
        invalid_sheetnames: List[str],
        metric_key_to_agency_datapoints: Dict[str, List[schema.Datapoint]],
    ) -> Tuple[
        Dict[str, List[Dict[str, Any]]],
        Dict[Optional[str], List[JusticeCountsBulkUploadException]],
    ]:
        """Generally, a file will only contain metrics for one system. In the case
        of supervision, the file could contain metrics for supervision, parole, or
        probation. This is indicated by the `system` column. In this case, we break
        up the rows by system, and then ingest one system at a time."""
        system_to_rows = self._get_system_to_rows(system=system, rows=rows)
        for current_system, current_rows in system_to_rows.items():

            # Redefine this here to properly handle sheets that contain
            # rows for multiple systems (e.g. a Supervision sheet can
            # contain rows for Parole and Probation)
            sheet_name_to_metricfile = SYSTEM_TO_FILENAME_TO_METRICFILE[
                current_system.value
            ]

            # Based on the system and the name of the CSV file, determine which
            # Justice Counts metric this file contains data for
            metricfile = self._get_metricfile(
                sheet_name=sheet_name, system=current_system
            )
            if not metricfile:
                invalid_sheetnames.append(sheet_name)
                return metric_key_to_datapoint_jsons, metric_key_to_errors

            metric_datapoints = metric_key_to_datapoint_jsons[metricfile.definition.key]

            try:
                datapoint_json_list = self._upload_rows_for_metricfile(
                    session=session,
                    rows=current_rows,
                    metricfile=metricfile,
                    agency_id=agency_id,
                    user_account=user_account,
                    reports_by_time_range=reports_by_time_range,
                    existing_datapoints_dict=existing_datapoints_dict,
                    metric_key_to_errors=metric_key_to_errors,
                )
            except Exception as e:
                datapoint_json_list = []
                curr_metricfile = sheet_name_to_metricfile[sheet_name]
                metric_key_to_errors[curr_metricfile.definition.key].append(
                    self._handle_error(
                        e=e,
                        sheet_name=sheet_name,
                    )
                )

            metric_key_to_datapoint_jsons[metricfile.definition.key] = (
                metric_datapoints + datapoint_json_list
            )

            if (
                metricfile.disaggregation is not None
                and len(metric_datapoints) == 0
                and len(datapoint_json_list) > 0
            ):
                metric_key_to_errors = self._add_missing_total_warning(
                    metric_definition=metricfile.definition,
                    sheet_name=sheet_name,
                    metric_key_to_errors=metric_key_to_errors,
                    metric_key_to_agency_datapoints=metric_key_to_agency_datapoints,
                )
            elif (
                len(datapoint_json_list) == 0
                and len(metric_datapoints) > 0
                and metricfile.disaggregation is not None
            ):
                # If the current sheet is a breakdown sheet and there are no datapoints associated
                # with the sheet, the uploaded breakdown sheet is empty.
                metric_key_to_errors = self._add_missing_breakdowns_error(
                    sheet_name,
                    metric_definition=metricfile.definition,
                    metric_key_to_errors=metric_key_to_errors,
                    is_sheet_provided=True,
                    disaggregation=metricfile.disaggregation,
                    metric_key_to_agency_datapoints=metric_key_to_agency_datapoints,
                )

        return metric_key_to_datapoint_jsons, metric_key_to_errors

    def _get_system_to_rows(
        self, system: schema.System, rows: List[Dict[str, Any]]
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
                "both": schema.System.SUPERVISION,
                "parole": schema.System.PAROLE,
                "probation": schema.System.PROBATION,
                "post release": schema.System.POST_RELEASE,
            }
            for system_value, system_rows in system_value_to_rows.items():
                normalized_system_value = system_value.lower().strip()
                mapped_system = normalized_system_value_to_system[
                    normalized_system_value
                ]
                system_to_rows[mapped_system] = system_rows
        else:
            system_to_rows[system] = rows
        return system_to_rows

    def _upload_rows_for_metricfile(
        self,
        session: Session,
        rows: List[Dict[str, Any]],
        metricfile: MetricFile,
        agency_id: int,
        user_account: schema.UserAccount,
        reports_by_time_range: Dict,
        existing_datapoints_dict: Dict[DatapointUniqueKey, schema.Datapoint],
        metric_key_to_errors: Dict[
            Optional[str], List[JusticeCountsBulkUploadException]
        ],
    ) -> List[Dict[str, Any]]:
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
        reporting_frequency = metric_definition.reporting_frequency

        # TODO(#13731): Warn if there are unexpected columns in the file

        # Step 1: Group the rows in this file by time range.
        (rows_by_time_range, time_range_to_year_month,) = self._get_rows_by_time_range(
            rows=rows, reporting_frequency=reporting_frequency
        )

        # Step 2: For each time range represented in the file, convert the
        # reported data into a MetricInterface object. If a report already
        # exists for this time range, update it with the MetricInterface.
        # Else, create a new report and add the MetricInterface.
        datapoint_jsons_list = []
        for time_range, rows_for_this_time_range in rows_by_time_range.items():
            existing_report = reports_by_time_range.get(time_range)
            if existing_report is not None:
                if len(existing_report) != 1:
                    raise ValueError(
                        f"Found {len(existing_report)} reports with time range {time_range}."
                    )
                report = existing_report[0]
            else:  # existing report is None, so create the report
                year, month = time_range_to_year_month[time_range]
                report = ReportInterface.create_report(
                    session=session,
                    agency_id=agency_id,
                    user_account_id=user_account.id,
                    year=year,
                    month=month,
                    frequency=reporting_frequency.value,
                )
                reports_by_time_range[time_range] = [report]

            try:
                report_metric = self._get_report_metric(
                    metricfile=metricfile,
                    time_range=time_range,
                    rows_for_this_time_range=rows_for_this_time_range,
                )

                datapoint_jsons_list += ReportInterface.add_or_update_metric(
                    session=session,
                    report=report,
                    report_metric=report_metric,
                    user_account=user_account,
                    # TODO(#15499) Infer aggregate value only if total sheet was not provided.
                    use_existing_aggregate_value=metricfile.disaggregation is not None,
                    existing_datapoints_dict=existing_datapoints_dict,
                )
            except Exception as e:
                metric_key_to_errors[metricfile.definition.key].append(
                    self._handle_error(
                        e=e,
                        sheet_name=metricfile.canonical_filename,
                    )
                )

        return datapoint_jsons_list

    def _get_metricfile(
        self, sheet_name: str, system: schema.System
    ) -> Optional[MetricFile]:
        stripped_sheet_name = sheet_name.split("/")[-1].split(".")[0].strip()
        filename_to_metricfile = SYSTEM_TO_FILENAME_TO_METRICFILE[system.value]
        return filename_to_metricfile.get(stripped_sheet_name)

    def _get_rows_by_time_range(
        self,
        rows: List[Dict[str, Any]],
        reporting_frequency: ReportingFrequency,
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
            year = self._get_column_value(row=row, column_name="year", column_type=int)
            if reporting_frequency == ReportingFrequency.MONTHLY:
                month = self._get_column_value(
                    row=row, column_name="month", column_type=int
                )
            else:
                # TODO(#13731): Look up whether this agency uses fiscal years
                month = 1

            date_range_start, date_range_end = ReportInterface.get_date_range(
                year=year, month=month, frequency=reporting_frequency.value
            )
            time_range_to_year_month[(date_range_start, date_range_end)] = (year, month)
            rows_by_time_range[(date_range_start, date_range_end)].append(row)
        return rows_by_time_range, time_range_to_year_month

    def _get_report_metric(
        self,
        metricfile: MetricFile,
        time_range: Tuple[datetime.date, datetime.date],
        rows_for_this_time_range: List[Dict[str, Any]],
    ) -> MetricInterface:
        """Given a a set of rows from the CSV that all correspond to a single
        time period, convert the data in these rows to a MetricInterface object.
        If the metric associated with this CSV has no disaggregations, there
        should only be a single row for a single time period, and it contains
        the aggregate metric value. If the metric does have a disaggregation,
        there will be several rows, one with the value for each category.
        """
        aggregate_value = None
        dimension_to_value: Optional[Dict[DimensionBase, Optional[float]]] = (
            {d: None for d in metricfile.disaggregation}  # type: ignore[attr-defined]
            if metricfile.disaggregation is not None
            else None
        )

        # If this file represents a metric without disaggregations,
        # there should only be one row for a given time period.
        if metricfile.disaggregation is None:
            if len(rows_for_this_time_range) != 1:
                description = (
                    "There should only be a single row containing data "
                    f"for the time period {time_range[0].month}/{time_range[0].year}."
                )

                raise JusticeCountsBulkUploadException(
                    title="Too Many Rows",
                    description=description,
                    message_type=BulkUploadMessageType.ERROR,
                    time_range=time_range,
                )
            row = rows_for_this_time_range[0]
            aggregate_value = self._get_column_value(
                row=row,
                column_name="value",
                column_type=float,
                time_range=time_range,
            )

        else:  # metricfile.disaggregation is not None
            if metricfile.disaggregation_column_name is None:
                raise ValueError(
                    "`disaggregation` is not None but `disaggregation_column_name` is None"
                )
            for row in rows_for_this_time_range:
                # If this file represents a metric with a disaggregation,
                # there will likely be more than one row for a given time range;
                # there will be one row for each dimension value. Each will have
                # a value (i.e. the number or count) and a disaggregation value
                # (i.e. the category the count refers to, e.g. Male or Female).
                value = self._get_column_value(
                    row=row, column_name="value", column_type=float
                )

                # disaggregation_value is either "All" or an enum member,
                # e.g. "Male" for Gender, "Asian" for Race, "Felony" for OffenseType, etc
                disaggregation_value = self._get_column_value(
                    row=row,
                    column_name=metricfile.disaggregation_column_name,
                    column_type=str,
                )

                try:
                    matching_disaggregation_member = metricfile.disaggregation(disaggregation_value)  # type: ignore
                except ValueError:
                    # A ValueError will be thrown by the line above if the user-entered disaggregation
                    # value is not actually a member of the disaggreation enum. In that case, we fuzzy
                    # match against the enum members and try again.
                    disaggregation_options = [
                        member.value for member in metricfile.disaggregation  # type: ignore[attr-defined]
                    ]
                    disaggregation_value = fuzzy_match_against_options(
                        analyzer=self.text_analyzer,
                        text=disaggregation_value,
                        options=disaggregation_options,
                        category_name=metricfile.disaggregation_column_name.replace(
                            "_", " "
                        ).title(),
                        time_range=time_range,
                    )
                    matching_disaggregation_member = metricfile.disaggregation(
                        disaggregation_value
                    )  # type: ignore[call-arg]
                dimension_to_value[matching_disaggregation_member] = value  # type: ignore[index]

            aggregate_value = sum(
                val  # type: ignore[misc]
                for val in dimension_to_value.values()  # type: ignore[union-attr]
                if val is not None
            )

        return MetricInterface(
            key=metricfile.definition.key,
            value=aggregate_value,
            contexts=[],
            aggregated_dimensions=[
                MetricAggregatedDimensionData(dimension_to_value=dimension_to_value)
            ]
            if dimension_to_value is not None
            else [],
        )

    def _get_column_value(
        self,
        row: Dict[str, Any],
        column_name: str,
        column_type: Type,
        time_range: Optional[Tuple[datetime.date, datetime.date]] = None,
    ) -> Any:
        """Given a row, a column name, and a column type, attempts to
        extract a value of the given type from the row."""
        if column_name not in row:
            description = (
                f'We expected to see a column named "{column_name}". '
                f"Only the following columns were found in the sheet: "
                f"{', '.join(row.keys())}."
            )
            raise JusticeCountsBulkUploadException(
                title="Missing Column",
                description=description,
                message_type=BulkUploadMessageType.ERROR,
                time_range=time_range,
            )

        column_value = row[column_name]
        # Allow numeric values with columns in them (e.g. 1,000)
        if isinstance(column_value, str):
            column_value = column_value.replace(",", "")

        try:
            value = column_type(column_value)
        except Exception as e:
            if column_name == "month":
                # Allow "month" column to be either numbers or month names
                column_value = self._get_month_column_value(column_value=column_value)
                value = column_type(column_value)
            elif column_name == "year" and "-" in str(column_value):
                column_value = self._get_annual_year_from_fiscal_year(
                    column_value=str(column_value)
                )
                value = column_type(column_value)
            else:
                raise JusticeCountsBulkUploadException(
                    title="Wrong Value Type",
                    message_type=BulkUploadMessageType.ERROR,
                    description=f'We expected all values in the column named "{column_name}" to '
                    f"be {PYTHON_TYPE_TO_READABLE_NAME.get(column_type.__name__, column_type.__name__)}. Instead we found the value "
                    f'"{column_value}", which is {PYTHON_TYPE_TO_READABLE_NAME.get(type(column_value).__name__, type(column_value).__name__)}.',
                ) from e

        # Round numbers to two decimal places
        if isinstance(value, float):
            value = round(value, 2)

        return value

    def _get_month_column_value(
        self,
        column_value: str,
    ) -> int:
        """Takes as input a string and attempts to find the corresponding month
        index using the calendar module's month_names enum. For instance,
        March -> 3. Uses fuzzy matching to handle typos, such as `Febuary`."""
        column_value = column_value.title()
        if column_value not in MONTH_NAMES:
            column_value = fuzzy_match_against_options(
                analyzer=self.text_analyzer,
                category_name="Month",
                text=column_value,
                options=MONTH_NAMES,
            )
        return MONTH_NAMES.index(column_value)

    def _get_annual_year_from_fiscal_year(self, column_value: str) -> Optional[str]:
        """Takes as input a string and attempts to find the corresponding year"""
        return column_value[0 : column_value.index("-")]

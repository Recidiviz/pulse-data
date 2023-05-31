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
"""Functionality for bulk upload of an Excel workbook into the Justice Counts database."""

import datetime
import logging
from collections import defaultdict
from itertools import groupby
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd
from sqlalchemy.orm import Session

from recidiviz.common.text_analysis import TextAnalyzer, TextMatchingConfiguration
from recidiviz.justice_counts.bulk_upload.spreadsheet_uploader import (
    SpreadsheetUploader,
)
from recidiviz.justice_counts.datapoint import DatapointInterface
from recidiviz.justice_counts.exceptions import (
    BulkUploadMessageType,
    JusticeCountsBulkUploadException,
)
from recidiviz.justice_counts.metricfile import MetricFile
from recidiviz.justice_counts.metricfiles.metricfile_registry import (
    SYSTEM_TO_FILENAME_TO_METRICFILE,
)
from recidiviz.justice_counts.metrics.metric_definition import MetricDefinition
from recidiviz.justice_counts.report import ReportInterface
from recidiviz.justice_counts.types import DatapointJson
from recidiviz.justice_counts.utils.constants import AUTOMATIC_UPLOAD_ID
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.schema.justice_counts.schema import ReportStatus


class WorkbookUploader:
    """Uploads Excel workbooks that comply with the Justice Counts technical specification"""

    def __init__(
        self,
        system: schema.System,
        agency: schema.Agency,
        metric_key_to_agency_datapoints: Dict[str, List[schema.Datapoint]],
        child_agency_name_to_id: Optional[Dict[str, int]] = None,
        user_account: Optional[schema.UserAccount] = None,
    ) -> None:
        self.system = system
        self.agency = agency
        self.user_account = user_account
        # metric_key_to_agency_datapoints maps metric keys to a list of datapoints
        # that represent an agency's configuration for that metric. These datapoints
        # hold information such as if the metric is enabled or disabled, the metric's
        # custom reporting frequency, etc. It is passed in already populated and won't be
        # edited during the upload.
        self.metric_key_to_agency_datapoints = metric_key_to_agency_datapoints
        # metric_key_to_errors starts out empty and will be populated with
        # each metric's errors.
        self.metric_key_to_errors: Dict[
            Optional[str], List[JusticeCountsBulkUploadException]
        ] = defaultdict(list)
        # metric_key_to_datapoint_jsons starts out empty and will be populated with
        # the ingested datapoints for each metric. We need these datapoints because
        # we send these to the frontend after the upload so it can render
        # the review page.
        self.metric_key_to_datapoint_jsons: Dict[
            str, List[DatapointJson]
        ] = defaultdict(list)
        self.text_analyzer = TextAnalyzer(
            configuration=TextMatchingConfiguration(
                # We don't want to treat "other" as a stop word,
                # because it's a valid breakdown category
                stop_words_to_remove={"other"}
            )
        )
        self.metric_key_to_timerange_to_total_value: Dict[
            str,
            Dict[Tuple[datetime.date, datetime.date], Optional[int]],
        ] = defaultdict(dict)
        # A list of existing report IDs
        self.existing_report_ids: List[int]
        # A set of existing report IDs that have been changed/updated after bulk upload
        self.updated_reports: Set[schema.Report] = set()
        # A set of uploaded report IDs will be used to create the `unchanged_reports` set
        self.uploaded_reports: Set[schema.Report] = set()
        # A child agency is an agency that the current agency has the permission to
        # upload data for. child_agency_name_to_id maps child agency name to id.
        self.child_agency_name_to_id = child_agency_name_to_id or {}

    def upload_workbook(
        self,
        session: Session,
        xls: pd.ExcelFile,
        metric_definitions: List[MetricDefinition],
    ) -> Tuple[
        Dict[str, List[DatapointJson]],
        Dict[Optional[str], List[JusticeCountsBulkUploadException]],
    ]:
        """Iterate through all tabs in an Excel spreadsheet and upload them
        to the Justice Counts database.
        """
        # 1. Fetch existing reports and datapoints for this agency, so that
        # we know what objects to update vs. what new objects to create.
        agency_ids = list(self.child_agency_name_to_id.values()) + [self.agency.id]
        reports = ReportInterface.get_reports_by_agency_ids(
            session, agency_ids=agency_ids, include_datapoints=True
        )
        self.existing_report_ids = [report.id for report in reports]
        reports_sorted_by_agency_id = sorted(reports, key=lambda x: x.source_id)
        reports_by_agency_id = {
            k: list(v)
            for k, v in groupby(
                reports_sorted_by_agency_id,
                key=lambda x: x.source_id,
            )
        }

        agency_id_to_time_range_to_reports: Dict[
            int, Dict[Tuple[Any, Any], List[schema.Report]]
        ] = defaultdict(dict)

        for agency_id, curr_reports in reports_by_agency_id.items():
            reports_sorted_by_time_range = sorted(
                curr_reports, key=lambda x: (x.date_range_start, x.date_range_end)
            )
            reports_by_time_range = {
                k: list(v)
                for k, v in groupby(
                    reports_sorted_by_time_range,
                    key=lambda x: (x.date_range_start, x.date_range_end),
                )
            }
            agency_id_to_time_range_to_reports[agency_id] = reports_by_time_range

        existing_datapoints_dict = ReportInterface.get_existing_datapoints_dict(
            reports=reports
        )
        existing_datapoints_dict_unchanged = {
            unique_key: datapoint.get_value()
            for unique_key, datapoint in existing_datapoints_dict.items()
        }
        sheet_name_to_metricfile = SYSTEM_TO_FILENAME_TO_METRICFILE[self.system.value]

        # 2. Sort sheet_names so that we process by aggregate sheets first
        # e.g. caseloads before caseloads_by_gender. This ensures we don't
        # infer an aggregate value when one is explicitly given.
        # Note that the regular sorting will work for this case, since
        # foobar will always come before foobar_by_xxx alphabetically.
        actual_sheet_names = sorted(xls.sheet_names)

        # 3. Now run through all sheets and process each in turn.
        invalid_sheet_names: List[str] = []
        sheet_name_to_df = pd.read_excel(xls, sheet_name=None)
        for sheet_name in actual_sheet_names:
            logging.info("Uploading %s", sheet_name)
            df = sheet_name_to_df[sheet_name]
            # Drop any rows that contain any NaN values
            try:
                df = df.dropna(axis=0, how="any", subset=["value"])
            except (KeyError, TypeError):
                # We will be in this case if the value column is missing,
                # and it's safe to ignore the error because we'll raise
                # an error about the missing value column later on in
                # _get_column_value.
                pass
            rows = df.to_dict("records")
            column_names = df.columns
            spreadsheet_uploader = SpreadsheetUploader(
                text_analyzer=self.text_analyzer,
                system=self.system,
                agency=self.agency,
                user_account=self.user_account,
                metric_key_to_agency_datapoints=self.metric_key_to_agency_datapoints,
                sheet_name=sheet_name,
                column_names=column_names,
                agency_id_to_time_range_to_reports=agency_id_to_time_range_to_reports,
                existing_datapoints_dict=existing_datapoints_dict,
                metric_key_to_timerange_to_total_value=self.metric_key_to_timerange_to_total_value,
                child_agency_name_to_id=self.child_agency_name_to_id,
            )
            spreadsheet_uploader.upload_sheet(
                session=session,
                rows=rows,
                invalid_sheet_names=invalid_sheet_names,
                metric_key_to_datapoint_jsons=self.metric_key_to_datapoint_jsons,
                metric_key_to_errors=self.metric_key_to_errors,
                uploaded_reports=self.uploaded_reports,
            )

        # 4. For any report that was updated, set its status to DRAFT
        self._update_report_status(
            existing_datapoints_dict_changed=existing_datapoints_dict,
            existing_datapoints_dict_unchanged=existing_datapoints_dict_unchanged,
            agency_id_to_time_range_to_reports=agency_id_to_time_range_to_reports,
        )

        # 5. Add any workbook errors to metric_key_to_errors
        self._add_workbook_errors(
            invalid_sheet_names=invalid_sheet_names,
            sheet_name_to_metricfile=sheet_name_to_metricfile,
            metric_definitions=metric_definitions,
            actual_sheet_names=actual_sheet_names,
        )

        return (
            self.metric_key_to_datapoint_jsons,
            self.metric_key_to_errors,
        )

    def _update_report_status(
        self,
        existing_datapoints_dict_changed: Dict[
            Tuple[datetime.date, datetime.date, int, str, Optional[str], Optional[str]],
            schema.Datapoint,
        ],
        existing_datapoints_dict_unchanged: Dict[
            Tuple[datetime.date, datetime.date, int, str, Optional[str], Optional[str]],
            Optional[float],
        ],
        agency_id_to_time_range_to_reports: Dict[
            int, Dict[Tuple[Any, Any], List[schema.Report]]
        ],
    ) -> None:
        """
        If a user uploads a workbook that changes a Report associated with
        previously uploaded data, that report's status is set to DRAFT. This can occur
        in the following cases:
            1. Previously uploaded datapoints have been changed
            2. A datapoint has been added to a previously uploaded Report
        """
        unique_key_difference = set(existing_datapoints_dict_changed.keys()).difference(
            existing_datapoints_dict_unchanged.keys()
        )
        for different_key in unique_key_difference:
            # datapoint that previously did not exist has been added to report
            agency_id = different_key[2]
            updated_report = agency_id_to_time_range_to_reports[agency_id][
                (different_key[0], different_key[1])
            ][0]
            # add report ID to set of updated report IDs to help the FE determine which existing reports have been updated
            if updated_report.id is not None and updated_report.id in set(
                self.existing_report_ids
            ):
                self.updated_reports.add(updated_report)

            if updated_report.status.value != "DRAFT":
                ReportInterface.update_report_metadata(
                    report=updated_report,
                    editor_id=self.user_account.id
                    if self.user_account is not None
                    else AUTOMATIC_UPLOAD_ID,
                    status=ReportStatus.DRAFT.value,
                )

        for (
            unique_key,
            datapoint,
        ) in existing_datapoints_dict_changed.items():
            if (
                unique_key in existing_datapoints_dict_unchanged
                and existing_datapoints_dict_unchanged[unique_key]
                != datapoint.get_value()
            ):
                # datapoint that previously existed has been updated/changed
                agency_id = unique_key[2]
                updated_report = agency_id_to_time_range_to_reports[agency_id][
                    (unique_key[0], unique_key[1])
                ][0]
                # add report ID to set of updated report IDs to help the FE determine which existing reports have been updated
                self.updated_reports.add(updated_report)

                if updated_report.status.value != "DRAFT":
                    ReportInterface.update_report_metadata(
                        report=updated_report,
                        editor_id=self.user_account.id
                        if self.user_account is not None
                        else AUTOMATIC_UPLOAD_ID,
                        status=ReportStatus.DRAFT.value,
                    )

    def _add_workbook_errors(
        self,
        invalid_sheet_names: List[str],
        sheet_name_to_metricfile: Dict[str, MetricFile],
        actual_sheet_names: List[str],
        metric_definitions: List[MetricDefinition],
    ) -> None:
        """Adds invalid sheet name errors to metric_key_to_errors."""

        # First, add invalid sheet name errors to metric_key_to_errors
        self._add_invalid_sheet_name_error(
            invalid_sheet_names=invalid_sheet_names,
            sheet_name_to_metricfile=sheet_name_to_metricfile,
        )

        # Next, add missing total, and breakdown warnings
        # to metric_key_to_errors
        self._add_missing_sheet_warnings(
            metric_definitions=metric_definitions,
            actual_sheet_names=actual_sheet_names,
        )

    def _add_invalid_sheet_name_error(
        self,
        invalid_sheet_names: List[str],
        sheet_name_to_metricfile: Dict[str, MetricFile],
    ) -> None:
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
            self.metric_key_to_errors[None].append(invalid_sheet_name_error)

    def _add_missing_sheet_warnings(
        self,
        metric_definitions: List[MetricDefinition],
        actual_sheet_names: List[str],
    ) -> None:
        """This function adds a Missing Breakdown Sheet warning if a breakdown sheet is missing.
        It also adds a Missing Total Sheet warning if the workbook is missing
        an aggregate total sheet."""
        for metric_definition in metric_definitions:
            sheet_name_to_metricfile = SYSTEM_TO_FILENAME_TO_METRICFILE[
                metric_definition.system.value
            ]
            if (
                metric_definition.disabled is True
                or DatapointInterface.is_metric_disabled(
                    metric_key_to_agency_datapoints=self.metric_key_to_agency_datapoints,
                    metric_key=metric_definition.key,
                )
            ):
                continue

            totals_filename = {
                sheet_name
                for sheet_name, metricfile in sheet_name_to_metricfile.items()
                if metricfile.definition.key == metric_definition.key
                and metricfile.disaggregation is None
            }.pop()
            breakdown_filenames = [
                sheet_name
                for sheet_name, metricfile in sheet_name_to_metricfile.items()
                if metricfile.definition.key == metric_definition.key
                and metricfile.disaggregation is not None
            ]

            # No longer raising 'Missing Metric' warning
            # If aggregate and breakdown sheets are not provided for a metric,
            # we will handle on the frontend
            for breakdown_filename in breakdown_filenames:
                # If the whole metric is not missing, add missing breakdown warning
                # for any missing breakdowns.
                if (
                    breakdown_filename not in actual_sheet_names
                    and totals_filename in actual_sheet_names
                ):
                    breakdown_warning = JusticeCountsBulkUploadException(
                        title="Missing Breakdown Sheet",
                        message_type=BulkUploadMessageType.WARNING,
                        description=f"Please provide data in a sheet named {breakdown_filename}.",
                        sheet_name=breakdown_filename,
                    )
                    self.metric_key_to_errors[metric_definition.key].append(
                        breakdown_warning
                    )

                # If the total sheet is missing, add missing totals warning.
                if (
                    totals_filename not in actual_sheet_names
                    and breakdown_filename in actual_sheet_names
                ):
                    missing_total_error = JusticeCountsBulkUploadException(
                        title="Missing Total Sheet",
                        message_type=BulkUploadMessageType.WARNING,
                        sheet_name=totals_filename,
                        description=(
                            f"No total values sheet was provided for this metric. The total values will be assumed "
                            f"to be equal to the sum of the breakdown values provided in {breakdown_filename}."
                        ),
                    )
                    self.metric_key_to_errors[metric_definition.key].append(
                        missing_total_error
                    )
                    # only need 1 totals warning per metric
                    break

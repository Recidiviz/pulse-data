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
from recidiviz.justice_counts.exceptions import JusticeCountsBulkUploadException
from recidiviz.justice_counts.metrics.metric_interface import MetricInterface
from recidiviz.justice_counts.report import ReportInterface
from recidiviz.justice_counts.types import DatapointJson
from recidiviz.justice_counts.utils.constants import AUTOMATIC_UPLOAD_ID, UploadMethod
from recidiviz.justice_counts.utils.datapoint_utils import get_value
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.schema.justice_counts.schema import ReportStatus


class WorkbookUploader:
    """Uploads Excel workbooks that comply with the Justice Counts technical specification"""

    def __init__(
        self,
        system: schema.System,
        agency: schema.Agency,
        metric_key_to_metric_interface: Dict[str, MetricInterface],
        child_agency_name_to_agency: Optional[Dict[str, schema.Agency]] = None,
        user_account: Optional[schema.UserAccount] = None,
        metric_key_to_errors: Optional[
            Dict[Optional[str], List[JusticeCountsBulkUploadException]]
        ] = None,  # TODO(#31446) Remove this parameter after complete rollout of WorkbookStandardizer
    ) -> None:
        self.system = system
        self.agency = agency
        self.user_account = user_account
        # metric_key_to_metric_interface maps metric keys to that agency's metric
        # interface for that metric. These metric interfaces only hold metric setting
        # information (such as if the metric is enabled or disabled, the metric's custom
        # reporting frequency, etc.), and do not hold information from reports (such as
        # aggregate or disaggregate report datapoints). The metric interfaces are passed
        # in already populated and won't be edited during the upload.
        self.metric_key_to_metric_interface = metric_key_to_metric_interface
        # metric_key_to_errors starts out empty and will be populated with
        # each metric's errors.
        self.metric_key_to_errors: Dict[
            Optional[str], List[JusticeCountsBulkUploadException]
        ] = (
            defaultdict(list) if metric_key_to_errors is None else metric_key_to_errors
        )
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
                # because it's a valid breakdown category. We
                # also don't want to treat "not" as a stop word because
                # it is an important distinction between breakdowns
                # (i.e Not Hispanic v. Hispanic).
                stop_words_to_remove={"other", "not"}
            )
        )
        self.agency_name_to_metric_key_to_timerange_to_total_value: Dict[
            str,
            Dict[str, Dict[Tuple[datetime.date, datetime.date], Optional[int]]],
        ] = defaultdict(lambda: defaultdict(dict))
        # A list of existing report IDs
        self.existing_report_ids: List[int]
        # A set of existing report IDs that have been changed/updated after bulk upload
        self.updated_reports: Set[schema.Report] = set()
        # A set of uploaded report IDs will be used to create the `unchanged_reports` set
        self.uploaded_reports: Set[schema.Report] = set()
        # A child agency is an agency that the current agency has the permission to
        # upload data for. child_agency_name_to_agency maps child agency name to agency.
        self.child_agency_name_to_agency = child_agency_name_to_agency or {}

    def upload_workbook(
        self,
        session: Session,
        xls: pd.ExcelFile,
        upload_method: UploadMethod,
    ) -> Tuple[
        Dict[str, List[DatapointJson]],
        Dict[Optional[str], List[JusticeCountsBulkUploadException]],
    ]:
        """
        Iterate through all tabs in an Excel spreadsheet and upload them
        to the Justice Counts database.
        upload_filetype: The type of file that was originally uploaded (CSV, XLSX, etc).
        """
        # 1. Fetch existing reports and datapoints for this agency, so that
        # we know what objects to update vs. what new objects to create.
        agency_ids = [a.id for a in self.child_agency_name_to_agency.values()] + [
            self.agency.id
        ]
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
            unique_key: get_value(datapoint=datapoint)
            for unique_key, datapoint in existing_datapoints_dict.items()
        }

        # 2. Sort sheet_names so that we process by aggregate sheets first
        # e.g. caseloads before caseloads_by_gender. This ensures we don't
        # infer an aggregate value when one is explicitly given.
        # Note that the regular sorting will work for this case, since
        # foobar will always come before foobar_by_xxx alphabetically.

        actual_sheet_names = sorted(xls.sheet_names)

        # 3. Now run through all sheets and process each in turn.
        sheet_name_to_df = pd.read_excel(xls, sheet_name=None)
        inserts: List[schema.Datapoint] = []
        updates: List[schema.Datapoint] = []
        histories: List[schema.DatapointHistory] = []
        for sheet_name in actual_sheet_names:
            logging.info("Uploading %s", sheet_name)
            df = sheet_name_to_df[sheet_name]
            # Drop any rows that contain any NaN values and make all column names lowercase.
            try:
                df = df.dropna(axis=0, how="any", subset=["value"])
                a = df.columns
                df.columns = [col.lower() for col in df.columns]
            except (KeyError, TypeError):
                # We will be in this case if the value column is missing,
                # and it's safe to ignore the error because we'll raise
                # an error about the missing value column later on in
                # _get_column_value.
                pass
            rows = df.to_dict("records")
            if len(rows) == 0:
                continue
            spreadsheet_uploader = SpreadsheetUploader(
                text_analyzer=self.text_analyzer,
                system=self.system,
                agency=self.agency,
                user_account=self.user_account,
                metric_key_to_metric_interface=self.metric_key_to_metric_interface,
                sheet_name=sheet_name,
                agency_id_to_time_range_to_reports=agency_id_to_time_range_to_reports,
                existing_datapoints_dict=existing_datapoints_dict,
                agency_name_to_metric_key_to_timerange_to_total_value=self.agency_name_to_metric_key_to_timerange_to_total_value,
                child_agency_name_to_agency=self.child_agency_name_to_agency,
            )
            spreadsheet_uploader.upload_sheet(
                session=session,
                inserts=inserts,
                updates=updates,
                histories=histories,
                rows=rows,
                metric_key_to_datapoint_jsons=self.metric_key_to_datapoint_jsons,
                metric_key_to_errors=self.metric_key_to_errors,
                uploaded_reports=self.uploaded_reports,
                upload_method=upload_method,
            )

        # 4. For any report that was updated, set its status to DRAFT
        self._update_report_status(
            existing_datapoints_dict_changed=existing_datapoints_dict,
            existing_datapoints_dict_unchanged=existing_datapoints_dict_unchanged,
            agency_id_to_time_range_to_reports=agency_id_to_time_range_to_reports,
        )

        DatapointInterface.flush_report_datapoints(
            session=session,
            inserts=inserts,
            updates=updates,
            histories=histories,
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

            # If a user tried to upload monthly data for a metric that is configured as
            # annual (or vice versa) and the agency has previously existing reports,
            # let's not assume that a previously existing report exists with the
            # correct/new time range. In other words, don't attempt to update report
            # statuses for reports that do not exist.
            updated_reports = agency_id_to_time_range_to_reports.get(agency_id, {}).get(
                ((different_key[0], different_key[1])), []
            )
            if len(updated_reports) == 0:
                continue
            updated_report = updated_reports[0]

            # add report ID to set of updated report IDs to help the FE determine which existing reports have been updated
            if updated_report.id is not None and updated_report.id in set(
                self.existing_report_ids
            ):
                self.updated_reports.add(updated_report)

            ReportInterface.update_report_metadata(
                report=updated_report,
                editor_id=(
                    self.user_account.id
                    if self.user_account is not None
                    else AUTOMATIC_UPLOAD_ID
                ),
                status=ReportStatus.DRAFT.value,
            )

        for (
            unique_key,
            datapoint,
        ) in existing_datapoints_dict_changed.items():
            if (
                unique_key in existing_datapoints_dict_unchanged
                and existing_datapoints_dict_unchanged[unique_key]
                != get_value(datapoint=datapoint)
            ):
                # datapoint that previously existed has been updated/changed
                agency_id = unique_key[2]

                # If a user tried to upload monthly data for a metric that is configured as
                # annual (or vice versa) and the agency has previously existing reports,
                # let's not assume that a previously existing report exists with the
                # correct/new time range. In other words, don't attempt to update report
                # statuses for reports that do not exist.
                updated_reports = agency_id_to_time_range_to_reports.get(
                    agency_id, {}
                ).get(((unique_key[0], unique_key[1])), [])
                if len(updated_reports) == 0:
                    continue
                updated_report = updated_reports[0]

                # add report ID to set of updated report IDs to help the FE determine which existing reports have been updated
                self.updated_reports.add(updated_report)

                ReportInterface.update_report_metadata(
                    report=updated_report,
                    editor_id=(
                        self.user_account.id
                        if self.user_account is not None
                        else AUTOMATIC_UPLOAD_ID
                    ),
                    status=ReportStatus.DRAFT.value,
                )

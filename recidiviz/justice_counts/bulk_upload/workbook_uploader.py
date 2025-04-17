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

import logging
from itertools import groupby
from typing import Any, Dict, List, Optional, Set

from recidiviz.justice_counts.bulk_upload.bulk_upload_metadata import BulkUploadMetadata
from recidiviz.justice_counts.bulk_upload.spreadsheet_uploader import (
    SpreadsheetUploader,
)
from recidiviz.justice_counts.bulk_upload.workbook_standardizer import (
    WorkbookStandardizer,
)
from recidiviz.justice_counts.datapoint import DatapointInterface, DatapointUniqueKey
from recidiviz.justice_counts.report import ReportInterface
from recidiviz.justice_counts.utils.constants import AUTOMATIC_UPLOAD_ID, UploadMethod
from recidiviz.justice_counts.utils.datapoint_utils import get_value
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.schema.justice_counts.schema import ReportStatus


class WorkbookUploader:
    """Uploads Excel workbooks that comply with the Justice Counts technical specification"""

    def __init__(
        self,
        metadata: BulkUploadMetadata,
    ) -> None:
        self.metadata = metadata
        self.existing_report_ids: List[int]
        # A set of existing report IDs that have been changed/updated after bulk upload
        self.updated_reports: Set[schema.Report] = set()
        # A set of uploaded report IDs will be used to create the `unchanged_reports` set
        self.uploaded_reports: Set[schema.Report] = set()
        # Number of new datapoints created from the upload of the workbook.
        self.num_new_datapoints = 0

    def upload_workbook(self, file: Any, file_name: str) -> None:
        """
        Uploads an Excel workbook to the Justice Counts database.

        This method iterates through all sheets in the workbook, processes data, and updates
        reports/datapoints accordingly.
        """

        # Fetch existing reports and datapoints for the agency to determine updates vs. new records.
        agency_ids = [agency.id for agency in self.metadata.child_agencies] + [
            self.metadata.agency.id
        ]
        reports = ReportInterface.get_reports_by_agency_ids(
            self.metadata.session, agency_ids=agency_ids, include_datapoints=True
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
            self.metadata.agency_id_to_time_range_to_reports[
                agency_id
            ] = reports_by_time_range

        # Store existing datapoints for comparison
        existing_datapoints_dict = ReportInterface.get_existing_datapoints_dict(reports)
        existing_datapoints_dict_unchanged = {
            unique_key: get_value(datapoint=datapoint)
            for unique_key, datapoint in existing_datapoints_dict.items()
        }

        # Initialize workbook processing
        workbook_standardizer = WorkbookStandardizer(metadata=self.metadata)

        # Prepare storage for database updates
        current_sheet_name, current_index, spreadsheet_uploader = (
            None,
            0,
            None,
        )
        for sheet_name, chunk in workbook_standardizer.get_dataframe_chunks(
            file, file_name
        ):
            if sheet_name != current_sheet_name:
                # If we're starting a new sheet, process the previous sheet.
                if spreadsheet_uploader is not None:
                    spreadsheet_uploader.compare_total_and_inferred_values()

                logging.info("Uploading sheet: %s", sheet_name)
                # Reset tracking variables for the new sheet
                current_sheet_name = sheet_name
                current_index = 0

                self.metadata.is_single_page_upload = any(
                    col in chunk.columns
                    for col in ["metric", "breakdown", "breakdown_category"]
                )

                spreadsheet_uploader = SpreadsheetUploader(
                    metadata=self.metadata,
                    sheet_name=sheet_name,
                    existing_datapoints_dict=existing_datapoints_dict,
                )

            rows = workbook_standardizer.standardize_dataframe(
                df=chunk, sheet_name=sheet_name, current_index=current_index
            )

            if len(rows) == 0:
                continue

            if spreadsheet_uploader is not None:
                spreadsheet_uploader.upload_dataframe_rows(
                    rows=rows,
                    uploaded_reports=self.uploaded_reports,
                )

            current_index += self.metadata.chunk_size
            DatapointInterface.flush_report_datapoints(
                session=self.metadata.session,
                inserts=self.metadata.inserts,
                updates=self.metadata.updates,
                histories=self.metadata.histories,
            )

        # Compare total and inferred values for the last file
        if spreadsheet_uploader is not None:
            spreadsheet_uploader.compare_total_and_inferred_values()
        workbook_standardizer.add_invalid_name_error()
        # Mark updated reports as DRAFT
        self._update_report_status(
            existing_datapoints_dict_changed=existing_datapoints_dict,
            existing_datapoints_dict_unchanged=existing_datapoints_dict_unchanged,
        )

    def _update_report_status(
        self,
        existing_datapoints_dict_changed: Dict[
            DatapointUniqueKey,
            schema.Datapoint,
        ],
        existing_datapoints_dict_unchanged: Dict[
            DatapointUniqueKey,
            Optional[float],
        ],
    ) -> None:
        """
        If a user uploads a workbook that changes a Report associated with
        previously uploaded data, that report's status is set to DRAFT. This can occur
        in the following cases:
            1. Previously uploaded datapoints have been changed
            2. A datapoint has been added to a previously uploaded Report
        """

        if self.metadata.upload_method == UploadMethod.AUTOMATED_BULK_UPLOAD:
            # When published reports are edited via SFTP, we do not revert them back to drafts.
            return

        unique_key_difference = set(existing_datapoints_dict_changed.keys()).difference(
            existing_datapoints_dict_unchanged.keys()
        )
        for different_key in unique_key_difference:
            # datapoint that previously did not exist has been added to report
            if get_value(existing_datapoints_dict_changed[different_key]) is not None:
                self.num_new_datapoints += 1
            agency_id = different_key[2]

            # If a user tried to upload monthly data for a metric that is configured as
            # annual (or vice versa) and the agency has previously existing reports,
            # let's not assume that a previously existing report exists with the
            # correct/new time range. In other words, don't attempt to update report
            # statuses for reports that do not exist.
            updated_reports = self.metadata.agency_id_to_time_range_to_reports.get(
                agency_id, {}
            ).get(((different_key[0], different_key[1])), [])
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
                    self.metadata.user_account.id
                    if self.metadata.user_account is not None
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
                if get_value(datapoint=datapoint) is None:
                    # If the datapoint was previously unreported (None) and now has a value,
                    # count it as a new datapoint since it was not reported before.
                    self.num_new_datapoints += 1

                # datapoint that previously existed has been updated/changed
                agency_id = unique_key[2]

                # If a user tried to upload monthly data for a metric that is configured as
                # annual (or vice versa) and the agency has previously existing reports,
                # let's not assume that a previously existing report exists with the
                # correct/new time range. In other words, don't attempt to update report
                # statuses for reports that do not exist.
                updated_reports = self.metadata.agency_id_to_time_range_to_reports.get(
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
                        self.metadata.user_account.id
                        if self.metadata.user_account is not None
                        else AUTOMATIC_UPLOAD_ID
                    ),
                    status=ReportStatus.DRAFT.value,
                )

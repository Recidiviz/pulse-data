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

import itertools
import logging
from collections import defaultdict
from itertools import groupby
from typing import Dict, List, Optional, Tuple

import pandas as pd
from sqlalchemy.orm import Session

from recidiviz.common.text_analysis import TextAnalyzer, TextMatchingConfiguration
from recidiviz.justice_counts.bulk_upload.bulk_upload import BulkUploader
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
from recidiviz.justice_counts.metrics.metric_registry import METRIC_KEY_TO_METRIC
from recidiviz.justice_counts.report import ReportInterface
from recidiviz.justice_counts.types import DatapointJson
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.schema.justice_counts.schema import ReportStatus


class WorkbookUploader:
    """Uploads Excel workbooks that comply with the Justice Counts technical specification"""

    def __init__(
        self,
        system: schema.System,
        agency_id: int,
        user_account: schema.UserAccount,
        metric_key_to_agency_datapoints: Dict[str, List[schema.Datapoint]],
    ) -> None:
        self.text_analyzer = TextAnalyzer(
            configuration=TextMatchingConfiguration(
                # We don't want to treat "other" as a stop word,
                # because it's a valid breakdown category
                stop_words_to_remove={"other"}
            )
        )
        self.system = system
        self.agency_id = agency_id
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
        to the Justice Counts database using the `BulkUpload.upload_rows` method defined in BulkUpload.py.
        If an error is encountered in a particular sheet, log it and continue.
        """
        # 1. Fetch existing reports and datapoints for this agency, so that
        # we know what objects to update vs. what new objects to create.
        reports = ReportInterface.get_reports_by_agency_id(
            session, agency_id=self.agency_id, include_datapoints=True
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
        # Sheets that have been successfully ingested in upload_rows will be added to
        # metric_key_to_successfully_ingested_sheet_names.
        metric_key_to_successfully_ingested_sheet_names: Dict[
            str, List[str]
        ] = defaultdict(list)
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
            bulk_upload = BulkUploader()
            bulk_upload.upload_rows(
                session=session,
                system=self.system,
                rows=rows,
                sheet_name=sheet_name,
                column_names=column_names,
                invalid_sheet_names=invalid_sheet_names,
                metric_key_to_successfully_ingested_sheet_names=metric_key_to_successfully_ingested_sheet_names,
                agency_id=self.agency_id,
                user_account=self.user_account,
                reports_by_time_range=reports_by_time_range,
                existing_datapoints_dict=existing_datapoints_dict,
                metric_key_to_datapoint_jsons=self.metric_key_to_datapoint_jsons,
                metric_key_to_errors=self.metric_key_to_errors,
                metric_key_to_agency_datapoints=self.metric_key_to_agency_datapoints,
            )

        # 4. For any report that was updated, set its status to DRAFT
        report: schema.Report  # make mypy happy
        for report in itertools.chain(*reports_by_time_range.values()):
            ReportInterface.update_report_metadata(
                report=report,
                editor_id=self.user_account.id,
                status=ReportStatus.DRAFT.value,
            )

        # 5. Add any workbook errors to metric_key_to_errors
        self._add_workbook_errors(
            invalid_sheet_names=invalid_sheet_names,
            sheet_name_to_metricfile=sheet_name_to_metricfile,
            metric_definitions=metric_definitions,
            metric_key_to_successfully_ingested_sheet_names=metric_key_to_successfully_ingested_sheet_names,
        )

        return self.metric_key_to_datapoint_jsons, self.metric_key_to_errors

    def _add_workbook_errors(
        self,
        invalid_sheet_names: List[str],
        sheet_name_to_metricfile: Dict[str, MetricFile],
        metric_key_to_successfully_ingested_sheet_names: Dict[str, List[str]],
        metric_definitions: List[MetricDefinition],
    ) -> None:
        """Adds invalid sheet name errors and missing metric errors to metric_key_to_errors."""

        # First, add invalid sheet name errors to metric_key_to_errors
        self._add_invalid_sheet_name_error(
            invalid_sheet_names=invalid_sheet_names,
            sheet_name_to_metricfile=sheet_name_to_metricfile,
        )

        # Next, add missing total, breakdown, and metric warnings
        # to metric_key_to_errors
        self._add_missing_sheet_warnings(
            metric_definitions=metric_definitions,
            metric_key_to_successfully_ingested_sheet_names=metric_key_to_successfully_ingested_sheet_names,
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
        metric_key_to_successfully_ingested_sheet_names: Dict[str, List[str]],
        metric_definitions: List[MetricDefinition],
    ) -> None:
        """This function adds an Missing Metric warning if all data is missing for a metric
        in the workbook. It adds a Missing Breakdown warning if a breakdown data is missing.
        Finally, it adds a Missing Total Value warning if the workbook is missing data in
        an aggregate total sheet."""
        successfully_ingested_sheet_names = set(
            itertools.chain(*metric_key_to_successfully_ingested_sheet_names.values())
        )
        for metric_definition in metric_definitions:
            sheet_name_to_metricfile = SYSTEM_TO_FILENAME_TO_METRICFILE[
                metric_definition.system.value
            ]
            if (
                metric_definition.disabled is not True
                and DatapointInterface.is_metric_disabled(
                    metric_key_to_agency_datapoints=self.metric_key_to_agency_datapoints,
                    metric_key=metric_definition.key,
                )
            ):
                continue
            unsuccessfully_ingested_sheet_names = {
                sheet_name
                for sheet_name, metricfile in sheet_name_to_metricfile.items()
                if metricfile.definition.key == metric_definition.key
                and metricfile.canonical_filename
                not in successfully_ingested_sheet_names
            }

            if len(unsuccessfully_ingested_sheet_names) == 0:
                continue

            totals_filename = {
                sheet_name
                for sheet_name, metricfile in sheet_name_to_metricfile.items()
                if metricfile.definition.key == metric_definition.key
                and metricfile.disaggregation is None
            }.pop()
            breakdown_filenames = {
                sheet_name
                for sheet_name, metricfile in sheet_name_to_metricfile.items()
                if metricfile.definition.key == metric_definition.key
                and metricfile.disaggregation is not None
            }
            # First add missing metric warning if all breakdowns and total sheets are missing.
            if (
                unsuccessfully_ingested_sheet_names
                == {totals_filename} | breakdown_filenames
            ):
                description_suffix = f"The following sheets were empty: {', '.join(unsuccessfully_ingested_sheet_names)}."
                missing_metric_warning = JusticeCountsBulkUploadException(
                    title="Missing Metric",
                    message_type=BulkUploadMessageType.WARNING,
                    description=(
                        f"No data for the {METRIC_KEY_TO_METRIC[metric_definition.key].display_name} metric was provided. "
                        + description_suffix
                    ),
                )
                self.metric_key_to_errors[metric_definition.key].append(
                    missing_metric_warning
                )
            else:
                # If the whole metric is not missing, add missing breakdown warning
                # for any missing breakdowns.
                for (
                    missing_breakdown_file
                ) in unsuccessfully_ingested_sheet_names.intersection(
                    breakdown_filenames
                ):
                    metricfile = sheet_name_to_metricfile[missing_breakdown_file]
                    description_suffix = f"Please provide data in a sheet named {missing_breakdown_file}."
                    breakdown_warning = JusticeCountsBulkUploadException(
                        title="Missing Breakdown Sheet",
                        message_type=BulkUploadMessageType.WARNING,
                        description=f"No data for the {metricfile.disaggregation.human_readable_name()} breakdown was provided. "  # type: ignore[union-attr]
                        + description_suffix,
                        sheet_name=missing_breakdown_file,
                    )
                    self.metric_key_to_errors[metric_definition.key].append(
                        breakdown_warning
                    )
                # If the total sheet is missing, add missing totals warning.
                if totals_filename in unsuccessfully_ingested_sheet_names:
                    breakdown_sheet = metric_key_to_successfully_ingested_sheet_names[
                        metric_definition.key
                    ].pop()
                    missing_total_error = JusticeCountsBulkUploadException(
                        title="Missing Total Value",
                        message_type=BulkUploadMessageType.WARNING,
                        sheet_name=totals_filename,
                        description=(
                            f"No total values were provided for this metric. The total values will be assumed "
                            f"to be equal to the sum of the breakdown values provided in {breakdown_sheet}."
                        ),
                    )
                    self.metric_key_to_errors[metric_definition.key].append(
                        missing_total_error
                    )

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
"""Functionality for upload of spreadsheet rows for a certain time range into the Justice Counts database."""

import datetime
from typing import Any, Dict, List, Optional, Tuple

from recidiviz.justice_counts.bulk_upload.bulk_upload_metadata import BulkUploadMetadata
from recidiviz.justice_counts.datapoint import DatapointUniqueKey
from recidiviz.justice_counts.dimensions.base import DimensionBase
from recidiviz.justice_counts.exceptions import (
    BulkUploadMessageType,
    JusticeCountsBulkUploadException,
)
from recidiviz.justice_counts.metricfile import MetricFile
from recidiviz.justice_counts.metrics.metric_disaggregation_data import (
    MetricAggregatedDimensionData,
)
from recidiviz.justice_counts.metrics.metric_interface import MetricInterface
from recidiviz.justice_counts.report import ReportInterface
from recidiviz.justice_counts.types import DatapointJson
from recidiviz.justice_counts.utils.constants import BREAKDOWN
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.utils.types import assert_type


class TimeRangeUploader:
    """Uploads rows from excel tables that comply with the Justice Counts technical specification"""

    def __init__(
        self,
        agency: schema.Agency,
        metadata: BulkUploadMetadata,
        time_range: Tuple[datetime.date, datetime.date],
        rows_for_this_time_range: List[Dict[str, Any]],
        metricfile: MetricFile,
        sheet_name: str,
        existing_datapoints_dict: Dict[DatapointUniqueKey, schema.Datapoint],
    ) -> None:
        self.time_range = time_range
        self.metadata = metadata
        self.existing_datapoints_dict = existing_datapoints_dict
        self.agency = agency
        self.rows_for_this_time_range = rows_for_this_time_range
        self.metricfile = metricfile
        self.sheet_name = sheet_name

    def upload_time_range(
        self,
        time_range_to_year_month: Dict[
            Tuple[datetime.date, datetime.date], Tuple[int, int]
        ],
        agency_name_to_metric_key_canonical_file_name_to_timerange_to_inferred_value: Dict[
            str,
            Dict[
                str,
                Dict[
                    str,
                    Dict[Tuple[datetime.date, datetime.date], Any],
                ],
            ],
        ],
        existing_report: Optional[schema.Report] = None,
    ) -> Tuple[schema.Report, List[DatapointJson]]:
        """Uploads rows for a certain time period and saves them in the JC database."""
        if existing_report is not None:
            report = existing_report
        else:  # existing report is None, so create the report
            year, month = time_range_to_year_month[self.time_range]
            reporting_frequency = ReportInterface.infer_reporting_frequency(
                self.time_range[0], self.time_range[1]
            )
            report = ReportInterface.create_report(
                session=self.metadata.session,
                agency_id=self.agency.id,
                year=year,
                month=month,
                frequency=reporting_frequency.value,
                user_account_id=(
                    self.metadata.user_account.id
                    if self.metadata.user_account is not None
                    else None
                ),
            )

        report_metric = self._get_report_metric_for_time_range(
            agency_name_to_metric_key_canonical_file_name_to_timerange_to_inferred_value=agency_name_to_metric_key_canonical_file_name_to_timerange_to_inferred_value
        )

        datapoint_json_list = ReportInterface.add_or_update_metric(
            session=self.metadata.session,
            inserts=self.metadata.inserts,
            updates=self.metadata.updates,
            histories=self.metadata.histories,
            report=report,
            report_metric=report_metric,
            user_account=self.metadata.user_account,
            uploaded_via_breakdown_sheet=self.metricfile.disaggregation is not None,
            existing_datapoints_dict=self.existing_datapoints_dict,
            agency=self.agency,
            upload_method=self.metadata.upload_method,
        )

        if existing_report is None:
            # If a report was created and no errors were thrown while adding data,
            # set it to draft mode.
            report.status = schema.ReportStatus.DRAFT

        return report, datapoint_json_list

    def _get_report_metric_for_time_range(
        self,
        agency_name_to_metric_key_canonical_file_name_to_timerange_to_inferred_value: Dict[
            str,
            Dict[
                str,
                Dict[
                    str,
                    Dict[Tuple[datetime.date, datetime.date], Any],
                ],
            ],
        ],
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
            {d: None for d in self.metricfile.disaggregation}  # type: ignore[attr-defined]
            if self.metricfile.disaggregation is not None
            else None
        )

        # If this file represents a metric without disaggregations,
        # there should only be one row for a given time period.
        for row in self.rows_for_this_time_range:
            # If this file represents a metric with a disaggregation,
            # there will likely be more than one row for a given time range;
            # there will be one row for each dimension value. Each will have
            # a value (i.e. the number or count) and a disaggregation value
            # (i.e. the category the count refers to, e.g. Male or Female).
            row_key = tuple(
                f"{k}:{v}" for k, v in row.items() if k not in ["value", "row_number"]
            )

            if row_key in self.metadata.sheet_name_to_seen_rows[self.sheet_name]:
                description = (
                    "There should only be a single row containing data "
                    f"for the time period {self.time_range[0].month}/{self.time_range[0].year}."
                )

                raise JusticeCountsBulkUploadException(
                    title="Too Many Rows",
                    description=description,
                    message_type=BulkUploadMessageType.ERROR,
                )

            self.metadata.sheet_name_to_seen_rows[self.sheet_name].add(row_key)

            value = (
                round(row["value"], 2)
                if isinstance(row["value"], float)
                else row["value"]
            )

            if self.metricfile.disaggregation is None:
                self.metadata.agency_name_to_metric_key_to_timerange_to_total_value[
                    self.agency.name
                ][self.metricfile.definition.key][self.time_range] = value
                aggregate_value = value
            if self.metricfile.disaggregation is not None:
                agency_name_to_metric_key_canonical_file_name_to_timerange_to_inferred_value[
                    self.agency.name
                ][
                    self.metricfile.definition.key
                ][
                    self.metricfile.canonical_filename
                ][
                    self.time_range
                ] += value
                # disaggregation_value is either "All" or an enum member,
                # e.g. "Male" for Gender, "Asian" for Race, "Felony" for OffenseType, etc
                disaggregation_value = (
                    row[assert_type(self.metricfile.disaggregation_column_name, str)]
                    if self.metadata.is_single_page_upload is False
                    else row[BREAKDOWN]
                )

                matching_disaggregation_member = self.metricfile.disaggregation(disaggregation_value)  # type: ignore
                dimension_to_value[matching_disaggregation_member] = value  # type: ignore[index]

        return MetricInterface(
            key=self.metricfile.definition.key,
            value=aggregate_value,
            aggregated_dimensions=(
                [MetricAggregatedDimensionData(dimension_to_value=dimension_to_value)]
                if dimension_to_value is not None
                else []
            ),
        )

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


import csv
import datetime
import logging
import os
from collections import defaultdict
from itertools import groupby
from typing import Any, Dict, List, Optional, Tuple, Type

from sqlalchemy.orm import Session

from recidiviz.justice_counts.bulk_upload.bulk_upload_helpers import (
    SYSTEM_TO_FILENAME_TO_METRICFILE,
    MetricFile,
)
from recidiviz.justice_counts.dimensions.base import DimensionBase
from recidiviz.justice_counts.metrics.report_metric import (
    ReportedAggregatedDimension,
    ReportMetric,
)
from recidiviz.justice_counts.report import ReportInterface
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.schema.justice_counts.schema import (
    ReportingFrequency,
    ReportStatus,
)


class BulkUploadInterface:
    """Functionality for bulk upload of data into the Justice Counts database."""

    @staticmethod
    def upload_directory(
        session: Session,
        directory: str,
        agency_id: int,
        system: schema.System,
        user_account: schema.UserAccount,
    ) -> List[Tuple[str, Exception]]:
        """Iterate through all CSV files in the given directory and upload them to the
        Justice Counts database using the `upload_file` method defined below.
        If an error is encountered on a particular file, log it and continue.
        """
        success_files = []
        error_files = []

        for filename in os.listdir(directory):
            if not filename.endswith(".csv"):
                continue

            filename = os.path.join(directory, filename)
            logging.info("Uploading %s", filename)
            try:
                BulkUploadInterface.upload_file(
                    session=session,
                    filename=filename,
                    agency_id=agency_id,
                    system=system,
                    user_account=user_account,
                )
                success_files.append(filename)
            except Exception as e:
                error_files.append((filename, e))
                raise e
        return error_files

    @staticmethod
    def upload_file(
        session: Session,
        filename: str,
        agency_id: int,
        system: schema.System,
        user_account: schema.UserAccount,
    ) -> None:
        """Takes as input a CSV file, formatted according to the technical specification,
        containing data for a particular metric across multiple time periods. Uploads this
        data into the Justice Counts database by breaking it up into Report objects, and
        either updating existing reports or creating new ones.

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
        reports = ReportInterface.get_reports_by_agency_id(session, agency_id=agency_id)

        # Step 1: Group any existing reports for this agency by time range.
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

        # Step 2: Determine which Justice Counts metric this file contains data for.
        metricfile = BulkUploadInterface._get_metricfile(
            filename=filename, system=system
        )
        metric_definition = metricfile.definition
        if len(metric_definition.reporting_frequencies) > 1:
            raise ValueError("Multiple reporting frequencies are not yet supported.")
        reporting_frequency = metric_definition.reporting_frequencies[0]

        # Step 3: Group the rows in this file by time range.
        with open(filename, "r", encoding="utf-8") as csvfile:
            rows = list(csv.DictReader(csvfile))

        # TODO(#13731): Make sure there are no unexpected columns in the file

        (
            rows_by_time_range,
            time_range_to_year_month,
        ) = BulkUploadInterface._get_rows_by_time_range(
            rows=rows, reporting_frequency=reporting_frequency
        )

        # Step 4: For each time range represented in the file, convert the
        # reported data into a ReportMetric object. If a report already
        # exists for this time range, update it with the ReportMetric.
        # Else, create a new report and add the ReportMetric.
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

            report_metric = BulkUploadInterface._get_report_metric(
                metricfile=metricfile,
                time_range=time_range,
                rows_for_this_time_range=rows_for_this_time_range,
            )

            ReportInterface.add_or_update_metric(
                session=session,
                report=report,
                report_metric=report_metric,
                user_account=user_account,
            )

            ReportInterface.update_report_metadata(
                session=session,
                report=report,
                editor_id=user_account.id,
                status=ReportStatus.DRAFT.value,
            )

    @staticmethod
    def _get_metricfile(filename: str, system: schema.System) -> MetricFile:
        try:
            stripped_filename = filename.split("/")[-1].split(".")[0]
        except Exception as e:
            raise ValueError(
                "Expected a filename of the format `metric_name.csv`."
            ) from e

        filename_to_metricfile = SYSTEM_TO_FILENAME_TO_METRICFILE[system.value]

        if stripped_filename not in filename_to_metricfile:
            raise ValueError(
                f"There is no metric corresponding to the filename `{stripped_filename}`. "
                "Check bulk_upload_helpers.py."
            )

        return filename_to_metricfile[stripped_filename]

    @staticmethod
    def _get_rows_by_time_range(
        rows: List[Dict[str, Any]], reporting_frequency: ReportingFrequency
    ) -> Tuple[
        Dict[Tuple[datetime.date, datetime.date], List[Dict[str, Any]]],
        Dict[Tuple[datetime.date, datetime.date], Tuple[int, int]],
    ]:
        rows_by_time_range = defaultdict(list)
        time_range_to_year_month = {}
        for row in rows:
            year = BulkUploadInterface._get_column_value(
                row=row, column_name="year", column_type=int
            )
            if reporting_frequency == ReportingFrequency.MONTHLY:
                month = BulkUploadInterface._get_column_value(
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

    @staticmethod
    def _get_report_metric(
        metricfile: MetricFile,
        time_range: Tuple[datetime.date, datetime.date],
        rows_for_this_time_range: List[Dict[str, Any]],
    ) -> ReportMetric:
        """Given a a set of rows from the CSV that all correspond to a single
        time period, convert the data in these rows to a ReportMetric object.
        If the metric associated with this CSV has no disaggregations, there
        should only be a single row for a single time period, and it contains
        the aggregate metric value. If the metric does have a disaggregation,
        there weill be several rows, one with the value for each category.
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
                raise ValueError(
                    f"Only expected one row for time range {time_range} "
                    f"because {metricfile.filename} doesn't have any disaggregations, "
                    f"but found {len(rows_for_this_time_range)} rows."
                )

            row = rows_for_this_time_range[0]
            aggregate_value = BulkUploadInterface._get_column_value(
                row=row, column_name="value", column_type=int
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
                value = BulkUploadInterface._get_column_value(
                    row=row, column_name="value", column_type=int
                )

                # disaggregation_value is either "All" or an enum member,
                # e.g. "Male" for Gender, "Asian" for Race, "Felony" for OffenseType, etc
                disaggregation_value = BulkUploadInterface._get_column_value(
                    row=row,
                    column_name=metricfile.disaggregation_column_name,
                    column_type=str,
                )
                # TODO(#13731): This should be more robust
                if disaggregation_value == "All":
                    aggregate_value = value
                else:
                    dimension_to_value[
                        metricfile.disaggregation(disaggregation_value)  # type: ignore
                    ] = value

        return ReportMetric(
            key=metricfile.definition.key,
            value=aggregate_value,
            contexts=[],
            aggregated_dimensions=[
                ReportedAggregatedDimension(dimension_to_value=dimension_to_value)
            ]
            if dimension_to_value is not None
            else [],
        )

    @staticmethod
    def _get_column_value(
        row: Dict[str, Any], column_name: str, column_type: Type
    ) -> Any:
        if column_name not in row:
            raise ValueError(f"Expected the column {column_name} to be present.")

        column_value = row[column_name]

        try:
            value = column_type(column_value)
        except Exception as e:
            raise ValueError(
                f"Expected the column {column_name} to be of type {column_type}."
            ) from e

        return value

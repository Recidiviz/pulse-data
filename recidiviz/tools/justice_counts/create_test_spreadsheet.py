# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
# You should have received a copy of the GNU General Public Licenses
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================

""" Creates a test spreadsheet that contains the errors specified at runtime.

python -m recidiviz.tools.justice_counts.create_test_spreadsheet \
  --system=LAW_ENFORCEMENT \
  --error=missing_metric \
  --opt_second_error-error=missing_total
"""

import argparse
import logging
import random
from typing import Any, Dict, List, Optional, Set

from recidiviz.justice_counts.metricfiles.metricfile_registry import (
    SYSTEM_TO_METRICFILES,
)
from recidiviz.justice_counts.metrics.metric_registry import METRICS_BY_SYSTEM
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.tests.justice_counts.spreadsheet_helpers import create_excel_file

logger = logging.getLogger(__name__)
error_options = [
    "missing_metric",
    "missing_total",
    "missing_breakdown",
    "invalid_month",
    "invalid_sheet_name",
    "unexpected_column",
    "unexpected_month",
    "unexpected_system",
    "unexpected_disaggregation",
    "missing_column",
    "invalid_value_type",
    "too_many_rows",
    "aggregate_error",
]


def create_parser() -> argparse.ArgumentParser:
    """Returns an argument parser for the script."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--system",
        required=True,
        choices=[system.value for system in schema.System],
    )

    parser.add_argument(
        "--error",
        required=True,
        choices=error_options + ["all", "success"],
    )

    parser.add_argument(
        "--opt_second_error",
        required=False,
        choices=error_options,
    )

    parser.add_argument(
        "--opt_third_error",
        required=False,
        choices=error_options,
    )

    parser.add_argument(
        "--opt_subsystem",
        required=False,
        choices=[system.value for system in schema.System.supervision_subsystems()],
    )

    parser.add_argument(
        "--opt_second_subsystem",
        required=False,
        choices=[system.value for system in schema.System.supervision_subsystems()],
    )

    parser.add_argument(
        "--opt_third_subsystem",
        required=False,
        choices=[system.value for system in schema.System.supervision_subsystems()],
    )

    return parser


class SpreadsheetGenerator:
    """Generates Excel workbooks with the errors provided."""

    def __init__(
        self,
        system: schema.System,
        errors: Set[Optional[str]],
        subsystems: Set[Optional[str]],
    ):
        self.system = system
        self.errors = (
            error_options if "all" in errors else {e for e in errors if e is not None}
        )
        self.subsystems = {s for s in subsystems if s is not None}
        self.used_sheet_names: Set[str] = set()

    def generate_test_spreadsheet(self) -> None:
        """
        Creates a sample spreadsheet based upon the error specified.
        """

        file_name = self._get_filename()
        error_to_sheet_name: Dict[str, Any] = {}
        error_to_sheet_name[
            "metric_key_to_subsystems"
        ] = self._generate_metric_key_to_subsystems()
        error_to_sheet_name["sheet_names_to_skip"] = self._create_sheet_names_to_skip()
        error_to_sheet_name["invalid_sheet_name"] = "invalid_sheet_name" in self.errors
        for error in self.errors:
            choose_monthly_metric, choose_breakdown_sheet, choose_aggregate_sheet = (
                False,
                False,
                False,
            )
            if error == "invalid_month":
                choose_monthly_metric = True
            if error == "aggregate_error":
                choose_breakdown_sheet = True
            if error == "unexpected_disaggregation":
                choose_aggregate_sheet = True

            sheet_name = self._get_random_sheet_name(
                current_error=error,
                choose_monthly_metric=choose_monthly_metric,
                choose_breakdown_sheet=choose_breakdown_sheet,
                choose_aggregate_sheet=choose_aggregate_sheet,
            )
            error_to_sheet_name[error] = (
                sheet_name if error != "aggregate_error" else {sheet_name}
            )

        create_excel_file(
            file_name=file_name,
            system=self.system,
            metric_key_to_subsystems=error_to_sheet_name.get(
                "metric_key_to_subsystems"
            ),
            sheet_names_to_skip=error_to_sheet_name.get("sheet_names_to_skip"),
            invalid_month_sheet_name=error_to_sheet_name.get("invalid_month"),
            add_invalid_sheet_name=error_to_sheet_name.get("invalid_sheet_name"),
            unexpected_column_sheet_name=error_to_sheet_name.get("unexpected_column"),
            unexpected_month_sheet_name=error_to_sheet_name.get("unexpected_month"),
            unexpected_disaggregation_sheet_name=error_to_sheet_name.get(
                "unexpected_disaggregation"
            ),
            missing_column_sheet_name=error_to_sheet_name.get("missing_column"),
            invalid_value_type_sheet_name=error_to_sheet_name.get("invalid_value_type"),
            too_many_rows_filename=error_to_sheet_name.get("too_many_rows"),
            sheet_names_to_vary_values=error_to_sheet_name.get("aggregate_error"),
            unexpected_system_sheet_name=error_to_sheet_name.get("unexpected_system"),
        )

    def _get_filename(self) -> str:
        return self.system.value + "_" + "_".join(self.errors) + ".xlsx"

    def _create_sheet_names_to_skip(self) -> Set[str]:
        missing_metric_filenames = ["expenses", "expenses_by_type"]
        missing_breakdown_filename = "funding_by_type"
        sheet_names_to_skip = set()
        system_to_missing_staff_sheet: Dict[schema.System, str] = {
            schema.System.DEFENSE: "judges_and_staff",
            schema.System.COURTS_AND_PRETRIAL: "judges_and_staff",
            schema.System.JAILS: "total_staff",
            schema.System.LAW_ENFORCEMENT: "staff",
            schema.System.PRISONS: "staff",
            schema.System.PROSECUTION: "total_staff",
            schema.System.SUPERVISION: "total_staff",
        }

        if "missing_metric" in self.errors or "all" in self.errors:
            sheet_names_to_skip = set(missing_metric_filenames)
        if "missing_total" in self.errors or "all" in self.errors:
            sheet_names_to_skip.add(system_to_missing_staff_sheet[self.system])
        if "missing_breakdown" in self.errors or "all" in self.errors:
            sheet_names_to_skip.add(missing_breakdown_filename)

        return sheet_names_to_skip

    def _get_random_sheet_name(
        self,
        current_error: str,
        choose_breakdown_sheet: bool = False,
        choose_monthly_metric: bool = False,
        choose_aggregate_sheet: bool = False,
    ) -> Optional[str]:
        """Returns a random sheet name to add an error. Each sheet name will
        only be chosen once, meaning there is only one error/warning per sheet.
        """
        if current_error not in self.errors and "all" not in self.errors:
            return None

        unused_sheet_names = set()
        for metricfile in SYSTEM_TO_METRICFILES[self.system]:
            # If the sheet already contains an error, skip.
            if metricfile.canonical_filename in self.used_sheet_names:
                continue

            # If the error should be in a breakdown sheet, but the current sheet
            # is for totals, skip.
            if choose_breakdown_sheet is True and metricfile.disaggregation is None:
                continue

            # If the error should be in a totals sheet, but the current sheet
            # is for breakdowns, skip.
            if choose_aggregate_sheet is True and metricfile.disaggregation is not None:
                continue

            # If the error should be for a monthly metric, but the current sheet
            # is for a metric that is reported annually, skip.
            if (
                choose_monthly_metric is True
                and metricfile.definition.reporting_frequency
                != schema.ReportingFrequency.MONTHLY
            ):
                continue
            unused_sheet_names.add(metricfile.canonical_filename)

        random_filename = unused_sheet_names.pop()
        self.used_sheet_names.add(random_filename)
        logging.info(
            "%s sheet contains %s error",
            random_filename,
            current_error,
        )
        return random_filename

    def _generate_metric_key_to_subsystems(
        self,
    ) -> Optional[Dict[str, List[schema.System]]]:
        if len(self.subsystems) == 0:
            return None

        metric_key_to_subsystems: Dict[str, List[schema.System]] = {}
        # Add "all" to subsystems list in order to add some metrics that are
        # not disaggregated subsystem.
        for metric in METRICS_BY_SYSTEM[schema.System.SUPERVISION.value]:
            subsystems = [schema.System[s] for s in self.subsystems]
            if random.choice([True, False]):
                metric_key_to_subsystems[metric.key] = subsystems
        return metric_key_to_subsystems


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = create_parser().parse_args()
    system_enum = schema.System(args.system)
    generator = SpreadsheetGenerator(
        system=system_enum,
        errors={args.error, args.opt_second_error, args.opt_third_error},
        subsystems={
            args.opt_subsystem,
            args.opt_second_subsystem,
            args.opt_third_subsystem,
        },
    )
    generator.generate_test_spreadsheet()

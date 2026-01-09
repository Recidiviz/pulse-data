# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Script for generating metrics describe Airflow environment creation time.

Example usage:

uv run python -m recidiviz.entrypoints.monitoring.report_airflow_environment_age \
    --project_id recidiviz-123
"""
import argparse

from recidiviz.entrypoints.entrypoint_interface import EntrypointInterface
from recidiviz.monitoring.airflow_monitoring import (
    report_airflow_environment_age_metrics,
)


class ReportAirflowEnvironmentAgeEntrypoint(EntrypointInterface):
    @staticmethod
    def get_parser() -> argparse.ArgumentParser:
        return EntrypointInterface.get_parser()

    @staticmethod
    def run_entrypoint(*, args: argparse.Namespace) -> None:
        report_airflow_environment_age_metrics()

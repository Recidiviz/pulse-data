# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Script for a Metric View Export to occur for a given export config to be called only
within the Airflow DAG's KubernetesPodOperator."""
import argparse

from recidiviz.common.constants.states import StateCode
from recidiviz.entrypoints.entrypoint_interface import EntrypointInterface
from recidiviz.metrics.export.view_export_manager import execute_metric_view_data_export


class MetricViewExportEntrypoint(EntrypointInterface):
    @staticmethod
    def get_parser() -> argparse.ArgumentParser:
        """Parses arguments for the Metric View Export process."""
        parser = argparse.ArgumentParser()

        parser.add_argument(
            "--state_code",
            help="The state code that the export should occur for",
            type=StateCode,
            choices=list(StateCode),
        )
        parser.add_argument(
            "--export_job_name",
            help="The export job name that the export should occur for",
            type=str,
            required=True,
        )
        return parser

    @staticmethod
    def run_entrypoint(*, args: argparse.Namespace) -> None:
        execute_metric_view_data_export(
            state_code=args.state_code,
            export_job_name=args.export_job_name,
        )

# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Entrypoint for generating SFTP ingest ready file upload timeliness monitoring metrics."""
import argparse
import json

from recidiviz.entrypoints.entrypoint_interface import EntrypointInterface
from recidiviz.monitoring.sftp_ingest_ready_file_timeliness import (
    report_sftp_ingest_ready_file_timeliness_metrics,
)


class ReportSftpIngestReadyFileTimelinessEntrypoint(EntrypointInterface):
    @staticmethod
    def get_parser() -> argparse.ArgumentParser:
        parser = EntrypointInterface.get_parser()
        parser.add_argument(
            "--upload_times_json",
            type=str,
            required=True,
            help="JSON string mapping region codes to ISO-formatted utc upload datetimes",
        )
        return parser

    @staticmethod
    def run_entrypoint(*, args: argparse.Namespace) -> None:
        upload_times = json.loads(args.upload_times_json)
        report_sftp_ingest_ready_file_timeliness_metrics(upload_times)

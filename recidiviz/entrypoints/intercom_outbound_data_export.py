# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Entrypoint for Intercom outbound content data export"""
import argparse
from datetime import datetime

from recidiviz.entrypoints.entrypoint_interface import EntrypointInterface


class IntercomOutboundDataExport(EntrypointInterface):
    """Entrypoint for Intercom outbound content data export"""

    @staticmethod
    def get_parser() -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser()

        parser.add_argument(
            "--destination-dataset",
            help="The dataset that the exported Intercom data will be added to",
            type=str,
            required=True,
        )
        parser.add_argument(
            "--start-datetime-inclusive",
            help="UTC datetime for start of date range, inclusive",
            type=datetime.fromisoformat,
            required=True,
        )
        parser.add_argument(
            "--end-datetime-inclusive",
            help="UTC datetime for end of date range, inclusive",
            type=datetime.fromisoformat,
            required=True,
        )

        return parser

    @staticmethod
    def run_entrypoint(*, args: argparse.Namespace) -> None:
        """Runs Intercom outbound content data export."""

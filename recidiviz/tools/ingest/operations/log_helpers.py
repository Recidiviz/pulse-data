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
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""Helpers for writing log files from ingest tools"""

import datetime
import os

import recidiviz

RECIDIVIZ_ROOT = os.path.abspath(os.path.join(recidiviz.__file__, "../.."))

# This directory is in the .gitignore so these files won't get committed
LOG_OUTPUT_DIR = "logs/"


def make_log_output_path(
    operation_name: str,
    region_code: str,
    date_string: str,
    dry_run: bool,
) -> str:
    folder = os.path.join(RECIDIVIZ_ROOT, LOG_OUTPUT_DIR)
    if not os.path.isdir(folder):
        os.mkdir(folder)
    return os.path.join(
        folder,
        f"{operation_name}_result_{region_code}_{date_string}_dry_run_{dry_run}_"
        f"{datetime.datetime.now().isoformat()}.txt",
    )

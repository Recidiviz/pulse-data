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
"""Utilities for static data generation."""

import os

from recidiviz.datasets.static_data import terraform_managed as large_data_csvs_module
from recidiviz.datasets.static_data.views import data as small_data_csvs_module


def make_small_static_data_file_output_path(name: str) -> str:
    """Output dir for static data CSVs that are small enough that they can be used
    to generate a static view query that contains this data.
    """
    return os.path.join(os.path.dirname(small_data_csvs_module.__file__), name)


def make_large_static_data_file_output_path(name: str) -> str:
    """Output dir for static data CSVs that are large enough that they can't be used
    to generate a static view query. In order to access these files, you must configure
    TF to generate a GCS-backed table from this CSV (using local-csv-backed-gcs-file).
    """
    return os.path.join(os.path.dirname(large_data_csvs_module.__file__), name)

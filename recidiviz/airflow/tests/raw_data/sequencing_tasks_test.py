# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Tests for sequencing_tasks"""
from unittest import TestCase

from recidiviz.airflow.dags.raw_data.sequencing_tasks import has_files_to_import


class SequencingTests(TestCase):
    """Tests for sequencing_tasks."""

    def test_has_files_to_import(self) -> None:
        assert not has_files_to_import.function(None)
        assert not has_files_to_import.function([])
        assert has_files_to_import.function(["a"])

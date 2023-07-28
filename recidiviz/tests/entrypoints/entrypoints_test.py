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
"""Tests for entrypoints"""
import os
import unittest
from typing import Dict, List

import yaml

import recidiviz
from recidiviz.entrypoints.entrypoint_executor import (
    ENTRYPOINTS_BY_NAME,
    parse_arguments,
)


class EntrypointsTest(unittest.TestCase):
    """Tests for entrypoints"""

    entrypoint_args_fixture: Dict[str, List[str]] = {}

    @classmethod
    def setUpClass(cls) -> None:
        with open(
            os.path.join(
                os.path.dirname(recidiviz.__file__),
                "airflow/tests/fixtures/entrypoints_args.yaml",
            ),
            "r",
            encoding="utf-8",
        ) as fixture_file:
            cls.entrypoint_args_fixture = yaml.safe_load(fixture_file)

    def test_all_known_entrypoint_args(self) -> None:
        # Test that all known entrypoint / entrypoint arguments can be parsed
        for argv in self.entrypoint_args_fixture.values():
            args, entrypoint_argv = parse_arguments(argv)
            entrypoint = ENTRYPOINTS_BY_NAME[args.entrypoint]
            entrypoint_args = entrypoint.get_parser().parse_args(entrypoint_argv)
            self.assertIsNotNone(entrypoint_args)

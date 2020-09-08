# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Tests checking that all the yamls deployed by our builds parse successfully.
"""
import os
import unittest

import yaml


class TestDeployYamls(unittest.TestCase):
    """Tests checking that all the yamls deployed by our builds parse
    successfully.
    """
    def path_for_build_file(self, file_name: str) -> str:
        return os.path.join(os.path.dirname(__file__),
                            '..', '..', file_name)

    def test_cron_yaml_parses(self):
        cron_file_path = self.path_for_build_file('cron.yaml')
        with open(cron_file_path, 'r') as ymlfile:
            file_contents = yaml.full_load(ymlfile)

        self.assertTrue(file_contents)

    def test_prod_yaml_parses(self):
        cron_file_path = self.path_for_build_file('prod.yaml')
        with open(cron_file_path, 'r') as ymlfile:
            file_contents = yaml.full_load(ymlfile)

        self.assertTrue(file_contents)

    def test_staging_yaml_parses(self):
        cron_file_path = self.path_for_build_file('staging.yaml')
        with open(cron_file_path, 'r') as ymlfile:
            file_contents = yaml.full_load(ymlfile)

        self.assertTrue(file_contents)

    def test_travis_yaml_parses(self):
        cron_file_path = self.path_for_build_file('.travis.yml')
        with open(cron_file_path, 'r') as ymlfile:
            file_contents = yaml.full_load(ymlfile)

        self.assertTrue(file_contents)

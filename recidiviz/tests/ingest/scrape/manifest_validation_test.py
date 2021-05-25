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

"""Contains logic to ensure the manifest files are completely populated"""
import glob
import os
import unittest

import yaml

import recidiviz.ingest.scrape.regions

_REGIONS_DIR = os.path.dirname(recidiviz.ingest.scrape.regions.__file__)


class ManifestValidationTest(unittest.TestCase):
    """Test to ensure manifest files are completely populated"""

    def test_AllSet(self) -> None:
        manifest_filenames = glob.glob(_REGIONS_DIR + "/**/manifest.yaml")
        for manifest_filename in manifest_filenames:
            with open(manifest_filename, "r") as yaml_file:
                manifest = yaml.full_load(yaml_file)
                for val in manifest.values():
                    self.assertIsNotNone(
                        val, "Must set all fields in {}".format(manifest_filename)
                    )

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
"""Tests that each regions direct ingest directory is set up properly."""
import os
import re
import unittest
from typing import List, Callable

from mock import patch
import yaml

import recidiviz
from recidiviz.ingest.direct.controllers.direct_ingest_view_collector import DirectIngestPreProcessedIngestViewCollector
from recidiviz.ingest.direct.controllers.direct_ingest_raw_file_import_manager import \
    DirectIngestRegionRawFileConfig
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_controller import GcsfsDirectIngestController
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.regions import get_region

_REGIONS_DIR = os.path.dirname(recidiviz.ingest.direct.regions.__file__)

_REGION_REGEX = re.compile(r'us_[a-z]{2}(_[a-z]+)?')


class DirectIngestRegionDirStructureTest(unittest.TestCase):
    """Tests that each regions direct ingest directory is set up properly."""

    def setUp(self) -> None:
        self.bq_client_patcher = patch('google.cloud.bigquery.Client')
        self.storage_client_patcher = patch('google.cloud.storage.Client')
        self.task_client_patcher = patch('google.cloud.tasks_v2.CloudTasksClient')
        self.bq_client_patcher.start()
        self.storage_client_patcher.start()
        self.task_client_patcher.start()

    def tearDown(self) -> None:
        self.bq_client_patcher.stop()
        self.storage_client_patcher.stop()
        self.task_client_patcher.stop()

    def _get_existing_region_dir_paths(self):
        return [os.path.join(_REGIONS_DIR, d) for d in self._get_existing_region_dir_names()]

    def _get_existing_region_dir_names(self) -> List[str]:
        return [d for d in os.listdir(_REGIONS_DIR)
                if os.path.isdir(os.path.join(_REGIONS_DIR, d)) and not d.startswith('__')]

    def test_region_dirname_matches_pattern(self):
        for d in self._get_existing_region_dir_names():
            self.assertIsNotNone(re.match(_REGION_REGEX, d),
                                 f'Region [{d}] does not match expected region pattern.')

    def run_check_valid_yamls_exist_in_all_regions(self,
                                                   generate_yaml_name_fn: Callable[[str], str],
                                                   validate_contents_fn: Callable[[str, object], None]):
        for dir_path in self._get_existing_region_dir_paths():
            region_code = os.path.basename(dir_path)

            yaml_path = os.path.join(dir_path, generate_yaml_name_fn(region_code))
            self.assertTrue(os.path.exists(yaml_path), f'Path [{yaml_path}] does not exist.')
            with open(yaml_path, 'r') as ymlfile:
                file_contents = yaml.full_load(ymlfile)
                self.assertTrue(file_contents)
                validate_contents_fn(yaml_path, file_contents)

    def test_manifest_yaml_format(self):
        def validate_manifest_contents(file_path: str, file_contents: object):

            if not isinstance(file_contents, dict):
                self.fail(f'File contents type [{type(file_contents)}], expected dict.')

            manifest_yaml_required_keys = [
                'agency_name',
                'agency_type',
                'timezone',
                'environment',
                'shared_queue',
                'jurisdiction_id',
            ]

            for k in manifest_yaml_required_keys:
                self.assertTrue(k in file_contents, f'Key [{k}] not in [{file_path}]')
                self.assertTrue(file_contents[k], f'Contents of key [{k}] are falsy')

        self.run_check_valid_yamls_exist_in_all_regions(lambda region_code: 'manifest.yaml',
                                                        validate_manifest_contents)

    def test_region_controller_exists_and_builds(self):
        for dir_path in self._get_existing_region_dir_paths():
            region_code = os.path.basename(dir_path)
            controller_path = os.path.join(dir_path, f'{region_code}_controller.py')
            self.assertTrue(os.path.exists(controller_path), f'Path [{controller_path}] does not exist.')

            region = get_region(region_code, is_direct_ingest=True)
            with local_project_id_override('project'):
                self.assertIsNotNone(region.get_ingestor_class())

    def test_region_controller_builds(self):
        for dir_path in self._get_existing_region_dir_paths():
            region_code = os.path.basename(dir_path)

            region = get_region(region_code, is_direct_ingest=True)
            with local_project_id_override('project'):
                self.assertIsNotNone(region.get_ingestor())

    def test_raw_files_yaml_parses_all_regions(self):
        for region_code in self._get_existing_region_dir_names():
            region = get_region(region_code, is_direct_ingest=True)

            raw_file_manager = DirectIngestRegionRawFileConfig(region_code=region.region_code)

            if region.raw_data_bq_imports_enabled_env is not None:
                self.assertTrue(raw_file_manager.raw_file_configs)
            config_file_tags = set()
            for config in raw_file_manager.raw_file_configs.values():
                self.assertTrue(config.file_tag not in config_file_tags,
                                f"Multiple raw file configs defined with the same file_tag [{config.file_tag}]")
                config_file_tags.add(config.file_tag)

    def test_collect_ingest_views(self):
        with local_project_id_override('project'):
            for region_code in self._get_existing_region_dir_names():
                region = get_region(region_code, is_direct_ingest=True)

                controller_class = region.get_ingestor_class()
                if not issubclass(controller_class, GcsfsDirectIngestController):
                    continue

                _ = DirectIngestPreProcessedIngestViewCollector(
                    region, controller_class.get_file_tag_rank_list()).collect_views()

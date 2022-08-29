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
import abc
import os
import re
import unittest
from datetime import datetime
from types import ModuleType
from typing import Callable, List, Optional, Tuple

import yaml
from mock import patch
from parameterized import parameterized

from recidiviz.big_query.big_query_utils import normalize_column_name_for_bq
from recidiviz.cloud_storage.gcsfs_path import GcsfsBucketPath, GcsfsFilePath
from recidiviz.common.constants.states import PLAYGROUND_STATE_INFO, StateCode
from recidiviz.ingest.direct import regions, templates
from recidiviz.ingest.direct.controllers import direct_ingest_controller_factory
from recidiviz.ingest.direct.controllers.base_direct_ingest_controller import (
    BaseDirectIngestController,
)
from recidiviz.ingest.direct.controllers.direct_ingest_controller_factory import (
    DirectIngestControllerFactory,
)
from recidiviz.ingest.direct.direct_ingest_regions import get_direct_ingest_region
from recidiviz.ingest.direct.gcs.direct_ingest_gcs_file_system import (
    to_normalized_unprocessed_raw_file_name,
)
from recidiviz.ingest.direct.gcs.filename_parts import filename_parts_from_path
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_import_manager import (
    DirectIngestRawFileConfig,
    DirectIngestRegionRawFileConfig,
)
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_table_migration import (
    DeleteFromRawTableMigration,
    UpdateRawTableMigration,
)
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_table_migration_collector import (
    DirectIngestRawTableMigrationCollector,
)
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_existing_region_dir_names,
    get_existing_region_dir_paths,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.errors import DirectIngestError
from recidiviz.ingest.direct.views.direct_ingest_view_collector import (
    DirectIngestPreProcessedIngestViewCollector,
)
from recidiviz.utils.environment import GCPEnvironment

_REGION_REGEX = re.compile(r"us_[a-z]{2}(_[a-z]+)?")


class DirectIngestRegionDirStructureBase:
    """Tests that each regions direct ingest directory is set up properly."""

    def setUp(self) -> None:
        self.bq_client_patcher = patch("google.cloud.bigquery.Client")
        self.storage_client_patcher = patch("google.cloud.storage.Client")
        self.task_client_patcher = patch("google.cloud.tasks_v2.CloudTasksClient")
        self.bq_client_patcher.start()
        self.storage_client_patcher.start()
        self.task_client_patcher.start()

    def tearDown(self) -> None:
        self.bq_client_patcher.stop()
        self.storage_client_patcher.stop()
        self.task_client_patcher.stop()

    @property
    @abc.abstractmethod
    def region_dir_names(self) -> List[str]:
        pass

    @property
    @abc.abstractmethod
    def region_dir_paths(self) -> List[str]:
        pass

    @property
    @abc.abstractmethod
    def region_module_override(self) -> ModuleType:
        pass

    @property
    @abc.abstractmethod
    def test(self) -> unittest.TestCase:
        pass

    def test_region_dirname_matches_pattern(self) -> None:
        for d in self.region_dir_names:
            self.test.assertIsNotNone(
                re.match(_REGION_REGEX, d),
                f"Region [{d}] does not match expected region pattern.",
            )

    def run_check_valid_yamls_exist_in_all_regions(
        self,
        generate_yaml_name_fn: Callable[[str], str],
        validate_contents_fn: Callable[[str, object], None],
    ) -> None:
        for dir_path in self.region_dir_paths:
            region_code = os.path.basename(dir_path)

            yaml_path = os.path.join(dir_path, generate_yaml_name_fn(region_code))
            self.test.assertTrue(
                os.path.exists(yaml_path), f"Path [{yaml_path}] does not exist."
            )
            with open(yaml_path, "r", encoding="utf-8") as ymlfile:
                file_contents = yaml.full_load(ymlfile)
                self.test.assertTrue(file_contents)
                validate_contents_fn(yaml_path, file_contents)

    def test_manifest_yaml_format(self) -> None:
        def validate_manifest_contents(file_path: str, file_contents: object) -> None:

            if not isinstance(file_contents, dict):
                self.test.fail(
                    f"File contents type [{type(file_contents)}], expected dict."
                )

            manifest_yaml_required_keys = [
                "agency_name",
                "environment",
            ]

            for k in manifest_yaml_required_keys:
                self.test.assertTrue(
                    k in file_contents, f"Key [{k}] not in [{file_path}]"
                )
                self.test.assertTrue(
                    file_contents[k], f"Contents of key [{k}] are falsy"
                )

        self.run_check_valid_yamls_exist_in_all_regions(
            lambda region_code: "manifest.yaml", validate_manifest_contents
        )

    def test_region_controller_exists_and_builds(self) -> None:
        for dir_path in self.region_dir_paths:
            region_code = os.path.basename(dir_path)
            controller_path = os.path.join(dir_path, f"{region_code}_controller.py")
            self.test.assertTrue(
                os.path.exists(controller_path),
                f"Path [{controller_path}] does not exist.",
            )

            region = get_direct_ingest_region(
                region_code, region_module_override=self.region_module_override
            )
            with patch(
                "recidiviz.utils.metadata.project_id", return_value="recidiviz-456"
            ):
                controller_class = DirectIngestControllerFactory.get_controller_class(
                    region
                )
                self.test.assertIsNotNone(controller_class)
                self.test.assertEqual(region_code, controller_class.region_code())

    def test_region_controller_builds(
        self,
    ) -> None:
        for region_code in self.region_dir_names:
            with patch(
                "recidiviz.utils.metadata.project_id", return_value="recidiviz-456"
            ):
                controller = DirectIngestControllerFactory.build(
                    region_code=region_code,
                    ingest_instance=DirectIngestInstance.PRIMARY,
                    allow_unlaunched=True,
                    region_module_override=self.region_module_override,
                )
                self.test.assertIsNotNone(controller)
                self.test.assertIsInstance(controller, BaseDirectIngestController)

    def test_raw_files_yaml_parses_all_regions(self) -> None:
        for region_code in self.region_dir_names:
            region = get_direct_ingest_region(
                region_code, region_module_override=self.region_module_override
            )

            with patch(
                "recidiviz.utils.metadata.project_id", return_value="recidiviz-456"
            ):
                controller = DirectIngestControllerFactory.build(
                    region_code=region_code,
                    ingest_instance=DirectIngestInstance.PRIMARY,
                    allow_unlaunched=True,
                    region_module_override=self.region_module_override,
                )

            builders = DirectIngestPreProcessedIngestViewCollector(
                region, controller.get_ingest_view_rank_list()
            ).collect_view_builders()

            raw_file_manager = DirectIngestRegionRawFileConfig(
                region_code=region_code,
                region_module=self.region_module_override,
            )

            if builders or raw_file_manager.raw_file_configs:
                if region.is_ingest_launched_in_env() is not None:
                    self.test.assertTrue(raw_file_manager.raw_file_configs)
                config_file_tags = set()
                for config in raw_file_manager.raw_file_configs.values():
                    self.test.assertTrue(
                        config.file_tag not in config_file_tags,
                        f"Multiple raw file configs defined with the same "
                        f"file_tag [{config.file_tag}]",
                    )
                    config_file_tags.add(config.file_tag)

                    path = GcsfsFilePath.from_directory_and_file_name(
                        GcsfsBucketPath("fake-bucket"),
                        to_normalized_unprocessed_raw_file_name(
                            f"{config.file_tag}.csv",
                        ),
                    )
                    parts = filename_parts_from_path(path)
                    self.test.assertEqual(parts.file_tag, config.file_tag)

                    # Assert that normalized column names in the config match the output of
                    # the column name normalizer function
                    for column in config.columns:
                        normalized_column_name = normalize_column_name_for_bq(
                            column.name
                        )
                        self.test.assertEqual(column.name, normalized_column_name)

    @parameterized.expand(
        [
            ("build_prod", "recidiviz-123", GCPEnvironment.PRODUCTION.value),
            ("build_staging", "recidiviz-staging", GCPEnvironment.STAGING.value),
        ]
    )
    def test_collect_and_build_ingest_view_builders(
        self, _name: str, project_id: str, environment: GCPEnvironment
    ) -> None:
        with patch(
            "recidiviz.utils.environment.get_gcp_environment", return_value=environment
        ):
            with patch("recidiviz.utils.metadata.project_id", return_value=project_id):
                for region_code in self.region_dir_names:
                    region = get_direct_ingest_region(
                        region_code, region_module_override=self.region_module_override
                    )

                    with patch(
                        "recidiviz.utils.metadata.project_id",
                        return_value="recidiviz-456",
                    ):
                        controller = DirectIngestControllerFactory.build(
                            region_code=region_code,
                            ingest_instance=DirectIngestInstance.PRIMARY,
                            allow_unlaunched=True,
                            region_module_override=self.region_module_override,
                        )

                    builders = DirectIngestPreProcessedIngestViewCollector(
                        region, controller.get_ingest_view_rank_list()
                    ).collect_view_builders()
                    for builder in builders:
                        builder.build()

    def test_collect_and_build_raw_table_migrations(self) -> None:
        with patch("recidiviz.utils.metadata.project_id", return_value="recidiviz-789"):
            for region_code in self.region_dir_names:
                raw_file_manager = DirectIngestRegionRawFileConfig(
                    region_code=region_code, region_module=self.region_module_override
                )
                collector = DirectIngestRawTableMigrationCollector(
                    region_code, regions_module_override=self.region_module_override
                )
                # Test this doesn't crash
                _ = collector.collect_raw_table_migration_queries(
                    sandbox_dataset_prefix=None
                )

                # Check that migrations are valid
                migrations = collector.collect_raw_table_migrations()
                for migration in migrations:
                    self.test.assertTrue(
                        migration.file_tag in raw_file_manager.raw_file_tags,
                        f"Tag {migration.file_tag} listed in migration for region "
                        f"[{region_code}] is not listed in config.",
                    )

                    raw_file_config = raw_file_manager.raw_file_configs[
                        migration.file_tag
                    ]
                    for col_name in migration.filters:
                        self.assertColumnIsDocumented(
                            migration.file_tag, col_name, raw_file_config
                        )
                    if isinstance(migration, UpdateRawTableMigration):
                        for col_name in migration.updates:
                            self.assertColumnIsDocumented(
                                migration.file_tag, col_name, raw_file_config
                            )

                # Check that update_datetime_filters, filters and optional updates are unique
                for migration in migrations:
                    if isinstance(migration, UpdateRawTableMigration):
                        distinct_update_values: List[
                            Tuple[
                                Optional[List[datetime]],
                                List[Tuple[str, str]],
                                List[Tuple[str, Optional[str]]],
                            ]
                        ] = []
                        update_values = (
                            migration.update_datetime_filters,
                            list(migration.filters.items()),
                            list(migration.updates.items()),
                        )
                        self.test.assertFalse(update_values in distinct_update_values)
                        distinct_update_values.append(update_values)
                    if isinstance(migration, DeleteFromRawTableMigration):
                        distinct_deletion_values: List[
                            Tuple[Optional[List[datetime]], List[Tuple[str, str]]]
                        ] = []
                        deletion_values = (
                            migration.update_datetime_filters,
                            list(migration.filters.items()),
                        )
                        self.test.assertFalse(
                            deletion_values in distinct_deletion_values
                        )
                        distinct_deletion_values.append(deletion_values)

    def assertColumnIsDocumented(
        self, file_tag: str, col_name: str, raw_file_config: DirectIngestRawFileConfig
    ) -> None:
        documented_column_names = {
            c.name for c in raw_file_config.columns if c.description
        }
        self.test.assertTrue(
            col_name in documented_column_names,
            f"Found column [{col_name}] listed as a filter column in a migration for file "
            f"tag [{file_tag}] which either not listed or missing a docstring.",
        )


class DirectIngestRegionDirStructure(
    DirectIngestRegionDirStructureBase, unittest.TestCase
):
    """Tests properties of recidiviz/ingest/direct/regions."""

    @property
    def region_dir_names(self) -> List[str]:
        return get_existing_region_dir_names()

    @property
    def region_dir_paths(self) -> List[str]:
        return get_existing_region_dir_paths()

    @property
    @abc.abstractmethod
    def test(self) -> unittest.TestCase:
        return self

    @property
    def region_module_override(self) -> ModuleType:
        return regions

    def test_regions_are_clean(self) -> None:
        """Check that all existing region directories start with a valid state code."""
        for region in self.region_dir_names:
            self.test.assertTrue(StateCode.is_state_code(region[:5]))

    def test_playground_regions_are_marked(self) -> None:
        for region_code in PLAYGROUND_STATE_INFO:
            region = get_direct_ingest_region(
                region_code, region_module_override=self.region_module_override
            )
            self.assertTrue(region.playground)

    def test_playground_regions_do_not_run_in_production(self) -> None:
        # The playground regions should be supported in staging
        with patch(
            "recidiviz.utils.environment.get_gcp_environment",
            return_value=GCPEnvironment.STAGING.value,
        ), patch(
            "recidiviz.utils.metadata.project_id", return_value="recidiviz-staging"
        ):
            for region_code in PLAYGROUND_STATE_INFO:
                DirectIngestControllerFactory.build(
                    region_code=region_code.lower(),
                    ingest_instance=DirectIngestInstance.PRIMARY,
                    allow_unlaunched=False,
                )

        # But they should not be supported in production
        with patch(
            "recidiviz.utils.environment.get_gcp_environment",
            return_value=GCPEnvironment.PRODUCTION.value,
        ), patch("recidiviz.utils.metadata.project_id", return_value="recidiviz-123"):
            for region_code in PLAYGROUND_STATE_INFO:
                with self.assertRaisesRegex(DirectIngestError, "Bad environment"):
                    DirectIngestControllerFactory.build(
                        region_code=region_code.lower(),
                        ingest_instance=DirectIngestInstance.PRIMARY,
                        allow_unlaunched=False,
                    )


class DirectIngestRegionTemplateDirStructure(
    DirectIngestRegionDirStructureBase, unittest.TestCase
):
    """Tests properties of recidiviz/ingest/direct/templates."""

    def setUp(self) -> None:
        super().setUp()

        # Ensures StateCode.US_XX is properly loaded
        self.supported_regions_patcher = patch(
            f"{direct_ingest_controller_factory.__name__}.get_supported_direct_ingest_region_codes"
        )
        self.mock_supported_regions = self.supported_regions_patcher.start()
        self.mock_supported_regions.return_value = self.region_dir_names

    def tearDown(self) -> None:
        super().tearDown()
        self.supported_regions_patcher.stop()

    @property
    def region_dir_names(self) -> List[str]:
        return [StateCode.US_XX.value.lower()]

    @property
    def region_dir_paths(self) -> List[str]:
        return [
            os.path.join(os.path.dirname(templates.__file__), d)
            for d in self.region_dir_names
        ]

    @property
    def test(self) -> unittest.TestCase:
        return self

    @property
    def region_module_override(self) -> ModuleType:
        return templates

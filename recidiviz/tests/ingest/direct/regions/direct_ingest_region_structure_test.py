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
from collections import defaultdict
from datetime import datetime
from types import ModuleType
from typing import Callable, Dict, List, Optional, Set, Tuple

import pytest
import yaml
from mock import patch
from parameterized import parameterized

from recidiviz.big_query.big_query_utils import normalize_column_name_for_bq
from recidiviz.cloud_storage.gcsfs_path import GcsfsBucketPath, GcsfsFilePath
from recidiviz.common.constants.operations.direct_ingest_instance_status import (
    DirectIngestStatus,
)
from recidiviz.common.constants.states import PLAYGROUND_STATE_INFO, StateCode
from recidiviz.common.file_system import is_non_empty_code_directory
from recidiviz.ingest.direct import direct_ingest_regions, regions, templates
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
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_collector import (
    IngestViewManifestCollector,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_results_parser_delegate import (
    IngestViewResultsParserDelegateImpl,
)
from recidiviz.ingest.direct.metadata.postgres_direct_ingest_instance_status_manager import (
    PostgresDirectIngestInstanceStatusManager,
)
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_table_migration import (
    DeleteFromRawTableMigration,
    UpdateRawTableMigration,
)
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_table_migration_collector import (
    DirectIngestRawTableMigrationCollector,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRawFileConfig,
    DirectIngestRegionRawFileConfig,
    RawTableColumnFieldType,
)
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_existing_direct_ingest_states,
    get_existing_region_dir_paths,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.errors import DirectIngestError
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder_collector import (
    DirectIngestViewQueryBuilderCollector,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tests.common.constants.state.external_id_types_test import (
    get_external_id_types,
)
from recidiviz.tests.ingest.direct import direct_ingest_fixtures
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers
from recidiviz.utils import environment, metadata
from recidiviz.utils.environment import GCPEnvironment

_REGION_REGEX = re.compile(r"us_[a-z]{2}(_[a-z]+)?")
YAML_LANGUAGE_SERVER_PRAGMA = re.compile(
    r"^# yaml-language-server: \$schema=(?P<schema_path>.*schema.json)$"
)


@pytest.mark.uses_db
class DirectIngestRegionDirStructureBase:
    """Tests that each regions direct ingest directory is set up properly."""

    # Stores the location of the postgres DB for this test run
    temp_db_dir: str

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    def setUp(self) -> None:
        self.operations_database_key = SQLAlchemyDatabaseKey.for_schema(
            SchemaType.OPERATIONS
        )
        local_persistence_helpers.use_on_disk_postgresql_database(
            self.operations_database_key
        )

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

        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.operations_database_key
        )

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )

    @property
    @abc.abstractmethod
    def state_codes(self) -> List[StateCode]:
        pass

    @property
    def region_dir_names(self) -> List[str]:
        return [state_code.value.lower() for state_code in self.state_codes]

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

    def _build_controller(
        self,
        region_code: str,
        ingest_instance: DirectIngestInstance,
        allow_unlaunched: bool,
        region_module_override: Optional[ModuleType],
    ) -> BaseDirectIngestController:
        # Seed the DB with an initial status
        PostgresDirectIngestInstanceStatusManager(
            region_code=region_code,
            ingest_instance=ingest_instance,
        ).add_instance_status(DirectIngestStatus.STANDARD_RERUN_STARTED)

        controller = DirectIngestControllerFactory.build(
            region_code=region_code,
            ingest_instance=DirectIngestInstance.PRIMARY,
            allow_unlaunched=allow_unlaunched,
            region_module_override=region_module_override,
        )
        if not isinstance(controller, BaseDirectIngestController):
            raise ValueError(
                f"Expected type BaseDirectIngestController, found [{controller}] "
                f"with type [{type(controller)}]."
            )
        return controller

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
                self._build_controller(
                    region_code=region_code,
                    ingest_instance=DirectIngestInstance.PRIMARY,
                    allow_unlaunched=True,
                    region_module_override=self.region_module_override,
                )

    def test_raw_files_yaml_parses_all_regions(self) -> None:
        for region_code in self.region_dir_names:
            region = get_direct_ingest_region(
                region_code, region_module_override=self.region_module_override
            )
            raw_file_manager = DirectIngestRegionRawFileConfig(
                region_code=region_code,
                region_module=self.region_module_override,
            )

            if not raw_file_manager.raw_file_configs:
                continue

            if region.is_ingest_launched_in_env() is not None:
                self.test.assertTrue(raw_file_manager.raw_file_configs)
            config_file_tags = set()
            possible_external_id_types = get_external_id_types()
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
                external_id_type_categories: Dict[str, RawTableColumnFieldType] = {}
                for column in config.columns:
                    normalized_column_name = normalize_column_name_for_bq(column.name)
                    self.test.assertEqual(column.name, normalized_column_name)

                    if not column.external_id_type:
                        continue

                    # Check that the external id type, if there is one, is a constant
                    self.test.assertIn(
                        column.external_id_type, possible_external_id_types
                    )

                    if column.external_id_type not in external_id_type_categories:
                        external_id_type_categories[
                            column.external_id_type
                        ] = column.field_type
                    else:
                        # Check that external id types are treated consistently as either person or staff
                        self.test.assertEqual(
                            external_id_type_categories[column.external_id_type],
                            column.field_type,
                        )

    def test_raw_files_yaml_define_schema_pragma(self) -> None:
        for region_code in self.region_dir_names:
            raw_file_manager = DirectIngestRegionRawFileConfig(
                region_code=region_code,
                region_module=self.region_module_override,
            )

            for config_path in raw_file_manager.get_raw_data_file_config_paths():
                with open(config_path, encoding="utf-8") as f:
                    line = f.readline()
                    match = re.match(YAML_LANGUAGE_SERVER_PRAGMA, line.strip())
                    if not match:
                        raise ValueError(
                            f"First line of raw data config file [{config_path}] does "
                            "not match expected pattern."
                        )
                    relative_schema_path = match.group("schema_path")
                    abs_schema_path = os.path.normpath(
                        os.path.join(os.path.dirname(config_path), relative_schema_path)
                    )
                    if not os.path.exists(abs_schema_path):
                        raise ValueError(
                            f"Schema path [{abs_schema_path}] does not exist."
                        )

    @parameterized.expand(
        [
            ("build_prod", "recidiviz-123"),
            ("build_staging", "recidiviz-staging"),
        ]
    )
    def test_collect_and_print_ingest_views(self, _name: str, project_id: str) -> None:
        with patch("recidiviz.utils.metadata.project_id", return_value=project_id):
            for region_code in self.region_dir_names:
                region = get_direct_ingest_region(
                    region_code, region_module_override=self.region_module_override
                )

                # Collect all views regardless of gating and make sure they build
                views = DirectIngestViewQueryBuilderCollector(
                    region, expected_ingest_views=[]
                ).collect_query_builders()
                for view in views:
                    view.build_and_print()

    def test_collect_and_build_raw_table_migrations(self) -> None:
        with patch("recidiviz.utils.metadata.project_id", return_value="recidiviz-789"):
            for region_code in self.region_dir_names:
                for instance in DirectIngestInstance:
                    raw_file_manager = DirectIngestRegionRawFileConfig(
                        region_code=region_code,
                        region_module=self.region_module_override,
                    )
                    collector = DirectIngestRawTableMigrationCollector(
                        region_code,
                        instance=instance,
                        regions_module_override=self.region_module_override,
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
                            self.test.assertFalse(
                                update_values in distinct_update_values
                            )
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
    def state_codes(self) -> List[StateCode]:
        return get_existing_direct_ingest_states()

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
                self._build_controller(
                    region_code=region_code.lower(),
                    ingest_instance=DirectIngestInstance.PRIMARY,
                    allow_unlaunched=False,
                    region_module_override=None,
                )

        # But they should not be supported in production
        with patch(
            "recidiviz.utils.environment.get_gcp_environment",
            return_value=GCPEnvironment.PRODUCTION.value,
        ), patch("recidiviz.utils.metadata.project_id", return_value="recidiviz-123"):
            for region_code in PLAYGROUND_STATE_INFO:
                with self.assertRaisesRegex(DirectIngestError, "Unsupported"):
                    self._build_controller(
                        region_code=region_code.lower(),
                        ingest_instance=DirectIngestInstance.PRIMARY,
                        allow_unlaunched=False,
                        region_module_override=None,
                    )


class DirectIngestRegionTemplateDirStructure(
    DirectIngestRegionDirStructureBase, unittest.TestCase
):
    """Tests properties of recidiviz/ingest/direct/templates."""

    def setUp(self) -> None:
        super().setUp()

        # Ensures StateCode.US_XX is properly loaded
        self.supported_regions_patcher = patch(
            f"{direct_ingest_controller_factory.__name__}.get_direct_ingest_states_existing_in_env"
        )
        self.mock_supported_regions = self.supported_regions_patcher.start()
        self.mock_supported_regions.return_value = self.state_codes

    def tearDown(self) -> None:
        super().tearDown()
        self.supported_regions_patcher.stop()

    @property
    def state_codes(self) -> List[StateCode]:
        return [StateCode.US_XX]

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


class TestControllerWithIngestManifestCollection(unittest.TestCase):
    """Test that various regions match the ingest rank list of the ingest controller."""

    def setUp(self) -> None:
        self.combinations = [
            (region_code, ingest_instance, project)
            for region_code in get_existing_direct_ingest_states()
            for ingest_instance in list(DirectIngestInstance)
            for project in sorted(environment.GCP_PROJECTS)
        ]

    def test_ingest_rank_list_matches_manifests(
        self,
    ) -> None:
        for region_code, ingest_instance, project in self.combinations:
            region = direct_ingest_regions.get_direct_ingest_region(
                region_code=region_code.value
            )
            controller_cls = DirectIngestControllerFactory.get_controller_class(region)
            ingest_view_manifest_collector = IngestViewManifestCollector(
                region=region,
                delegate=IngestViewResultsParserDelegateImpl(
                    region=region,
                    schema_type=SchemaType.STATE,
                    ingest_instance=ingest_instance,
                    results_update_datetime=datetime.now(),
                ),
            )
            with patch.object(
                metadata, "project_id", return_value=project
            ), patch.object(
                environment,
                "in_gcp_staging",
                return_value=(project == environment.GCP_PROJECT_STAGING),
            ), patch.object(
                environment,
                "in_gcp_production",
                return_value=(project == environment.GCP_PROJECT_PRODUCTION),
            ):
                # pylint: disable=protected-access
                self.assertListEqual(
                    sorted(
                        controller_cls._get_ingest_view_rank_list(
                            ingest_instance=ingest_instance
                        )
                    ),
                    sorted(ingest_view_manifest_collector.launchable_ingest_views()),
                    "The ingest rank list does not match the ingest view manifests for "
                    f"[{region_code.value}] [{ingest_instance.value}] [{project}]",
                )

    def _get_related_ingest_view_pairs(
        self, ingest_view_names: List[str]
    ) -> List[Tuple[str, str]]:
        pairs = set()
        for ingest_view in ingest_view_names:
            for ingest_view_2 in ingest_view_names:
                if ingest_view >= ingest_view_2:
                    # Skip if view 2 is alphabetically less than view 1 to avoid dupes
                    continue
                regex = r"(?P<name>.*)(?P<version>_v[0-9])"
                match_1 = re.match(regex, ingest_view)
                match_2 = re.match(regex, ingest_view_2)

                base_ingest_view_name_1 = (
                    match_1.group("name") if match_1 else ingest_view
                )
                base_ingest_view_name_2 = (
                    match_2.group("name") if match_2 else ingest_view_2
                )
                if base_ingest_view_name_1 == base_ingest_view_name_2:
                    pairs.add((ingest_view, ingest_view_2))
        return list(pairs)

    def test_get_related_ingest_view_pairs(self) -> None:
        self.assertEqual(
            [("my_ip_view", "my_ip_view_v2")],
            self._get_related_ingest_view_pairs(
                ["my_ip_view", "my_ip_view_v2", "my_other_view"]
            ),
        )

        self.assertEqual(
            [("state_person", "state_person_v2")],
            self._get_related_ingest_view_pairs(
                ["state_person", "state_person_external_id", "state_person_v2"]
            ),
        )

        self.assertEqual(
            [("assessments_v2", "assessments_v3")],
            self._get_related_ingest_view_pairs(
                ["state_person", "assessments_v2", "assessments_v3"]
            ),
        )

        self.assertEqual([], self._get_related_ingest_view_pairs([]))
        self.assertEqual([], self._get_related_ingest_view_pairs(["state_person"]))

    def test_ingest_views_with_similar_names_are_in_different_environments(
        self,
    ) -> None:
        for region_code, ingest_instance, project in self.combinations:
            region = direct_ingest_regions.get_direct_ingest_region(
                region_code=region_code.value
            )
            with patch.object(
                metadata, "project_id", return_value=project
            ), patch.object(
                environment,
                "in_gcp_staging",
                return_value=(project == environment.GCP_PROJECT_STAGING),
            ), patch.object(
                environment,
                "in_gcp_production",
                return_value=(project == environment.GCP_PROJECT_PRODUCTION),
            ):
                ingest_view_manifest_collector = IngestViewManifestCollector(
                    region=region,
                    delegate=IngestViewResultsParserDelegateImpl(
                        region=region,
                        schema_type=SchemaType.STATE,
                        ingest_instance=ingest_instance,
                        results_update_datetime=datetime.now(),
                    ),
                )
                ingest_view_names = list(
                    ingest_view_manifest_collector.ingest_view_to_manifest
                )
                related_ingest_view_pairs = self._get_related_ingest_view_pairs(
                    ingest_view_names
                )
                for ingest_view, ingest_view_2 in related_ingest_view_pairs:
                    manifest = ingest_view_manifest_collector.ingest_view_to_manifest[
                        ingest_view
                    ]
                    manifest_2 = ingest_view_manifest_collector.ingest_view_to_manifest[
                        ingest_view_2
                    ]
                    self.assertFalse(
                        manifest.should_launch and manifest_2.should_launch,
                        f"Found related {region_code.value} views, [{ingest_view}] and "
                        f"[{ingest_view_2}], which are both configured to launch in "
                        f"[{project}] and [{ingest_instance}]",
                    )

    def test_ingest_view_result_fixture_files_have_corresponding_yaml(self) -> None:
        for region_code in get_existing_direct_ingest_states():
            region = direct_ingest_regions.get_direct_ingest_region(
                region_code=region_code.value
            )
            ingest_view_manifest_collector = IngestViewManifestCollector(
                region=region,
                delegate=IngestViewResultsParserDelegateImpl(
                    region=region,
                    schema_type=SchemaType.STATE,
                    # Pick an arbitrary instance, it doesn't matter
                    ingest_instance=DirectIngestInstance.SECONDARY,
                    results_update_datetime=datetime.now(),
                ),
            )
            fixtures_directory = os.path.join(
                os.path.dirname(direct_ingest_fixtures.__file__),
                region_code.value.lower(),
            )
            fixture_file_names = [
                os.path.splitext(fixture_file)[0]
                for fixture_file in os.listdir(fixtures_directory)
                if fixture_file.endswith(".csv")
            ]

            extra_fixtures = set(fixture_file_names) - set(
                ingest_view_manifest_collector.ingest_view_to_manifest.keys()
            )

            self.assertSetEqual(
                extra_fixtures,
                set(),
                f"Found fixtures in {fixtures_directory} with no corresponding "
                f"ingest mappings (candidates for cleanup): {extra_fixtures}",
            )

    def test_ingest_view_fixture_files_is_subset_of_ingest_mappings(self) -> None:
        for region_code in get_existing_direct_ingest_states():
            region = direct_ingest_regions.get_direct_ingest_region(
                region_code=region_code.value
            )
            ingest_view_manifest_collector = IngestViewManifestCollector(
                region=region,
                delegate=IngestViewResultsParserDelegateImpl(
                    region=region,
                    schema_type=SchemaType.STATE,
                    # Pick an arbitrary instance, it doesn't matter
                    ingest_instance=DirectIngestInstance.SECONDARY,
                    results_update_datetime=datetime.now(),
                ),
            )
            region_fixtures_directory = os.path.join(
                os.path.dirname(direct_ingest_fixtures.__file__),
                region_code.value.lower(),
            )
            if "ingest_view" not in os.listdir(region_fixtures_directory):
                continue
            ingest_view_fixture_directories = [
                directory
                for directory in os.listdir(
                    os.path.join(region_fixtures_directory, "ingest_view")
                )
                if os.path.isdir(
                    os.path.join(region_fixtures_directory, "ingest_view", directory)
                )
                and is_non_empty_code_directory(
                    os.path.join(region_fixtures_directory, "ingest_view", directory)
                )
            ]
            set_of_ingest_view_fixture_directories = set(
                ingest_view_fixture_directories
            )
            set_of_ingest_views = set(
                ingest_view_manifest_collector.ingest_view_to_manifest.keys()
            )

            extra_fixture_directories = (
                set_of_ingest_view_fixture_directories - set_of_ingest_views
            )

            self.assertSetEqual(
                extra_fixture_directories,
                set(),
                f"Found fixtures directories in {region_fixtures_directory} with "
                f"no corresponding ingest mappings (candidates for cleanup): "
                f"{extra_fixture_directories}",
            )

    def test_ingest_views_have_corresponding_yaml(self) -> None:
        for region_code in get_existing_direct_ingest_states():
            region = direct_ingest_regions.get_direct_ingest_region(
                region_code=region_code.value
            )
            ingest_view_manifest_collector = IngestViewManifestCollector(
                region=region,
                delegate=IngestViewResultsParserDelegateImpl(
                    region=region,
                    schema_type=SchemaType.STATE,
                    # Pick an arbitrary instance, it doesn't matter
                    ingest_instance=DirectIngestInstance.SECONDARY,
                    results_update_datetime=datetime.now(),
                ),
            )
            if region.region_module.__file__ is None:
                raise ValueError(f"No file associated with {region.region_module}.")
            ingest_view_directory = os.path.join(
                os.path.dirname(region.region_module.__file__),
                region.region_code.lower(),
                "ingest_views",
            )
            ingest_view_files = [
                os.path.splitext(ingest_view)[0].replace("view_", "")
                for ingest_view in os.listdir(ingest_view_directory)
                if ingest_view.startswith("view_")
            ]

            extra_ingest_view_files = set(ingest_view_files) - set(
                ingest_view_manifest_collector.ingest_view_to_manifest.keys()
            )
            self.assertSetEqual(
                extra_ingest_view_files,
                set(),
                f"Found ingest views in {ingest_view_directory} with "
                f"no corresponding ingest mappings (candidates for cleanup): "
                f"{extra_ingest_view_files}",
            )

    def test_raw_input_fixture_has_corresponding_ingest_view_output_file(self) -> None:
        for region_code in get_existing_direct_ingest_states():
            if region_code == StateCode.US_OZ:
                continue
            region = direct_ingest_regions.get_direct_ingest_region(
                region_code=region_code.value
            )
            ingest_view_manifest_collector = IngestViewManifestCollector(
                region=region,
                delegate=IngestViewResultsParserDelegateImpl(
                    region=region,
                    schema_type=SchemaType.STATE,
                    # Pick an arbitrary instance, it doesn't matter
                    ingest_instance=DirectIngestInstance.SECONDARY,
                    results_update_datetime=datetime.now(),
                ),
            )
            view_collector = DirectIngestViewQueryBuilderCollector(
                region,
                list(ingest_view_manifest_collector.ingest_view_to_manifest.keys()),
            )
            ingest_view_to_raw_data_dependencies: Dict[str, Set[str]] = {}
            for (
                ingest_view,
                _,
            ) in ingest_view_manifest_collector.ingest_view_to_manifest.items():
                ingest_view_to_raw_data_dependencies[ingest_view] = {
                    raw_data_dependency.raw_file_config.file_tag
                    for raw_data_dependency in view_collector.get_query_builder_by_view_name(
                        ingest_view
                    ).raw_table_dependency_configs
                }

            if region.region_module.__file__ is None:
                raise ValueError(f"No file associated with {region.region_module}.")
            region_fixtures_directory = os.path.join(
                os.path.dirname(direct_ingest_fixtures.__file__),
                region_code.value.lower(),
            )

            if "raw" not in os.listdir(
                region_fixtures_directory
            ) or "ingest_view" not in os.listdir(region_fixtures_directory):
                continue

            raw_data_fixtures_directory = os.path.join(region_fixtures_directory, "raw")
            file_name_to_raw_data_folder = defaultdict(list)
            for raw_data_dir in os.listdir(raw_data_fixtures_directory):
                if not os.path.isdir(
                    os.path.join(raw_data_fixtures_directory, raw_data_dir)
                ):
                    continue
                raw_data_files = [
                    file
                    for file in os.listdir(
                        os.path.join(raw_data_fixtures_directory, raw_data_dir)
                    )
                    if file.endswith(".csv")
                ]
                file_name_to_raw_data_folder[raw_data_dir] = raw_data_files

            ingest_view_fixtures_directory = os.path.join(
                region_fixtures_directory, "ingest_view"
            )
            file_name_to_ingest_view_folder = defaultdict(list)
            for ingest_view_dir in os.listdir(ingest_view_fixtures_directory):
                if not os.path.isdir(
                    os.path.join(ingest_view_fixtures_directory, ingest_view_dir)
                ):
                    continue
                ingest_view_files = [
                    file
                    for file in os.listdir(
                        os.path.join(ingest_view_fixtures_directory, ingest_view_dir)
                    )
                    if file.endswith(".csv")
                ]
                file_name_to_ingest_view_folder[ingest_view_dir] = ingest_view_files

            for (
                ingest_view,
                raw_data_dependencies,
            ) in ingest_view_to_raw_data_dependencies.items():
                for raw_data_dependency in raw_data_dependencies:
                    if (
                        ingest_view in file_name_to_ingest_view_folder
                        and raw_data_dependency in file_name_to_raw_data_folder
                    ):
                        raw_file_fixtures = file_name_to_raw_data_folder[
                            raw_data_dependency
                        ]
                        if f"{raw_data_dependency}@ALL" in file_name_to_raw_data_folder:
                            raw_file_fixtures = (
                                raw_file_fixtures
                                + file_name_to_raw_data_folder[
                                    f"{raw_data_dependency}@ALL"
                                ]
                            )
                        for file in file_name_to_ingest_view_folder[ingest_view]:
                            self.assertTrue(
                                file in raw_file_fixtures,
                                f"Found extra output fixture files for {ingest_view} in [{os.path.join(ingest_view_fixtures_directory, ingest_view)}] "
                                f"that do not correspond to any input fixture files in [{os.path.join(raw_data_fixtures_directory, raw_data_dependency)}]: "
                                f"extra file: {file}, files in ingest view: {file_name_to_ingest_view_folder[ingest_view]}, files in raw data: {raw_file_fixtures}",
                            )

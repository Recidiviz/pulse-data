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
import inspect
import os
import re
import unittest
import unittest.mock
from datetime import datetime
from types import ModuleType
from typing import Any, Callable, Dict, List, Optional, Tuple

import pytest
import yaml
from mock import patch
from more_itertools import one
from parameterized import parameterized

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_utils import normalize_column_name_for_bq
from recidiviz.cloud_storage.gcsfs_path import GcsfsBucketPath, GcsfsFilePath
from recidiviz.common.constants.operations.direct_ingest_instance_status import (
    DirectIngestStatus,
)
from recidiviz.common.constants.states import PLAYGROUND_STATE_INFO, StateCode
from recidiviz.common.file_system import is_valid_code_path
from recidiviz.common.module_collector_mixin import ModuleCollectorMixin
from recidiviz.ingest.direct import direct_ingest_regions, regions, templates
from recidiviz.ingest.direct.controllers import (
    legacy_ingest_raw_file_import_controller_factory,
)
from recidiviz.ingest.direct.controllers.legacy_ingest_raw_file_import_controller import (
    LegacyIngestRawFileImportController,
)
from recidiviz.ingest.direct.controllers.legacy_ingest_raw_file_import_controller_factory import (
    LegacyIngestRawFileImportControllerFactory,
)
from recidiviz.ingest.direct.gcs.direct_ingest_gcs_file_system import (
    to_normalized_unprocessed_raw_file_name,
)
from recidiviz.ingest.direct.gcs.filename_parts import filename_parts_from_path
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_collector import (
    IngestViewManifestCollector,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_compiler_delegate import (
    StateSchemaIngestViewManifestCompilerDelegate,
)
from recidiviz.ingest.direct.metadata.direct_ingest_instance_status_manager import (
    DirectIngestInstanceStatusManager,
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
from recidiviz.tests.ingest.direct import regions as regions_tests_module
from recidiviz.tests.ingest.direct.fixture_util import (
    DIRECT_INGEST_FIXTURES_ROOT,
    INGEST_MAPPING_OUTPUT_SUBDIR,
    DirectIngestTestFixturePath,
    fixture_path_for_address,
    ingest_mapping_output_fixture_path,
)
from recidiviz.tests.ingest.direct.regions.base_ingest_test_cases import (
    StateIngestMappingTestCase,
)
from recidiviz.tests.ingest.direct.regions.ingest_view_query_test_case import (
    LegacyIngestViewEmulatorQueryTestCase,
    StateIngestViewTestCase,
)
from recidiviz.tests.ingest.direct.regions.state_ingest_view_parser_test_base import (
    LegacyStateIngestViewParserTestBase,
)
from recidiviz.tests.ingest.direct.regions.state_specific_ingest_pipeline_integration_test_case import (
    PIPELINE_INTEGRATION_TEST_NAME,
)
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers
from recidiviz.utils import environment
from recidiviz.utils.environment import GCPEnvironment
from recidiviz.utils.types import assert_type

_REGION_REGEX = re.compile(r"us_[a-z]{2}(_[a-z]+)?")
YAML_LANGUAGE_SERVER_PRAGMA = re.compile(
    r"^# yaml-language-server: \$schema=(?P<schema_path>.*schema.json)$"
)
UNTESTED_INGEST_VIEWS = {
    StateCode.US_MO: {
        # TODO(#19825): Write tests for this view and remove exemption
        "tak028_tak042_tak076_tak024_violation_reports",
        # TODO(#19826): Write tests for this view and remove exemption
        "tak034_tak026_tak039_apfx90_apfx91_supervision_enhancements_supervision_periods",
        # TODO(#19827): Write tests for this view and remove exemption
        "tak291_tak292_tak024_citations",
    },
    StateCode.US_PA: {
        # TODO(#19828): Write tests for this view and remove exemption
        "board_action",
        # TODO(#19829): Write tests for this view and remove exemption
        "dbo_Miscon",
        # TODO(#19830): Write tests for this view and remove exemption
        "dbo_Offender_v2",
        # TODO(#19831): Write tests for this view and remove exemption
        "dbo_Senrec_v2",
        # TODO(#19832): Write tests for this view and remove exemption
        "doc_person_info",
        # TODO(#19834): Write tests for this view and remove exemption
        "supervision_violation",
        # TODO(#19835): Write tests for this view and remove exemption
        "supervision_violation_response",
    },
    StateCode.US_IA: {
        # TODO(#37074): Write tests for this view and remove exemption
        "incarceration_periods",
    },
}


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
    ) -> LegacyIngestRawFileImportController:
        """Builds a controller for the given region code and ingest instance."""
        # Seed the DB with an initial status
        DirectIngestInstanceStatusManager(
            region_code=region_code,
            ingest_instance=ingest_instance,
        ).add_instance_status(DirectIngestStatus.INITIAL_STATE)

        controller = LegacyIngestRawFileImportControllerFactory.build(
            region_code=region_code,
            ingest_instance=DirectIngestInstance.PRIMARY,
            allow_unlaunched=allow_unlaunched,
            region_module_override=region_module_override,
        )
        if not isinstance(controller, LegacyIngestRawFileImportController):
            raise ValueError(
                f"Expected type LegacyIngestRawFileImportController, found [{controller}] "
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
            region = direct_ingest_regions.get_direct_ingest_region(
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
                for column in config.current_columns:
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
                region = direct_ingest_regions.get_direct_ingest_region(
                    region_code, region_module_override=self.region_module_override
                )

                # Collect all views regardless of gating and make sure they build
                views = DirectIngestViewQueryBuilderCollector(
                    region, expected_ingest_views=[]
                ).get_query_builders()
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
                    for (
                        file_tag
                    ) in collector.collect_raw_table_migrations_by_file_tag():
                        # Test this doesn't crash
                        _ = collector.get_raw_table_migration_queries_for_file_tag(
                            file_tag,
                            raw_table_address=BigQueryAddress(
                                dataset_id="some_dataset", table_id=file_tag
                            ),
                            data_update_datetime=None,
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
            c.name for c in raw_file_config.current_columns if c.description
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
            region = direct_ingest_regions.get_direct_ingest_region(
                region_code, region_module_override=self.region_module_override
            )
            self.assertTrue(region.playground)

    @patch(
        "recidiviz.ingest.direct.controllers.legacy_ingest_raw_file_import_controller_factory.is_raw_data_import_dag_enabled",
        return_value=False,
    )
    @patch(
        "recidiviz.ingest.direct.controllers.legacy_ingest_raw_file_import_controller.is_raw_data_import_dag_enabled",
        return_value=False,
    )
    def test_playground_regions_do_not_run_in_production_for_legacy_raw_data_import(
        self,
        _raw_data_enabled_mock: unittest.mock.MagicMock,
        _raw_data_enabled_mock2: unittest.mock.MagicMock,
    ) -> None:
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
            f"{legacy_ingest_raw_file_import_controller_factory.__name__}.get_direct_ingest_states_existing_in_env"
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
            (region_code, project)
            for region_code in get_existing_direct_ingest_states()
            for project in sorted(environment.DATA_PLATFORM_GCP_PROJECTS)
        ]

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
        for region_code, project in self.combinations:
            region = direct_ingest_regions.get_direct_ingest_region(
                region_code=region_code.value
            )
            view_collector = DirectIngestViewQueryBuilderCollector(region=region)
            launchable_views = view_collector.launchable_ingest_views(project)
            related_ingest_view_pairs = self._get_related_ingest_view_pairs(
                list(launchable_views.keys())
            )
            if any(related_ingest_view_pairs):
                msg = (
                    f"Found related views which are both configured to launch in {region_code.value}, [{project}]:"
                    "\n- ".join(
                        f"[{iv1}] and [{iv2}]" for iv1, iv2 in related_ingest_view_pairs
                    )
                )
                self.assertFalse(msg)

    # TODO(#22059): Update integration test fixtures
    def test_integration_test_ingest_view_result_fixture_files_have_corresponding_yaml(
        self,
    ) -> None:
        """Fails if there are integration test fixture files that are no longer used but
        have not been deleted.
        """
        for region_code in get_existing_direct_ingest_states():
            region = direct_ingest_regions.get_direct_ingest_region(
                region_code=region_code.value
            )
            ingest_view_manifest_collector = IngestViewManifestCollector(
                region=region,
                delegate=StateSchemaIngestViewManifestCompilerDelegate(region=region),
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

            ingest_view_names = (
                ingest_view_manifest_collector.ingest_view_to_manifest.keys()
            )
            extra_fixtures = set(fixture_file_names) - set(
                ingest_view_manifest_collector.ingest_view_to_manifest.keys()
            )

            self.assertSetEqual(
                extra_fixtures,
                set(),
                f"Found fixtures in {fixtures_directory} with no corresponding "
                f"ingest mappings (candidates for cleanup): {extra_fixtures}",
            )

            if not fixture_file_names and ingest_view_names:
                raise ValueError(
                    f"[{region_code.value}] Found no integration test fixture files "
                    f"in [{fixtures_directory}] even though there are ingest views for "
                    f"this state - is this test looking in the right place?"
                )


def get_all_ingest_tests_for_state(
    state_code: StateCode,
) -> tuple[
    dict[
        str, type[LegacyIngestViewEmulatorQueryTestCase] | type[StateIngestViewTestCase]
    ],
    dict[
        str,
        type[LegacyStateIngestViewParserTestBase] | type[StateIngestMappingTestCase],
    ],
]:
    """
    This helper function gathers all ingest view and mapping tests for the given state code.
    """

    def _test_class_from_module(module: ModuleType, parent_class: Any) -> Any | None:
        """Pulls the single given test class for the given module."""
        try:
            return one(
                test_class
                for _cls_name, test_class in inspect.getmembers(module, inspect.isclass)
                if issubclass(test_class, parent_class)
                and _cls_name != parent_class.__name__
            )
        except ValueError:
            return None

    state_test_module = ModuleCollectorMixin.get_relative_module(
        regions_tests_module, [state_code.value.lower()]
    )
    ingest_view_test_module = ModuleCollectorMixin.get_relative_module(
        state_test_module, ["ingest_views"]
    )
    ingest_view_tests: dict[
        str, type[LegacyIngestViewEmulatorQueryTestCase] | type[StateIngestViewTestCase]
    ] = {}
    ingest_mapping_tests: dict[
        str,
        type[LegacyStateIngestViewParserTestBase] | type[StateIngestMappingTestCase],
    ] = {}
    for ingest_view_test_file_module in ModuleCollectorMixin.get_submodules(
        ingest_view_test_module, submodule_name_prefix_filter=None
    ):
        # TODO(#38322) Drop v1 entirely
        v1_test_class = _test_class_from_module(
            ingest_view_test_file_module, LegacyIngestViewEmulatorQueryTestCase
        )
        v2_test_class = _test_class_from_module(
            ingest_view_test_file_module, StateIngestViewTestCase
        )
        if not (v1_test_class or v2_test_class):
            raise ValueError(
                f"{ingest_view_test_file_module} should have either an LegacyIngestViewEmulatorQueryTestCase or StateIngestViewTestCase"
            )
        if v1_test_class and v2_test_class:
            raise ValueError(
                f"{ingest_view_test_file_module} should NOT have both an LegacyIngestViewEmulatorQueryTestCase and StateIngestViewTestCase"
            )

        if v1_test_class:
            ingest_view_tests[
                assert_type(v1_test_class.ingest_view_name(), str)
            ] = v1_test_class
        if v2_test_class:
            ingest_view_tests[
                assert_type(v2_test_class.ingest_view_builder().ingest_view_name, str)
            ] = v2_test_class

            if v2_mapping_test_class := _test_class_from_module(
                ingest_view_test_file_module, StateIngestMappingTestCase
            ):
                ingest_mapping_tests[
                    v2_test_class.ingest_view_builder().ingest_view_name
                ] = v2_mapping_test_class
                v2_mapping_tests = {
                    t for t in dir(v2_mapping_test_class) if t.startswith("test_")
                }
                v2_view_tests = {t for t in dir(v2_test_class) if t.startswith("test_")}
                if unmatched_tests := v2_view_tests.symmetric_difference(
                    v2_mapping_tests
                ):
                    raise ValueError(
                        f"{v2_test_class} and {v2_mapping_test_class} do not have the same tests. "
                        f"Found: {unmatched_tests}"
                    )

    # TODO(#38321) Remove v1 mapping tests
    try:
        v1_mapping_test = _test_class_from_module(
            ModuleCollectorMixin.get_relative_module(
                state_test_module,
                [
                    f"{state_code.value.lower()}_ingest_view_parser_test",
                ],
            ),
            LegacyStateIngestViewParserTestBase,
        )
    except ModuleNotFoundError:
        v1_mapping_test = None
    if v1_mapping_test and issubclass(
        v1_mapping_test, LegacyStateIngestViewParserTestBase
    ):
        for method in dir(v1_mapping_test):
            if method.startswith("test_parse"):
                ingest_mapping_tests[
                    method.removeprefix("test_parse_")
                ] = v1_mapping_test

    return (ingest_view_tests, ingest_mapping_tests)


def get_all_ingest_view_fixtures_for_state(
    state_code: StateCode,
) -> tuple[set[str], set[str]]:
    """Returns all raw data and ingest view result fixtures for a given state."""

    raw_data_fixture_paths = os.walk(
        os.path.join(DIRECT_INGEST_FIXTURES_ROOT, state_code.value.lower(), "raw")
    )
    all_raw_data_fixture_paths = set()
    for path, _, file_names in raw_data_fixture_paths:
        for file_name in file_names:
            if file_name in ("__init__.py", f"{PIPELINE_INTEGRATION_TEST_NAME}.csv"):
                continue
            fixture_path = os.path.join(path, file_name)
            if is_valid_code_path(fixture_path):
                all_raw_data_fixture_paths.add(fixture_path)

    ### TODO(#38322) Drop v1 entirely
    v1_result_fixture_paths = os.walk(
        os.path.join(
            DIRECT_INGEST_FIXTURES_ROOT, state_code.value.lower(), "ingest_view"
        )
    )
    v2_result_fixture_paths = os.walk(
        os.path.join(
            DIRECT_INGEST_FIXTURES_ROOT,
            state_code.value.lower(),
            f"{state_code.value.lower()}_ingest_view_results",
        )
    )

    ### SETUP: Gather all ingest view results fixtures
    all_ingest_view_result_fixture_paths = set()
    for path, _, file_names in list(v1_result_fixture_paths) + list(
        v2_result_fixture_paths
    ):
        for file_name in file_names:
            if file_name in ("__init__.py", f"{PIPELINE_INTEGRATION_TEST_NAME}.csv"):
                continue
            fixture_path = os.path.join(path, file_name)
            if is_valid_code_path(fixture_path):
                all_ingest_view_result_fixture_paths.add(fixture_path)

    return all_raw_data_fixture_paths, all_ingest_view_result_fixture_paths


def get_fixtures_used_by_ingest_view_tests(
    state_code: StateCode,
    ### TODO(#38322) Remove this argument when we remove v1 tests
    view_collector: DirectIngestViewQueryBuilderCollector,
    ingest_view_tests: list[
        type[LegacyIngestViewEmulatorQueryTestCase] | type[StateIngestViewTestCase]
    ],
) -> tuple[set[str], set[str]]:
    """Returns the raw data and ingest view results fixtures used by tests in the given state."""
    used_raw_data_fixture_paths = set()
    used_ingest_view_result_fixture_paths = set()
    for ingest_view_test in ingest_view_tests:
        for method in dir(ingest_view_test):
            if not method.startswith("test_"):
                continue
            ### TODO(#38322) Drop v1 entirely
            if issubclass(ingest_view_test, LegacyIngestViewEmulatorQueryTestCase):
                characteristic = method.removeprefix("test_")
                result_fixture_path = (
                    DirectIngestTestFixturePath.for_ingest_view_test_results_fixture(
                        region_code=state_code.value.lower(),
                        ingest_view_name=ingest_view_test.ingest_view_name(),
                        file_name=characteristic + ".csv",
                    ).full_path()
                )
                view_builder = view_collector.get_query_builder_by_view_name(
                    ingest_view_test.ingest_view_name()
                )
            elif issubclass(ingest_view_test, StateIngestViewTestCase):
                (
                    iv_name,
                    characteristic,
                ) = ingest_view_test.get_ingest_view_name_and_characteristic(method)

                result_fixture_path = fixture_path_for_address(
                    state_code,
                    BigQueryAddress(
                        dataset_id=f"{state_code.value.lower()}_ingest_view_results",
                        table_id=iv_name,
                    ),
                    characteristic,
                )
                view_builder = ingest_view_test.ingest_view_builder()
            else:
                raise ValueError(
                    "Received unexpected ingest view test class. We expect either LegacyIngestViewEmulatorQueryTestCase or StateIngestViewTestCase. "
                    f"Received: {ingest_view_test.__mro__}"
                )
            used_ingest_view_result_fixture_paths.add(result_fixture_path)
            for raw_dep_config in view_builder.raw_table_dependency_configs:
                used_raw_data_fixture_paths.add(
                    DirectIngestTestFixturePath.for_raw_file_fixture(
                        region_code=state_code.value.lower(),
                        raw_file_dependency_config=raw_dep_config,
                        file_name=characteristic + ".csv",
                    ).full_path()
                )
    return used_raw_data_fixture_paths, used_ingest_view_result_fixture_paths


@pytest.mark.parametrize("state_code", get_existing_direct_ingest_states())
def test_ingest_view_and_mapping_structure(state_code: StateCode) -> None:
    """
    Tests that:
        - Every ingest view has an ingest mapping
        - Every ingest mapping has an ingest view
        - Every ingest view has an ingest view query test
        - Every ingest view has an ingest mapping test
        - Every ingest view results fixture has a defined test
        - Every mapping fixture has a defined test
    """
    ### SETUP: Get all ingest view builders and ingest mappings
    region = direct_ingest_regions.get_direct_ingest_region(
        region_code=state_code.value
    )
    view_collector = DirectIngestViewQueryBuilderCollector.from_state_code(state_code)
    mapping_collector = IngestViewManifestCollector(
        region=region,
        delegate=StateSchemaIngestViewManifestCompilerDelegate(region=region),
    )
    all_view_builders = view_collector.get_query_builders()
    all_ingest_view_names = {vb.ingest_view_name for vb in all_view_builders}
    all_ingest_view_names_with_mapping = set(mapping_collector.ingest_view_to_manifest)

    ### TEST: An ingest view exists if and only if an ingest mapping by the same name exists
    if (
        views_with_no_mapping := all_ingest_view_names
        - all_ingest_view_names_with_mapping
    ):
        raise ValueError(
            f"Found ingest views in {state_code} with no ingest mapping: {views_with_no_mapping}"
        )

    ### TEST: An ingest mapping exists if and only if an ingest view by the same name exists
    if mappings_w_no_view := all_ingest_view_names_with_mapping - all_ingest_view_names:
        raise ValueError(
            f"Found ingest mappings in {state_code} with no ingest view (candidates for cleanup): {mappings_w_no_view}"
        )

    ### SETUP: Get all ingest view and mapping tests
    ingest_view_tests, ingest_mapping_tests = get_all_ingest_tests_for_state(state_code)

    exemptions = UNTESTED_INGEST_VIEWS.get(state_code, set())

    ### TEST: An ingest view exists if and only if there are corresponding ingest view test(s)
    tested_ingest_view_names = set(ingest_view_tests)
    if untested := all_ingest_view_names - tested_ingest_view_names - exemptions:
        raise ValueError(
            f"Found the following ingest views for [{state_code.value}] which "
            f"do not have a corresponding test: {untested}"
        )
    if unnecessary_exemptions := exemptions.intersection(tested_ingest_view_names):
        raise ValueError(
            f"Found ingest view tests in [{state_code.value}] for {unnecessary_exemptions}, "
            "these can now be removed from UNTESTED_INGEST_VIEWS exemption list."
        )

    ### TEST: An ingest mapping exists if and only if there are corresponding ingest mapping test(s)
    if untested := all_ingest_view_names - set(ingest_mapping_tests) - exemptions:
        raise ValueError(
            f"Found ingest views/mappings in [{state_code.value}] with no ingest mapping test {untested}"
        )

    (
        used_raw_data_fixture_paths,
        used_ingest_view_result_fixture_paths,
    ) = get_fixtures_used_by_ingest_view_tests(
        state_code, view_collector, list(ingest_view_tests.values())
    )

    (
        all_raw_data_fixture_paths,
        all_ingest_view_result_fixture_paths,
    ) = get_all_ingest_view_fixtures_for_state(state_code)

    ### TEST: A raw data fixture exists if and only if there are tests referencing that fixture file.
    if unused_fixtures := all_raw_data_fixture_paths - used_raw_data_fixture_paths:
        _unused_printout = "\n".join(unused_fixtures)
        raise ValueError(
            f"Found unused raw data fixtures in paths:\n{_unused_printout}\n"
            "Either you added fixtures and forgot to write the test, the test is in an incorrect "
            "location, or these are old fixtures which need to be deleted."
        )

    ### TEST: An ingest view results fixture exists if and only if there are tests referencing that fixture file.
    if (
        unused_fixtures := all_ingest_view_result_fixture_paths
        - used_ingest_view_result_fixture_paths
    ):
        _unused_printout = "\n".join(unused_fixtures)
        raise ValueError(
            f"Found unused ingest view results fixtures:\n{_unused_printout}"
        )

    ### TEST: An ingest mapping fixture exists if and only if there are tests referencing that fixture file
    all_ingest_mapping_output_fixtures = set()
    mapping_fixture_paths = os.walk(
        os.path.join(
            DIRECT_INGEST_FIXTURES_ROOT,
            state_code.value.lower(),
            INGEST_MAPPING_OUTPUT_SUBDIR,
        )
    )
    for path, _, file_names in mapping_fixture_paths:
        for file_name in file_names:
            fixture_path = os.path.join(path, file_name)
            if is_valid_code_path(fixture_path):
                all_ingest_mapping_output_fixtures.add(fixture_path)

    expected_ingest_mapping_output_fixtures = set()
    for mapping_test in ingest_mapping_tests.values():
        if issubclass(mapping_test, StateIngestMappingTestCase):
            for method in dir(mapping_test):
                if not method.startswith("test_"):
                    continue
                (
                    view_name,
                    characteristic,
                ) = mapping_test.get_ingest_view_name_and_characteristic(method)
                expected_ingest_mapping_output_fixtures.add(
                    ingest_mapping_output_fixture_path(
                        state_code, view_name, characteristic
                    )
                )

    if (
        unused_fixtures := all_ingest_mapping_output_fixtures
        - expected_ingest_mapping_output_fixtures
    ):
        _unused_printout = "\n".join(unused_fixtures)
        raise ValueError(
            f"Found unused ingest mapping output fixtures:\n{_unused_printout}"
        )

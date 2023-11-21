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
"""Class with basic functionality for tests of all region-specific
BaseDirectIngestControllers.
"""
import abc
import datetime
import os
from copy import deepcopy
from types import ModuleType
from typing import Any, Callable, Dict, Iterable, List, Optional, Type, Union, cast

import apache_beam as beam
import pytest
from apache_beam.pvalue import PBegin
from apache_beam.testing.util import assert_that
from mock import patch

from recidiviz.common.constants.operations.direct_ingest_instance_status import (
    DirectIngestStatus,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.fakes.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.ingest.direct import regions
from recidiviz.ingest.direct.controllers.base_direct_ingest_controller import (
    BaseDirectIngestController,
)
from recidiviz.ingest.direct.direct_ingest_regions import (
    DirectIngestRegion,
    get_direct_ingest_region,
)
from recidiviz.ingest.direct.gating import is_ingest_in_dataflow_enabled
from recidiviz.ingest.direct.types.cloud_task_args import IngestViewMaterializationArgs
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.instance_database_key import database_key_for_state
from recidiviz.persistence.database.base_schema import StateBase
from recidiviz.persistence.database.schema.operations import schema as operations_schema
from recidiviz.persistence.database.schema.state import dao
from recidiviz.persistence.database.schema_entity_converter.state.schema_entity_converter import (
    StateSchemaToEntityConverter,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.entity.base_entity import (
    Entity,
    ExternalIdEntity,
    HasMultipleExternalIdsEntity,
    RootEntity,
)
from recidiviz.persistence.entity.core_entity import CoreEntity
from recidiviz.persistence.entity.entity_utils import (
    CoreEntityFieldIndex,
    get_all_entities_from_tree,
)
from recidiviz.persistence.entity.state.entities import StatePerson, StateStaff
from recidiviz.persistence.persistence import (
    DATABASE_INVARIANT_THRESHOLD,
    ENTITY_MATCHING_THRESHOLD,
    ENUM_THRESHOLD,
    OVERALL_THRESHOLD,
)
from recidiviz.pipelines.ingest.pipeline_parameters import MaterializationMethod
from recidiviz.pipelines.ingest.state.generate_ingest_view_results import (
    LOWER_BOUND_DATETIME_COL_NAME,
    MATERIALIZATION_TIME_COL_NAME,
    UPPER_BOUND_DATETIME_COL_NAME,
    GenerateIngestViewResults,
)
from recidiviz.pipelines.ingest.state.run_validations import RunValidations
from recidiviz.tests.ingest.direct.direct_ingest_test_util import (
    run_task_queues_to_empty,
)
from recidiviz.tests.ingest.direct.fakes.fake_direct_ingest_controller import (
    FakeIngestViewMaterializer,
    build_fake_direct_ingest_controller,
)
from recidiviz.tests.ingest.direct.fakes.fake_instance_ingest_view_contents import (
    FakeInstanceIngestViewContents,
)
from recidiviz.tests.ingest.direct.fixture_util import (
    DirectIngestFixtureDataFileType,
    direct_ingest_fixture_path,
    load_dataframe_from_path,
)
from recidiviz.tests.ingest.direct.regions.state_ingest_view_parser_test_base import (
    DEFAULT_UPDATE_DATETIME,
)
from recidiviz.tests.persistence.entity.state.entities_test_utils import (
    assert_no_unexpected_entities_in_db,
    clear_db_ids,
)
from recidiviz.tests.pipelines.ingest.state.test_case import (
    BaseStateIngestPipelineTestCase,
)
from recidiviz.tests.pipelines.utils.run_pipeline_test_utils import (
    default_arg_list_for_pipeline,
    pipeline_constructor,
)
from recidiviz.tests.test_debug_helpers import launch_entity_tree_html_diff_comparison
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers
from recidiviz.utils.environment import in_ci
from recidiviz.utils.types import assert_type

FULL_INTEGRATION_TEST_NAME = "test_run_full_ingest_all_files_specific_order"

PROJECT_ID = "test-project"


class FakeGenerateIngestViewResults(GenerateIngestViewResults):
    def __init__(
        self,
        project_id: str,
        state_code: StateCode,
        ingest_view_name: str,
        raw_data_tables_to_upperbound_dates: Dict[str, Optional[str]],
        ingest_instance: DirectIngestInstance,
        materialization_method: MaterializationMethod,
        fake_ingest_view_results: Iterable[Dict[str, Any]],
    ) -> None:
        super().__init__(
            project_id,
            state_code,
            ingest_view_name,
            raw_data_tables_to_upperbound_dates,
            ingest_instance,
            materialization_method,
        )
        self.fake_ingest_view_results = fake_ingest_view_results

    def expand(self, input_or_inputs: PBegin) -> beam.PCollection[Dict[str, Any]]:
        return input_or_inputs | beam.Create(self.fake_ingest_view_results)


class RunValidationsWithOutputChecking(RunValidations):
    """An override of RunValidations that runs the original code but provides a way
    to also check the output of the root entity trees we expect."""

    def __init__(
        self,
        expected_output_entities: Iterable[str],
        field_index: CoreEntityFieldIndex,
        state_code: StateCode,
        expected_root_entity_output: Iterable[RootEntity],
        debug: bool = False,
    ) -> None:
        super().__init__(expected_output_entities, field_index, state_code)
        self.expected_root_entity_output = expected_root_entity_output
        self.debug = debug

    def expand(
        self, input_or_inputs: beam.PCollection[RootEntity]
    ) -> beam.PCollection[RootEntity]:
        root_entities = super().expand(input_or_inputs)
        assert_that(
            root_entities,
            self.equal_to(
                self.expected_root_entity_output, self.state_code, self.debug
            ),
        )
        return root_entities

    @staticmethod
    def equal_to(
        expected_root_entity_output: Iterable[RootEntity],
        state_code: StateCode,
        debug: bool = False,
    ) -> Callable[[Iterable[RootEntity]], bool]:
        def _equal_to(actual: Iterable[RootEntity]) -> bool:
            copy_of_actual = deepcopy(actual)
            clear_db_ids([cast(CoreEntity, entity) for entity in copy_of_actual])
            if copy_of_actual != expected_root_entity_output:
                if debug:
                    launch_entity_tree_html_diff_comparison(
                        found_root_entities=copy_of_actual,  # type: ignore
                        expected_root_entities=expected_root_entity_output,  # type: ignore
                        field_index=CoreEntityFieldIndex(),
                        region_code=state_code.value.lower(),
                    )
                return False
            return True

        return _equal_to


@pytest.mark.uses_db
class RegionDirectIngestControllerTestCase(BaseStateIngestPipelineTestCase):
    """Class with basic functionality for tests of all region-specific
    BaseDirectIngestControllers.
    """

    @classmethod
    @abc.abstractmethod
    def state_code(cls) -> StateCode:
        pass

    @classmethod
    def region_code(cls) -> StateCode:
        return cls.state_code()

    @classmethod
    def region_module_override(cls) -> Optional[ModuleType]:
        return regions

    @classmethod
    def region(cls) -> DirectIngestRegion:
        return get_direct_ingest_region(
            region_code=cls.region_code().value.lower(),
            region_module_override=cls.region_module_override() or regions,
        )

    @classmethod
    def state_code_str_upper(cls) -> str:
        return cls.state_code().value.upper()

    @classmethod
    @abc.abstractmethod
    def controller_cls(cls) -> Type[BaseDirectIngestController]:
        pass

    @classmethod
    @abc.abstractmethod
    def schema_type(cls) -> SchemaType:
        pass

    # Stores the location of the postgres DB for this test run
    temp_db_dir: Optional[str]

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    @classmethod
    def _main_ingest_instance(cls) -> DirectIngestInstance:
        # We assume we're ingesting into the SECONDARY ingest instance, which
        # should always have the latest ingest logic updates released to it.
        return DirectIngestInstance.SECONDARY

    @classmethod
    def _main_database_key(cls) -> "SQLAlchemyDatabaseKey":
        if cls.schema_type() == SchemaType.STATE:
            return database_key_for_state(
                cls._main_ingest_instance(),
                cls.state_code(),
            )
        return SQLAlchemyDatabaseKey.for_schema(cls.schema_type())

    def setUp(self) -> None:
        super().setUp()
        self.maxDiff = None

        self.metadata_patcher = patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = "recidiviz-staging"

        self.environment_patcher = patch("recidiviz.utils.environment.in_gcp_staging")
        self.mock_environment_fn = self.environment_patcher.start()
        self.mock_environment_fn.return_value = True

        self.main_database_key = self._main_database_key()
        self.operations_database_key = SQLAlchemyDatabaseKey.for_schema(
            SchemaType.OPERATIONS
        )
        local_persistence_helpers.use_on_disk_postgresql_database(
            self.main_database_key
        )
        local_persistence_helpers.use_on_disk_postgresql_database(
            self.operations_database_key
        )

        if not is_ingest_in_dataflow_enabled(
            self.region_code(), self.ingest_instance()
        ):
            self.controller = build_fake_direct_ingest_controller(
                self.controller_cls(),
                ingest_instance=self._main_ingest_instance(),
                initial_statuses=[DirectIngestStatus.STANDARD_RERUN_STARTED],
                run_async=False,
                regions_module=regions,
            )

        # Set entity matching error threshold to a diminishingly small number
        # for tests. We cannot set it to 0 because we throw when errors *equal*
        # the error threshold.
        self.entity_matching_error_threshold_patcher = patch.dict(
            "recidiviz.persistence.persistence.SYSTEM_TYPE_TO_ERROR_THRESHOLD",
            {
                OVERALL_THRESHOLD: 0,
                ENUM_THRESHOLD: 0,
                ENTITY_MATCHING_THRESHOLD: 0,
                DATABASE_INVARIANT_THRESHOLD: 0,
            },
        )

        self.entity_matching_error_threshold_patcher.start()

        self.file_tags_processed: List[str] = []
        self.did_rerun_for_idempotence = False

    def tearDown(self) -> None:
        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.operations_database_key
        )
        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.main_database_key
        )
        self.metadata_patcher.stop()
        self.environment_patcher.stop()
        self.entity_matching_error_threshold_patcher.stop()

        if not is_ingest_in_dataflow_enabled(
            self.region_code(), self.ingest_instance()
        ):
            self._validate_integration_test()

    def _validate_integration_test(self) -> None:
        """If this test is the main integration test, validates that all expected files
        were processed during this test.
        """
        if FULL_INTEGRATION_TEST_NAME not in dir(self):
            raise ValueError(
                f"Must define integration test with name "
                f"[{FULL_INTEGRATION_TEST_NAME}] in this test class."
            )

        if (
            self._outcome.errors  # type: ignore
            or self._testMethodName != FULL_INTEGRATION_TEST_NAME
        ):
            # If test fails or this is not the integration test, do not validate
            return

        expected_tags = self.controller.get_ingest_view_rank_list()

        expected_tags_set = set(expected_tags)
        processed_tags_set = set(self.file_tags_processed)

        if skipped := expected_tags_set.difference(processed_tags_set):
            self.fail(f"Failed to run test for ingest view files: {skipped}")

        if extra := processed_tags_set.difference(expected_tags_set):
            self.fail(f"Found test for extra, unexpected ingest view files: {extra}")

        self.assertEqual(
            expected_tags,
            self.file_tags_processed,
            "Expected and processed tags do not match.",
        )

        self.assertTrue(
            self.did_rerun_for_idempotence,
            "No rerun for idempotence. Make sure the integration test calls "
            "_do_ingest_job_rerun_for_tags().",
        )

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )

    def invalidate_ingest_view_metadata(self) -> None:
        with SessionFactory.using_database(self.operations_database_key) as session:
            session.query(
                operations_schema.DirectIngestViewMaterializationMetadata
            ).update(
                {
                    operations_schema.DirectIngestViewMaterializationMetadata.is_invalidated: True
                }
            )

    def _run_ingest_job_for_filename(
        self, filename: str, is_rerun: bool = False
    ) -> None:
        """Runs ingest for the ingest view file with the given unnormalized file name.

        It reads the input from the following file:
        `recidiviz/tests/ingest/direct/direct_ingest_fixtures/ux_xx/{ingest_view_name}.csv`
        """
        # TODO(#15801): Move the fixture files to `ingest_view` subdirectory and pass
        # along a test case name to the materialization args.

        environ_patcher = patch.dict("os.environ", {"PERSIST_LOCALLY": "true"})
        environ_patcher.start()

        if not isinstance(self.controller.fs.gcs_file_system, FakeGCSFileSystem):
            raise ValueError(
                f"Controller fs must have type "
                f"FakeGCSFileSystem. Found instead "
                f"type [{type(self.controller.fs.gcs_file_system)}]"
            )

        now = DEFAULT_UPDATE_DATETIME
        yesterday = now - datetime.timedelta(days=1)
        file_tag, _ext = os.path.splitext(filename)

        if file_tag not in self.controller.get_ingest_view_rank_list():
            raise ValueError(
                f"Cannot run test for tag [{file_tag}] which is not returned by the "
                f"controller's get_ingest_view_rank_list() function."
            )

        if not is_rerun:
            self.file_tags_processed.append(file_tag)

        materialization_job_args = self._register_materialization_job(
            controller=self.controller,
            ingest_view_name=file_tag,
            upper_bound_datetime=now,
            lower_bound_datetime=yesterday,
        )

        self.controller.ingest_instance_status_manager.change_status_to(
            DirectIngestStatus.INGEST_VIEW_MATERIALIZATION_IN_PROGRESS
        )
        self.controller.do_ingest_view_materialization(materialization_job_args)

        run_task_queues_to_empty(self.controller)

        environ_patcher.stop()

    @staticmethod
    def _register_materialization_job(
        controller: BaseDirectIngestController,
        ingest_view_name: str,
        upper_bound_datetime: datetime.datetime,
        lower_bound_datetime: Optional[datetime.datetime],
    ) -> IngestViewMaterializationArgs:
        metadata_manager = controller.view_materialization_metadata_manager
        args = IngestViewMaterializationArgs(
            ingest_view_name=ingest_view_name,
            lower_bound_datetime_exclusive=lower_bound_datetime,
            upper_bound_datetime_inclusive=upper_bound_datetime,
            ingest_instance=metadata_manager.ingest_instance,
        )
        metadata_manager.register_ingest_materialization_job(args)
        return args

    def _do_ingest_job_rerun_for_tags(self, file_tags: List[str]) -> None:
        self.invalidate_ingest_view_metadata()
        ingest_view_contents = assert_type(
            self.controller.ingest_view_contents, FakeInstanceIngestViewContents
        )
        ingest_view_contents.test_clear_data()
        ingest_view_materializer = assert_type(
            self.controller.ingest_view_materializer(), FakeIngestViewMaterializer
        )
        ingest_view_materializer.processed_args.clear()
        for file_tag in file_tags:
            self._run_ingest_job_for_filename(f"{file_tag}.csv", is_rerun=True)
        self.did_rerun_for_idempotence = True

    @staticmethod
    def convert_and_clear_db_ids(db_entities: List[StateBase]) -> List[Entity]:
        converted = StateSchemaToEntityConverter().convert_all(
            db_entities, populate_back_edges=True
        )
        clear_db_ids(converted)
        return converted

    def assert_expected_db_root_entities(
        self,
        expected_db_root_entities: List[RootEntity],
        debug: bool = False,
        single_root_entity_to_debug: Optional[str] = None,
        print_tree_structure_only: bool = False,
        # Default arg caches across calls to this function
        field_index: CoreEntityFieldIndex = CoreEntityFieldIndex(),
    ) -> None:
        """Asserts that the set of expected people matches all the people that currently
         exist in the database.

        Args:
            expected_db_root_entities: (List[RootEntity) The list of entities we expect
                to find in the DB.
            debug: (bool) If true, prints out both the found and expected entity trees.
            single_root_entity_to_debug: (str) A string external_id of a root entity,
            such as StatePerson. If debug=True and this is not None, this
                will only check for equality between the root entities with that
                external_id. This should be used for debugging only and this function
                will throw if this value is set in CI.
            print_tree_structure_only: (bool) If True and debug=True, then the printed
                result only shows the tree structure - external ids and parent-child
                relationships.
        """

        if debug:
            print("\n\n************** ASSERTING *************")

        if not self.schema_type() == SchemaType.STATE:
            raise ValueError(f"Unsupported schema type [{self.schema_type()}]")

        expected_root_entities = cast(List[Entity], expected_db_root_entities)

        found_root_entities: List[Entity] = []
        found_schema_root_entities = []
        with SessionFactory.using_database(
            self.main_database_key, autocommit=False
        ) as session:
            found_people_from_db = dao.read_all_people(session)
            found_schema_root_entities.extend(found_people_from_db)
            found_root_entities.extend(
                cast(
                    List[StatePerson],
                    self.convert_and_clear_db_ids(found_people_from_db),
                )
            )
            found_staff_from_db = dao.read_all_staff(session)
            found_schema_root_entities.extend(found_staff_from_db)
            found_root_entities.extend(
                cast(
                    List[StateStaff],
                    self.convert_and_clear_db_ids(found_staff_from_db),
                )
            )

        if debug:
            if in_ci():
                self.fail("The |debug| flag should only be used for local debugging.")
            if single_root_entity_to_debug is not None:
                found_root_entities = [
                    e
                    for e in found_root_entities
                    if isinstance(e, (HasMultipleExternalIdsEntity, ExternalIdEntity))
                    and matches_external_id(e, single_root_entity_to_debug)
                ]
                expected_root_entities = [
                    e
                    for e in expected_root_entities
                    if isinstance(e, (HasMultipleExternalIdsEntity, ExternalIdEntity))
                    and matches_external_id(e, single_root_entity_to_debug)
                ]

            launch_entity_tree_html_diff_comparison(
                found_root_entities=found_root_entities,
                expected_root_entities=expected_db_root_entities,
                field_index=field_index,
                region_code=self.state_code().value.lower(),
                print_tree_structure_only=print_tree_structure_only,
            )

        self.assertCountEqual(found_root_entities, expected_root_entities)

        assert_no_unexpected_entities_in_db(found_schema_root_entities, session)

    def get_ingest_view_results_from_fixture(
        self, *, ingest_view_name: str, test_name: str
    ) -> List[Dict[str, Any]]:
        """Returns the ingest view results for a given ingest view from the fixture file."""
        fixture_columns = (
            self.ingest_view_manifest_collector()
            .ingest_view_to_manifest[ingest_view_name]
            .input_columns
        )
        records = load_dataframe_from_path(
            raw_fixture_path=direct_ingest_fixture_path(
                region_code=self.region_code().value,
                fixture_file_type=DirectIngestFixtureDataFileType.EXTRACT_AND_MERGE_INPUT,
                file_name=f"{test_name}.csv",
            ),
            fixture_columns=fixture_columns,
        ).to_dict("records")

        for record in records:
            record[MATERIALIZATION_TIME_COL_NAME] = datetime.datetime.now().isoformat()
            record[UPPER_BOUND_DATETIME_COL_NAME] = DEFAULT_UPDATE_DATETIME.isoformat()
            record[LOWER_BOUND_DATETIME_COL_NAME] = None

        return records

    def generate_ingest_view_results_for_one_date(
        self,
        project_id: str,
        state_code: StateCode,
        ingest_view_name: str,
        raw_data_tables_to_upperbound_dates: Dict[str, Optional[str]],
        ingest_instance: DirectIngestInstance,
        materialization_method: MaterializationMethod,
    ) -> FakeGenerateIngestViewResults:
        """Returns a constructor that generates ingest view results for a given ingest view assuming a single date."""
        return FakeGenerateIngestViewResults(
            project_id,
            state_code,
            ingest_view_name,
            raw_data_tables_to_upperbound_dates,
            ingest_instance,
            materialization_method,
            self.get_ingest_view_results_from_fixture(
                ingest_view_name=ingest_view_name,
                test_name=ingest_view_name,
            ),
        )

    def run_validations_with_output(
        self,
        expected_root_entities: List[RootEntity],
        debug: bool = False,
    ) -> Callable[
        [
            Iterable[str],
            CoreEntityFieldIndex,
            StateCode,
        ],
        RunValidationsWithOutputChecking,
    ]:
        def _inner_constructor(
            expected_output_entities: Iterable[str],
            field_index: CoreEntityFieldIndex,
            state_code: StateCode,
        ) -> RunValidationsWithOutputChecking:
            return RunValidationsWithOutputChecking(
                expected_output_entities,
                field_index,
                state_code,
                expected_root_entities,
                debug,
            )

        return _inner_constructor

    # TODO(#22059): Remove this method and replace with the default implementation once
    # all states can start an ingest integration test using raw data fixtures.
    def run_test_state_pipeline(
        self,
        ingest_view_results: Dict[str, Iterable[Dict[str, Any]]],
        expected_root_entities: List[RootEntity],
        ingest_view_results_only: bool = False,
        ingest_views_to_run: Optional[str] = None,
        field_index: CoreEntityFieldIndex = CoreEntityFieldIndex(),
        debug: bool = False,
    ) -> None:
        """This runs a test for an ingest pipeline where the ingest view results are
        passed in directly."""
        if not ingest_view_results:
            ingest_view_results = {
                ingest_view: []
                for ingest_view in self.ingest_view_manifest_collector().launchable_ingest_views(
                    ingest_instance=self.ingest_instance()
                )
            }
        expected_entities = [
            entity
            for root_entity in expected_root_entities
            for entity in get_all_entities_from_tree(
                cast(Entity, root_entity), field_index
            )
        ]

        expected_entity_types_to_expected_entities = {
            entity_type: [
                entity
                for entity in expected_entities
                if entity.get_entity_name() == entity_type
            ]
            for ingest_view in self.ingest_view_manifest_collector().launchable_ingest_views(
                ingest_instance=self.ingest_instance()
            )
            for entity_type in self.get_expected_output_entity_types(
                ingest_view_name=ingest_view,
            )
        }

        pipeline_args = default_arg_list_for_pipeline(
            pipeline=self.pipeline_class(),
            state_code=self.region_code().value,
            project_id=PROJECT_ID,
            unifying_id_field_filter_set=None,
            ingest_view_results_only=ingest_view_results_only,
            ingest_views_to_run=ingest_views_to_run,
        )

        with patch(
            "recidiviz.pipelines.ingest.state.pipeline.GenerateIngestViewResults",
            self.generate_ingest_view_results_for_one_date,
        ):
            with patch(
                "recidiviz.pipelines.ingest.state.pipeline.RunValidations",
                self.run_validations_with_output(expected_root_entities, debug),
            ):
                with patch(
                    "recidiviz.pipelines.ingest.state.pipeline.WriteToBigQuery",
                    self.create_fake_bq_write_sink_constructor(
                        self.get_expected_output(
                            ingest_view_results,
                            expected_entity_types_to_expected_entities,
                        )
                    ),
                ):
                    with patch(
                        "recidiviz.pipelines.base_pipeline.Pipeline",
                        pipeline_constructor(PROJECT_ID),
                    ):
                        self.pipeline_class().build_from_args(pipeline_args).run()


def matches_external_id(
    entity: Union[HasMultipleExternalIdsEntity, ExternalIdEntity], external_id: str
) -> bool:
    if isinstance(entity, HasMultipleExternalIdsEntity):
        return entity.has_external_id(external_id)
    if isinstance(entity, ExternalIdEntity):
        return entity.external_id == external_id
    raise ValueError(f"Unexpected entity type [{type(entity)}]")

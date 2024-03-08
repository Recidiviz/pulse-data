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
"""A class that represents a test case to be used for testing the ingest pipeline."""
import unittest
from collections import defaultdict
from copy import deepcopy
from datetime import datetime
from types import ModuleType
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple, Type, cast

import apache_beam
from apache_beam.testing.util import BeamAssertException
from google.cloud import bigquery
from mock import patch

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_utils import schema_for_sqlalchemy_table
from recidiviz.calculator.query.state.dataset_config import state_dataset_for_state_code
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import (
    ingest_view_materialization_results_dataset,
    raw_tables_dataset_for_region,
)
from recidiviz.ingest.direct.direct_ingest_regions import DirectIngestRegion
from recidiviz.ingest.direct.ingest_mappings.ingest_view_contents_context import (
    IngestViewContentsContextImpl,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_collector import (
    IngestViewManifestCollector,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_compiler_delegate import (
    StateSchemaIngestViewManifestCompilerDelegate,
)
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_import_manager import (
    DirectIngestRawFileImportManager,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRegionRawFileConfig,
)
from recidiviz.ingest.direct.types.direct_ingest_constants import (
    MATERIALIZATION_TIME_COL_NAME,
    UPPER_BOUND_DATETIME_COL_NAME,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewRawFileDependency,
    RawFileHistoricalRowsFilterType,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder_collector import (
    DirectIngestViewQueryBuilderCollector,
)
from recidiviz.persistence.database.schema.state import schema as state_schema
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.schema_utils import (
    get_all_table_classes_in_schema,
    get_database_entities_by_association_table,
    get_database_entity_by_table_name,
    get_state_database_association_with_names,
    is_association_table,
)
from recidiviz.persistence.entity.base_entity import Entity, RootEntity
from recidiviz.persistence.entity.entity_utils import (
    CoreEntityFieldIndex,
    get_all_entities_from_tree,
    get_all_entity_associations_from_tree,
)
from recidiviz.pipelines.base_pipeline import BasePipeline
from recidiviz.pipelines.ingest.state.generate_ingest_view_results import (
    ADDITIONAL_SCHEMA_COLUMNS,
)
from recidiviz.pipelines.ingest.state.generate_primary_keys import (
    generate_primary_key,
    string_representation,
)
from recidiviz.pipelines.ingest.state.pipeline import StateIngestPipeline
from recidiviz.pipelines.ingest.state.serialize_entities import (
    serialize_entity_into_json,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BQ_EMULATOR_PROJECT_ID,
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.ingest.direct import fake_regions
from recidiviz.tests.ingest.direct.fixture_util import (
    DirectIngestTestFixturePath,
    load_dataframe_from_path,
)
from recidiviz.tests.pipelines.fake_bigquery import (
    FakeReadAllFromBigQueryWithEmulator,
    FakeReadFromBigQueryWithEmulator,
    FakeWriteOutputToBigQueryWithValidator,
)
from recidiviz.tests.pipelines.utils.run_pipeline_test_utils import run_test_pipeline
from recidiviz.tests.utils.fake_region import fake_region

INGEST_INTEGRATION = "ingest_integration"


class BaseStateIngestPipelineTestCase(unittest.TestCase):
    """Base test case for testing ingest pipelines that does not use the BQ emulator."""

    @classmethod
    def region_code(cls) -> StateCode:
        return StateCode.US_DD

    @classmethod
    def region_module_override(cls) -> Optional[ModuleType]:
        return fake_regions

    @classmethod
    def region(cls) -> DirectIngestRegion:
        return fake_region(
            region_code=cls.region_code().value.lower(),
            environment="staging",
            region_module=cls.region_module_override(),
        )

    @classmethod
    def ingest_instance(cls) -> DirectIngestInstance:
        return DirectIngestInstance.SECONDARY

    @classmethod
    def ingest_view_manifest_collector(cls) -> IngestViewManifestCollector:
        return IngestViewManifestCollector(
            cls.region(),
            delegate=StateSchemaIngestViewManifestCompilerDelegate(cls.region()),
        )

    @classmethod
    def ingest_view_collector(cls) -> DirectIngestViewQueryBuilderCollector:
        return DirectIngestViewQueryBuilderCollector(
            cls.region(),
            cls.ingest_view_manifest_collector().launchable_ingest_views(
                ingest_instance=cls.ingest_instance()
            ),
        )

    @classmethod
    def pipeline_class(cls) -> Type[BasePipeline]:
        return StateIngestPipeline

    @classmethod
    def expected_ingest_view_dataset(cls) -> str:
        return ingest_view_materialization_results_dataset(
            cls.region_code(), cls.ingest_instance(), "sandbox"
        )

    @classmethod
    def expected_state_dataset(cls) -> str:
        return state_dataset_for_state_code(
            cls.region_code(), cls.ingest_instance(), "sandbox"
        )

    def setUp(self) -> None:
        super().setUp()
        self.region_patcher = patch(
            "recidiviz.ingest.direct.direct_ingest_regions.get_direct_ingest_region"
        )
        self.region_patcher.start().return_value = self.region()

    def tearDown(self) -> None:
        self.region_patcher.stop()
        super().tearDown()

    def get_ingest_view_results_from_fixture(
        self,
        *,
        ingest_view_name: str,
        test_name: str,
    ) -> Iterable[Dict[str, Any]]:
        """Reads in fixtures for ingest view results - if there is no metadata associated
        with the ingest view results, generates a default metadata for the results."""
        return load_dataframe_from_path(
            raw_fixture_path=DirectIngestTestFixturePath.for_ingest_view_test_results_fixture(
                region_code=self.region().region_code,
                ingest_view_name=ingest_view_name,
                file_name=f"{test_name}.csv",
            ).full_path(),
            fixture_columns=[
                *self.ingest_view_manifest_collector()
                .ingest_view_to_manifest[ingest_view_name]
                .input_columns,
                MATERIALIZATION_TIME_COL_NAME,
                UPPER_BOUND_DATETIME_COL_NAME,
            ],
        ).to_dict("records")

    def get_expected_root_entities_from_fixture(
        self,
        *,
        ingest_view_name: str,
        test_name: str,
    ) -> Iterable[Entity]:
        rows = list(
            self.get_ingest_view_results_from_fixture(
                ingest_view_name=ingest_view_name,
                test_name=test_name,
            )
        )
        for row in rows:
            for column in ADDITIONAL_SCHEMA_COLUMNS:
                if column.name in row:
                    row.pop(column.name)
        return (
            self.ingest_view_manifest_collector()
            .ingest_view_to_manifest[ingest_view_name]
            .parse_contents(
                contents_iterator=iter(rows),
                context=IngestViewContentsContextImpl(
                    ingest_instance=self.ingest_instance()
                ),
            )
        )

    def get_expected_output_entity_types(
        self,
        *,
        ingest_view_name: str,
    ) -> Set[str]:
        return {
            entity_cls.get_entity_name()
            for entity_cls in self.ingest_view_manifest_collector()
            .ingest_view_to_manifest[ingest_view_name]
            .hydrated_entity_classes()
        }

    def get_expected_output(
        self,
        ingest_view_results: Dict[str, Iterable[Dict[str, Any]]],
        expected_entity_type_to_entities: Dict[str, List[Entity]],
        expected_entity_association_type_to_associations: Dict[
            str, Set[Tuple[str, str]]
        ],
        field_index: CoreEntityFieldIndex = CoreEntityFieldIndex(),
    ) -> Dict[BigQueryAddress, Iterable[Dict[str, Any]]]:
        """Forms a BigQueryAddress to expected output mapping for the pipeline to validate"""
        expected_output: Dict[BigQueryAddress, Iterable[Dict[str, Any]]] = {}
        for ingest_view, results in ingest_view_results.items():
            expected_output[
                BigQueryAddress(
                    dataset_id=self.expected_ingest_view_dataset(), table_id=ingest_view
                )
            ] = results
        for entity_type, entities in expected_entity_type_to_entities.items():
            expected_output[
                BigQueryAddress(
                    dataset_id=self.expected_state_dataset(), table_id=entity_type
                )
            ] = [serialize_entity_into_json(entity, field_index) for entity in entities]
        for (
            entity_association,
            associations,
        ) in expected_entity_association_type_to_associations.items():
            child_cls, parent_cls = get_database_entities_by_association_table(
                state_schema, entity_association
            )
            expected_output[
                BigQueryAddress(
                    dataset_id=self.expected_state_dataset(),
                    table_id=entity_association,
                )
            ] = [
                {
                    parent_cls.get_primary_key_column_name(): generate_primary_key(
                        string_representation(
                            {(parent_external_id, parent_cls.get_class_id_name())}
                        ),
                        self.region_code(),
                    ),
                    child_cls.get_primary_key_column_name(): generate_primary_key(
                        string_representation(
                            {(child_external_id, child_cls.get_class_id_name())}
                        ),
                        self.region_code(),
                    ),
                    "state_code": self.region_code().value,
                }
                for child_external_id, parent_external_id in associations
            ]
        return expected_output

    def create_fake_bq_write_sink_constructor(
        self, expected_output: Dict[BigQueryAddress, Iterable[Dict[str, Any]]]
    ) -> Callable[
        [
            str,
            str,
            apache_beam.io.BigQueryDisposition,
        ],
        FakeWriteOutputToBigQueryWithValidator,
    ]:
        """Creates a fake BQ write sink constructor that validates the output of both
        ingest view output and state entity output."""

        def _write_constructor(
            output_table: str,
            output_dataset: str,
            write_disposition: apache_beam.io.BigQueryDisposition,
        ) -> FakeWriteOutputToBigQueryWithValidator:
            if write_disposition != apache_beam.io.BigQueryDisposition.WRITE_TRUNCATE:
                raise ValueError(
                    f"Write disposition [{write_disposition}] does not match expected disposition "
                    f"[{apache_beam.io.BigQueryDisposition.WRITE_TRUNCATE}] writing to table [{output_table}]"
                )

            output_address = BigQueryAddress(
                dataset_id=output_dataset, table_id=output_table
            )
            if is_association_table(output_table):
                child_cls, parent_cls = get_database_entities_by_association_table(
                    state_schema, output_table
                )
                child_output_address = BigQueryAddress(
                    dataset_id=output_dataset, table_id=child_cls.__tablename__
                )
                parent_output_address = BigQueryAddress(
                    dataset_id=output_dataset, table_id=parent_cls.__tablename__
                )
                if child_output_address not in expected_output:
                    raise ValueError(
                        f"Output table [{child_output_address.to_str()}] not in expected output "
                        f"[{list(expected_output.keys())}]"
                    )
                if parent_output_address not in expected_output:
                    raise ValueError(
                        f"Output table [{parent_output_address.to_str()}] not in expected output "
                        f"[{list(expected_output.keys())}]"
                    )

            elif output_address not in expected_output:
                raise ValueError(
                    f"Output table [{output_address.to_str()}] not in expected output "
                    f"[{list(expected_output.keys())}]"
                )

            if output_dataset not in (
                self.expected_ingest_view_dataset(),
                self.expected_state_dataset(),
            ):
                raise ValueError(
                    f"Output dataset {output_dataset} does not match expected datasets: {self.expected_ingest_view_dataset()}, {self.expected_state_dataset()}"
                )

            validator_fn_generator = (
                self.validate_entity_results
                if output_dataset == self.expected_state_dataset()
                else self.validate_ingest_view_results
            )

            return FakeWriteOutputToBigQueryWithValidator(
                output_address=output_address,
                expected_output=expected_output.get(output_address, []),
                validator_fn_generator=validator_fn_generator,
            )

        return _write_constructor

    @staticmethod
    def validate_ingest_view_results(
        expected_output: Iterable[Dict[str, Any]],
        _output_table: str,
    ) -> Callable[[Iterable[Dict[str, Any]]], None]:
        """Allows for validating the output of ingest view results without worrying about
        the output of the materialization time."""

        def _validate_ingest_view_results_output(
            output: Iterable[Dict[str, Any]]
        ) -> None:
            # TODO(#22059): Remove this once all states can start an ingest integration
            # test using raw data fixtures.
            if not expected_output:
                return
            copy_of_expected_output = deepcopy(expected_output)
            for record in copy_of_expected_output:
                record.pop(MATERIALIZATION_TIME_COL_NAME)
            copy_of_output = deepcopy(output)
            for record in copy_of_output:
                if not MATERIALIZATION_TIME_COL_NAME in record:
                    raise BeamAssertException("Missing materialization time column")
                record.pop(MATERIALIZATION_TIME_COL_NAME)
                record[UPPER_BOUND_DATETIME_COL_NAME] = datetime.fromisoformat(
                    record[UPPER_BOUND_DATETIME_COL_NAME]
                ).isoformat()
            if copy_of_output != copy_of_expected_output:
                raise BeamAssertException(
                    f"Output does not match expected output: output is {copy_of_output}, expected is {copy_of_expected_output}"
                )

        return _validate_ingest_view_results_output

    # TODO(#29030): Failures inside this validator are really hard to debug (only show
    #  one row that isn't present in expected) and the |debug| doesn't thread through
    #  here to launch an HTML diff showing the difference.
    @staticmethod
    def validate_entity_results(
        expected_output: Iterable[Dict[str, Any]],
        output_table: str,
    ) -> Callable[[Iterable[Dict[str, Any]]], None]:
        """Asserts that the pipeline produces dictionaries with the expected keys
        corresponding to the column names in the table into which the output will be
        written."""

        def _validate_entity_output(output: Iterable[Dict[str, Any]]) -> None:
            # TODO(#24080) Transition to using entities.py to get the expected column names
            schema: List[bigquery.SchemaField] = []

            tables_by_name = {
                t.name: t for t in get_all_table_classes_in_schema(SchemaType.STATE)
            }
            if is_association_table(output_table):
                child_cls, parent_cls = get_database_entities_by_association_table(
                    state_schema, output_table
                )
                schema = schema_for_sqlalchemy_table(
                    get_state_database_association_with_names(
                        child_cls.__name__, parent_cls.__name__
                    ),
                    add_state_code_field=True,
                )
            elif output_table in tables_by_name:
                schema = schema_for_sqlalchemy_table(tables_by_name[output_table])

            expected_column_names = {field.name for field in schema}
            for output_dict in output:
                if output_dict.keys() != expected_column_names:
                    raise BeamAssertException(
                        "Output dictionary does not have "
                        f"the expected keys. Expected: [{expected_column_names}], "
                        f"found: [{list(output_dict.keys())}]."
                    )

            if is_association_table(output_table):
                for record in output:
                    if record not in expected_output:
                        raise BeamAssertException(
                            f"Unexpected output: {record} not in {expected_output}"
                        )
            else:
                entity = get_database_entity_by_table_name(state_schema, output_table)
                id_columns = {
                    column_name
                    for column_name in expected_column_names
                    if column_name == entity.get_primary_key_column_name()
                    or column_name in entity.get_foreign_key_names()
                }
                copy_of_expected_output = deepcopy(expected_output)
                copy_of_output = deepcopy(output)
                for record in copy_of_output:
                    for column in id_columns:
                        if column in record:
                            record[column] = None
                    if record not in copy_of_expected_output:
                        raise BeamAssertException(
                            f"Unexpected output: {record} not in {copy_of_expected_output}"
                        )

        return _validate_entity_output


class StateIngestPipelineTestCase(
    BaseStateIngestPipelineTestCase, BigQueryEmulatorTestCase
):
    """A test case class to test the ingest pipeline, from raw data all the way to entity
    generation and merging. This does use the BQ emulator."""

    def setUp(self) -> None:
        super().setUp()
        self.raw_data_tables_dataset = raw_tables_dataset_for_region(
            self.region_code(), self.ingest_instance()
        )
        self.direct_ingest_raw_file_config = DirectIngestRegionRawFileConfig(
            region_code=self.region_code().value, region_module=fake_regions
        )

        self.raw_file_config_patcher = patch(
            "recidiviz.ingest.direct.views.direct_ingest_view_query_builder"
            ".get_region_raw_file_config"
        )
        self.raw_file_config_patcher.start().return_value = (
            self.direct_ingest_raw_file_config
        )

    def tearDown(self) -> None:
        self.raw_file_config_patcher.stop()
        super().tearDown()

    def setup_single_ingest_view_raw_data_bq_tables(
        self, ingest_view_name: str, test_name: str
    ) -> None:
        ingest_view_builder = (
            self.ingest_view_collector().get_query_builder_by_view_name(
                ingest_view_name
            )
        )
        for (
            raw_table_dependency_config
        ) in ingest_view_builder.raw_table_dependency_configs:
            self._load_bq_table_for_raw_dependency(
                raw_table_dependency_config, test_name=test_name
            )

    def setup_region_raw_data_bq_tables(self, test_name: str) -> None:
        # Deduplicate raw table dependencies where multiple tables are read from
        # same order.
        configs_by_name_and_filter_type: Dict[
            str,
            Dict[RawFileHistoricalRowsFilterType, DirectIngestViewRawFileDependency],
        ] = defaultdict(dict)
        for (
            ingest_view
        ) in self.ingest_view_manifest_collector().launchable_ingest_views(
            ingest_instance=self.ingest_instance()
        ):
            ingest_view_builder = (
                self.ingest_view_collector().get_query_builder_by_view_name(ingest_view)
            )
            for (
                raw_table_dependency_config
            ) in ingest_view_builder.raw_table_dependency_configs:
                configs_by_name_and_filter_type[
                    raw_table_dependency_config.raw_file_config.file_tag
                ][raw_table_dependency_config.filter_type] = raw_table_dependency_config

        for config_by_type in configs_by_name_and_filter_type.values():
            for raw_table_dependency_config in config_by_type.values():
                self._load_bq_table_for_raw_dependency(
                    raw_table_dependency_config, test_name=test_name
                )

    def _load_bq_table_for_raw_dependency(
        self,
        raw_table_dependency_config: DirectIngestViewRawFileDependency,
        test_name: str,
    ) -> None:
        """Sets up the BQ emulator with appropriate raw data tables for the test region."""
        table_address = BigQueryAddress(
            dataset_id=self.raw_data_tables_dataset,
            table_id=raw_table_dependency_config.raw_file_config.file_tag,
        )

        schema = DirectIngestRawFileImportManager.create_raw_table_schema(
            raw_file_config=raw_table_dependency_config.raw_file_config
        )
        self.create_mock_table(table_address, schema)

        self.load_rows_into_table(
            table_address,
            data=load_dataframe_from_path(
                raw_fixture_path=DirectIngestTestFixturePath.for_raw_file_fixture(
                    region_code=self.region().region_code,
                    raw_file_dependency_config=raw_table_dependency_config,
                    file_name=f"{test_name}.csv",
                ).full_path(),
                fixture_columns=[c.name for c in schema],
            ).to_dict("records"),
        )

    def create_fake_bq_read_source_constructor(
        self,
        query: str,
    ) -> FakeReadFromBigQueryWithEmulator:
        return FakeReadFromBigQueryWithEmulator(query=query, test_case=self)

    def create_fake_bq_read_all_source_constructor(
        self,
    ) -> FakeReadAllFromBigQueryWithEmulator:
        return FakeReadAllFromBigQueryWithEmulator()

    def run_test_state_pipeline(
        self,
        ingest_view_results: Dict[str, Iterable[Dict[str, Any]]],
        expected_root_entities: List[RootEntity],
        ingest_view_results_only: bool = False,
        ingest_views_to_run: Optional[str] = None,
        field_index: CoreEntityFieldIndex = CoreEntityFieldIndex(),
        debug: bool = False,  # pylint: disable=unused-argument
    ) -> None:
        """Runs the state ingest pipeline and validates the output."""
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
        expected_entity_association_type_to_associations = defaultdict(set)
        for root_entity in expected_root_entities:
            for (
                association_table,
                associations,
            ) in get_all_entity_associations_from_tree(
                cast(Entity, root_entity), field_index
            ).items():
                expected_entity_association_type_to_associations[
                    association_table
                ].update(associations)

        run_test_pipeline(
            pipeline_cls=self.pipeline_class(),
            state_code=self.region_code().value,
            project_id=BQ_EMULATOR_PROJECT_ID,
            read_from_bq_constructor=self.create_fake_bq_read_source_constructor,
            write_to_bq_constructor=self.create_fake_bq_write_sink_constructor(
                expected_output=self.get_expected_output(
                    ingest_view_results,
                    expected_entity_types_to_expected_entities,
                    expected_entity_association_type_to_associations,
                )
            ),
            read_all_from_bq_constructor=self.create_fake_bq_read_all_source_constructor,
            ingest_view_results_only=ingest_view_results_only,
            ingest_views_to_run=ingest_views_to_run,
        )

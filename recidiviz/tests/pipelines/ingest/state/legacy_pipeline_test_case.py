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
from copy import deepcopy
from datetime import datetime
from typing import Any, Callable, Dict, Iterable, List, Set, Tuple, Type

import apache_beam
from apache_beam.testing.util import BeamAssertException
from google.cloud import bigquery
from mock import patch

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_utils import schema_for_sqlalchemy_table
from recidiviz.ingest.direct.types.direct_ingest_constants import (
    MATERIALIZATION_TIME_COL_NAME,
    UPPER_BOUND_DATETIME_COL_NAME,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema.state import schema as state_schema
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.schema_utils import (
    get_all_table_classes_in_schema,
    get_database_entities_by_association_table,
    get_database_entity_by_table_name,
    get_state_database_association_with_names,
    is_association_table,
)
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entity_utils import get_module_for_entity_class
from recidiviz.pipelines.base_pipeline import BasePipeline
from recidiviz.pipelines.ingest.dataset_config import (
    ingest_view_materialization_results_dataset,
    state_dataset_for_state_code,
)
from recidiviz.pipelines.ingest.state.generate_primary_keys import (
    generate_primary_key,
    string_representation,
)
from recidiviz.pipelines.ingest.state.pipeline import StateIngestPipeline
from recidiviz.pipelines.ingest.state.serialize_entities import (
    serialize_entity_into_json,
)
from recidiviz.source_tables.collect_all_source_table_configs import (
    build_raw_data_source_table_collections_for_state_and_instance,
)
from recidiviz.source_tables.source_table_config import SourceTableCollection
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.ingest.direct.direct_ingest_raw_fixture_loader import (
    DirectIngestRawDataFixtureLoader,
)
from recidiviz.tests.pipelines.fake_bigquery import (
    FakeWriteOutputToBigQueryWithValidator,
)
from recidiviz.tests.pipelines.ingest.state.ingest_region_test_mixin import (
    IngestRegionTestMixin,
)
from recidiviz.tests.pipelines.utils.run_pipeline_test_utils import (
    DEFAULT_TEST_PIPELINE_OUTPUT_SANDBOX_PREFIX,
)


# TODO(#22059): Standardize ingest view result fixtures for pipeline and ingest view
# tests, and update this test case to load those fixtures using the same
# common code as the ingest view test.
# TODO(#29517): Deprecate this class and all subclasses in favor of the new
#  StateIngestPipelineTestCase
class BaseLegacyStateIngestPipelineTestCase(
    BigQueryEmulatorTestCase, IngestRegionTestMixin
):
    """Base test case for testing ingest dataflow pipelines using the BigQueryEmulator."""

    wipe_emulator_data_on_teardown = False

    @classmethod
    def pipeline_class(cls) -> Type[BasePipeline]:
        return StateIngestPipeline

    @classmethod
    def expected_ingest_view_dataset(cls) -> str:
        return ingest_view_materialization_results_dataset(
            cls.state_code(), DEFAULT_TEST_PIPELINE_OUTPUT_SANDBOX_PREFIX
        )

    @classmethod
    def expected_state_dataset(cls) -> str:
        return state_dataset_for_state_code(
            cls.state_code(), DEFAULT_TEST_PIPELINE_OUTPUT_SANDBOX_PREFIX
        )

    @classmethod
    def get_source_tables(cls) -> list[SourceTableCollection]:
        return build_raw_data_source_table_collections_for_state_and_instance(
            cls.state_code(),
            DirectIngestInstance.PRIMARY,
            region_module_override=cls.region_module_override(),
        )

    def setUp(self) -> None:
        super().setUp()
        self.region_patcher = patch(
            "recidiviz.ingest.direct.direct_ingest_regions.get_direct_ingest_region"
        )
        self.region_patcher.start().return_value = self.region()
        self.raw_fixture_loader = DirectIngestRawDataFixtureLoader(
            state_code=self.state_code(),
            emulator_test=self,
            region_module=self.region_module_override(),
        )

    def tearDown(self) -> None:
        self.region_patcher.stop()
        self._clear_emulator_table_data()
        super().tearDown()

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
            ] = [
                serialize_entity_into_json(
                    entity, entities_module=get_module_for_entity_class(type(entity))
                )
                for entity in entities
            ]
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
                        self.state_code(),
                    ),
                    child_cls.get_primary_key_column_name(): generate_primary_key(
                        string_representation(
                            {(child_external_id, child_cls.get_class_id_name())}
                        ),
                        self.state_code(),
                    ),
                    "state_code": self.state_code().value,
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
                        # TODO(#24080) Transition to using entities.py to get the expected column names
                        "Are BOTH entities.py and schema.py up to date?"
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

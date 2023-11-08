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
from collections import defaultdict
from copy import deepcopy
from datetime import datetime
from typing import Any, Callable, Dict, Iterable, List, Set

import apache_beam
from apache_beam.testing.util import BeamAssertException
from google.cloud import bigquery
from mock import patch

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_utils import schema_for_sqlalchemy_table
from recidiviz.calculator.query.state.dataset_config import state_dataset_for_state_code
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import (
    ingest_view_materialization_results_dataflow_dataset,
    raw_tables_dataset_for_region,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_contents_context import (
    IngestViewContentsContextImpl,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_collector import (
    IngestViewManifestCollector,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_compiler_delegate import (
    IngestViewManifestCompilerDelegateImpl,
)
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_import_manager import (
    DirectIngestRawFileImportManager,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRegionRawFileConfig,
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
    get_database_entities_by_association_table,
    get_state_database_association_with_names,
    get_state_entity_names,
    get_state_table_classes,
    get_table_class_by_name,
    is_association_table,
)
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entity_utils import (
    CoreEntityFieldIndex,
    get_all_entities_from_tree,
)
from recidiviz.pipelines.ingest.state.generate_ingest_view_results import (
    ADDITIONAL_SCHEMA_COLUMNS,
    LOWER_BOUND_DATETIME_COL_NAME,
    MATERIALIZATION_TIME_COL_NAME,
    UPPER_BOUND_DATETIME_COL_NAME,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.ingest.direct import fake_regions
from recidiviz.tests.ingest.direct.fixture_util import (
    ingest_view_raw_table_dependency_fixture_path,
    ingest_view_results_fixture_path,
    load_dataframe_from_path,
)
from recidiviz.tests.pipelines.fake_bigquery import (
    FakeReadAllFromBigQueryWithEmulator,
    FakeReadFromBigQueryWithEmulator,
    FakeWriteOutputToBigQueryWithValidator,
)
from recidiviz.tests.utils.fake_region import fake_region


class StateIngestPipelineTestCase(BigQueryEmulatorTestCase):
    """A test case class to test the ingest pipeline."""

    def setUp(self) -> None:
        super().setUp()
        self.region_code = StateCode.US_DD
        self.raw_data_tables_dataset = raw_tables_dataset_for_region(
            StateCode.US_DD, DirectIngestInstance.PRIMARY
        )
        self.region = fake_region(
            region_code=self.region_code.value.lower(),
            environment="staging",
            region_module=fake_regions,
        )
        self.direct_ingest_raw_file_config = DirectIngestRegionRawFileConfig(
            region_code=StateCode.US_DD.value, region_module=fake_regions
        )

        self.region_patcher = patch(
            "recidiviz.ingest.direct.direct_ingest_regions.get_direct_ingest_region"
        )
        self.region_patcher.start().return_value = self.region

        self.raw_file_config_patcher = patch(
            "recidiviz.ingest.direct.views.direct_ingest_view_query_builder"
            ".get_region_raw_file_config"
        )
        self.raw_file_config_patcher.start().return_value = (
            self.direct_ingest_raw_file_config
        )

        self.ingest_instance = DirectIngestInstance.PRIMARY
        self.ingest_view_manifest_collector = IngestViewManifestCollector(
            self.region,
            delegate=IngestViewManifestCompilerDelegateImpl(
                self.region,
                schema_type=SchemaType.STATE,
            ),
        )
        self.view_collector = DirectIngestViewQueryBuilderCollector(
            self.region,
            self.ingest_view_manifest_collector.launchable_ingest_views(
                ingest_instance=self.ingest_instance
            ),
        )
        self.field_index = CoreEntityFieldIndex()

        self.expected_ingest_view_dataset = (
            ingest_view_materialization_results_dataflow_dataset(
                self.region_code, self.ingest_instance, "sandbox"
            )
        )
        self.expected_state_dataset = state_dataset_for_state_code(
            self.region_code, self.ingest_instance, "sandbox"
        )

    def tearDown(self) -> None:
        super().tearDown()
        self.region_patcher.stop()
        self.raw_file_config_patcher.stop()

    def setup_single_ingest_view_raw_data_bq_tables(
        self, ingest_view_name: str, test_name: str
    ) -> None:
        ingest_view_builder = self.view_collector.get_query_builder_by_view_name(
            ingest_view_name
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
        for ingest_view in self.ingest_view_manifest_collector.launchable_ingest_views(
            ingest_instance=self.ingest_instance
        ):
            ingest_view_builder = self.view_collector.get_query_builder_by_view_name(
                ingest_view
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
                raw_fixture_path=ingest_view_raw_table_dependency_fixture_path(
                    self.region.region_code,
                    raw_file_dependency_config=raw_table_dependency_config,
                    file_name=f"{test_name}.csv",
                ),
                fixture_columns=[c.name for c in schema],
            ).to_dict("records"),
        )

    def get_ingest_view_results_from_fixture(
        self, *, ingest_view_name: str, test_name: str
    ) -> Iterable[Dict[str, Any]]:
        return load_dataframe_from_path(
            raw_fixture_path=ingest_view_results_fixture_path(
                region_code=self.region.region_code,
                ingest_view_name=ingest_view_name,
                file_name=f"{test_name}.csv",
            ),
            fixture_columns=[
                *self.ingest_view_manifest_collector.ingest_view_to_manifest[
                    ingest_view_name
                ].input_columns,
                MATERIALIZATION_TIME_COL_NAME,
                UPPER_BOUND_DATETIME_COL_NAME,
                LOWER_BOUND_DATETIME_COL_NAME,
            ],
        ).to_dict("records")

    def get_expected_root_entities(
        self, *, ingest_view_name: str, test_name: str
    ) -> Iterable[Entity]:
        rows = list(
            self.get_ingest_view_results_from_fixture(
                ingest_view_name=ingest_view_name, test_name=test_name
            )
        )
        for row in rows:
            for column in ADDITIONAL_SCHEMA_COLUMNS:
                row.pop(column.name)
        return self.ingest_view_manifest_collector.ingest_view_to_manifest[
            ingest_view_name
        ].parse_contents(
            contents_iterator=iter(rows),
            context=IngestViewContentsContextImpl(
                ingest_instance=self.ingest_instance,
                results_update_datetime=datetime.now(),
            ),
        )

    def get_expected_output_entity_types(
        self, *, ingest_view_name: str, test_name: str
    ) -> Set[str]:
        return {
            entity.get_entity_name()
            for root_entity in self.get_expected_root_entities(
                ingest_view_name=ingest_view_name, test_name=test_name
            )
            for entity in get_all_entities_from_tree(root_entity, self.field_index)
        }

    def create_fake_bq_read_source_constructor(
        self,
        query: str,
    ) -> FakeReadFromBigQueryWithEmulator:
        return FakeReadFromBigQueryWithEmulator(query=query, test_case=self)

    def create_fake_bq_read_all_source_constructor(
        self,
    ) -> FakeReadAllFromBigQueryWithEmulator:
        return FakeReadAllFromBigQueryWithEmulator()

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
            if output_address not in expected_output:
                raise ValueError(
                    f"Output table [{output_address.to_str()}] not in expected output "
                    f"[{list(expected_output.keys())}]"
                )

            if output_dataset not in (
                self.expected_ingest_view_dataset,
                self.expected_state_dataset,
            ):
                raise ValueError(
                    f"Output dataset {output_dataset} does not match expected datasets: {self.expected_ingest_view_dataset}, {self.expected_state_dataset}"
                )

            validator_fn_generator = (
                self.validate_entity_results
                if output_dataset == self.expected_state_dataset
                else self.validate_ingest_view_results
            )

            return FakeWriteOutputToBigQueryWithValidator(
                output_address=output_address,
                expected_output=expected_output[output_address],
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
            copy_of_expected_output = deepcopy(expected_output)
            for record in copy_of_expected_output:
                record.pop(MATERIALIZATION_TIME_COL_NAME)
            copy_of_output = deepcopy(output)
            for record in copy_of_output:
                if not MATERIALIZATION_TIME_COL_NAME in record:
                    raise BeamAssertException("Missing materialization time column")
                record.pop(MATERIALIZATION_TIME_COL_NAME)
                if record[LOWER_BOUND_DATETIME_COL_NAME]:
                    record[LOWER_BOUND_DATETIME_COL_NAME] = datetime.fromisoformat(
                        record[LOWER_BOUND_DATETIME_COL_NAME]
                    ).isoformat()
                record[UPPER_BOUND_DATETIME_COL_NAME] = datetime.fromisoformat(
                    record[UPPER_BOUND_DATETIME_COL_NAME]
                ).isoformat()
            if copy_of_output != copy_of_expected_output:
                raise BeamAssertException(
                    f"Output does not match expected output: output is {copy_of_output}, expected is {copy_of_expected_output}"
                )

        return _validate_ingest_view_results_output

    @staticmethod
    def validate_entity_results(
        _expected_output: Iterable[Dict[str, Any]],
        output_table: str,
    ) -> Callable[[Iterable[Dict[str, Any]]], None]:
        """Asserts that the pipeline produces dictionaries with the expected keys
        corresponding to the column names in the table into which the output will be
        written."""

        def _validate_entity_output(output: Iterable[Dict[str, Any]]) -> None:
            # TODO(#24080) Transition to using entities.py to get the expected column names
            schema: List[bigquery.SchemaField] = []
            if output_table in get_state_entity_names():
                schema = schema_for_sqlalchemy_table(
                    get_table_class_by_name(
                        output_table, list(get_state_table_classes())
                    )
                )

            elif is_association_table(output_table):
                child_cls, parent_cls = get_database_entities_by_association_table(
                    state_schema, output_table
                )
                schema = schema_for_sqlalchemy_table(
                    get_state_database_association_with_names(
                        child_cls.__name__, parent_cls.__name__
                    )
                )

            expected_column_names = {field.name for field in schema}
            for output_dict in output:
                if set(output_dict.keys()) != expected_column_names:
                    raise BeamAssertException(
                        "Output dictionary does not have "
                        f"the expected keys. Expected: [{expected_column_names}], "
                        f"found: [{list(output_dict.keys())}]."
                    )

            # TODO(#24202) Add ability to check exact state output.

        return _validate_entity_output

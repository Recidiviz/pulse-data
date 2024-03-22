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
IngestRawFileImportControllers.
"""
import abc
import datetime
from collections import defaultdict
from copy import deepcopy
from functools import cmp_to_key
from types import ModuleType
from typing import Any, Callable, Dict, Iterable, List, Optional, Union, cast

import apache_beam as beam
from apache_beam.pvalue import PBegin
from apache_beam.testing.util import BeamAssertException, assert_that
from mock import patch

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct import regions
from recidiviz.ingest.direct.direct_ingest_regions import (
    DirectIngestRegion,
    get_direct_ingest_region,
)
from recidiviz.ingest.direct.types.direct_ingest_constants import (
    MATERIALIZATION_TIME_COL_NAME,
    UPPER_BOUND_DATETIME_COL_NAME,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
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
    get_all_entity_associations_from_tree,
)
from recidiviz.persistence.entity_matching.entity_merger_utils import (
    root_entity_external_id_keys,
)
from recidiviz.pipelines.ingest.state.generate_ingest_view_results import (
    GenerateIngestViewResults,
)
from recidiviz.pipelines.ingest.state.run_validations import RunValidations
from recidiviz.tests.ingest.direct.fixture_util import (
    DirectIngestTestFixturePath,
    load_dataframe_from_path,
)
from recidiviz.tests.ingest.direct.regions.state_ingest_view_parser_test_base import (
    DEFAULT_UPDATE_DATETIME,
)
from recidiviz.tests.persistence.entity.state.entities_test_utils import clear_db_ids
from recidiviz.tests.pipelines.ingest.state.test_case import (
    BaseStateIngestPipelineTestCase,
)
from recidiviz.tests.pipelines.utils.run_pipeline_test_utils import (
    default_arg_list_for_pipeline,
    pipeline_constructor,
)
from recidiviz.tests.test_debug_helpers import launch_entity_tree_html_diff_comparison
from recidiviz.utils.environment import in_ci


class FakeGenerateIngestViewResults(GenerateIngestViewResults):
    def __init__(
        self,
        project_id: str,
        state_code: StateCode,
        ingest_view_name: str,
        raw_data_tables_to_upperbound_dates: Dict[str, Optional[str]],
        ingest_instance: DirectIngestInstance,
        fake_ingest_view_results: Iterable[Dict[str, Any]],
    ) -> None:
        super().__init__(
            project_id,
            state_code,
            ingest_view_name,
            raw_data_tables_to_upperbound_dates,
            ingest_instance,
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
    ) -> Callable[[Iterable[RootEntity]], None]:
        def _equal_to(actual: Iterable[RootEntity]) -> None:
            copy_of_actual = deepcopy(actual)
            clear_db_ids([cast(CoreEntity, entity) for entity in copy_of_actual])
            sorted_actual = _sort_root_entities(copy_of_actual)
            sorted_expected = _sort_root_entities(expected_root_entity_output)
            if sorted_actual != sorted_expected:
                if debug:
                    if in_ci():
                        raise ValueError(
                            "The |debug| flag should only be used for local debugging."
                        )
                    launch_entity_tree_html_diff_comparison(
                        found_root_entities=sorted_actual,  # type: ignore
                        expected_root_entities=sorted_expected,  # type: ignore
                        field_index=CoreEntityFieldIndex(),
                        region_code=state_code.value.lower(),
                    )
                raise BeamAssertException(
                    "Entity lists not equal. Rerun this test with debug=True to see diff."
                )

        return _equal_to


def _sort_root_entities(root_entities: Iterable[RootEntity]) -> List[RootEntity]:
    """Sorts the input collection of root_entities so that they can be compared to
    another collection of root entities. Sort is only deterministic if all root entities
    have external ids (this should be checked on pipeline output by a previous pipeline
    step) and no two root entities share a matching external id (for correct output,
    this should never be the case).
    """

    def _root_entity_comparator(
        root_entity_a: RootEntity, root_entity_b: RootEntity
    ) -> int:
        if isinstance(root_entity_a, HasMultipleExternalIdsEntity):
            external_id_keys_a = sorted(root_entity_external_id_keys(root_entity_a))
        else:
            raise ValueError(
                f"Expected root entity to be an instance of "
                f"HasMultipleExternalIdsEntity, found: {type(root_entity_a)}"
            )

        if isinstance(root_entity_b, HasMultipleExternalIdsEntity):
            external_id_keys_b = sorted(root_entity_external_id_keys(root_entity_b))
        else:
            raise ValueError(
                f"Expected root entity to be an instance of "
                f"HasMultipleExternalIdsEntity, found: {type(root_entity_b)}"
            )

        if external_id_keys_a == external_id_keys_b:
            return 0

        return -1 if external_id_keys_a < external_id_keys_b else 1

    return sorted(root_entities, key=cmp_to_key(_root_entity_comparator))


class StateSpecificIngestPipelineIntegrationTestCase(BaseStateIngestPipelineTestCase):
    """Class with basic functionality for all region-specific ingest pipeline
    integration tests.
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
        return None

    @classmethod
    def region(cls) -> DirectIngestRegion:
        return get_direct_ingest_region(
            region_code=cls.region_code().value.lower(),
            region_module_override=cls.region_module_override() or regions,
        )

    @classmethod
    def state_code_str_upper(cls) -> str:
        return cls.state_code().value.upper()

    def setUp(self) -> None:
        super().setUp()
        self.maxDiff = None

        self.project_id = "recidiviz-staging"
        self.metadata_patcher = patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = self.project_id

        self.environment_patcher = patch("recidiviz.utils.environment.in_gcp_staging")
        self.mock_environment_fn = self.environment_patcher.start()
        self.mock_environment_fn.return_value = True

    def tearDown(self) -> None:
        self.metadata_patcher.stop()
        self.environment_patcher.stop()
        super().tearDown()

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
            raw_fixture_path=DirectIngestTestFixturePath.for_extract_and_merge_fixture(
                region_code=self.region_code().value,
                file_name=f"{test_name}.csv",
            ).full_path(),
            fixture_columns=fixture_columns,
        ).to_dict("records")

        for record in records:
            record[MATERIALIZATION_TIME_COL_NAME] = datetime.datetime.now().isoformat()
            record[UPPER_BOUND_DATETIME_COL_NAME] = DEFAULT_UPDATE_DATETIME.isoformat()

        return records

    def generate_ingest_view_results_for_one_date(
        self,
        project_id: str,
        state_code: StateCode,
        ingest_view_name: str,
        raw_data_tables_to_upperbound_dates: Dict[str, Optional[str]],
        ingest_instance: DirectIngestInstance,
    ) -> FakeGenerateIngestViewResults:
        """Returns a constructor that generates ingest view results for a given ingest view assuming a single date."""
        return FakeGenerateIngestViewResults(
            project_id,
            state_code,
            ingest_view_name,
            raw_data_tables_to_upperbound_dates,
            ingest_instance,
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

        pipeline_args = default_arg_list_for_pipeline(
            pipeline=self.pipeline_class(),
            state_code=self.region_code().value,
            project_id=self.project_id,
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
                            expected_entity_association_type_to_associations,
                        )
                    ),
                ):
                    with patch(
                        "recidiviz.pipelines.base_pipeline.Pipeline",
                        pipeline_constructor(self.project_id),
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

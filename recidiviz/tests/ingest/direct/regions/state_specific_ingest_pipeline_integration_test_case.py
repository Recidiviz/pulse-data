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
from collections import defaultdict
from copy import deepcopy
from functools import cmp_to_key
from typing import Any, Callable, Dict, Iterable, List, Optional, Union, cast

import apache_beam as beam
from apache_beam.pvalue import PBegin
from apache_beam.testing.util import BeamAssertException, assert_that
from mock import patch

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.ingest_mappings.ingest_view_contents_context import (
    IngestViewContentsContextImpl,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.entity.base_entity import (
    Entity,
    ExternalIdEntity,
    HasMultipleExternalIdsEntity,
    RootEntity,
)
from recidiviz.persistence.entity.entity_utils import (
    get_all_entities_from_tree,
    get_all_entity_associations_from_tree,
)
from recidiviz.persistence.entity_matching.entity_merger_utils import (
    root_entity_external_id_keys,
)
from recidiviz.pipelines.ingest.state import write_root_entities_to_bq
from recidiviz.pipelines.ingest.state.generate_ingest_view_results import (
    GenerateIngestViewResults,
)
from recidiviz.pipelines.ingest.state.run_validations import RunValidations
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
        raw_data_tables_to_upperbound_dates: Dict[str, str],
        raw_data_source_instance: DirectIngestInstance,
        fake_ingest_view_results: Iterable[Dict[str, Any]],
    ) -> None:
        super().__init__(
            project_id,
            state_code,
            ingest_view_name,
            raw_data_tables_to_upperbound_dates,
            raw_data_source_instance,
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
        state_code: StateCode,
        expected_root_entity_output: Iterable[RootEntity],
        debug: bool = False,
    ) -> None:
        super().__init__(expected_output_entities, state_code)
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
            clear_db_ids([cast(Entity, entity) for entity in copy_of_actual])
            sorted_actual = _sort_root_entities(copy_of_actual)
            sorted_expected = _sort_root_entities(expected_root_entity_output)
            if sorted_actual != sorted_expected:
                if debug:
                    if in_ci():
                        raise ValueError(
                            "The |debug| flag should only be used for local debugging."
                        )
                    launch_entity_tree_html_diff_comparison(
                        found_root_entities=sorted_actual,
                        expected_root_entities=sorted_expected,
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
    """
    This class provides an integration test to be used by all states that do ingest.

    It currently reads in fixture files of ingest view output, rather than beginning with
    raw data.
    TODO(#22059): Standardize fixture formats and consolidate this with StateIngestPipelineTestCase
    """

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

        self.context_patcher = patch(
            "recidiviz.pipelines.ingest.state.pipeline.IngestViewContentsContextImpl.build_for_project"
        )
        self.context_patcher_fn = self.context_patcher.start()
        self.context_patcher_fn.return_value = (
            IngestViewContentsContextImpl.build_for_tests()
        )

    def tearDown(self) -> None:
        self.metadata_patcher.stop()
        self.environment_patcher.stop()
        self.context_patcher_fn.stop()
        super().tearDown()

    def generate_ingest_view_results_for_one_date(
        self,
        project_id: str,
        state_code: StateCode,
        ingest_view_name: str,
        raw_data_tables_to_upperbound_dates: Dict[str, str],
        raw_data_source_instance: DirectIngestInstance,
    ) -> FakeGenerateIngestViewResults:
        """Returns a constructor that generates ingest view results for a given ingest view assuming a single date."""
        return FakeGenerateIngestViewResults(
            project_id,
            state_code,
            ingest_view_name,
            raw_data_tables_to_upperbound_dates,
            raw_data_source_instance,
            self.get_ingest_view_results_from_fixture(
                ingest_view_name=ingest_view_name,
                test_name=ingest_view_name,
                fixture_has_metadata_columns=False,
                generate_default_metadata=True,
                use_results_fixture=False,
            ),
        )

    def run_validations_with_output(
        self,
        expected_root_entities: List[RootEntity],
        debug: bool = False,
    ) -> Callable[[Iterable[str], StateCode], RunValidationsWithOutputChecking,]:
        def _inner_constructor(
            expected_output_entities: Iterable[str],
            state_code: StateCode,
        ) -> RunValidationsWithOutputChecking:
            return RunValidationsWithOutputChecking(
                expected_output_entities,
                state_code,
                expected_root_entities,
                debug,
            )

        return _inner_constructor

    # TODO(#22059): Remove this method and replace with the implementation on
    # StateIngestPipelineTestCase when fixture formats and data loading is standardized.
    def run_test_state_pipeline(
        self,
        ingest_view_results: Dict[str, Iterable[Dict[str, Any]]],
        expected_root_entities: List[RootEntity],
        ingest_view_results_only: bool = False,
        ingest_views_to_run: Optional[str] = None,
        debug: bool = False,
    ) -> None:
        """This runs a test for an ingest pipeline where the ingest view results are
        passed in directly."""
        if not ingest_view_results:
            ingest_view_results = {
                ingest_view: [] for ingest_view in self.launchable_ingest_views()
            }
        expected_entities = [
            entity
            for root_entity in expected_root_entities
            for entity in get_all_entities_from_tree(cast(Entity, root_entity))
        ]

        expected_entity_types_to_expected_entities = {
            entity_type: [
                entity
                for entity in expected_entities
                if entity.get_entity_name() == entity_type
            ]
            for ingest_view in self.launchable_ingest_views()
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
                cast(Entity, root_entity)
            ).items():
                expected_entity_association_type_to_associations[
                    association_table
                ].update(associations)

        pipeline_args = default_arg_list_for_pipeline(
            pipeline=self.pipeline_class(),
            state_code=self.state_code().value,
            project_id=self.project_id,
            root_entity_id_filter_set=None,
            ingest_view_results_only=ingest_view_results_only,
            ingest_views_to_run=ingest_views_to_run,
            raw_data_upper_bound_dates_json=self.raw_fixture_loader.default_upper_bound_dates_json,
        )
        fake_bq_write_constructor = self.create_fake_bq_write_sink_constructor(
            self.get_expected_output(
                ingest_view_results,
                expected_entity_types_to_expected_entities,
                expected_entity_association_type_to_associations,
            )
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
                    fake_bq_write_constructor,
                ), patch(
                    f"{write_root_entities_to_bq.__name__}.WriteToBigQuery",
                    fake_bq_write_constructor,
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

# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Tests deployed_views"""
import datetime
import re
import unittest
from collections import defaultdict
from typing import Callable, Dict, List, Sequence, Set
from unittest.mock import MagicMock, patch

import sqlglot
from parameterized import parameterized

from recidiviz.aggregated_metrics.aggregated_metrics_view_builder import (
    AggregatedMetricsBigQueryViewBuilder,
)
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_sqlglot_helpers import (
    get_state_code_literal_references,
)
from recidiviz.big_query.big_query_view import BigQueryView, BigQueryViewBuilder
from recidiviz.big_query.big_query_view_dag_walker import BigQueryViewDagWalker
from recidiviz.big_query.big_query_view_utils import build_views_to_update
from recidiviz.big_query.union_all_big_query_view_builder import (
    UnionAllBigQueryViewBuilder,
)
from recidiviz.calculator.query.operations.dataset_config import (
    OPERATIONS_BASE_DATASET,
    OPERATIONS_BASE_REGIONAL_DATASET,
)
from recidiviz.calculator.query.state.views.analyst_data.all_task_type_eligibility_spans import (
    ALL_TASK_TYPE_ELIGIBILITY_SPANS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.all_task_type_ineligible_criteria_sessions import (
    ALL_TASK_TYPE_INELIGIBLE_CRITERIA_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.insights_user_impact_funnel_status_sessions import (
    INSIGHTS_USER_IMPACT_FUNNEL_STATUS_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.workflows_person_impact_funnel_status_sessions import (
    WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_mo_supervision_tasks_record import (
    US_MO_SUPERVISION_TASKS_RECORD_VIEW_BUILDER,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.gating import (
    automatic_raw_data_pruning_enabled_for_state_and_instance,
)
from recidiviz.ingest.direct.views.direct_ingest_latest_view_collector import (
    DirectIngestRawDataTableLatestViewBuilder,
)
from recidiviz.ingest.views.dataset_config import (
    NORMALIZED_STATE_VIEWS_DATASET,
    STATE_BASE_DATASET,
    STATE_BASE_VIEWS_DATASET,
)
from recidiviz.metrics.export.export_config import VIEW_COLLECTION_EXPORT_INDEX
from recidiviz.observations.views.events.global_provisioned_user.global_user_active_usage_event import (
    VIEW_BUILDER as GLOBAL_USER_ACTIVE_USAGE_EVENT_VIEW_BUILDER,
)
from recidiviz.observations.views.events.person.task_completed import (
    VIEW_BUILDER as TASK_COMPLETED_OBSERVATIONS_VIEW_BUILDER,
)
from recidiviz.observations.views.events.person.task_eligibility_start import (
    VIEW_BUILDER as TASK_ELIGIBILITY_START_TES_VIEW_BUILDER,
)
from recidiviz.observations.views.events.person.task_eligible_7_days import (
    VIEW_BUILDER as TASK_ELIGIBLE_7_DAYS_TES_VIEW_BUILDER,
)
from recidiviz.observations.views.events.person.task_eligible_30_days import (
    VIEW_BUILDER as TASK_ELIGIBLE_30_DAYS_TES_VIEW_BUILDER,
)
from recidiviz.observations.views.spans.person.task_eligibility_session import (
    VIEW_BUILDER as TASK_ELIGIBILITY_SESSION_OBSERVATIONS_VIEW_BUILDER,
)
from recidiviz.pipelines.ingest.dataset_config import (
    ingest_view_materialization_results_dataset,
    state_dataset_for_state_code,
)
from recidiviz.source_tables.collect_all_source_table_configs import (
    build_source_table_repository_for_collected_schemata,
    get_all_source_table_addresses,
    get_source_table_addresses,
)
from recidiviz.utils import metadata
from recidiviz.utils.environment import (
    DATA_PLATFORM_GCP_PROJECTS,
    GCP_PROJECT_PRODUCTION,
    GCP_PROJECT_STAGING,
)
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.types import assert_type
from recidiviz.validation.views.dataset_config import (
    METADATA_DATASET as VALIDATION_METADATA_DATASET,
)
from recidiviz.validation.views.dataset_config import (
    VIEWS_DATASET as VALIDATION_VIEWS_DATASET,
)
from recidiviz.view_registry.address_to_complexity_score_mapping import (
    ParentAddressComplexityScoreMapper,
)
from recidiviz.view_registry.deployed_views import (
    all_deployed_view_builders,
    deployed_view_builders,
)
from recidiviz.view_registry.deprecated_view_reference_exemptions import (
    DEPRECATED_VIEWS_AND_USAGE_EXEMPTIONS,
)
from recidiviz.view_registry.query_complexity_score_2025 import (
    get_query_complexity_score_2025,
)


class DeployedViewsTest(unittest.TestCase):
    """Tests the deployed views configuration"""

    def test_view_builders_can_be_collected_without_a_project_id_set(self) -> None:
        """Tests that view *builder* collection should not crash when there is no
        metadata.project_id() value configured.
        """
        with self.assertRaisesRegex(
            RuntimeError,
            r"May not be called from test, should this have a local override\?",
        ):
            # Confirm that no project id is set before we run the actual test.
            project_id = metadata.project_id()
            raise ValueError(
                f"Found project_id [{project_id}] when no project_id should be set "
                f"for this test."
            )
        _ = all_deployed_view_builders()

    def test_view_builders_do_not_change_between_projects(self) -> None:
        """Tests that if view builders are collected when a project_id is set, the list
        of builders does not differ between projects, nor do the query templates of the
        built views.
        """
        with local_project_id_override(GCP_PROJECT_STAGING):
            staging_builders = {b.address: b for b in all_deployed_view_builders()}
        self.assertGreater(len(staging_builders), 0)

        with local_project_id_override(GCP_PROJECT_PRODUCTION):
            prod_builders = {b.address: b for b in all_deployed_view_builders()}
        self.assertGreater(len(prod_builders), 0)

        staging_builder_addresses = set(staging_builders)
        prod_builder_addresses = set(prod_builders)
        if staging_not_prod := staging_builder_addresses - prod_builder_addresses:
            raise ValueError(
                f"Found view builders that are returned only in staging. The list of"
                f"builders / addresses should be the same across projects. "
                f"Staging-only addresses: {staging_not_prod}"
            )

        if prod_not_staging := prod_builder_addresses - staging_builder_addresses:
            raise ValueError(
                f"Found view builders that are returned only in prod. The list of "
                f"builders / addresses should be the same across projects. "
                f"Prod-only addresses: {prod_not_staging}"
            )

        for address, staging_builder in staging_builders.items():
            prod_builder = prod_builders[address]

            if staging_builder.__class__ != prod_builder.__class__:
                raise ValueError(
                    f"Builders for {address.to_str()} have different types between "
                    f"projects. Staging type: [{staging_builder.__class__}]. Prod "
                    f"type: [{staging_builder.__class__}]."
                )

            # Make sure the builder builds for either project
            with local_project_id_override(GCP_PROJECT_STAGING):
                staging_view = staging_builder.build()
            with local_project_id_override(GCP_PROJECT_PRODUCTION):
                prod_view = prod_builder.build()

            # Skip the check for identical templates for UnionAllBigQueryViewBuilders.
            # We expect the list of parent views unioned together by these builders
            # to vary by project, so the view template may change.
            if isinstance(staging_builder, UnionAllBigQueryViewBuilder):
                continue

            # Skip the check for identical templates on latest views where raw data
            # pruning gating is different between projects. That feature flag helps
            # determine the latest view query structure.
            # TODO(#12390): Delete once raw data pruning is live and the pruning feature
            #  gate can be deleted.
            if isinstance(staging_builder, DirectIngestRawDataTableLatestViewBuilder):
                state_code = StateCode(staging_builder.region_code.upper())
                instance = staging_builder.raw_data_source_instance
                with local_project_id_override(GCP_PROJECT_STAGING):
                    staging_is_pruning_enabled = (
                        automatic_raw_data_pruning_enabled_for_state_and_instance(
                            state_code, instance
                        )
                    )
                with local_project_id_override(GCP_PROJECT_PRODUCTION):
                    prod_is_pruning_enabled = (
                        automatic_raw_data_pruning_enabled_for_state_and_instance(
                            state_code, instance
                        )
                    )
                if staging_is_pruning_enabled != prod_is_pruning_enabled:
                    continue

            self.assertEqual(
                staging_view.view_query_template,
                prod_view.view_query_template,
                f"Found view [{address.to_str()}] whose view template differs between "
                f"projects",
            )

    def test_unique_addresses(self) -> None:
        view_addresses: Dict[BigQueryAddress, BigQueryViewBuilder] = {}
        for view_builder in all_deployed_view_builders():
            address = view_builder.address

            existing_view_builder = view_addresses.get(address)
            if not existing_view_builder:
                view_addresses[address] = view_builder

                continue
            if id(view_builder) == id(existing_view_builder):
                self.fail(
                    f"View builder for address [{address}] added to "
                    f"all_deployed_view_builders() list twice."
                )
            if (
                view_builder.build().view_query
                == existing_view_builder.build().view_query
            ):
                self.fail(
                    f"Two view builders with identical view queries defined for "
                    f"address [{address}]"
                )

            self.fail(f"Two different view builders defined with address [{address}].")

    @patch(
        "recidiviz.utils.metadata.project_id", MagicMock(return_value="test-project")
    )
    def test_build_all_views_perf(self) -> None:
        all_view_builders = all_deployed_view_builders()
        # some view builders are constants (which run logic on import, which happens before the test starts)
        # and others are functions (which will run during the test itself), so don't start the timer until
        # after we've collected all the view builders
        start = datetime.datetime.now()
        for builder in all_view_builders:
            builder.build()

        end = datetime.datetime.now()
        total_seconds = (end - start).total_seconds()
        # Building all our views should take less than 5s (as of 12/12/2024 it takes
        # about 3 seconds to update for 3500 views).
        self.assertLessEqual(total_seconds, 5)

    @patch(
        "recidiviz.utils.metadata.project_id", MagicMock(return_value="test-project")
    )
    def test_view_descriptions(self) -> None:
        for view_builder in all_deployed_view_builders():
            view = view_builder.build()

            # This shouldn't crash
            _ = view.bq_description
            if view.materialized_address:
                _ = view.materialized_table_bq_description

    def test_views_have_valid_parents(self) -> None:
        for project_id in DATA_PLATFORM_GCP_PROJECTS:
            errors: list[Exception] = []

            with local_project_id_override(project_id):
                candidate_view_builders = deployed_view_builders()

                source_table_addresses = get_source_table_addresses(project_id)

                views = build_views_to_update(
                    candidate_view_builders=candidate_view_builders,
                    sandbox_context=None,
                )
                dag_walker = BigQueryViewDagWalker(views)

            all_known_addresses = set()
            all_known_addresses |= source_table_addresses
            for view in views:
                all_known_addresses.add(view.address)
                if view.materialized_address:
                    all_known_addresses.add(view.materialized_address)

            for view in views:
                view_node = dag_walker.node_for_view(view)
                invalid_parents = {
                    parent_address
                    for parent_address in view.parent_tables
                    if parent_address not in all_known_addresses
                }
                if invalid_parents:
                    errors.append(
                        ValueError(
                            f"Found view {view.address.to_str()} that references parent "
                            f"tables that are neither registered views nor tables in a "
                            f"valid source table dataset: "
                            f"{BigQueryAddress.addresses_to_str(invalid_parents)}"
                        )
                    )

                deprecated_parents = {
                    parent_address
                    for parent_address in (
                        view_node.parent_node_addresses | view_node.source_addresses
                    )
                    if parent_address in DEPRECATED_VIEWS_AND_USAGE_EXEMPTIONS
                    and (
                        view.address
                        not in DEPRECATED_VIEWS_AND_USAGE_EXEMPTIONS[parent_address]
                    )
                }
                if deprecated_parents:
                    errors.append(
                        ValueError(
                            f"Found view {view.address.to_str()} that references parent "
                            f"tables that are DEPRECATED. Please find an alternate source "
                            f"for this data or reach out to #platform-team if you are "
                            f"unsure which views to use instead. Deprecated parent views: "
                            f"{BigQueryAddress.addresses_to_str(deprecated_parents)}"
                        )
                    )

            for (
                deprecated_address,
                exemptions,
            ) in DEPRECATED_VIEWS_AND_USAGE_EXEMPTIONS.items():
                exempted_addresses = set(exemptions.keys())

                if deprecated_address in dag_walker.nodes_by_address:
                    actual_children = dag_walker.nodes_by_address[
                        deprecated_address
                    ].child_node_addresses
                else:
                    # The DAG walker doesn't index children for source tables so we
                    # must find them in a less efficient way
                    actual_children = {
                        view.address
                        for view in views
                        if deprecated_address
                        in dag_walker.node_for_view(view).source_addresses
                    }

                stale_exemptions = exempted_addresses - actual_children
                if stale_exemptions:
                    errors.append(
                        ValueError(
                            f"Found child view exemptions for deprecated view "
                            f"[{deprecated_address.to_str()}] that are no longer actual "
                            f"references. These should be removed from the exemption list: "
                            f"{BigQueryAddress.addresses_to_str(stale_exemptions)}"
                        )
                    )

            if errors:
                raise ExceptionGroup(
                    f"Found view parent validation errors for project [{project_id}]",
                    errors,
                )

    @patch(
        "recidiviz.utils.metadata.project_id", MagicMock(return_value="recidiviz-456")
    )
    def test_table_references_conform_to_expected_format(self) -> None:
        regex = re.compile(
            r"(?P<leading_char>.)recidiviz-456\.(?P<dataset_id>[\w-]*)\.(?P<table_id>[\w-]*)(?P<trailing_char>.?)"
        )

        views = build_views_to_update(
            candidate_view_builders=deployed_view_builders(),
            sandbox_context=None,
        )
        for view in views:
            view_query = view.view_query
            table_refs = re.findall(regex, view_query)
            for leading_char, dataset_id, table_id, trailing_char in table_refs:
                if leading_char != "`" and trailing_char != "`":
                    self.fail(
                        f"Found view [{view.address.to_str()}] with table/view "
                        f"dependency [{dataset_id}.{table_id}] which is not properly "
                        f"formatted. All table and view references should be surrounded "
                        f"by backticks, e.g. `{{project_id}}.{dataset_id}.{table_id}`."
                    )


class ViewDagInvariantTests(unittest.TestCase):
    """Tests that certain views have the correct descendants."""

    dag_walker: BigQueryViewDagWalker
    all_deployed_view_builders_by_address: Dict[BigQueryAddress, BigQueryViewBuilder]

    @classmethod
    def setUpClass(cls) -> None:
        with patch("recidiviz.utils.metadata.project_id", return_value="recidiviz-456"):
            view_builders = deployed_view_builders()
            views = build_views_to_update(
                candidate_view_builders=view_builders,
                sandbox_context=None,
            )
            cls.dag_walker = BigQueryViewDagWalker(views)

            # All view builders deployed to any project.
            all_deployed_builders = all_deployed_view_builders()
            cls.all_deployed_view_builders_by_address = {
                b.address: b for b in all_deployed_builders
            }

    def test_no_expensive_union_all_view_queries(self) -> None:
        """Test that fails when a view is doing an overly expensive query of a UNION ALL
        view when it could instead be querying one of the component parent views.
        """
        union_all_view_addresses = {
            address
            for address, vb in self.all_deployed_view_builders_by_address.items()
            if isinstance(vb, UnionAllBigQueryViewBuilder)
            # UnionAllBigQueryViewBuilder that union AggregatedMetricsBigQueryViewBuilder
            # for different time periods together are relatively small and ok to query.
            and not isinstance(vb.parents[0], AggregatedMetricsBigQueryViewBuilder)
        }

        # TODO(#29291): Update these exemptions to explicitly list which parent UNION
        #  ALL view is allowed.
        allowed_union_all_view_children = {
            # These views produce generic analysis based on all TES spans.
            WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSIONS_VIEW_BUILDER.address,
            # TODO(#29291): Revisit whether we need to have these observations views
            #  that look at all tasks.
            TASK_COMPLETED_OBSERVATIONS_VIEW_BUILDER.address,
            TASK_ELIGIBILITY_SESSION_OBSERVATIONS_VIEW_BUILDER.address,
            TASK_ELIGIBILITY_START_TES_VIEW_BUILDER.address,
            TASK_ELIGIBLE_7_DAYS_TES_VIEW_BUILDER.address,
            TASK_ELIGIBLE_30_DAYS_TES_VIEW_BUILDER.address,
            ALL_TASK_TYPE_ELIGIBILITY_SPANS_VIEW_BUILDER.address,
            ALL_TASK_TYPE_INELIGIBLE_CRITERIA_SESSIONS_VIEW_BUILDER.address,
            BigQueryAddress(
                dataset_id="observations__person_event", table_id="impact_transition"
            ),
            # Active usage event observation unions across all events for a given product
            BigQueryAddress(
                dataset_id="observations__tasks_primary_user_event",
                table_id="tasks_active_usage_event",
            ),
            BigQueryAddress(
                dataset_id="observations__insights_primary_user_event",
                table_id="insights_active_usage_event",
            ),
            BigQueryAddress(
                dataset_id="observations__workflows_primary_user_event",
                table_id="workflows_active_usage_event",
            ),
            # Funnel analysis requires referencing all unioned segment events.
            INSIGHTS_USER_IMPACT_FUNNEL_STATUS_SESSIONS_VIEW_BUILDER.address,
            GLOBAL_USER_ACTIVE_USAGE_EVENT_VIEW_BUILDER.address,
            # Compliance Tasks product export views pull from the unioned view of all tasks for a given state.
            US_MO_SUPERVISION_TASKS_RECORD_VIEW_BUILDER.address,
            # Views to help calculate compliance metrics pull from the unioned view of all tasks for a given state.
            BigQueryAddress(
                dataset_id="analyst_data", table_id="assessment_compliance_spans"
            ),
            BigQueryAddress(
                dataset_id="observations__person_event",
                table_id="supervision_contact_due",
            ),
            # Views of all contact events with state-specific contact types for calculating
            # contact compliance metrics.
            BigQueryAddress(
                dataset_id="observations__person_event",
                table_id="supervision_contact",
            ),
        }

        allowed_union_all_datasets_to_query_from = {
            # Views in this dataset produce the `normalized_state` dataset which
            # we do expect downstream views to query from
            NORMALIZED_STATE_VIEWS_DATASET,
        }

        for exempt_child_address in allowed_union_all_view_children:
            if exempt_child_address not in self.all_deployed_view_builders_by_address:
                raise ValueError(
                    f"Address [{exempt_child_address.to_str()}] is not a valid view "
                    f"address."
                )

        for parent_address, node in self.dag_walker.nodes_by_address.items():
            for child_address in node.child_node_addresses:
                if parent_address not in union_all_view_addresses:
                    continue
                if (
                    parent_address.dataset_id
                    in allowed_union_all_datasets_to_query_from
                ):
                    continue

                if child_address in union_all_view_addresses:
                    # Views that further union the results of a UNION ALL view are
                    # allowed.
                    continue
                if child_address.dataset_id in {
                    VALIDATION_VIEWS_DATASET,
                    VALIDATION_METADATA_DATASET,
                }:
                    # Validation views may look for generic issues in, for example,
                    # all TES spans at once.
                    continue
                if child_address in allowed_union_all_view_children:
                    continue

                raise ValueError(
                    f"Found view [{child_address.to_str()}] referencing "
                    f"UnionAllBigQueryViewBuilder view [{parent_address.to_str()}]."
                    f"Generally, you should only query the specific component"
                    f"view you need, not the view that unions all sub-views together. "
                    f"If [{child_address.to_str()}] is doing generic, state-agnostic "
                    f"analysis of all data in [{parent_address.to_str()}], you may add "
                    f"it to the allowed_union_all_view_children list above."
                )

        for exempt_child_address in allowed_union_all_view_children:
            node = self.dag_walker.nodes_by_address[exempt_child_address]
            if not any(
                (
                    a in union_all_view_addresses
                    and a.dataset_id not in allowed_union_all_datasets_to_query_from
                )
                for a in node.parent_node_addresses
            ):
                raise ValueError(
                    f"Found child address [{exempt_child_address.to_str()}] which does "
                    f"not have any UnionAllBigQueryViewBuilder type parents. It should "
                    f"be removed from the allowed_union_all_view_children list."
                )

    @parameterized.expand(
        [
            (
                "supervision_population_in_state",
                "most_recent_supervision_population_metrics",
                "most_recent_single_day_supervision_population_metrics",
            ),
            (
                "supervision_population_out_of_state",
                "most_recent_supervision_out_of_state_population_metrics",
                "most_recent_single_day_supervision_out_of_state_population_metrics",
            ),
        ]
    )
    def test_only_lantern_usages_of_legacy_population_metrics(
        self,
        _name: str,
        original_table_id: str,
        descendent_table_id: str,
    ) -> None:
        """Tests that the legacy population_metrics tables are only referenced by Lantern
        views so that it is safe to turn pipelines off for all non-Lantern states."""
        original_address = BigQueryAddress(
            dataset_id="dataflow_metrics_materialized", table_id=original_table_id
        )

        lantern_view_addresses = {
            vb.address
            for vb in VIEW_COLLECTION_EXPORT_INDEX["LANTERN"].view_builders_to_export
        }
        other_valid_descendants = {
            BigQueryAddress(
                dataset_id="dataflow_metrics_materialized",
                table_id=descendent_table_id,
            ),
            BigQueryAddress(
                dataset_id="lantern_revocations_matrix",
                table_id="supervision_matrix_by_person",
            ),
        }
        valid_descendants = {
            *lantern_view_addresses,
            *other_valid_descendants,
        }

        view = self.dag_walker.view_for_address(original_address)
        sub_dag = self.dag_walker.get_descendants_sub_dag([view])

        descendant_addresses: Set[BigQueryAddress] = {
            v.address for v in sub_dag.views
        } - {original_address}

        invalid_descendants = {
            # We do not care if validation views point to data in the legacy pipeline
            a
            for a in descendant_addresses
            if a.dataset_id != "validation_views"
        } - valid_descendants

        if invalid_descendants:
            self.fail(
                f"Found invalid descendants: {invalid_descendants}",
            )

    def test_no_project_ids_in_view_templates(self) -> None:
        """Validates that the view_query_template does not contain any raw GCP
        project_id values. Note that this prevents views from referencing project IDs
        directly in any comments.
        """
        for view in self.dag_walker.views:
            for project_id in DATA_PLATFORM_GCP_PROJECTS:
                self.assertNotIn(
                    project_id,
                    view.view_query_template,
                    msg=f"view_query_template for view [{view.dataset_id}."
                    f"{view.view_id}] cannot contain raw"
                    f" value: {project_id}.",
                )

    def test_views_use_materialized_if_present(self) -> None:
        """Checks that each view is using the materialized version of a parent view, if
        one exists."""
        views_with_issues = {}

        table_for_query_to_view = {
            view.table_for_query: view for view in self.dag_walker.views
        }

        for view in self.dag_walker.views:
            node = self.dag_walker.node_for_view(view)
            should_be_materialized_addresses = set()
            for parent_table_address in node.parent_tables:
                if parent_table_address in table_for_query_to_view:
                    # This is an address of a table materialized from a view or a view
                    # that is not materialized
                    continue
                if parent_table_address in node.source_addresses:
                    continue
                parent_view: BigQueryView = self.dag_walker.view_for_address(
                    parent_table_address
                )
                if parent_view.materialized_address is None:
                    raise ValueError(
                        f"Expected view [{view.address.to_str()}] to have a "
                        f"materialized address if we have made it to this point"
                    )
                should_be_materialized_addresses.add(parent_view.materialized_address)
            if should_be_materialized_addresses:
                views_with_issues[view.address] = should_be_materialized_addresses

        if views_with_issues:
            raise ValueError(
                f"Found views referencing un-materialized versions of a view when a "
                f"materialized version exists: {views_with_issues}"
            )

    def test_children_match_parent_projects_to_deploy(self) -> None:
        """Checks that if any parents have the projects_to_deploy field set, all
        children have equal or more restrictive projects.
        """
        failing_views: Dict[BigQueryViewBuilder, Set[str]] = {}

        def process_check_using_materialized(
            view: BigQueryView, parent_results: Dict[BigQueryView, Set[str]]
        ) -> Set[str]:
            view_builder = self.all_deployed_view_builders_by_address[view.address]

            parent_constraints: List[Set[str]] = [
                parent_projects_to_deploy
                for parent_projects_to_deploy in parent_results.values()
                if parent_projects_to_deploy is not None
            ]
            view_projects_to_deploy = (
                view_builder.projects_to_deploy
                if view_builder.projects_to_deploy is not None
                else {*DATA_PLATFORM_GCP_PROJECTS}
            )
            if not parent_constraints:
                # If the parents have no constraints, constraints are just those on
                # this view.
                return view_projects_to_deploy

            # This view can only be deployed to all the projects that its parents allow
            expected_projects_to_deploy = set.intersection(*parent_constraints)

            extra_projects = view_projects_to_deploy - expected_projects_to_deploy

            if extra_projects:
                failing_views[view_builder] = expected_projects_to_deploy

            return expected_projects_to_deploy.intersection(view_projects_to_deploy)

        _ = self.dag_walker.process_dag(
            process_check_using_materialized, synchronous=True
        )

        if failing_views:
            error_message_rows = []
            for view_builder, expected in failing_views.items():
                error_message_rows.append(
                    f"\t{view_builder.dataset_id}.{view_builder.view_id} - "
                    f"allowed projects: {expected}"
                )

            error_message_rows_str = "\n".join(error_message_rows)
            error_message = f"""
The following views have less restrictive projects_to_deploy than their parents:
{error_message_rows_str}
"""
            raise ValueError(error_message)

    def test_views_only_query_from_allowed_datasets(self) -> None:
        """Test that ensures that views in our view graph only reference valid datasets."""
        disallowed_view_parent_datasets = {
            # Ingest view results are data pipeline intermediate outputs and should
            # be used for debugging purposes.
            *{
                ingest_view_materialization_results_dataset(state_code)
                for state_code in StateCode
            },
            # The `state` dataset is a data pipeline intermediate output and should only
            # be used for debugging purposes. Views should use the `normalized_state`
            # dataset instead.
            STATE_BASE_DATASET,
            STATE_BASE_VIEWS_DATASET,
            *{state_dataset_for_state_code(state_code) for state_code in StateCode},
            # The `operations` dataset shows a potentially stale view of data platform
            # operations data and should only be used for debugging / one-off analysis
            # purposes.
            OPERATIONS_BASE_REGIONAL_DATASET,
            OPERATIONS_BASE_DATASET,
        }

        # views that are allowed to references specific views in the datasets in disallowed_view_parent_datasets
        # dict of exempt_view -> views in |disallowed_view_parent_datasets| it is allowed to reference
        disallowed_view_parent_dataset_exceptions: dict[str, set] = {
            STATE_BASE_VIEWS_DATASET: {
                state_dataset_for_state_code(state_code) for state_code in StateCode
            },
            STATE_BASE_DATASET: {STATE_BASE_VIEWS_DATASET},
        }
        views_with_issues = defaultdict(set)
        for view in self.dag_walker.views:
            if view.dataset_id in disallowed_view_parent_dataset_exceptions:
                continue
            for parent_address in view.parent_tables:
                parent_dataset = parent_address.dataset_id
                if parent_dataset not in disallowed_view_parent_datasets:
                    continue
                views_with_issues[parent_dataset].add(view.address)

        errors = []
        for dataset_id, views_referencing_dataset in views_with_issues.items():
            address_strs = [a.to_str() for a in views_referencing_dataset]
            errors.append(
                ValueError(
                    f"Views in our primary view graph cannot reference dataset "
                    f"[{dataset_id}], which is present for non-production use cases only ("
                    f"e.g. debugging). Found views referencing [{dataset_id}]: "
                    f"{address_strs}"
                )
            )

        for (
            exempt_dataset,
            allowed_disallowed_references,
        ) in disallowed_view_parent_dataset_exceptions.items():
            if (
                extra_datasets := allowed_disallowed_references
                - disallowed_view_parent_datasets
            ):
                errors.append(
                    ValueError(
                        f"Found dataset [{exempt_dataset}] that is exempted from not referencing "
                        f"the following datasets that we are no longer enforcing as disallowed: "
                        f"{extra_datasets}"
                    )
                )

        if errors:
            raise ExceptionGroup(
                "Found the following failures while enforcing disallowed parent datasets references:",
                errors,
            )

    def test_no_conflicts_between_source_tables_and_views(self) -> None:
        view_builder_addresses = set(self.all_deployed_view_builders_by_address)
        source_table_addresses = get_all_source_table_addresses()
        if not view_builder_addresses.isdisjoint(source_table_addresses):
            overlapping_elements = ", ".join(
                [
                    address.to_str()
                    for address in view_builder_addresses.intersection(
                        source_table_addresses
                    )
                ]
            )
            raise ValueError(
                f"Found overlapping addresses between source tables and views: {overlapping_elements}"
            )

    def test_union_all_big_query_view_parents_valid(self) -> None:
        for view_address, node in self.dag_walker.nodes_by_address.items():
            builder = self.all_deployed_view_builders_by_address[view_address]
            if not isinstance(builder, UnionAllBigQueryViewBuilder):
                continue

            for parent in builder.parents:
                if isinstance(parent, BigQueryAddress):
                    # If the parent is a raw BigQueryAddress, it must be a source table,
                    # not another view!
                    self.assertIn(
                        parent,
                        node.source_addresses,
                        f"Found parent BigQueryAddress [{parent.to_str()}] for "
                        f"UnionAllBigQueryViewBuilder [{view_address.to_str()}] which "
                        f"is not a source table. If the parent is a view address, the "
                        f"BigQueryViewBuilder for that view must be referenced as a "
                        f"parent instead.",
                    )
                    continue

                if isinstance(parent, BigQueryViewBuilder):
                    # All listed parents should end up as actual parent nodes in the DAG
                    # unless they can't be deployed inthat project.
                    if parent.should_deploy_in_project("recidiviz-456"):
                        self.assertIn(
                            parent.address,
                            node.parent_node_addresses,
                            f"Found view [{parent.address.to_str()}] which was listed "
                            f"as a parent of UnionAllBigQueryViewBuilder view "
                            f"[{view_address}], but which did not end up being a "
                            f"parent in the actual built view graph.",
                        )
                    else:
                        self.assertNotIn(
                            parent.address,
                            node.parent_node_addresses,
                            f"View [{parent.address.to_str()}] which was listed as a "
                            f"parent of UnionAllBigQueryViewBuilder view "
                            f"[{view_address}] cannot be deployed in the test project "
                            f"recidiviz-456, so should not be included as a parent in "
                            f"the actual view graph.",
                        )

                    parent_view_builder = self.all_deployed_view_builders_by_address[
                        parent.address
                    ]
                    self.assertIsNotNone(
                        parent_view_builder.materialized_address,
                        f"Found view {parent.address.to_str()} which is a parent of "
                        f"UnionAllBigQueryViewBuilder view [{view_address.to_str()}] "
                        f"but which is not materialized. All "
                        f"UnionAllBigQueryViewBuilder parent views must be "
                        f"materialized.",
                    )
                    continue

                raise ValueError(f"Unexpected parent type [{type(parent)}]")


class ViewQueryFormatTest(unittest.TestCase):
    """Tests that use the query syntax tree to enforce certain rules about view query
    format / correctness.
    """

    all_deployed_builders: list[BigQueryViewBuilder]
    all_deployed_views_by_address: Dict[BigQueryAddress, BigQueryView]
    all_deployed_parsed_view_trees_by_address: Dict[BigQueryAddress, sqlglot.exp.Query]

    @classmethod
    def setUpClass(cls) -> None:
        # All view builders deployed to any project.
        cls.all_deployed_builders = all_deployed_view_builders()
        with local_project_id_override("recidiviz-456"):
            cls.all_deployed_views_by_address = {
                vb.address: vb.build() for vb in cls.all_deployed_builders
            }

        cls.all_deployed_parsed_view_trees_by_address = {}
        for address in cls.all_deployed_views_by_address:
            view = cls.all_deployed_views_by_address[address]
            try:
                tree = sqlglot.parse_one(view.view_query, dialect="bigquery")
            except Exception as e:
                raise ValueError(
                    f"Failure to parse view [{view.address.table_id}]"
                ) from e
            cls.all_deployed_parsed_view_trees_by_address[view.address] = assert_type(
                tree, sqlglot.expressions.Query
            )

    def _run_query_format_test(
        self,
        view_check_fn: Callable[[BigQueryView, sqlglot.exp.Query], Sequence[Exception]],
    ) -> None:
        """Runs a query format check, raising all errors at once in a single
        ExceptionGroup.
        """
        view_level_exceptions = []

        for address in sorted(
            self.all_deployed_views_by_address, key=lambda a: a.to_str()
        ):
            view = self.all_deployed_views_by_address[address]
            tree_expression = self.all_deployed_parsed_view_trees_by_address[address]
            exceptions = view_check_fn(view, tree_expression)
            if exceptions:
                view_level_exceptions.append(
                    ExceptionGroup(
                        f"Found query format errors for view [{address.to_str()}]",
                        exceptions,
                    )
                )

        if view_level_exceptions:
            raise ExceptionGroup(
                "Found view query format errors", view_level_exceptions
            )

    def test_view_query_format__parent_table_parsing(self) -> None:
        """In production code we use an (imperfect) regex mechanism to determine the
        parent tables of each of our BigQuery views because it is ~800x faster than the
        more correct alternative. This test makes sure that table references in our
        BigQuery views are all formatted correctly so that the same set of parents is
        getting identified as those that would be identified by the slower alternative
        (sqlglot).
        """

        def _get_view_errors(
            view: BigQueryView, query_expression: sqlglot.exp.Query
        ) -> list[ValueError]:
            sqlglot_addresses = {
                BigQueryAddress(dataset_id=table.db, table_id=table.name)
                for table in list(query_expression.find_all(sqlglot.exp.Table))
                # If there's no dataset, then the reference is a CTE reference
                if table.db
            }

            regex_addresses = view.parent_tables

            if sqlglot_addresses == regex_addresses:
                return []

            if missing_from_regex := sqlglot_addresses - regex_addresses:
                return [
                    ValueError(
                        f"Found parent tables for view [{view.address.to_str()}] "
                        f"identified by the sqlglot library that were not identified "
                        f"by a regex. This will happen if you have a table that is not "
                        f"formatted like `{{project_id}}.dataset.table` (surrounded "
                        f"by backticks). Addresses not parsed by regexes: "
                        f"{sorted(a.to_str() for a in missing_from_regex)}."
                    )
                ]
            missing_from_sqlglot = regex_addresses - sqlglot_addresses
            return [
                ValueError(
                    f"Found parent tables for view [{view.address.to_str()}] that were "
                    f"identified by regex that were not identified by the sqlglot "
                    f"library. This may happen when you have a table reference inside "
                    f"of a commented out piece of SQL code. You can fix this by "
                    f"removing the backticks from the table definitions so the table "
                    f"is not picked up by our regex matcher. Addresses not parsed by "
                    f"sqlglot: {sorted(a.to_str() for a in missing_from_sqlglot)}"
                )
            ]

        self._run_query_format_test(_get_view_errors)

    def test_view_query_format__valid_column_references(self) -> None:
        """Asserts that any column references in the view query are properly
        formatted.
        """

        def _get_view_errors(
            view: BigQueryView, query_expression: sqlglot.exp.Query
        ) -> list[ValueError]:
            exceptions = []
            for column_expression in query_expression.find_all(sqlglot.exp.Column):
                if column_expression.catalog in {
                    view.project,
                    GCP_PROJECT_STAGING,
                    GCP_PROJECT_PRODUCTION,
                }:
                    exceptions.append(
                        ValueError(
                            f"Found column [{column_expression}] in view "
                            f"[{view.address.to_str()}] which uses a fully-qualified "
                            f"reference to its table. If you want to qualify a column "
                            f"name, use a table alias. Columns formatted like "
                            f"`recidiviz-staging.dataset.table`.column interfere with "
                            f"our regex-based parent table inference."
                        )
                    )
            return exceptions

        self._run_query_format_test(_get_view_errors)

    def test_view_query_format__aggregation_functions_use_order_by(self) -> None:
        """Asserts that all usages of an ARRAY_AGG or STRING_AGG function use an
        ORDER BY clause so that the results are deterministically sorted.

        For example, raises for `SELECT ARRAY_AGG(a) FROM ...` - expects
            `SELECT ARRAY_AGG(a ORDER BY a) FROM ..` instead.
        """

        def _get_view_errors(
            view: BigQueryView, query_expression: sqlglot.exp.Query
        ) -> list[ValueError]:
            exceptions = []
            for func in query_expression.find_all(
                sqlglot.exp.ArrayAgg, sqlglot.exp.GroupConcat
            ):
                is_analytic_function = isinstance(func.parent, sqlglot.exp.Window)
                if is_analytic_function:
                    # Skip usages of ARRAY_AGG and STRING_AGG in the context of window
                    # functions - the ordering will need to be handled by the window
                    # expression.
                    continue

                order_by_clause = func.find(sqlglot.exp.Order)
                if not order_by_clause:
                    # sqlglot internally treats STRING_AGG as GROUP_CONCAT - translate
                    # back so error message is more decipherable.
                    sql_for_display = func.sql().replace("GROUP_CONCAT", "STRING_AGG")
                    exceptions.append(
                        ValueError(
                            f"Found view [{view.address.to_str()}] with aggregation "
                            f"expression [{sql_for_display}] that does not have an "
                            f"ORDER BY clause. Add an ORDER BY that will produce "
                            f"deterministically sorted results."
                        )
                    )
            return exceptions

        self._run_query_format_test(_get_view_errors)

    def test_view_query_format__can_compute_complexity_score(self) -> None:
        repository = build_source_table_repository_for_collected_schemata(
            project_id=None
        )

        address_to_table_complexity_score_mapper = ParentAddressComplexityScoreMapper(
            source_table_repository=repository,
            all_view_builders=self.all_deployed_builders,
        )

        def _get_view_errors(
            view: BigQueryView, query_expression: sqlglot.exp.Query
        ) -> list[ValueError]:
            exceptions = []
            try:
                get_query_complexity_score_2025(
                    query_expression,
                    address_to_table_complexity_score_mapper.get_parent_complexity_map_for_view_2025(
                        view.address
                    ),
                )
            except ValueError as e:
                exceptions.append(e)
            return exceptions

        self._run_query_format_test(_get_view_errors)

    def test_view_query_format__can_get_state_code_literal_references(self) -> None:
        def _get_view_errors(
            view: BigQueryView,  # pylint: disable=unused-argument
            query_expression: sqlglot.exp.Query,
        ) -> list[ValueError]:
            exceptions = []
            try:
                get_state_code_literal_references(query_expression)
            except ValueError as e:
                exceptions.append(e)
            return exceptions

        self._run_query_format_test(_get_view_errors)

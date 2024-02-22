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
from typing import Dict, List, Set, Tuple
from unittest.mock import MagicMock, patch

from parameterized import parameterized

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_utils import build_views_to_update
from recidiviz.big_query.big_query_view import BigQueryView, BigQueryViewBuilder
from recidiviz.big_query.big_query_view_dag_walker import BigQueryViewDagWalker
from recidiviz.big_query.union_all_big_query_view_builder import (
    UnionAllBigQueryViewBuilder,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.direct_ingest_regions import (
    raw_data_pruning_enabled_in_state_and_instance,
)
from recidiviz.ingest.direct.views.direct_ingest_latest_view_collector import (
    DirectIngestRawDataTableLatestViewBuilder,
)
from recidiviz.metrics.export.export_config import VIEW_COLLECTION_EXPORT_INDEX
from recidiviz.utils import metadata
from recidiviz.utils.environment import (
    GCP_PROJECT_PRODUCTION,
    GCP_PROJECT_STAGING,
    GCP_PROJECTS,
)
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.view_registry.datasets import VIEW_SOURCE_TABLE_DATASETS
from recidiviz.view_registry.deployed_views import (
    all_deployed_view_builders,
    deployed_view_builders,
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
                f"Found view builders that are returned only in prod. The list of"
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
                        raw_data_pruning_enabled_in_state_and_instance(
                            state_code, instance
                        )
                    )
                with local_project_id_override(GCP_PROJECT_PRODUCTION):
                    prod_is_pruning_enabled = (
                        raw_data_pruning_enabled_in_state_and_instance(
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
        # Building all our views should take less than 5s (as of 4/11/2022 it takes
        # about .28 seconds).
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
        for project_id in GCP_PROJECTS:
            with local_project_id_override(project_id):
                candidate_view_builders = deployed_view_builders()

                views = build_views_to_update(
                    view_source_table_datasets=VIEW_SOURCE_TABLE_DATASETS,
                    candidate_view_builders=candidate_view_builders,
                    address_overrides=None,
                )
            view_addresses = set()
            for view in views:
                view_addresses.add(view.address)
                if view.materialized_address:
                    view_addresses.add(view.materialized_address)
            for view in views:
                invalid_parents = {
                    parent_address
                    for parent_address in view.parent_tables
                    if (
                        parent_address.dataset_id not in VIEW_SOURCE_TABLE_DATASETS
                        and parent_address not in view_addresses
                    )
                }
                if invalid_parents:
                    parent_strs = sorted(a.to_str() for a in invalid_parents)
                    raise ValueError(
                        f"Found view {view.address.to_str()} that references parent tables that"
                        f"are neither registered views nor tables in a valid source table dataset: {parent_strs}"
                    )

    @patch(
        "recidiviz.utils.metadata.project_id", MagicMock(return_value="recidiviz-456")
    )
    def test_table_references_conform_to_expected_format(self) -> None:
        regex = re.compile(
            r"(?P<leading_char>.)recidiviz-456\.(?P<dataset_id>[\w-]*)\.(?P<table_id>[\w-]*)(?P<trailing_char>.?)"
        )

        views = build_views_to_update(
            view_source_table_datasets=VIEW_SOURCE_TABLE_DATASETS,
            candidate_view_builders=deployed_view_builders(),
            address_overrides=None,
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
    all_deployed_view_builders: List[BigQueryViewBuilder]

    @classmethod
    def setUpClass(cls) -> None:
        with patch("recidiviz.utils.metadata.project_id", return_value="recidiviz-456"):
            view_builders = deployed_view_builders()
            views = build_views_to_update(
                view_source_table_datasets=VIEW_SOURCE_TABLE_DATASETS,
                candidate_view_builders=view_builders,
                address_overrides=None,
            )
            cls.dag_walker = BigQueryViewDagWalker(views)

            # All view builders deployed to any project.
            cls.all_deployed_view_builders = all_deployed_view_builders()

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
                dataset_id="shared_metric_views",
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
            for project_id in GCP_PROJECTS:
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
        for view in self.dag_walker.views:
            node = self.dag_walker.node_for_view(view)
            should_be_materialized_addresses = set()
            for parent_table_address in node.parent_tables:
                if parent_table_address.table_id.endswith("_materialized"):
                    # This address is already materialized
                    continue
                if parent_table_address in node.source_addresses:
                    continue
                parent_view: BigQueryView = self.dag_walker.view_for_address(
                    parent_table_address
                )
                if parent_view.materialized_address is not None:
                    should_be_materialized_addresses.add(
                        parent_view.materialized_address
                    )
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
        builders_by_address: Dict[Tuple[str, str], BigQueryViewBuilder] = {
            (b.dataset_id, b.view_id): b for b in self.all_deployed_view_builders
        }

        failing_views: Dict[BigQueryViewBuilder, Set[str]] = {}

        def process_check_using_materialized(
            view: BigQueryView, parent_results: Dict[BigQueryView, Set[str]]
        ) -> Set[str]:
            view_builder = builders_by_address[
                (view.address.dataset_id, view.address.table_id)
            ]

            parent_constraints: List[Set[str]] = [
                parent_projects_to_deploy
                for parent_projects_to_deploy in parent_results.values()
                if parent_projects_to_deploy is not None
            ]
            view_projects_to_deploy = (
                view_builder.projects_to_deploy
                if view_builder.projects_to_deploy is not None
                else {*GCP_PROJECTS}
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

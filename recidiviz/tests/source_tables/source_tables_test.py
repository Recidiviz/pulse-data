# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Tests for source table registry."""
import os
import unittest
from typing import Iterable

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view_dag_walker import BigQueryViewDagWalker
from recidiviz.source_tables import yaml_managed
from recidiviz.source_tables.collect_all_source_table_configs import (
    build_source_table_repository_for_collected_schemata,
)
from recidiviz.source_tables.collect_source_tables_from_yamls import (
    collect_source_tables_from_yamls_by_dataset,
)
from recidiviz.source_tables.source_table_config import (
    SourceTableConfigDoesNotExistError,
)
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.view_registry.deployed_views import all_deployed_view_builders

COMMON_VESTIGES = [
    # This table provides a list of active feature variants that will populate dropdown
    # menus in the admin panel.
    "google_sheet_backed_tables.feature_variants",
    # These are unused but will be referenced soon in Doppler KPI views.
    #  TODO(#34767): Remove these tracker tables from the list once they're referenced
    #   in downstream views.
    "google_sheet_backed_tables.ingest_timeline_tracker",
    "google_sheet_backed_tables.ORAS_results_sheet",
    "view_update_metadata.refresh_bq_dataset_tracker",
    "view_update_metadata.rematerialization_tracker",
    "view_update_metadata.view_update_tracker",
    "view_update_metadata.per_view_update_stats",
    "all_billing_data.gcp_billing_export_v1_01338E_BE3FD6_363B4C",
    # This is a potentially useful general reference table for getting information about
    # a given zip code.
    "static_reference_tables.zip_city_county_state",
    # This is used from time to time for oneoff validation
    "static_reference_tables.us_tn_standards_due",
    # TODO(#16661): Remove this section once US_ID fully deprecated
    "static_reference_tables.state_incarceration_facilities",
    # These tables are output datasets for our population projection model code
    # (see recidiviz/calculator/modeling/population_projection) but are not referenced
    # by any deployed views.
    "population_projection_output_data.cost_avoidance_estimate_raw",
    "population_projection_output_data.cost_avoidance_non_cumulative_estimate_raw",
    "population_projection_output_data.life_years_estimate_raw",
    "population_projection_output_data.microsim_projected_outflows_raw",
    "population_projection_output_data.microsim_projection_raw",
    "population_projection_output_data.population_estimate_raw",
    "spark_public_output_data.cost_avoidance_estimate_raw",
    "spark_public_output_data.cost_avoidance_non_cumulative_estimate_raw",
    "spark_public_output_data.life_years_estimate_raw",
    "spark_public_output_data.population_estimate_raw",
    # As of 7/23/25 this is unused, but we may still want to reference Day Zero info in
    # the future.
    "static_reference_tables.day_zero_reports",
    # As of 12/1/25 these intercom tables are unused, but may be referenced for ad hoc analysis
    "intercom_export.intercom_checkpoint",
    "intercom_export.intercom_click",
    "intercom_export.intercom_completion",
    "intercom_export.intercom_dismissal",
    "intercom_export.intercom_fin_step_reached",
    "intercom_export.intercom_goal_success",
    "intercom_export.intercom_hard_bounce",
    "intercom_export.intercom_open",
    "intercom_export.intercom_overview",
    "intercom_export.intercom_reaction",
    "intercom_export.intercom_reply",
    "intercom_export.intercom_series_completion",
    "intercom_export.intercom_series_disengagement",
    "intercom_export.intercom_soft_bounce",
    "intercom_export.intercom_tour_step_failure",
    "intercom_export.intercom_tour_step_view",
    "intercom_export.intercom_unsubscribe",
    # Segment infrastructure tables (tracks/identifies/users/pages) and event tables that
    # are not currently referenced in the view graph but are tracked for source table coverage.
    "auth0_events.failed_login",
    "auth0_events.tracks",
    "auth0_prod_action_logs.tracks",
    "case_planning_production.identifies",
    "case_planning_production.tracks",
    "case_planning_production.users",
    "jii_auth0_production_segment_metrics.tracks",
    "jii_backend_production_segment_metrics.backend_edovo_login_denied",
    "jii_backend_production_segment_metrics.backend_edovo_login_failed",
    "jii_backend_production_segment_metrics.backend_edovo_login_internal_error",
    "jii_backend_production_segment_metrics.tracks",
    "pulse_dashboard_segment_metrics.hello",
    "pulse_dashboard_segment_metrics.pages_view",
    "pulse_dashboard_segment_metrics.tracks",
    "pulse_dashboard_segment_metrics.tracks_view",
    "pulse_dashboard_segment_metrics.users",
    # GCS-backed reference tables not currently referenced in the view graph
    "gcs_backed_tables.county_resident_adult_populations",
    "gcs_backed_tables.county_resident_populations",
]

# these are source tables which are in use, but not necessarily used by the main view graph
ALLOWED_VESTIGIAL_CONFIGURATIONS = {
    GCP_PROJECT_STAGING: {
        BigQueryAddress.from_str(address_str=address_str)
        for address_str in [
            # This source table is not currently in use for measuring logins, but may be used again in the future
            "pulse_dashboard_segment_metrics.identifies",
            # Daily archives of the case_insights_record export for sentencing
            # TODO(#42347): remove from this list once we use this has downstream view graph references
            "sentencing_views.case_insights_record_archive_flattened",
            "sentencing_views.case_insights_record_flattened",
            # Daily archives of tasks_record exports. Will be used for impact tracking.
            "export_archives.us_ix_supervision_tasks_record_archive",
            "export_archives.us_nd_supervision_tasks_record_archive",
            "export_archives.us_ne_supervision_tasks_record_archive",
            "export_archives.us_tx_supervision_tasks_record_archive",
            # This source table only exists & in-use in production
            "all_billing_data.gcp_billing_export_resource_v1_01338E_BE3FD6_363B4C",
            *COMMON_VESTIGES,
        ]
    },
    GCP_PROJECT_PRODUCTION: {
        BigQueryAddress.from_str(address_str=address_str)
        for address_str in [
            # This source table is not currently in use for measuring logins, but may be used again in the future
            "pulse_dashboard_segment_metrics.identifies",
            # Daily archives of the case_insights_record export for sentencing
            # TODO(#42347): remove from this list once we use this has downstream view graph references
            "sentencing_views.case_insights_record_archive_flattened",
            "sentencing_views.case_insights_record_flattened",
            # Daily archives of tasks_record exports. Will be used for impact tracking.
            "export_archives.us_ix_supervision_tasks_record_archive",
            "export_archives.us_nd_supervision_tasks_record_archive",
            "export_archives.us_ne_supervision_tasks_record_archive",
            "export_archives.us_tx_supervision_tasks_record_archive",
            *COMMON_VESTIGES,
        ]
    },
}


def build_string_for_addresses(addresses: Iterable[BigQueryAddress]) -> str:
    return "\n".join(sorted(f"\t-{address.to_str()}" for address in addresses))


class SourceTablesTest(unittest.TestCase):
    """Tests for the source tables in our view graph."""

    def run_source_tables_test(self, project_id: str) -> None:
        """Tests referenced source table definitions"""
        with local_project_id_override(project_id):
            referenced_source_tables = BigQueryViewDagWalker(
                [
                    view_builder.build()
                    for view_builder in all_deployed_view_builders()
                    if view_builder.should_deploy_in_project(project_id)
                ]
            ).get_referenced_source_tables()

            missing_source_table_definitions = set()
            source_table_repository = (
                build_source_table_repository_for_collected_schemata(
                    project_id=project_id
                )
            )

            for source_table_address in referenced_source_tables:
                try:
                    source_table_repository.get_config(source_table_address)
                except SourceTableConfigDoesNotExistError:
                    missing_source_table_definitions.add(source_table_address)

            missing_definitions = build_string_for_addresses(
                addresses=missing_source_table_definitions
            )
            # Assert all source tables have YAML definitions
            self.assertEqual(
                missing_definitions,
                "",
                "\nFound source tables that were referenced in views, but whose view definitions do not exist: \n"
                f"{missing_definitions} \n\n"
                "If this is a table that is externally managed, add a YAML definition to recidiviz/source_tables/<dataset_id>/<table_id>.yaml"
                "If we expect this table to be defined in code, be sure that it is included in a SourceTableCollection inside recidiviz.source_tables.collect_all_source_table_configs.build_source_table_repository_for_collected_schemata",
            )
            # Assert there are not any vestigial YAML files for tables that are no longer used in the view graph
            yaml_definition_addresses = set(
                source_table_config.address
                for source_table_config in source_table_repository.source_tables.values()
                if source_table_config.yaml_definition_path is not None
            )
            vestigial_definitions = (
                referenced_source_tables ^ yaml_definition_addresses
            ) & yaml_definition_addresses
            extraneous_vestiges = build_string_for_addresses(
                vestigial_definitions - ALLOWED_VESTIGIAL_CONFIGURATIONS[project_id]
            )
            self.assertEqual(
                extraneous_vestiges,
                "",
                "\nFound vestigial recidiviz/source_tables/schema/<dataset_id>/<table_id>.yaml files"
                " for the following tables that are no longer in use: \n"
                f"{extraneous_vestiges} \n\n"
                "To fix: \n"
                "- For any NEW table(s) that will be used in future work, add to ALLOWED_VESTIGIAL_CONFIGURATIONS with a to-do, "
                "your issue number, and a comment describing the intended use.\n"
                "- For table(s) that are, in fact, unused--please delete the corresponding YAML config(s)\n"
                "- Otherwise, if this table will be unused but should still exist (rare!) add an exemption to ALLOWED_VESTIGIAL_CONFIGURATIONS with an explanation",
            )

            # Assert all vestigial YAML files are only for tables that are not used in the view graph
            now_used_definitions = (
                referenced_source_tables & ALLOWED_VESTIGIAL_CONFIGURATIONS[project_id]
            ) & ALLOWED_VESTIGIAL_CONFIGURATIONS[project_id]
            now_used_definition_str = build_string_for_addresses(now_used_definitions)
            self.assertEqual(
                now_used_definition_str,
                "",
                "\nFound recidiviz/source_tables/schema/<dataset_id>/<table_id>.yaml files"
                " for the following tables that are now in use: \n"
                f"{now_used_definition_str} \n\n"
                "To fix, please remove exemption from ALLOWED_VESTIGIAL_CONFIGURATIONS",
            )

    def test_that_all_referenced_source_tables_exist_staging(self) -> None:
        self.run_source_tables_test(GCP_PROJECT_STAGING)

    def test_that_all_referenced_source_tables_exist_production(self) -> None:
        self.run_source_tables_test(GCP_PROJECT_PRODUCTION)

    def test_gcs_backed_tables_have_valid_external_data_configuration(self) -> None:
        """
        All tables in the gcs_backed_tables directory must have:
        1. An external_data_configuration section
        2. source_uris with at least one gs:// URI
        3. source_format of either CSV or NEWLINE_DELIMITED_JSON
        """

        yaml_managed_path = os.path.dirname(yaml_managed.__file__)
        gcs_backed_tables_path = os.path.join(yaml_managed_path, "gcs_backed_tables")

        # Use project ID override for loading configs
        with local_project_id_override(GCP_PROJECT_STAGING):
            # Collect all source tables from yaml_managed directory
            source_tables_by_dataset = collect_source_tables_from_yamls_by_dataset(
                yamls_root_path=yaml_managed_path
            )

            # Filter to only tables from the gcs_backed_tables subdirectory
            all_gcs_backed_tables = [
                source_table
                for source_tables in source_tables_by_dataset.values()
                for source_table in source_tables
                if source_table.yaml_definition_path
                and source_table.yaml_definition_path.startswith(gcs_backed_tables_path)
            ]

        self.assertGreater(
            len(all_gcs_backed_tables),
            0,
            "Expected to find GCS-backed tables in the gcs_backed_tables directory",
        )

        for source_table in all_gcs_backed_tables:
            with local_project_id_override(GCP_PROJECT_STAGING):
                table_name = (
                    f"{source_table.address.dataset_id}.{source_table.address.table_id}"
                )

                # Check that external_data_configuration exists
                self.assertIsNotNone(
                    source_table.external_data_configuration,
                    f"{table_name} must have an external_data_configuration section",
                )

                config = source_table.external_data_configuration
                assert config is not None  # for type checker

                # Check that source_uris exists and has at least one URI
                self.assertIsNotNone(
                    config.source_uris,
                    f"{table_name} must have source_uris in external_data_configuration",
                )
                self.assertGreater(
                    len(config.source_uris),
                    0,
                    f"{table_name} must have at least one source_uri",
                )

                # Check that all source_uris start with gs://
                for uri in config.source_uris:
                    self.assertTrue(
                        uri.startswith("gs://"),
                        f"{table_name} source_uri must start with gs://, found: {uri}",
                    )

                # Check that source_format is CSV or NEWLINE_DELIMITED_JSON
                self.assertIn(
                    config.source_format,
                    ["CSV", "NEWLINE_DELIMITED_JSON"],
                    f"{table_name} source_format must be either CSV or NEWLINE_DELIMITED_JSON, "
                    f"found: {config.source_format}",
                )

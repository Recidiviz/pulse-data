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
import unittest
from typing import Iterable

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view_dag_walker import BigQueryViewDagWalker
from recidiviz.source_tables.collect_all_source_table_configs import (
    build_source_table_repository_for_collected_schemata,
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
    # These reference tables are managed by Terraform so must have schema definitions.
    "gcs_backed_tables.county_fips",
    "gcs_backed_tables.us_tn_incarceration_facility_names",
    "gcs_backed_tables.us_tn_supervision_facility_names",
    # TODO(#16661): Remove this section once US_ID fully deprecated
    "static_reference_tables.state_incarceration_facilities",
    # This view is used by Polaris to monitor/analyze incoming messages to our Twilio phone numbers
    "twilio_webhook_requests.jii_texting_incoming_messages",
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
            "export_archives.case_insights_record_archive",
            # This source table only exists & in-use in production
            "all_billing_data.gcp_billing_export_resource_v1_01338E_BE3FD6_363B4C",
            *COMMON_VESTIGES,
        ]
    },
    GCP_PROJECT_PRODUCTION: {
        BigQueryAddress.from_str(address_str=address_str)
        for address_str in [
            # This table is only referenced in staging
            "spark_public_output_data.cost_avoidance_estimate_raw",
            # This table is only referenced in staging
            "spark_public_output_data.cost_avoidance_non_cumulative_estimate_raw",
            # This table is only referenced in staging
            "spark_public_output_data.life_years_estimate_raw",
            # This table is only referenced in staging
            "spark_public_output_data.population_estimate_raw",
            # This source table is not currently in use for measuring logins, but may be used again in the future
            "pulse_dashboard_segment_metrics.identifies",
            # Daily archives of the case_insights_record export for sentencing
            # TODO(#42347): remove from this list once we use this has downstream view graph references
            "export_archives.case_insights_record_archive",
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

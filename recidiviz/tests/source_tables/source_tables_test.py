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
from recidiviz.source_tables.source_table_config import SourceTableCouldNotGenerateError
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.view_registry.deployed_views import all_deployed_view_builders

COMMON_VESTIGES = [
    # Validation results are referenced outside the view graph via the Admin Panel
    "validation_results.validation_results",
    # It is Polaris-convention to archive all exports for historical reference, even when the archive isn't used
    "export_archives.workflows_snooze_status_archive",
    # The archive is not currently used, but will be referenced in a future view
    "export_archives.insights_supervision_officer_metrics_archive",
    # This is a potentially useful general reference table for getting information about
    # a given zip code.
    "static_reference_tables.zip_city_county_state",
    # This view will be referenced by other workflows metadata views and events/spans as part of #31645
    "static_reference_tables.workflows_launch_metadata_materialized",
    # This is used from time to time for oneoff validation
    "static_reference_tables.us_tn_standards_due",
    # These Justice Counts V1 reference tables are managed by Terraform so must have schema definitions.
    # TODO(#29814): Determine whether we can delete these tables
    "external_reference.county_fips",
    "external_reference.county_resident_adult_populations",
    "external_reference.county_resident_populations",
    "external_reference.us_tn_incarceration_facility_names",
    "external_reference.us_tn_supervision_facility_names",
]

# these are source tables which are in use, but not necessarily used by the main view graph
ALLOWED_VESTIGIAL_CONFIGURATIONS = {
    GCP_PROJECT_STAGING: {
        BigQueryAddress.from_str(address_str=address_str)
        for address_str in [
            # This source table is only in-use in production
            "pulse_dashboard_segment_metrics.frontend_opportunity_snoozed",
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
            *COMMON_VESTIGES,
        ]
    },
}


def build_string_for_addresses(addresses: Iterable[BigQueryAddress]) -> str:
    return ", ".join(sorted(address.to_str() for address in addresses))


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
                    source_table_repository.build_config(source_table_address)
                except SourceTableCouldNotGenerateError:
                    missing_source_table_definitions.add(source_table_address)

            missing_definitions = build_string_for_addresses(
                addresses=missing_source_table_definitions
            )
            # Assert all source tables have YAML definitions
            self.assertEqual(
                missing_definitions,
                "",
                "Found source tables that were referenced in views, but whose view definitions do not exist: \n"
                f"{missing_definitions} \n\n"
                "If this is a table that is externally managed, add a YAML definition to recidiviz/source_tables/{dataset_id}/{table_id}.yaml"
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
                "Found vestigial recidiviz/source_tables/schema/{dataset_id}/{table_id}.yaml files"
                " for the following tables that are no longer in use: \n"
                f"{extraneous_vestiges} \n\n"
                "To fix: \n"
                f"- determine that the table(s) is, in fact, unused then delete the corresponding YAML configs \n"
                "- or add an exemption to ALLOWED_VESTIGIAL_CONFIGURATIONS with a comment about why an exemption makes sense (rare)",
            )

    def test_that_all_referenced_source_tables_exist_staging(self) -> None:
        self.run_source_tables_test(GCP_PROJECT_STAGING)

    def test_that_all_referenced_source_tables_exist_production(self) -> None:
        self.run_source_tables_test(GCP_PROJECT_PRODUCTION)

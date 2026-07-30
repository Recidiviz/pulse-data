# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""
Analyzes each entrypoint in our codebase and ensures that it only depends on
code from a fixed set of modules.

This analysis includes transitive dependencies, not just direct ones.

This is the inverse of how most code visibility enforcement works, where a module
would define what other modules can depend on it. In the future it may be useful to
move to that model.

Note, this currently only deals with recidiviz source, it does not check external
package dependencies and whether they should be allowed. This could be a
potential extension, but would be easiest if we used a dependency analysis tool
that allowed us to limit to only the first layer of external packages, and omit
any packages that those packages depend on.


Usage examples:
    # Run all validation tests
    uv run pytest recidiviz/tests/tools/validate_source_visibility_integration_test.py
"""

from recidiviz.pipelines.utils.pipeline_run_utils import collect_all_pipeline_modules
from recidiviz.tools.validate_source_visibility import (
    make_module_matcher,
    validate_dependencies_for_entrypoint,
)

# ============================================================================
# PIPELINE VALIDATION TESTS
# ============================================================================


# TODO(#6862): Move entrypoint/visibility configuration to a global yaml or package
# specific yamls.
# TODO(#6861): Support enforcing which external packages can be used as well.
def test_pipeline_dependencies() -> None:
    """Test that all pipelines have valid dependencies.

    This test validates that each Apache Beam pipeline only depends on
    allowed modules based on its type (metrics, ingest, etc.).
    """
    for pipeline in collect_all_pipeline_modules():
        if pipeline.__file__ is None:
            raise ValueError(f"No file associated with {pipeline}.")

        valid_prefixes = {
            "recidiviz.big_query.address_overrides",
            "recidiviz.big_query.big_query_address",
            "recidiviz.big_query.big_query_query_provider",
            "recidiviz.big_query.big_query_utils",
            "recidiviz.big_query.big_query_job_labels",
            "recidiviz.big_query.constants",
            "recidiviz.pipelines",
            "recidiviz.cloud_resources",
            "recidiviz.cloud_storage",
            "recidiviz.common",
            "recidiviz.utils",
        }

        if "metrics" in pipeline.__name__:
            valid_prefixes = valid_prefixes.union(
                {
                    "recidiviz.calculator.query.state.dataset_config",
                    "recidiviz.ingest.views.dataset_config",
                    "recidiviz.big_query.big_query_address",
                    "recidiviz.big_query.big_query_utils",
                    "recidiviz.big_query.constants",
                    # TODO(#8118): Remove this dependency once IP pre-processing no
                    #  longer relies on ingest mappings
                    "recidiviz.ingest.direct",
                    "recidiviz.persistence",
                }
            )
        if (
            "us_ix_case_note" in pipeline.__name__
            or "us_me_snoozed_opportunities" in pipeline.__name__
        ):
            valid_prefixes = valid_prefixes.union(
                {
                    "recidiviz.big_query.big_query_address_formatter",
                    "recidiviz.big_query.big_query_query_builder",
                    "recidiviz.ingest.direct.types.direct_ingest_instance",
                    "recidiviz.ingest.direct.dataset_config",
                    "recidiviz.persistence",
                }
            )
        if "activity" in pipeline.__name__:
            valid_prefixes = valid_prefixes.union(
                {
                    "recidiviz.big_query.big_query_address_formatter",
                    "recidiviz.big_query.big_query_client",
                    "recidiviz.big_query.big_query_create_or_replace_view_query_provider",
                    "recidiviz.big_query.big_query_query_builder",
                    "recidiviz.big_query.big_query_view",
                    "recidiviz.big_query.big_query_view_column",
                    "recidiviz.big_query.big_query_view_sandbox_context",
                    "recidiviz.big_query.config",
                    "recidiviz.big_query.export.export_query_config",
                    "recidiviz.big_query.row_access_policy_query_builder",
                    "recidiviz.calculator.query.sessions_query_fragments",
                    "recidiviz.calculator.query.bq_utils",
                    "recidiviz.ingest",
                    "recidiviz.metrics.metric_big_query_view",
                    "recidiviz.monitoring",
                    "recidiviz.persistence",
                }
            )
        if "identity" in pipeline.__name__:
            valid_prefixes = valid_prefixes.union(
                {
                    "recidiviz.big_query.big_query_address_formatter",
                    "recidiviz.big_query.big_query_query_builder",
                    "recidiviz.calculator.query.bq_utils",
                    "recidiviz.calculator.query.sessions_query_fragments",
                    "recidiviz.ingest.direct",
                    "recidiviz.monitoring",
                    "recidiviz.persistence",
                }
            )

        validate_dependencies_for_entrypoint(
            pipeline.__name__,
            valid_module_prefixes=make_module_matcher(valid_prefixes),
        )


# ============================================================================
# AIRFLOW DAG VALIDATION TESTS
# ============================================================================


VALID_CALCULATION_DAG_PREFIXES = {
    "recidiviz.airflow.dags",
    "recidiviz.calculator.query.state.dataset_config",
    "recidiviz.big_query.big_query_job_labels",
    "recidiviz.big_query.address_overrides",
    "recidiviz.big_query.big_query_address",
    "recidiviz.common",
    "recidiviz.cloud_resources",
    "recidiviz.cloud_storage.gcsfs_path",
    "recidiviz.ingest.direct.dataset_config",
    "recidiviz.ingest.direct.direct_ingest_regions",
    "recidiviz.ingest.direct.raw_data.watermark_utils",
    "recidiviz.ingest.direct.regions.direct_ingest_region_utils",
    "recidiviz.ingest.direct.types.direct_ingest_instance",
    "recidiviz.ingest.direct.types.ingest_pipeline_type",
    "recidiviz.metrics.export.products",
    "recidiviz.persistence.database",
    "recidiviz.pipelines.config_paths",
    "recidiviz.pipelines.ingest.activity.dataset_config",
    "recidiviz.pipelines.ingest.activity.pipeline_parameters",
    "recidiviz.pipelines.ingest.activity.pipeline_utils",
    "recidiviz.pipelines.metrics.pipeline_parameters",
    "recidiviz.pipelines.pipeline_names",
    "recidiviz.pipelines.pipeline_parameters",
    "recidiviz.pipelines.supplemental.pipeline_parameters",
    "recidiviz.pipelines.supplemental.dataset_config",
    "recidiviz.utils",
}


def test_calculation_dag_dependencies() -> None:
    """Test that calculation_dag has valid dependencies."""
    validate_dependencies_for_entrypoint(
        "recidiviz.airflow.dags.calculation_dag",
        valid_module_prefixes=make_module_matcher(VALID_CALCULATION_DAG_PREFIXES),
    )


def test_calculation_dag_test_dependencies() -> None:
    """Test that calculation_dag_test has valid dependencies."""
    validate_dependencies_for_entrypoint(
        "recidiviz.airflow.tests.calculation_dag_test",
        valid_module_prefixes=make_module_matcher(
            {
                "recidiviz.airflow.tests",
                "recidiviz.persistence",
                "recidiviz.tests.ingest.direct.fake_regions",
                "recidiviz.tests.metrics.export.fixtures",
                "recidiviz.tests.pipelines",
                "recidiviz.tests.test_setup_utils",
                "recidiviz.tools.postgres.local_postgres_helpers",
                "recidiviz.tools.utils.script_helpers",
                *VALID_CALCULATION_DAG_PREFIXES,
            }
        ),
    )


VALID_RAW_DATA_IMPORT_DAG_PREFIXES = {
    "recidiviz.airflow.dags",
    "recidiviz.big_query.address_overrides",
    "recidiviz.big_query.big_query_address",
    "recidiviz.big_query.big_query_job_labels",
    "recidiviz.big_query.big_query_address_formatter",
    "recidiviz.big_query.big_query_client",
    "recidiviz.big_query.big_query_create_or_replace_view_query_provider",
    "recidiviz.big_query.big_query_query_builder",
    "recidiviz.big_query.big_query_query_provider",
    "recidiviz.big_query.big_query_utils",
    "recidiviz.big_query.big_query_view",
    "recidiviz.big_query.big_query_view_column",
    "recidiviz.big_query.big_query_view_sandbox_context",
    "recidiviz.big_query.config",
    "recidiviz.big_query.constants",
    "recidiviz.big_query.export.export_query_config",
    "recidiviz.big_query.row_access_policy_query_builder",
    "recidiviz.calculator.query.sessions_query_fragments",
    "recidiviz.calculator.query.bq_utils",
    "recidiviz.cloud_resources",
    "recidiviz.cloud_storage.gcs_file_system",
    "recidiviz.cloud_storage.gcs_file_system_impl",
    "recidiviz.cloud_storage.gcsfs_factory",
    "recidiviz.cloud_storage.gcsfs_path",
    "recidiviz.cloud_storage.types",
    "recidiviz.cloud_storage.verifiable_bytes_reader",
    "recidiviz.common",
    "recidiviz.ingest.direct.dataset_config",
    "recidiviz.ingest.direct.direct_ingest_bucket_name_utils",
    "recidiviz.ingest.direct.direct_ingest_regions",
    "recidiviz.ingest.direct.gating",
    "recidiviz.ingest.direct.gcs.direct_ingest_gcs_file_system",
    "recidiviz.ingest.direct.gcs.directory_path_utils",
    "recidiviz.ingest.direct.gcs.filename_parts",
    "recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_load_manager",
    "recidiviz.ingest.direct.raw_data.direct_ingest_raw_table_migration",
    "recidiviz.ingest.direct.raw_data.direct_ingest_raw_table_migration_collector",
    "recidiviz.ingest.direct.raw_data.direct_ingest_raw_table_migration_generator",
    "recidiviz.ingest.direct.raw_data.direct_ingest_raw_table_pre_import_validator",
    "recidiviz.ingest.direct.raw_data.direct_ingest_raw_table_schema_builder",
    "recidiviz.ingest.direct.raw_data.direct_ingest_raw_table_transformation_query_builder",
    "recidiviz.ingest.direct.raw_data.documentation_exemptions",
    "recidiviz.ingest.direct.raw_data.raw_data_import_chunked_file_handler",
    "recidiviz.ingest.direct.raw_data.state_raw_file_chunking_metadata_factory",
    "recidiviz.ingest.direct.raw_data.raw_data_pruning_bq_utils",
    "recidiviz.ingest.direct.raw_data.raw_file_chunking_metadata",
    "recidiviz.ingest.direct.raw_data.raw_file_chunking_metadata_history",
    "recidiviz.ingest.direct.raw_data.raw_file_config_enums",
    "recidiviz.ingest.direct.raw_data.raw_file_config_utils",
    "recidiviz.ingest.direct.raw_data.raw_file_configs",
    "recidiviz.ingest.direct.raw_data.raw_table_relationship_info",
    "recidiviz.ingest.direct.raw_data.datetime_sql_parser_exemptions",
    "recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_header_reader",
    "recidiviz.ingest.direct.raw_data.validations",
    "recidiviz.ingest.direct.regions",
    "recidiviz.ingest.direct.types.direct_ingest_constants",
    "recidiviz.ingest.direct.types.direct_ingest_instance",
    "recidiviz.ingest.direct.types.errors",
    "recidiviz.ingest.direct.types.ingest_pipeline_type",
    "recidiviz.ingest.direct.views.direct_ingest_view_query_builder",
    "recidiviz.ingest.direct.views.direct_ingest_view_query_builder_collector",
    "recidiviz.ingest.direct.types.raw_data_pre_import_validation",
    "recidiviz.ingest.direct.types.raw_data_pre_import_validation_collector",
    "recidiviz.ingest.direct.types.raw_data_pre_import_validation_type",
    "recidiviz.ingest.direct.types.raw_data_import_types",
    "recidiviz.ingest.direct.views.raw_data_diff_query_builder",
    "recidiviz.ingest.direct.views.raw_table_query_builder",
    "recidiviz.metrics.metric_big_query_view",
    "recidiviz.persistence.database.reserved_words",
    "recidiviz.persistence.database.schema_type",
    "recidiviz.persistence.entity",
    "recidiviz.persistence.errors",
    # Pulled in transitively by `IngestPipelineType.pipeline_name`.
    "recidiviz.pipelines.pipeline_names",
    "recidiviz.utils",
    "recidiviz.utils.environment",
}


def test_raw_data_import_dag_dependencies() -> None:
    """Test that raw_data_import_dag has valid dependencies."""
    validate_dependencies_for_entrypoint(
        "recidiviz.airflow.dags.raw_data_import_dag",
        valid_module_prefixes=make_module_matcher(VALID_RAW_DATA_IMPORT_DAG_PREFIXES),
    )


def test_raw_data_import_dag_test_dependencies() -> None:
    """Test that raw_data_import_dag_test has valid dependencies."""
    validate_dependencies_for_entrypoint(
        "recidiviz.airflow.tests.raw_data_import_dag_test",
        valid_module_prefixes=make_module_matcher(
            {
                "recidiviz.airflow.tests",
                "recidiviz.persistence",
                "recidiviz.cloud_storage.bytes_chunk_reader",
                "recidiviz.cloud_storage.read_only_csv_normalizing_stream",
                "recidiviz.cloud_storage.gcsfs_csv_chunk_boundary_finder",
                "recidiviz.entrypoints.entrypoint_interface",
                "recidiviz.entrypoints.raw_data.divide_raw_file_into_chunks",
                "recidiviz.entrypoints.raw_data.normalize_raw_file_chunks",
                "recidiviz.entrypoints.entrypoint_utils",
                "recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_pre_import_normalizer",
                "recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_header_reader",
                "recidiviz.persistence.database.schema.operations.schema",
                "recidiviz.persistence.database.database_entity",
                "recidiviz.persistence.entity.core_entity",
                "recidiviz.tests.ingest.direct.fake_regions",
                "recidiviz.tests.test_setup_utils",
                "recidiviz.tests.cloud_storage.fake_gcs_file_system",
                "recidiviz.tools.utils.script_helpers",
                "recidiviz.tools.postgres.local_postgres_helpers",
                *VALID_RAW_DATA_IMPORT_DAG_PREFIXES,
            }
        ),
    )


VALID_MONITORING_DAG_PREFIXES = {
    "recidiviz.airflow.dags",
    "recidiviz.big_query.address_overrides",
    "recidiviz.big_query.big_query_address",
    "recidiviz.big_query.big_query_address_formatter",
    "recidiviz.big_query.big_query_client",
    "recidiviz.big_query.big_query_create_or_replace_view_query_provider",
    "recidiviz.big_query.big_query_job_labels",
    "recidiviz.big_query.big_query_query_builder",
    "recidiviz.big_query.big_query_query_provider",
    "recidiviz.big_query.big_query_utils",
    "recidiviz.big_query.big_query_view",
    "recidiviz.big_query.big_query_view_column",
    "recidiviz.big_query.big_query_view_sandbox_context",
    "recidiviz.big_query.big_query_row_streamer",
    "recidiviz.big_query.config",
    "recidiviz.big_query.constants",
    "recidiviz.big_query.export.export_query_config",
    "recidiviz.big_query.row_access_policy_query_builder",
    "recidiviz.cloud_resources",
    "recidiviz.cloud_storage.gcsfs_path",
    "recidiviz.common.alias_type_strings",
    "recidiviz.common.attr_converters",
    "recidiviz.common.attr_validators",
    "recidiviz.common.attr_utils",
    "recidiviz.common.constants",
    "recidiviz.common.demographics",
    "recidiviz.common.demographics_strings",
    "recidiviz.common.entity_enum",
    "recidiviz.common.entity_enum_strings",
    "recidiviz.common.file_system",
    "recidiviz.common.google_cloud_attr_validators",
    "recidiviz.common.google_cloud.utils",
    "recidiviz.common.attr_mixins",
    "recidiviz.common.date",
    "recidiviz.common.module_collector_mixin",
    "recidiviz.common.retry",
    "recidiviz.common.state_exempted_attrs_validator",
    "recidiviz.common.str_field_utils",
    "recidiviz.common.retry_predicate",
    "recidiviz.ingest.direct.dataset_config",
    "recidiviz.ingest.direct.direct_ingest_documentation_generator",
    "recidiviz.ingest.direct.direct_ingest_regions",
    "recidiviz.ingest.direct.gating",
    "recidiviz.ingest.direct.raw_data.datetime_sql_parser_exemptions",
    "recidiviz.ingest.direct.raw_data.documentation_exemptions",
    "recidiviz.ingest.direct.raw_data.raw_file_config_enums",
    "recidiviz.ingest.direct.raw_data.raw_file_config_utils",
    "recidiviz.ingest.direct.raw_data.raw_file_configs",
    "recidiviz.ingest.direct.raw_data.direct_ingest_raw_table_schema_builder",
    "recidiviz.ingest.direct.raw_data.raw_file_references_utils",
    "recidiviz.ingest.direct.raw_data.raw_table_relationship_info",
    "recidiviz.ingest.direct.regions",
    "recidiviz.ingest.direct.views",
    "recidiviz.ingest.direct.types.direct_ingest_constants",
    "recidiviz.ingest.direct.types.direct_ingest_instance",
    "recidiviz.ingest.direct.types.ingest_pipeline_type",
    "recidiviz.ingest.direct.types.raw_data_pre_import_validation_type",
    "recidiviz.calculator.query.bq_utils",
    "recidiviz.calculator.query.sessions_query_fragments",
    "recidiviz.metrics.metric_big_query_view",
    "recidiviz.persistence.database.reserved_words",
    "recidiviz.persistence.database.schema_type",
    "recidiviz.persistence.entity",
    # Pulled in transitively by `IngestPipelineType.pipeline_name`.
    "recidiviz.pipelines.pipeline_names",
    "recidiviz.github.github_client",
    "recidiviz.github.github_constants",
    "recidiviz.github.github_issue",
    "recidiviz.github.github_pull_request",
    "recidiviz.issue_tracking.issue",
    "recidiviz.issue_tracking.issue_parsing",
    "recidiviz.issue_tracking.labels",
    "recidiviz.issue_tracking.linear.linear_issue",
    "recidiviz.issue_tracking.linear.linear_types",
    "recidiviz.tools.docs.utils",
    "recidiviz.tools.raw_data_reference_reasons_yaml_loader",
    "recidiviz.utils.environment",
    "recidiviz.utils.types",
    "recidiviz.utils.string",
    "recidiviz.utils.string_formatting",
    "recidiviz.utils.airflow_types",
    "recidiviz.utils.encoding",
    "recidiviz.utils.metadata",
    "recidiviz.utils.size",
    "recidiviz.utils.secrets",
    "recidiviz.utils.yaml_dict",
}


def test_monitoring_dag_dependencies() -> None:
    """Test that monitoring_dag has valid dependencies."""
    validate_dependencies_for_entrypoint(
        "recidiviz.airflow.dags.monitoring_dag",
        valid_module_prefixes=make_module_matcher(VALID_MONITORING_DAG_PREFIXES),
    )


def test_monitoring_dag_test_dependencies() -> None:
    """Test that monitoring_dag_test has valid dependencies."""
    validate_dependencies_for_entrypoint(
        "recidiviz.airflow.tests.monitoring_dag_test",
        valid_module_prefixes=make_module_matcher(
            {
                "recidiviz.airflow.tests",
                "recidiviz.common",
                "recidiviz.persistence",
                "recidiviz.tests.test_setup_utils",
                "recidiviz.tools.utils.script_helpers",
                "recidiviz.tools.postgres.local_postgres_helpers",
                *VALID_MONITORING_DAG_PREFIXES,
            }
        ),
    )


VALID_SFTP_DAG_PREFIXES = {
    "recidiviz.airflow.dags",
    "recidiviz.big_query.big_query_job_labels",
    "recidiviz.big_query.big_query_address",
    "recidiviz.big_query.big_query_utils",
    "recidiviz.big_query.constants",
    "recidiviz.cloud_resources",
    "recidiviz.cloud_storage",
    "recidiviz.common",
    "recidiviz.ingest.direct",
    "recidiviz.persistence.database.reserved_words",
    "recidiviz.persistence.database.schema_type",
    "recidiviz.persistence.errors",
    "recidiviz.utils.airflow_types",
    "recidiviz.utils.encoding",
    "recidiviz.utils.environment",
    "recidiviz.utils.metadata",
    "recidiviz.utils.string",
    "recidiviz.utils.string_formatting",
    "recidiviz.utils.yaml_dict",
    "recidiviz.utils.types",
}


def test_sftp_dag_dependencies() -> None:
    """Test that sftp_dag has valid dependencies."""
    validate_dependencies_for_entrypoint(
        "recidiviz.airflow.dags.sftp_dag",
        valid_module_prefixes=make_module_matcher(VALID_SFTP_DAG_PREFIXES),
    )


def test_sftp_dag_test_dependencies() -> None:
    """Test that sftp_dag_test has valid dependencies."""
    validate_dependencies_for_entrypoint(
        "recidiviz.airflow.tests.sftp_dag_test",
        valid_module_prefixes=make_module_matcher(
            {
                *VALID_SFTP_DAG_PREFIXES,
                "recidiviz.airflow.tests",
                "recidiviz.persistence",
                "recidiviz.persistence.database.schema.operations.schema",
                "recidiviz.persistence.database.database_entity",
                "recidiviz.persistence.entity.core_entity",
                "recidiviz.tools.utils.script_helpers",
                "recidiviz.tools.postgres.local_postgres_helpers",
                "recidiviz.tests.cloud_storage.fake_gcs_file_system",
                "recidiviz.tests.test_setup_utils",
            }
        ),
    )


# ============================================================================
# CLOUD FUNCTION VALIDATION TESTS
# ============================================================================


def test_cloud_functions_main_dependencies() -> None:
    """Test that cloud_functions.main has valid dependencies."""
    validate_dependencies_for_entrypoint(
        "recidiviz.cloud_functions.main",
        valid_module_prefixes=make_module_matcher({"recidiviz.cloud_functions.main"}),
    )


def test_cloud_function_ingest_filename_normalization_dependencies() -> None:
    """Test that ingest_filename_normalization cloud function has valid dependencies."""
    validate_dependencies_for_entrypoint(
        "recidiviz.cloud_functions.ingest_filename_normalization",
        valid_module_prefixes=make_module_matcher(
            {
                "recidiviz.cloud_functions.cloud_function_utils",
                "recidiviz.cloud_storage.gcsfs_factory",
                "recidiviz.cloud_storage.gcs_file_system",
                "recidiviz.cloud_storage.gcs_file_system_impl",
                "recidiviz.cloud_storage.verifiable_bytes_reader",
                "recidiviz.cloud_storage.gcsfs_path",
                "recidiviz.common.attr_validators",
                "recidiviz.common.io.contents_handle",
                "recidiviz.common.io.file_contents_handle",
                "recidiviz.common.io.local_file_contents_handle",
                "recidiviz.common.io.zip_file_contents_handle",
                "recidiviz.common.retry_predicate",
                "recidiviz.ingest.direct.direct_ingest_bucket_name_utils",
                "recidiviz.ingest.direct.gcs.filename_parts",
                "recidiviz.ingest.direct.types.direct_ingest_constants",
                "recidiviz.ingest.direct.types.direct_ingest_instance_factory",
                "recidiviz.ingest.direct.types.errors",
                "recidiviz.utils.environment",
                "recidiviz.utils.metadata",
                "recidiviz.utils.string_formatting",
                "recidiviz.ingest.direct.gcs.direct_ingest_gcs_file_system",
                "recidiviz.ingest.direct.gcs.directory_path_utils",
                "recidiviz.ingest.direct.types.direct_ingest_instance",
            }
        ),
    )


# ============================================================================
# SERVER APPLICATION VALIDATION TESTS
# ============================================================================


def test_case_triage_server_dependencies() -> None:
    """Test that case_triage.server has valid dependencies."""
    validate_dependencies_for_entrypoint(
        "recidiviz.case_triage.server",
        valid_module_prefixes=make_module_matcher(
            {
                # TODO(#24506): Clean up this dependency
                "recidiviz.aggregated_metrics",
                "recidiviz.calculator",
                "recidiviz.big_query",
                "recidiviz.task_eligibility",
                "recidiviz.calculator.query.state.views.dashboard.pathways",
                "recidiviz.calculator.query.state.views.outliers.outliers_enabled_states",
                "recidiviz.case_triage",
                "recidiviz.cloud_memorystore",
                "recidiviz.cloud_resources",
                "recidiviz.cloud_storage",
                "recidiviz.common",
                "recidiviz.firestore",
                "recidiviz.ingest.direct.dataset_config",
                "recidiviz.ingest.direct.regions.us_mi.constants",
                "recidiviz.ingest.direct.types.direct_ingest_instance",
                "recidiviz.ingest.views.dataset_config",
                "recidiviz.intercom",
                # Pulled in transitively by BigQueryClientImpl, which the Edovo
                # course-completion route uses to resolve a person by DOC id.
                "recidiviz.metrics",
                "recidiviz.monitoring",
                "recidiviz.observations",
                "recidiviz.outliers",
                "recidiviz.pipelines.ingest.activity.dataset_config",
                "recidiviz.pipelines.supplemental.dataset_config",
                "recidiviz.workflows",
                "recidiviz.persistence",
                "recidiviz.segment.product_type",
                # Used to wire up Case Triage scoped sessions (current_session).
                "recidiviz.server_config",
                "recidiviz.tools.jii.hydrate_test_data",
                "recidiviz.tools.utils.fixture_helpers",
                "recidiviz.utils",
            }
        ),
    )


def test_identity_service_server_dependencies() -> None:
    """Test that services.identity.server has valid dependencies."""
    validate_dependencies_for_entrypoint(
        "recidiviz.services.identity.server",
        valid_module_prefixes=make_module_matcher(
            {
                "recidiviz.common",
                "recidiviz.monitoring",
                "recidiviz.persistence.database",
                "recidiviz.persistence.entity",
                "recidiviz.services.identity",
                "recidiviz.utils",
            }
        ),
    )


def test_justice_counts_server_dependencies() -> None:
    """Test that justice_counts.control_panel.server has valid dependencies."""
    validate_dependencies_for_entrypoint(
        "recidiviz.justice_counts.control_panel.server",
        valid_module_prefixes=make_module_matcher(
            {
                "recidiviz.justice_counts",
                "recidiviz.common",
                "recidiviz.persistence",
                "recidiviz.utils",
                "recidiviz.auth",
                "recidiviz.cloud_storage",
                "recidiviz.monitoring",
            }
        ),
    )


def test_admin_panel_server_dependencies() -> None:
    """Test that admin_panel.server has valid dependencies."""
    validate_dependencies_for_entrypoint(
        "recidiviz.admin_panel.server",
        valid_module_prefixes=make_module_matcher(
            {
                "recidiviz.admin_panel",
                "recidiviz.aggregated_metrics",
                "recidiviz.auth",
                "recidiviz.big_query",
                "recidiviz.calculator",
                "recidiviz.case_triage",
                "recidiviz.cloud_resources",
                "recidiviz.cloud_storage",
                "recidiviz.common",
                "recidiviz.datasets",
                "recidiviz.documents",
                "recidiviz.firestore",
                "recidiviz.ingest",
                "recidiviz.intercom",
                "recidiviz.llm_eval",
                "recidiviz.metrics",
                "recidiviz.monitoring",
                "recidiviz.observations",
                "recidiviz.outcome_metrics",
                "recidiviz.outliers",
                "recidiviz.persistence",
                "recidiviz.pipelines",
                "recidiviz.reporting",
                "recidiviz.segment",
                "recidiviz.server_config",
                "recidiviz.source_tables",
                "recidiviz.task_eligibility",
                "recidiviz.utils",
                "recidiviz.validation",
                "recidiviz.view_registry",
                "recidiviz.workflows",
            }
        ),
    )


def test_application_data_import_server_dependencies() -> None:
    """Test that application_data_import.server has valid dependencies."""
    validate_dependencies_for_entrypoint(
        "recidiviz.application_data_import.server",
        valid_module_prefixes=make_module_matcher(
            {
                "recidiviz.admin_panel.constants",
                "recidiviz.aggregated_metrics",
                "recidiviz.application_data_import",
                "recidiviz.auth",
                "recidiviz.datasets.static_data.views.data",
                "recidiviz.datasets.static_data.views.dataset_config",
                "recidiviz.backup.backup_manager",
                "recidiviz.big_query",
                "recidiviz.calculator",
                "recidiviz.case_triage",
                "recidiviz.cloud_sql",
                "recidiviz.cloud_resources",
                "recidiviz.cloud_storage",
                "recidiviz.cloud_memorystore",
                "recidiviz.common",
                "recidiviz.firestore.firestore_client",
                "recidiviz.ingest",
                "recidiviz.metrics",
                "recidiviz.monitoring",
                "recidiviz.observations",
                "recidiviz.outliers",
                "recidiviz.persistence",
                "recidiviz.pipelines",
                "recidiviz.public_pathways",
                "recidiviz.reporting",
                "recidiviz.segment.product_type",
                "recidiviz.source_tables",
                "recidiviz.task_eligibility",
                "recidiviz.tools.archive",
                "recidiviz.utils",
                "recidiviz.validation",
                "recidiviz.workflows",
            }
        ),
    )


# ============================================================================
# ENTRYPOINT VALIDATION TESTS
# ============================================================================


def test_entrypoint_executor_dependencies() -> None:
    """Test that entrypoint_executor has valid dependencies."""
    validate_dependencies_for_entrypoint(
        "recidiviz.entrypoints.entrypoint_executor",
        valid_module_prefixes=make_module_matcher(
            {
                "recidiviz.aggregated_metrics",
                "recidiviz.big_query",
                "recidiviz.calculator",
                "recidiviz.case_triage.views",
                "recidiviz.view_registry",
                "recidiviz.tools.deploy.logging",
                "recidiviz.cloud_resources",
                "recidiviz.cloud_storage",
                "recidiviz.common",
                "recidiviz.datasets",
                "recidiviz.documents",
                "recidiviz.entrypoints",
                "recidiviz.ingest",
                "recidiviz.intercom",
                "recidiviz.llm_eval",
                "recidiviz.metrics",
                "recidiviz.monitoring",
                "recidiviz.observations",
                "recidiviz.outcome_metrics",
                "recidiviz.outliers",
                "recidiviz.persistence",
                "recidiviz.pipelines",
                "recidiviz.github.github_client",
                "recidiviz.github.github_issue",
                "recidiviz.github.github_pull_request",
                "recidiviz.issue_tracking.issue",
                "recidiviz.issue_tracking.labels",
                "recidiviz.segment",
                "recidiviz.source_tables",
                "recidiviz.task_eligibility",
                "recidiviz.utils",
                "recidiviz.validation",
                "recidiviz.workflows",
            },
        ),
        explicitly_invalid_package_dependencies=["apache_beam"],
    )


def test_entrypoint_eomis_writeback_dependencies() -> None:
    """Test that the eomis_writeback entrypoint has valid dependencies."""
    validate_dependencies_for_entrypoint(
        "recidiviz.entrypoints.eomis_writeback",
        valid_module_prefixes=make_module_matcher(
            {
                "recidiviz.common",
                "recidiviz.entrypoints.entrypoint_interface",
                "recidiviz.entrypoints.eomis_writeback",
                "recidiviz.eomis",
                "recidiviz.utils",
            },
        ),
        explicitly_invalid_package_dependencies=["apache_beam"],
    )


def test_entrypoint_report_metric_export_timeliness_dependencies() -> None:
    """Test that report_metric_export_timeliness entrypoint has valid dependencies."""
    validate_dependencies_for_entrypoint(
        "recidiviz.entrypoints.monitoring.report_metric_export_timeliness",
        valid_module_prefixes=make_module_matcher(
            {
                "recidiviz.common",
                "recidiviz.entrypoints",
                "recidiviz.monitoring",
                "recidiviz.pipelines",
                "recidiviz.ingest.direct",
                "recidiviz.utils",
                "recidiviz.aggregated_metrics",
                "recidiviz.big_query",
                "recidiviz.calculator",
                "recidiviz.cloud_resources",
                "recidiviz.cloud_storage",
                "recidiviz.metrics",
                "recidiviz.persistence",
                "recidiviz.source_tables",
                "recidiviz.task_eligibility",
                "recidiviz.ingest.views",
                "recidiviz.validation",
                "recidiviz.observations",
                "recidiviz.outliers",
                "recidiviz.workflows",
                "recidiviz.segment.product_type",
                "recidiviz.datasets.static_data.views.data",
                "recidiviz.datasets.static_data.views.dataset_config",
            }
        ),
        # TODO(#3828): We won't have to explicitly disallow apache_beam once we've
        #  isolated the Dataflow pipeline code completely
        explicitly_invalid_package_dependencies=["apache_beam"],
    )


# ============================================================================
# VIEW REGISTRY VALIDATION TESTS
# ============================================================================


def test_view_registry_deployed_views_dependencies() -> None:
    """Test that view_registry.deployed_views has valid dependencies."""
    validate_dependencies_for_entrypoint(
        "recidiviz.view_registry.deployed_views",
        valid_module_prefixes=make_module_matcher(
            {
                # general bq things and utils
                "recidiviz.big_query",
                "recidiviz.cloud_resources",
                "recidiviz.cloud_storage.gcsfs_path",
                "recidiviz.common",
                "recidiviz.utils",
                # dataset or const imports where we want to be strict-ish
                "recidiviz.case_triage.views.dataset_config",
                "recidiviz.datasets.static_data.terraform_managed.config",
                "recidiviz.pipelines.dataflow_config",
                "recidiviz.pipelines.ingest.activity.dataset_config",
                # Pulled in transitively by `IngestPipelineType.pipeline_name`.
                "recidiviz.pipelines.pipeline_names",
                "recidiviz.pipelines.supplemental.dataset_config",
                "recidiviz.source_tables.externally_managed.datasets",
                "recidiviz.source_tables.yaml_managed.datasets",
                "recidiviz.validation.views.dataset_config",
                "recidiviz.source_tables",
                "recidiviz.view_registry",
                # view code
                "recidiviz.aggregated_metrics",
                "recidiviz.calculator.query",
                "recidiviz.datasets.static_data.views",
                "recidiviz.ingest.views",
                "recidiviz.ingest.direct",
                "recidiviz.llm_eval",
                "recidiviz.monitoring.platform_kpis",
                "recidiviz.observations",
                "recidiviz.segment",
                "recidiviz.task_eligibility",
                "recidiviz.validation.views",
                "recidiviz.outcome_metrics",
                # code pulled in by above views that we want to be strict-ish
                "recidiviz.persistence.entity",
                "recidiviz.persistence.database",
                "recidiviz.persistence.errors",
                "recidiviz.pipelines.utils.identifier_models",
                "recidiviz.outliers.constants",
                "recidiviz.outliers.outliers_configs",
                "recidiviz.outliers.types",
                "recidiviz.validation.checks",
                "recidiviz.validation.config",
                "recidiviz.validation.validation_models",
                "recidiviz.validation.validation_output_views",
                "recidiviz.validation.validation_config",
                "recidiviz.validation.configured_validations",
                "recidiviz.workflows.types",
                #   - by most_recent_dataflow_population_span_to_single_day_metrics
                "recidiviz.metrics.export.products.product_configs",
                "recidiviz.metrics.metric_big_query_view",
                #   - by invalid_null_pfi_in_metrics validation
                #     # TODO(#46066) consider refactoring pipelines.metrics to be safer
                #       and not allow bringing beam in
                "recidiviz.pipelines.metrics",
                "recidiviz.pipelines.supplemental.us_ix_case_note_extracted_entities.us_ix_note_content_text_analysis_configuration",
                "recidiviz.pipelines.supplemental.us_ix_case_note_extracted_entities.us_ix_note_title_text_analysis_configuration",
            },
        ),
        explicitly_invalid_package_dependencies=["apache_beam"],
    )

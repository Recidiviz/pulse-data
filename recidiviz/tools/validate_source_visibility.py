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
"""Verifies that code only depends on modules that are visible to it

Example usage:
$ python -m recidiviz.tools.validate_source_visibility

If you add a new dependency that causes this to fail, you should evaluate whether
this dependency (1) should exist at all and (2) if it should, whether it could be
cleaner. For example, if you need to access some constants related to persistence
from higher-level application code, consider pulling these constants out into a
shared module instead of importing persistence logic into this server.
"""
import sys
from typing import Dict, Iterable, List, Optional, Set, Tuple

import attr
import pygtrie

from recidiviz.pipelines.utils.pipeline_run_utils import collect_all_pipeline_modules
from recidiviz.tools.file_dependencies import Callsite, EntrypointDependencies


def make_module_matcher(modules: Iterable[str]) -> pygtrie.PrefixSet:
    return pygtrie.PrefixSet(
        iterable=modules, factory=pygtrie.StringTrie, separator="."
    )


def is_valid_module_dependency(
    module_name: str,
    valid_module_prefixes: pygtrie.PrefixSet,
) -> bool:
    # Checks if module or a prefix of this module is allowed
    if module_name in valid_module_prefixes:
        return True

    # Checks if a child of this module is allowed
    children = list(valid_module_prefixes.iter(prefix=module_name))
    return len(children) > 0


@attr.s(frozen=True, kw_only=True)
class DependencyAnalysisResult:
    invalid_modules: Dict[str, List[Tuple[str, Callsite]]] = attr.ib()
    unused_valid_module_prefixes: Set[str] = attr.ib()


def get_invalid_dependencies_for_entrypoint(
    entrypoint_module: str,
    valid_module_prefixes: pygtrie.PrefixSet,
    explicitly_invalid_package_dependencies: Optional[List[str]] = None,
) -> DependencyAnalysisResult:
    """Gets the transitive dependencies for the entrypoints and checks their validity.

    Returns two elements. The first is a dictionary of invalid dependency names to the
    call chain that includes them. The second is a list of dependency prefixes that
    were explicitly allowed but that no actual dependencies relied on.
    """
    dependencies = EntrypointDependencies().add_dependencies_for_entrypoint(
        entrypoint_module
    )

    valid_dependencies: Set[str] = set()
    invalid_dependencies: Dict[str, List[Tuple[str, Callsite]]] = {}

    for module_name, callers in dependencies.modules.items():
        if module_name == entrypoint_module or is_valid_module_dependency(
            module_name, valid_module_prefixes
        ):
            valid_dependencies.add(module_name)
            continue

        if not callers:
            raise ValueError(
                f"Found dependency module [{module_name}] of entrypoint "
                f"[{entrypoint_module}] with no callers. This should not be possible."
            )

        valid_callers = [
            c
            for c in callers
            if is_valid_module_dependency(c, valid_module_prefixes)
            or c == entrypoint_module
        ]

        if valid_callers:
            # If this module is directly imported by a module that is a valid
            # dependency, arbitrarily pick one of the of those parent modules and store
            # the full call chain for display later.
            caller = valid_callers[0]
            invalid_dependencies[module_name] = [
                (caller, callers[caller][0])
            ] + dependencies.sample_call_chain_for_module(caller)
        # Otherwise, this module is not called directly by any valid module. We assume
        # that one of its invalid parents in the call chain has a valid caller, so an
        # error will be collected via the block above.

    for package_name, callers in dependencies.packages.items():
        if (
            not explicitly_invalid_package_dependencies
            or package_name not in explicitly_invalid_package_dependencies
        ):
            valid_dependencies.add(package_name)
            continue

        if not callers:
            raise ValueError(
                f"Found dependency package [{package_name}] of entrypoint "
                f"[{entrypoint_module}] with no callers. This should not be possible."
            )

        valid_callers = [
            c for c in callers if is_valid_module_dependency(c, valid_module_prefixes)
        ]

        if valid_callers:
            # If this packages is directly imported by a module that is a valid
            # dependency, arbitrarily pick one of the of those parent modules and store
            # the full call chain for display later.
            caller = valid_callers[0]
            invalid_dependencies[package_name] = [
                (caller, callers[caller][0])
            ] + dependencies.sample_call_chain_for_module(caller)

    unused_valid = valid_module_prefixes - valid_dependencies

    return DependencyAnalysisResult(
        invalid_modules=invalid_dependencies,
        unused_valid_module_prefixes={"".join(entry) for entry in unused_valid},
    )


# Define a constant list of disallowed module prefixes
DISALLOWED_MODULE_PREFIXES = [
    "recidiviz.research",  # Add other disallowed prefixes here if needed
]


def check_dependencies_for_entrypoint(
    entrypoint_module: str,
    valid_module_prefixes: pygtrie.PrefixSet,
    explicitly_invalid_package_dependencies: Optional[List[str]] = None,
) -> bool:
    """Analyzes dependencies for a given entrypoint and prints information about failures.

    Returns True for success and False for failure.
    """
    # Check if any of the valid_module_prefixes match disallowed modules
    for disallowed_prefix in DISALLOWED_MODULE_PREFIXES:
        if any(
            str(prefix).startswith(disallowed_prefix)
            for prefix in valid_module_prefixes
        ):
            raise ValueError(
                f"Invalid configuration: {disallowed_prefix} is a disallowed module "
                f"and cannot be included in valid_module_prefixes for {entrypoint_module}."
            )

    dependency_result = get_invalid_dependencies_for_entrypoint(
        entrypoint_module,
        valid_module_prefixes=valid_module_prefixes,
        explicitly_invalid_package_dependencies=explicitly_invalid_package_dependencies,
    )

    result = True

    if dependency_result.invalid_modules:
        print(f"Invalid dependencies for {entrypoint_module}:")
        for dependency, call_chain in sorted(dependency_result.invalid_modules.items()):
            print(f"\t{dependency}")
            for caller, callsite in call_chain:
                print(
                    f"\t\t{caller} ({callsite.filepath}:{callsite.lineno}:{callsite.col_offset})"
                )
        result = False

    if dependency_result.unused_valid_module_prefixes:
        print(f"Unused valid dependency prefixes for {entrypoint_module}:")
        for dependency in sorted(dependency_result.unused_valid_module_prefixes):
            print(f"\t{dependency}")
        result = False

    return result


def main() -> int:
    """Analyzes each entrypoint in our codebase and ensures that it only depends on
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
    """
    # TODO(#6862): Move entrypoint/visibility configuration to a global yaml or package
    # specific yamls.
    # TODO(#6861): Support enforcing which external packages can be used as well.
    success = True

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
                }
            )
        if "metrics" in pipeline.__name__:
            valid_prefixes = valid_prefixes.union(
                {
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
        if "ingest" in pipeline.__name__:
            valid_prefixes = valid_prefixes.union(
                {
                    "recidiviz.big_query.big_query_address_formatter",
                    "recidiviz.big_query.big_query_query_builder",
                    "recidiviz.calculator.query.sessions_query_fragments",
                    "recidiviz.calculator.query.bq_utils",
                    "recidiviz.ingest",
                    "recidiviz.persistence",
                }
            )
        success &= check_dependencies_for_entrypoint(
            pipeline.__name__,
            valid_module_prefixes=make_module_matcher(valid_prefixes),
        )

    valid_calculation_dag_prefixes = {
        "recidiviz.airflow.dags",
        "recidiviz.calculator.query.state.dataset_config",
        "recidiviz.big_query.big_query_job_labels",
        "recidiviz.big_query.address_overrides",
        "recidiviz.big_query.big_query_address",
        "recidiviz.big_query.big_query_utils",
        "recidiviz.big_query.constants",
        "recidiviz.common",
        "recidiviz.cloud_resources",
        "recidiviz.cloud_storage.gcsfs_path",
        "recidiviz.ingest.direct.dataset_config",
        "recidiviz.ingest.direct.direct_ingest_regions",
        "recidiviz.ingest.direct.ingest_mappings",
        "recidiviz.ingest.direct.regions.direct_ingest_region_utils",
        "recidiviz.ingest.direct.types.direct_ingest_instance",
        "recidiviz.metrics.export.products",
        "recidiviz.persistence.database",
        "recidiviz.persistence.entity",
        "recidiviz.persistence.entity.state.state_entity_utils",
        "recidiviz.persistence.errors",
        "recidiviz.pipelines.config_paths",
        "recidiviz.pipelines.ingest.dataset_config",
        "recidiviz.pipelines.ingest.pipeline_parameters",
        "recidiviz.pipelines.ingest.pipeline_utils",
        "recidiviz.pipelines.metrics.pipeline_parameters",
        "recidiviz.pipelines.pipeline_names",
        "recidiviz.pipelines.pipeline_parameters",
        "recidiviz.pipelines.supplemental.pipeline_parameters",
        "recidiviz.pipelines.supplemental.dataset_config",
        "recidiviz.utils",
    }

    success &= check_dependencies_for_entrypoint(
        "recidiviz.airflow.dags.calculation_dag",
        valid_module_prefixes=make_module_matcher(valid_calculation_dag_prefixes),
    )

    success &= check_dependencies_for_entrypoint(
        "recidiviz.airflow.tests.calculation_dag_test",
        valid_module_prefixes=make_module_matcher(
            {
                "recidiviz.airflow.tests",
                "recidiviz.tests.ingest.direct.fake_regions",
                "recidiviz.tests.metrics.export.fixtures",
                "recidiviz.tests.pipelines",
                "recidiviz.tests.test_setup_utils",
                "recidiviz.tools.postgres.local_postgres_helpers",
                "recidiviz.tools.utils.script_helpers",
                *valid_calculation_dag_prefixes,
            }
        ),
    )

    valid_raw_data_import_dag_prefixes = {
        "recidiviz.airflow.dags",
        "recidiviz.big_query.address_overrides",
        "recidiviz.big_query.big_query_address",
        "recidiviz.big_query.big_query_job_labels",
        "recidiviz.big_query.big_query_address_formatter",
        "recidiviz.big_query.big_query_client",
        "recidiviz.big_query.big_query_query_builder",
        "recidiviz.big_query.big_query_query_provider",
        "recidiviz.big_query.big_query_utils",
        "recidiviz.big_query.big_query_view",
        "recidiviz.big_query.big_query_view_sandbox_context",
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
        "recidiviz.ingest.direct.raw_data.base_raw_data_import_delegate",
        "recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_load_manager",
        "recidiviz.ingest.direct.raw_data.direct_ingest_raw_table_migration",
        "recidiviz.ingest.direct.raw_data.direct_ingest_raw_table_migration_collector",
        "recidiviz.ingest.direct.raw_data.direct_ingest_raw_table_migration_generator",
        "recidiviz.ingest.direct.raw_data.direct_ingest_raw_table_pre_import_validator",
        "recidiviz.ingest.direct.raw_data.direct_ingest_raw_table_schema_builder",
        "recidiviz.ingest.direct.raw_data.direct_ingest_raw_table_transformation_query_builder",
        "recidiviz.ingest.direct.raw_data.documentation_exemptions",
        "recidiviz.ingest.direct.raw_data.mixins.sequential_chunked_file_mixin",
        "recidiviz.ingest.direct.raw_data.raw_data_import_delegate_factory",
        "recidiviz.ingest.direct.raw_data.raw_file_config_enums",
        "recidiviz.ingest.direct.raw_data.raw_file_config_utils",
        "recidiviz.ingest.direct.raw_data.raw_file_configs",
        "recidiviz.ingest.direct.raw_data.raw_table_relationship_info",
        "recidiviz.ingest.direct.raw_data.datetime_sql_parser_exemptions",
        "recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_header_reader",
        "recidiviz.ingest.direct.raw_data.validations",
        "recidiviz.ingest.direct.regions",  # there is a lot of code here, but is preferable to having to enumerate all region subidrs
        "recidiviz.ingest.direct.types.direct_ingest_constants",
        "recidiviz.ingest.direct.types.direct_ingest_instance",
        "recidiviz.ingest.direct.types.errors",
        "recidiviz.ingest.direct.views.direct_ingest_view_query_builder",
        "recidiviz.ingest.direct.views.direct_ingest_view_query_builder_collector",
        "recidiviz.ingest.direct.types.raw_data_import_blocking_validation",
        "recidiviz.ingest.direct.types.raw_data_import_blocking_validation_type",
        "recidiviz.ingest.direct.types.raw_data_import_types",
        "recidiviz.ingest.direct.views.raw_data_diff_query_builder",
        "recidiviz.ingest.direct.views.raw_table_query_builder",
        "recidiviz.metrics.metric_big_query_view",
        "recidiviz.persistence.database.reserved_words",
        "recidiviz.persistence.database.schema_type",
        "recidiviz.persistence.entity",
        "recidiviz.persistence.errors",
        "recidiviz.utils",
        "recidiviz.utils.environment",
    }

    success &= check_dependencies_for_entrypoint(
        "recidiviz.airflow.dags.raw_data_import_dag",
        valid_module_prefixes=make_module_matcher(valid_raw_data_import_dag_prefixes),
    )

    success &= check_dependencies_for_entrypoint(
        "recidiviz.airflow.tests.raw_data_import_dag_test",
        valid_module_prefixes=make_module_matcher(
            {
                "recidiviz.airflow.tests",
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
                *valid_raw_data_import_dag_prefixes,
            }
        ),
    )

    valid_monitoring_dag_prefixes = {
        "recidiviz.airflow.dags",
        "recidiviz.big_query.address_overrides",
        "recidiviz.big_query.big_query_address",
        "recidiviz.big_query.big_query_address_formatter",
        "recidiviz.big_query.big_query_client",
        "recidiviz.big_query.big_query_job_labels",
        "recidiviz.big_query.big_query_query_builder",
        "recidiviz.big_query.big_query_query_provider",
        "recidiviz.big_query.big_query_utils",
        "recidiviz.big_query.big_query_view",
        "recidiviz.big_query.big_query_view_sandbox_context",
        "recidiviz.big_query.big_query_row_streamer",
        "recidiviz.big_query.constants",
        "recidiviz.big_query.export.export_query_config",
        "recidiviz.big_query.row_access_policy_query_builder",
        "recidiviz.cloud_resources",
        "recidiviz.cloud_storage.gcsfs_path",
        "recidiviz.common.attr_validators",
        "recidiviz.common.attr_utils",
        "recidiviz.common.constants",
        "recidiviz.common.google_cloud_attr_validators",
        "recidiviz.common.google_cloud.utils",
        "recidiviz.common.retry",
        "recidiviz.common.retry_predicate",
        "recidiviz.ingest.direct.types.direct_ingest_instance",
        "recidiviz.metrics.metric_big_query_view",
        "recidiviz.persistence.database.reserved_words",
        "recidiviz.persistence.database.schema_type",
        "recidiviz.utils.environment",
        "recidiviz.utils.types",
        "recidiviz.utils.string",
        "recidiviz.utils.string_formatting",
        "recidiviz.utils.airflow_types",
        "recidiviz.utils.encoding",
        "recidiviz.utils.metadata",
        "recidiviz.utils.size",
        "recidiviz.utils.github",
        "recidiviz.utils.secrets",
    }

    success &= check_dependencies_for_entrypoint(
        "recidiviz.airflow.dags.monitoring_dag",
        valid_module_prefixes=make_module_matcher(valid_monitoring_dag_prefixes),
    )

    success &= check_dependencies_for_entrypoint(
        "recidiviz.airflow.tests.monitoring_dag_test",
        valid_module_prefixes=make_module_matcher(
            {
                "recidiviz.airflow.tests",
                "recidiviz.tests.test_setup_utils",
                "recidiviz.tools.utils.script_helpers",
                "recidiviz.tools.postgres.local_postgres_helpers",
                *valid_monitoring_dag_prefixes,
            }
        ),
    )

    valid_sftp_dag_prefixes = {
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
    success &= check_dependencies_for_entrypoint(
        "recidiviz.airflow.dags.sftp_dag",
        valid_module_prefixes=make_module_matcher(valid_sftp_dag_prefixes),
    )

    success &= check_dependencies_for_entrypoint(
        "recidiviz.airflow.tests.sftp_dag_test",
        valid_module_prefixes=make_module_matcher(
            {
                *valid_sftp_dag_prefixes,
                "recidiviz.airflow.tests",
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

    success &= check_dependencies_for_entrypoint(
        "recidiviz.cloud_functions.main",
        valid_module_prefixes=make_module_matcher({"recidiviz.cloud_functions.main"}),
    )

    success &= check_dependencies_for_entrypoint(
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
                "recidiviz.ingest.direct.gcs.direct_ingest_gcs_file_system",
                "recidiviz.ingest.direct.gcs.directory_path_utils",
                "recidiviz.ingest.direct.types.direct_ingest_instance",
            }
        ),
    )

    success &= check_dependencies_for_entrypoint(
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
                "recidiviz.ingest.direct.regions.us_tx.ingest_views.us_tx_view_query_fragments",
                "recidiviz.ingest.direct.types.direct_ingest_instance",
                "recidiviz.ingest.views.dataset_config",
                "recidiviz.monitoring",
                "recidiviz.observations",
                "recidiviz.outliers",
                "recidiviz.pipelines.ingest.dataset_config",
                "recidiviz.pipelines.supplemental.dataset_config",
                "recidiviz.workflows",
                "recidiviz.persistence",
                "recidiviz.segment.product_type",
                "recidiviz.tools.jii.hydrate_test_data",
                "recidiviz.tools.utils.fixture_helpers",
                "recidiviz.utils",
            }
        ),
    )

    success &= check_dependencies_for_entrypoint(
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

    success &= check_dependencies_for_entrypoint(
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
                "recidiviz.task_eligibility",
                "recidiviz.ingest.views",
                "recidiviz.validation",
                "recidiviz.observations",
                "recidiviz.outliers",
                "recidiviz.workflows",
                "recidiviz.segment.product_type",
            }
        ),
        # TODO(#3828): We won't have to explicitly disallow apache_beam once we've
        #  isolated the Dataflow pipeline code completely
        explicitly_invalid_package_dependencies=["apache_beam"],
    )

    success &= check_dependencies_for_entrypoint(
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
                "recidiviz.firestore",
                "recidiviz.ingest",
                "recidiviz.metrics",
                "recidiviz.monitoring",
                "recidiviz.observations",
                "recidiviz.outliers",
                "recidiviz.persistence",
                "recidiviz.pipelines",
                "recidiviz.reporting",
                "recidiviz.segment.product_type",
                "recidiviz.server_config",
                "recidiviz.source_tables",
                "recidiviz.task_eligibility",
                "recidiviz.utils",
                "recidiviz.validation",
                "recidiviz.workflows",
            }
        ),
    )

    success &= check_dependencies_for_entrypoint(
        "recidiviz.application_data_import.server",
        valid_module_prefixes=make_module_matcher(
            {
                "recidiviz.admin_panel.constants",
                "recidiviz.aggregated_metrics",
                "recidiviz.application_data_import",
                "recidiviz.auth",
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
                "recidiviz.reporting",
                "recidiviz.segment.product_type",
                "recidiviz.task_eligibility",
                "recidiviz.tools.archive",
                "recidiviz.utils",
                "recidiviz.validation",
                "recidiviz.workflows",
            }
        ),
    )

    success &= check_dependencies_for_entrypoint(
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
                "recidiviz.entrypoints",
                "recidiviz.ingest",
                "recidiviz.metrics",
                "recidiviz.monitoring",
                "recidiviz.observations",
                "recidiviz.outcome_metrics",
                "recidiviz.outliers",
                "recidiviz.persistence",
                "recidiviz.pipelines",
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

    success &= check_dependencies_for_entrypoint(
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
                "recidiviz.pipelines.ingest.dataset_config",
                "recidiviz.pipelines.supplemental.dataset_config",
                "recidiviz.source_tables.externally_managed.datasets",
                "recidiviz.source_tables.yaml_managed.datasets",
                "recidiviz.validation.views.dataset_config",
                "recidiviz.view_registry",
                # view code
                "recidiviz.aggregated_metrics",
                "recidiviz.calculator.query",
                "recidiviz.datasets.static_data.views",
                "recidiviz.ingest.views",
                "recidiviz.ingest.direct",
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
                "recidiviz.source_tables.collect_source_tables_from_yamls",
                "recidiviz.source_tables.externally_managed.collect_externally_managed_source_table_configs",
                "recidiviz.source_tables.source_table_config",
                "recidiviz.source_tables.source_table_repository",
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

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())

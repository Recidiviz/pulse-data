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
"""Script for validating that a sandbox workflows and outliers dataset is equivalent to a reference
dataset, even if they have different root entity id schemes.

Usage:
    python -m recidiviz.tools.ingest.testing.validate_workflows_outliers \
        --output_sandbox_prefix PREFIX \
        --state_code_filter STATE_CODE
        --sandbox_prefix_to_validate SANDBOX_PREFIX \
        [--sandbox_project_id PROJECT_ID] \
        [--reference_project_id PROJECT_ID]


    python -m recidiviz.tools.ingest.testing.validate_workflows_outliers \
        --output_sandbox_prefix my_prefix \
        --state_code_filter US_CA \
        --sandbox_prefix_to_validate prefix_validating_against
"""

import argparse
import re
from collections import defaultdict
from typing import Dict, List, NamedTuple, Optional, Tuple

import attr
import pandas as pd

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_address import (
    BigQueryAddress,
    ProjectSpecificBigQueryAddress,
)
from recidiviz.big_query.big_query_client import BigQueryClient, BigQueryClientImpl
from recidiviz.calculator.query.state.views.outliers.metric_benchmarks import (
    METRIC_BENCHMARKS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.outliers.outliers_enabled_states import (
    get_outliers_enabled_states,
)
from recidiviz.calculator.query.state.views.outliers.supervision_officer_outlier_status import (
    SUPERVISION_OFFICER_OUTLIER_STATUS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.outliers.supervision_officer_supervisors import (
    SUPERVISION_OFFICER_SUPERVISORS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.firestore_views import (
    FIRESTORE_VIEW_BUILDERS,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.views.dataset_config import STATE_BASE_DATASET
from recidiviz.task_eligibility.single_task_eligibility_spans_view_collector import (
    SingleTaskEligibilityBigQueryViewCollector,
)
from recidiviz.tools.calculator.create_or_update_dataflow_sandbox import (
    TEMP_DATAFLOW_DATASET_DEFAULT_TABLE_EXPIRATION_MS,
)
from recidiviz.tools.ingest.testing.validate_state_dataset import (
    ONE_TO_ONE_LINKS_TEMPLATE,
)
from recidiviz.tools.utils.bigquery_helpers import run_operation_for_given_tables
from recidiviz.tools.utils.compare_tables_helper import (
    STATS_NEW_ERROR_RATE_COL,
    STATS_ORIGINAL_ERROR_RATE_COL,
    CompareTablesResult,
    compare_table_or_view,
)
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.string import StrictStringFormatter

PRIMARY_KEY_OVERRIDES: dict[BigQueryAddress, str] = {}

ValidationAddress = NamedTuple(
    "ValidationAddress",
    [
        ("state_code", StateCode),
        ("address", BigQueryAddress),
        ("entity_primary_key", str),
        ("excluded_keys", List[str]),
        ("additional_primary_keys", List[str]),
    ],
)


def _get_workflow_addresses() -> List[ValidationAddress]:
    """Returns a list of workflow table ids for that state.

    The tuple contains the state code, the BigQueryAddress, the key used to identify person
    (usually external_id or person_id), and the list of columns to exclude in the comparison
    (like foreign keys and primary keys).
    """
    workflow_addresses: List[ValidationAddress] = []
    firestore_workflows_addresses = [
        vb.address
        for vb in FIRESTORE_VIEW_BUILDERS
        if re.match(r"^(us_[a-z]{2}).*_record$", vb.address.table_id)
    ]
    for address in firestore_workflows_addresses:
        match = re.match(r"^(us_[a-z]{2}).*", address.table_id)
        if not match:
            raise ValueError(
                f"Could not parse state code from table id {address.table_id}"
            )

        state_code = StateCode(match.group(1).upper())
        primary_key = PRIMARY_KEY_OVERRIDES.get(address, "external_id")
        workflow_addresses.append(
            ValidationAddress(state_code, address, primary_key, [], [])
        )
    return workflow_addresses


def _get_single_eligibility_task_span_addresses() -> List[ValidationAddress]:
    """Returns the list of eligibility_task_span tables for any state.

    The tuple contains the state code, the BigQueryAddress, the key used to identify person
    (usually external_id or person_id), and the list of columns to exclude in the comparison
    (like foreign keys and primary keys).
    """
    eligibility_task_span_addresses: List[ValidationAddress] = []
    eligibility_view_by_state = (
        SingleTaskEligibilityBigQueryViewCollector().collect_view_builders_by_state()
    )
    for state_code, eligibility_view_builders in eligibility_view_by_state.items():
        eligibility_task_span_addresses.extend(
            [
                ValidationAddress(
                    state_code,
                    vb.address,
                    "person_id",
                    ["task_eligibility_span_id"],
                    ["start_date", "end_date"],
                )
                for vb in eligibility_view_builders
            ]
        )

    return eligibility_task_span_addresses


def _get_outliers_addresses() -> List[ValidationAddress]:
    """Returns the list of outliers tables for any state.

    The tuple contains the state code, the BigQueryAddress, the key used to identify officer
    (usually officer_id), and the list of columns to exclude in the comparison (like foreign
    keys and primary keys).
    """
    outliers_addresses: List[ValidationAddress] = []
    for state_code in get_outliers_enabled_states():
        outliers_addresses.extend(
            [
                ValidationAddress(
                    StateCode(state_code),
                    SUPERVISION_OFFICER_OUTLIER_STATUS_VIEW_BUILDER.address,
                    "officer_id",
                    [],
                    ["metric_id", "caseload_type", "end_date", "state_code"],
                ),
                ValidationAddress(
                    StateCode(state_code),
                    SUPERVISION_OFFICER_SUPERVISORS_VIEW_BUILDER.address,
                    "staff_id",
                    [],
                    [],
                ),
                ValidationAddress(
                    StateCode(state_code),
                    METRIC_BENCHMARKS_VIEW_BUILDER.address,
                    "metric_id",
                    [],
                    ["state_code", "end_date"],
                ),
            ]
        )
    return outliers_addresses


def _get_address_by_state() -> (
    Dict[StateCode, Dict[BigQueryAddress, Tuple[str, List[str], List[str]]]]
):
    """Returns a dictionary of addresses and their primary keys for a different state codes.

    The dictionary is keyed by state code, and the value is a dictionary of BigQueryAddress
    to a tuple of the key used to identify person (usually external_id or person_id)
    and a list of columns to exclude in the comparison (like foreign keys and primary keys).
    """
    address_by_state: Dict[
        StateCode, Dict[BigQueryAddress, Tuple[str, List[str], List[str]]]
    ] = defaultdict(dict)
    for address in (
        _get_workflow_addresses()
        + _get_single_eligibility_task_span_addresses()
        + _get_outliers_addresses()
    ):
        address_by_state[address.state_code][address.address] = (
            address.entity_primary_key,
            address.excluded_keys,
            address.additional_primary_keys,
        )
    return address_by_state


ADDRESS_BY_STATE = _get_address_by_state()


class WorkflowsOutliersDatasetValidator:
    """Helper class for validating workflows and outliers dataset against a reference
    dataset for a given state.
    """

    def __init__(
        self,
        reference_project_id: str,
        sandbox_prefix_to_validate: str,
        sandbox_project_id: str,
        output_sandbox_prefix: str,
        state_code_filter: StateCode,
    ) -> None:
        self.state_code_filter = state_code_filter

        self.reference_project_id = reference_project_id

        self.sandbox_prefix_to_validate = sandbox_prefix_to_validate
        self.sandbox_project_id = sandbox_project_id

        self.output_project_id = sandbox_project_id
        self.output_dataset_id = BigQueryAddressOverrides.format_sandbox_dataset(
            prefix=output_sandbox_prefix,
            dataset_id=f"workflows_outliers_validation_results_{state_code_filter.value.lower()}",
        )
        self.bq_client = BigQueryClientImpl(project_id=self.output_project_id)

    def build_id_mapping(self, entity_name: str) -> None:
        mappings_address = ProjectSpecificBigQueryAddress(
            dataset_id=self.output_dataset_id,
            table_id=f"{entity_name}_id_mappings",
            project_id=self.output_project_id,
        )

        query = StrictStringFormatter().format(
            ONE_TO_ONE_LINKS_TEMPLATE,
            root_entity_id_col=f"{entity_name}_id",
            reference_root_entity_external_id_table=ProjectSpecificBigQueryAddress(
                dataset_id=STATE_BASE_DATASET,
                table_id=f"state_{entity_name}_external_id",
                project_id=self.reference_project_id,
            ).format_address_for_query(),
            sandbox_root_entity_external_id_table=ProjectSpecificBigQueryAddress(
                dataset_id=BigQueryAddressOverrides.format_sandbox_dataset(
                    prefix=self.sandbox_prefix_to_validate,
                    dataset_id=STATE_BASE_DATASET,
                ),
                table_id=f"state_{entity_name}_external_id",
                project_id=self.sandbox_project_id,
            ).format_address_for_query(),
            state_code_filter=self.state_code_filter.value,
        )
        self.bq_client.create_table_from_query(
            address=mappings_address.to_project_agnostic_address(),
            query=query,
            use_query_cache=False,
            overwrite=True,
        )

    def _filtered_table_rows_clause_for_table(
        self,
        table_address: ProjectSpecificBigQueryAddress,
        dataset_name: str,
        entity_name: str,
    ) -> str:
        """Returns a formatted query clause that filters the table in |table_address|
        to only include rows that have a person_id that exists in the person_id_mappings
        or a staff_id that exists in the staff_id_mappings.
        """

        return f"""
  SELECT 
    * EXCEPT(
     sandbox_{entity_name}_id,
     reference_{entity_name}_id
    ), 
    reference_{entity_name}_id AS comparable_{entity_name}_id
  FROM {table_address.format_address_for_query()} t
  JOIN `{self.output_project_id}.{self.output_dataset_id}.{entity_name}_id_mappings`
  ON t.{entity_name}_id = {dataset_name}_{entity_name}_id
"""

    def _build_comparable_entity_rows_query(
        self,
        table_address: ProjectSpecificBigQueryAddress,
        dataset_name: str,
        entity_name: str,
        original_reference_table_address: Optional[BigQueryAddress] = None,
    ) -> str:
        """Returns a formatted query that queries the provided state entity in the
        provided |dataset|, which can be used to compare against the same table in
        a different dataset.
        """
        entity_primary_key, excluded_keys, _ = ADDRESS_BY_STATE[self.state_code_filter][
            (
                table_address.to_project_agnostic_address()
                if not original_reference_table_address
                else original_reference_table_address
            )
        ]

        filtered_with_new_cols = self._filtered_table_rows_clause_for_table(
            table_address=table_address,
            dataset_name=dataset_name,
            entity_name=entity_name,
        )

        columns_to_exclude = [
            entity_primary_key,
            *excluded_keys,
        ]
        columns_to_exclude_str = ",".join(columns_to_exclude)
        return f"""SELECT * EXCEPT ({columns_to_exclude_str})
    FROM ({filtered_with_new_cols})"""

    def _materialize_comparable_table(
        self,
        table_address: ProjectSpecificBigQueryAddress,
        dataset_name: str,
        entity_name: str,
        original_reference_table_address: Optional[BigQueryAddress] = None,
    ) -> ProjectSpecificBigQueryAddress:
        """For the given state dataset table, generates a query that produces results
        for that table that can be compared against results from another dataset, then
        materializes the results of that query and returns the results address.
        """
        comparable_rows_query = self._build_comparable_entity_rows_query(
            table_address=table_address,
            dataset_name=dataset_name,
            entity_name=entity_name,
            original_reference_table_address=original_reference_table_address,
        )

        comparable_table_address = ProjectSpecificBigQueryAddress(
            project_id=self.bq_client.project_id,
            dataset_id=self.output_dataset_id,
            table_id=f"{table_address.table_id}_{dataset_name}_comparable",
        )

        self.bq_client.create_table_from_query(
            address=comparable_table_address.to_project_agnostic_address(),
            query=comparable_rows_query,
            use_query_cache=False,
            overwrite=True,
        )

        return comparable_table_address

    def _generate_table_specific_comparison_result(
        self,
        client: BigQueryClient,  # pylint: disable=unused-argument
        reference_table_address: ProjectSpecificBigQueryAddress,
    ) -> CompareTablesResult:
        """For a given table in the workflows or outliers dataset, runs a set of
        validation queries that will get materialized to tables in the output dataset,
        then returns a result with some stats about the comparison.

        Returns None if the table is not supported for comparison.
        """
        bigquery_address = BigQueryAddress(
            dataset_id=reference_table_address.dataset_id,
            table_id=reference_table_address.table_id,
        )

        entity_primary_key, _, additional_primary_keys = ADDRESS_BY_STATE[
            self.state_code_filter
        ][bigquery_address]
        sandbox_dataset_id = BigQueryAddressOverrides.format_sandbox_dataset(
            prefix=self.sandbox_prefix_to_validate,
            dataset_id=reference_table_address.dataset_id,
        )

        sandbox_table_address = ProjectSpecificBigQueryAddress(
            project_id=self.sandbox_project_id,
            dataset_id=sandbox_dataset_id,
            table_id=reference_table_address.table_id,
        )

        if entity_primary_key in ("person_id", "staff_id"):
            entity_name = entity_primary_key.strip("_id")
            reference_comparable_address = self._materialize_comparable_table(
                table_address=reference_table_address,
                dataset_name="reference",
                entity_name=entity_name,
            )
            sandbox_comparable_address = self._materialize_comparable_table(
                table_address=sandbox_table_address,
                dataset_name="sandbox",
                entity_name=entity_name,
                original_reference_table_address=bigquery_address,
            )
            entity_primary_key = f"comparable_{entity_name}_id"
        else:
            reference_comparable_address = reference_table_address
            sandbox_comparable_address = sandbox_table_address

        return compare_table_or_view(
            address_original=reference_comparable_address,
            address_new=sandbox_comparable_address,
            comparison_output_dataset_id=self.output_dataset_id,
            primary_keys=[entity_primary_key] + additional_primary_keys,
            grouping_columns=None,
        )

    def run_validation(self) -> None:
        """Runs the full suite of workflows and outliers dataset validation jobs."""
        self.bq_client.create_dataset_if_necessary(
            self.output_dataset_id,
            default_table_expiration_ms=TEMP_DATAFLOW_DATASET_DEFAULT_TABLE_EXPIRATION_MS,
        )

        print("Building StatePerson mapping...")
        self.build_id_mapping(entity_name="person")
        print("Building StateStaff mapping...")
        self.build_id_mapping(entity_name="staff")

        print("Outputting table-specific validation results...")
        results = run_operation_for_given_tables(
            client=BigQueryClientImpl(project_id=self.output_project_id),
            operation=self._generate_table_specific_comparison_result,
            tables=self._get_tables_to_validate(),
        )

        table_to_comparison_result = {
            table.table_id: result for table, result in results.items()
        }

        validation_result = ValidationResult(
            table_to_comparison_result=table_to_comparison_result,
        )

        validation_result.print()

        print(f"Results can be found in dataset `{self.output_dataset_id}`.")

    def _get_tables_to_validate(self) -> List[ProjectSpecificBigQueryAddress]:
        """Returns the list of tables to validate."""
        tables = []
        for address in ADDRESS_BY_STATE[self.state_code_filter]:
            tables.append(
                ProjectSpecificBigQueryAddress(
                    project_id=self.reference_project_id,
                    dataset_id=address.dataset_id,
                    table_id=address.table_id,
                )
            )
        return tables


@attr.define(kw_only=True)
class ValidationResult:
    """Stores the result of the workflows and outliers tables validation."""

    table_to_comparison_result: Dict[str, CompareTablesResult]

    @property
    def full_results_df(self) -> pd.DataFrame:
        non_zero_rows_dfs = []
        for table, result in self.table_to_comparison_result.items():
            if result.count_original == 0 and result.count_new == 0:
                # Filter out rows for entities with no data in either original or new
                continue

            labeled_df = result.comparison_stats_df.copy()
            labeled_df.insert(0, "table_id", table)
            non_zero_rows_dfs.append(labeled_df)

        return pd.concat(non_zero_rows_dfs, axis=0, ignore_index=True)

    def print(self) -> None:
        """Prints the validation results."""
        print("Table by table comparison results:")
        print(
            self.full_results_df.sort_values(by="table_id")
            .sort_values(by=STATS_NEW_ERROR_RATE_COL, ascending=False)
            .sort_values(by=STATS_ORIGINAL_ERROR_RATE_COL, ascending=False)
            .to_string(index=False)
        )


def parse_arguments() -> argparse.Namespace:
    """Parses the arguments needed to call the desired function."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--reference_project_id",
        type=str,
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        default=GCP_PROJECT_STAGING,
        help="The project the reference tables live in.",
    )
    parser.add_argument(
        "--sandbox_prefix_to_validate",
        type=str,
        required=True,
        help="The dataset prefix for the dataset to validate.",
    )
    parser.add_argument(
        "--sandbox_project_id",
        type=str,
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        default=GCP_PROJECT_STAGING,
        help="The project the sandbox tables live in.",
    )
    parser.add_argument(
        "--state_code_filter",
        type=StateCode,
        choices=list(StateCode),
        required=True,
        help="The state code to compare data for.",
    )
    parser.add_argument(
        "--output_sandbox_prefix",
        type=str,
        required=True,
        help="The dataset prefix for the dataset to output results to.",
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()

    WorkflowsOutliersDatasetValidator(
        reference_project_id=args.reference_project_id,
        sandbox_prefix_to_validate=args.sandbox_prefix_to_validate,
        sandbox_project_id=args.sandbox_project_id,
        output_sandbox_prefix=args.output_sandbox_prefix,
        state_code_filter=args.state_code_filter,
    ).run_validation()

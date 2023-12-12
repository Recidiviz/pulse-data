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
from typing import Dict, List

import attr
import pandas as pd
from google.cloud.bigquery import DatasetReference

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_address import (
    BigQueryAddress,
    ProjectSpecificBigQueryAddress,
)
from recidiviz.big_query.big_query_client import BigQueryClient, BigQueryClientImpl
from recidiviz.calculator.query.state.dataset_config import WORKFLOWS_VIEWS_DATASET
from recidiviz.calculator.query.state.views.workflows.firestore.firestore_views import (
    FIRESTORE_VIEW_BUILDERS,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.entity_utils import CoreEntityFieldIndex
from recidiviz.tools.calculator.create_or_update_dataflow_sandbox import (
    TEMP_DATAFLOW_DATASET_DEFAULT_TABLE_EXPIRATION_MS,
)
from recidiviz.tools.utils.bigquery_helpers import run_operation_for_given_tables
from recidiviz.tools.utils.compare_tables_helper import (
    STATS_NEW_ERROR_RATE_COL,
    STATS_ORIGINAL_ERROR_RATE_COL,
    CompareTablesResult,
    compare_table_or_view,
)
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING

PRIMARY_KEY_OVERRIDES = {
    BigQueryAddress(
        dataset_id=WORKFLOWS_VIEWS_DATASET, table_id="us_ix_supervision_tasks_record"
    ): "person_external_id",
}


def _get_workflow_address_by_state() -> Dict[StateCode, Dict[BigQueryAddress, str]]:
    """Returns a dictionary of state code to list of workflow table ids for that state."""
    workflow_address_by_state: Dict[StateCode, Dict[BigQueryAddress, str]] = {}
    workflows_addresses = [
        vb.address
        for vb in FIRESTORE_VIEW_BUILDERS
        if re.match(r"^(us_[a-z]{2}).*_record$", vb.address.table_id)
    ]
    for address in workflows_addresses:
        match = re.match(r"^(us_[a-z]{2}).*", address.table_id)
        if not match:
            raise ValueError(
                f"Could not parse state code from table id {address.table_id}"
            )

        state_code = StateCode(match.group(1).upper())
        if state_code not in workflow_address_by_state:
            workflow_address_by_state[state_code] = {}

        workflow_address_by_state[state_code][address] = PRIMARY_KEY_OVERRIDES.get(
            address, "external_id"
        )
    return workflow_address_by_state


ADDRESS_BY_STATE = _get_workflow_address_by_state()


def _primary_keys_for_differences_output(address: BigQueryAddress) -> List[str]:
    """Returns the list of columns that should not be nulled out for convenience
    (when the values are the same) in the differences output for this table.
    """
    return [PRIMARY_KEY_OVERRIDES.get(address, "external_id")]


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
        self.field_index = CoreEntityFieldIndex()
        self.state_code_filter = state_code_filter

        self.reference_project_id = reference_project_id

        self.sandbox_prefix_to_validate = sandbox_prefix_to_validate
        self.sandbox_project_id = sandbox_project_id

        self.output_project_id = GCP_PROJECT_STAGING
        self.output_dataset_id = BigQueryAddressOverrides.format_sandbox_dataset(
            prefix=output_sandbox_prefix,
            dataset_id=f"workflows_outliers_validation_results_{state_code_filter.value.lower()}",
        )
        self.bq_client = BigQueryClientImpl(project_id=self.output_project_id)

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
        table_id = reference_table_address.table_id

        reference_dataset = DatasetReference.from_string(
            reference_table_address.dataset_id,
            default_project=reference_table_address.project_id,
        )

        sandbox_dataset = DatasetReference.from_string(
            BigQueryAddressOverrides.format_sandbox_dataset(
                prefix=self.sandbox_prefix_to_validate,
                dataset_id=reference_dataset.dataset_id,
            ),
            default_project=self.sandbox_project_id,
        )

        return compare_table_or_view(
            address_original=ProjectSpecificBigQueryAddress(
                dataset_id=reference_dataset.dataset_id,
                table_id=table_id,
                project_id=reference_dataset.project,
            ),
            address_new=ProjectSpecificBigQueryAddress(
                dataset_id=sandbox_dataset.dataset_id,
                table_id=table_id,
                project_id=sandbox_dataset.project,
            ),
            comparison_output_dataset_id=self.output_dataset_id,
            primary_keys=_primary_keys_for_differences_output(
                BigQueryAddress(
                    dataset_id=reference_dataset.dataset_id, table_id=table_id
                )
            ),
            grouping_columns=None,
        )

    def run_validation(self) -> None:
        """Runs the full suite of workflows and outliers dataset validation jobs."""
        self.bq_client.create_dataset_if_necessary(
            self.bq_client.dataset_ref_for_id(self.output_dataset_id),
            default_table_expiration_ms=TEMP_DATAFLOW_DATASET_DEFAULT_TABLE_EXPIRATION_MS,
        )

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

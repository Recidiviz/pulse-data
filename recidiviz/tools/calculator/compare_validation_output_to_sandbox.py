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
"""Script to compare two different runs for validation results, the latest run
for a sandbox prefix against the latest standard run.

python -m recidiviz.tools.calculator.compare_validation_output_to_sandbox \
    --project_id recidiviz-staging \
    --state_code US_CA \
    --ingest_instance PRIMARY \
    --sandbox_prefix emily

"""
import argparse
from collections import defaultdict
from enum import Enum
from typing import Dict, Optional, Tuple

from more_itertools import one

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.string import StrictStringFormatter
from recidiviz.utils.types import non_optional
from recidiviz.validation.validation_models import ValidationCheckType
from recidiviz.validation.validation_result_for_storage import (
    ValidationResultForStorage,
    ValidationResultStatus,
)

REFERENCE_RUN_ID_QUERY_TEMPLATE = """
SELECT run_id FROM `{project_id}.validation_results.validation_results`
WHERE sandbox_dataset_prefix IS NULL AND region_code = '{state_code}' 
AND ingest_instance = '{ingest_instance}' QUALIFY ROW_NUMBER() OVER (ORDER BY run_datetime DESC) = 1;
"""

SANDBOX_RUN_ID_QUERY_TEMPLATE = """
SELECT run_id FROM `{project_id}.validation_results.validation_results`
WHERE sandbox_dataset_prefix = '{sandbox_prefix}' AND region_code = '{state_code}'
AND ingest_instance = '{ingest_instance}' QUALIFY ROW_NUMBER() OVER (ORDER BY run_datetime DESC) = 1;
"""

QUERY_FOR_VALIDATION_RESULTS = """
SELECT * FROM `{project_id}.validation_results.validation_results` WHERE run_id = '{run_id}';"""


class ComparisonResultType(Enum):
    IMPROVED_IN_SANDBOX = "IMPROVED_IN_SANDBOX"
    REGRESSED_IN_SANDBOX = "REGRESSED_IN_SANDBOX"
    IMPROVED_IN_SANDBOX_NO_STATUS_CHANGE = "IMPROVED_IN_SANDBOX_NO_STATUS_CHANGE"
    REGRESSED_IN_SANDBOX_NO_STATUS_CHANGE = "REGRESSED_IN_SANDBOX_NO_STATUS_CHANGE"
    NO_STATUS_CHANGE = "NO_CHANGE"


class ValidationDatasetValidator:
    """A class to compare two different run_ids for validation results"""

    def __init__(
        self,
        project_id: str,
        state_code: StateCode,
        ingest_instance: DirectIngestInstance,
        sandbox_prefix: str,
    ):
        self.project_id = project_id
        self.state_code = state_code
        self.ingest_instance = ingest_instance
        self.sandbox_prefix = sandbox_prefix

        self.bq_client = BigQueryClientImpl(project_id)

        reference_query_job = self.bq_client.run_query_async(
            query_str=StrictStringFormatter().format(
                REFERENCE_RUN_ID_QUERY_TEMPLATE,
                project_id=self.project_id,
                state_code=self.state_code.value,
                ingest_instance=self.ingest_instance.value,
            ),
            use_query_cache=False,
        )
        self.reference_run_id = one(
            [row["run_id"] for row in reference_query_job.result()]
        )
        sandbox_query_job = self.bq_client.run_query_async(
            query_str=StrictStringFormatter().format(
                SANDBOX_RUN_ID_QUERY_TEMPLATE,
                project_id=self.project_id,
                state_code=self.state_code.value,
                ingest_instance=self.ingest_instance.value,
                sandbox_prefix=self.sandbox_prefix,
            ),
            use_query_cache=False,
        )
        self.sandbox_run_id = one([row["run_id"] for row in sandbox_query_job.result()])

    def read_rows_from_bq(
        self, run_id: str
    ) -> Dict[Tuple[str, ValidationCheckType], ValidationResultForStorage]:
        query_job = self.bq_client.run_query_async(
            query_str=StrictStringFormatter().format(
                QUERY_FOR_VALIDATION_RESULTS,
                project_id=self.project_id,
                run_id=run_id,
            ),
            use_query_cache=False,
        )
        result = {}
        for row in query_job:
            storage = ValidationResultForStorage.from_serializable(row)
            result[(storage.validation_name, storage.check_type)] = storage
        return result

    def compare_reference_to_sandbox(
        self, reference: ValidationResultForStorage, sandbox: ValidationResultForStorage
    ) -> Tuple[ComparisonResultType, Optional[str]]:
        """Compares the reference results to the sandbox results and returns the comparison type and error message."""
        comparison_type = ComparisonResultType.NO_STATUS_CHANGE
        error_message = None

        if (
            not reference.validation_result_status
            or not sandbox.validation_result_status
        ):
            raise ValueError(
                f"Validation result status is missing for reference or sandbox: {reference.validation_name}"
            )
        if not reference.result_details or not sandbox.result_details:
            raise ValueError(
                f"Validation result details is missing for reference or sandbox: {reference.validation_name}"
            )

        if reference.validation_result_status != sandbox.validation_result_status:
            if (
                reference.validation_result_status == ValidationResultStatus.SUCCESS
                or (
                    reference.validation_result_status
                    == ValidationResultStatus.FAIL_SOFT
                    and sandbox.validation_result_status
                    == ValidationResultStatus.FAIL_HARD
                )
            ):
                comparison_type = ComparisonResultType.REGRESSED_IN_SANDBOX
            elif sandbox.validation_result_status == ValidationResultStatus.SUCCESS or (
                sandbox.validation_result_status == ValidationResultStatus.FAIL_SOFT
                and reference.validation_result_status
                == ValidationResultStatus.FAIL_HARD
            ):
                comparison_type = ComparisonResultType.IMPROVED_IN_SANDBOX

        if comparison_type == ComparisonResultType.NO_STATUS_CHANGE:
            if sandbox.result_details.is_better(reference.result_details):
                comparison_type = (
                    ComparisonResultType.IMPROVED_IN_SANDBOX_NO_STATUS_CHANGE
                )
            elif reference.result_details.is_better(sandbox.result_details):
                comparison_type = (
                    ComparisonResultType.REGRESSED_IN_SANDBOX_NO_STATUS_CHANGE
                )
            else:
                comparison_type = ComparisonResultType.NO_STATUS_CHANGE

        if comparison_type != ComparisonResultType.NO_STATUS_CHANGE:
            error_message = f"Sandbox: {sandbox.result_details.failure_description()}\nReference: {reference.result_details.failure_description()}"

        return comparison_type, error_message

    def run_validation(self) -> None:
        """This runs validations on reference and sandbox validation runs."""
        print(
            f"Comparing reference run {self.reference_run_id} to sandbox run {self.sandbox_run_id} for {self.state_code.value}"
        )
        reference_results = self.read_rows_from_bq(self.reference_run_id)
        sandbox_results = self.read_rows_from_bq(self.sandbox_run_id)

        comparison_info_by_key: Dict[
            ComparisonResultType, Dict[Tuple[str, ValidationCheckType], Optional[str]]
        ] = defaultdict(dict)

        for key in set(reference_results.keys()).intersection(sandbox_results.keys()):
            result_type, error_str_opt = self.compare_reference_to_sandbox(
                reference_results[key], sandbox_results[key]
            )
            comparison_info_by_key[result_type][key] = error_str_opt

        no_change_results = comparison_info_by_key[
            ComparisonResultType.NO_STATUS_CHANGE
        ]
        print(f"\n✅ Found {len(no_change_results)} with no change")

        improved_results = comparison_info_by_key[
            ComparisonResultType.IMPROVED_IN_SANDBOX
        ]
        print(f"\n✅ Found {len(improved_results)} with improved results:")
        self.print_validation_comparisons(improved_results)

        improved_no_status_change_results = comparison_info_by_key[
            ComparisonResultType.IMPROVED_IN_SANDBOX_NO_STATUS_CHANGE
        ]
        print(
            f"\n✅ Found {len(improved_no_status_change_results)} with improved results but no status change:"
        )
        self.print_validation_comparisons(improved_no_status_change_results)

        regressed_results = comparison_info_by_key[
            ComparisonResultType.REGRESSED_IN_SANDBOX
        ]
        print(f"\n❌ Found {len(regressed_results)} with regressed results:")
        self.print_validation_comparisons(regressed_results)

        regressed_no_status_change_results = comparison_info_by_key[
            ComparisonResultType.REGRESSED_IN_SANDBOX_NO_STATUS_CHANGE
        ]
        print(
            f"\n⚠️ Found {len(regressed_no_status_change_results)} with regressed results but no status change:"
        )
        self.print_validation_comparisons(regressed_no_status_change_results)

        for validation_name, check_type in set(sandbox_results.keys()).difference(
            set(reference_results.keys())
        ):
            print(
                f"\nFound new validation in sandbox: {validation_name}, {check_type.value}"
            )
            print(
                f"status: {sandbox_results[(validation_name, check_type)].validation_result_status}"
            )
            print(
                f"{non_optional(sandbox_results[(validation_name, check_type)].result_details).failure_description()}"
            )

    def print_validation_comparisons(
        self, comparison_info: Dict[Tuple[str, ValidationCheckType], Optional[str]]
    ) -> None:
        for (validation_name, check_type), error_str in comparison_info.items():
            print(f"\n{validation_name}, {check_type.value}")
            if error_str:
                print(error_str)


def parse_arguments() -> argparse.Namespace:
    """Parses the arguments needed to call the desired function."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--project_id",
        dest="project_id",
        type=str,
        choices=[GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING],
        default=GCP_PROJECT_STAGING,
        help="The project the validation runs are in.",
    )
    parser.add_argument(
        "--state_code",
        dest="state_code",
        type=StateCode,
        choices=list(StateCode),
        required=True,
        help="The state code of the validation runs.",
    )
    parser.add_argument(
        "--ingest_instance",
        dest="ingest_instance",
        type=DirectIngestInstance,
        choices=list(DirectIngestInstance),
        required=True,
        help="The ingest instance of the validation runs.",
    )
    parser.add_argument(
        "--sandbox_prefix",
        dest="sandbox_prefix",
        type=str,
        required=True,
        help="The sandbox dataset prefix of the validation run to check.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()

    ValidationDatasetValidator(
        args.project_id,
        args.state_code,
        args.ingest_instance,
        args.sandbox_prefix,
    ).run_validation()

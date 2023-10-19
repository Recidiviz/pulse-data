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
"""Script for validating that a sandbox `state` dataset is equivalent to a reference
state dataset, even if they have different root entity id schemes.

Usage:
    python -m recidiviz.tools.ingest.testing.validate_state_dataset \
        --output_sandbox_prefix PREFIX \
        --state_code_filter STATE_CODE
        --sandbox_state_dataset DATASET_ID \
        [--reference_state_dataset DATASET_ID] \
        [--sandbox_state_project_id PROJECT_ID] \
        [--reference_state_project_id PROJECT_ID]


    python -m recidiviz.tools.ingest.testing.validate_state_dataset \
        --output_sandbox_prefix my_prefix \
        --state_code_filter US_CA \
        --sandbox_state_dataset another_prefix_us_ca_state_primary

"""
import argparse
from typing import Dict, List, Optional, Type, cast

import attr
import pandas as pd
from google.cloud.bigquery import DatasetReference, WriteDisposition
from more_itertools import one

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_address import ProjectSpecificBigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClient, BigQueryClientImpl
from recidiviz.calculator.query.state.dataset_config import STATE_BASE_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.database_entity import DatabaseEntity
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.database.schema_utils import (
    get_database_entity_by_table_name,
    is_association_table,
)
from recidiviz.persistence.entity.base_entity import Entity, EnumEntity, RootEntity
from recidiviz.persistence.entity.entity_utils import (
    get_entity_class_in_module_with_name,
)
from recidiviz.persistence.entity.root_entity_utils import (
    get_root_entity_class_for_entity,
)
from recidiviz.persistence.entity.state import entities
from recidiviz.tools.calculator.compare_views import compare_table_or_view
from recidiviz.tools.calculator.create_or_update_dataflow_sandbox import (
    TEMP_DATAFLOW_DATASET_DEFAULT_TABLE_EXPIRATION_MS,
)
from recidiviz.tools.utils.bigquery_helpers import (
    dataset_id_to_filter_regex,
    run_operation_for_tables,
)
from recidiviz.tools.utils.compare_tables_helper import (
    STATS_NEW_ERROR_RATE_COL,
    STATS_ORIGINAL_ERROR_RATE_COL,
    CompareTablesResult,
)
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.string import StrictStringFormatter

REFERENCE_DATASET_NAME = "reference"
SANDBOX_DATASET_NAME = "sandbox"

ROOT_ENTITY_ID_LINKS_CLAUSE_TEMPLATE = """
links AS (
  SELECT 
    state_code,
    external_id,
    id_type,
    reference.{root_entity_id_col} AS reference_{root_entity_id_col},
    sandbox.{root_entity_id_col} AS sandbox_{root_entity_id_col}
  FROM {reference_root_entity_external_id_table} reference
  FULL OUTER JOIN
  {sandbox_root_entity_external_id_table} sandbox
  USING (state_code, external_id, id_type)
  WHERE state_code = "{state_code_filter}"
)"""

ONE_TO_ONE_LINKS_TEMPLATE = f"""
WITH {ROOT_ENTITY_ID_LINKS_CLAUSE_TEMPLATE}
SELECT DISTINCT reference_{{root_entity_id_col}}, sandbox_{{root_entity_id_col}}
FROM links
WHERE sandbox_{{root_entity_id_col}} IS NOT NULL AND reference_{{root_entity_id_col}} IS NOT NULL
QUALIFY
  -- Pick sandbox root entity PKs with exactly one associated reference PK
  COUNT(DISTINCT reference_{{root_entity_id_col}}) OVER (PARTITION BY sandbox_{{root_entity_id_col}}) = 1 AND
  -- Pick reference root entity PKs with exactly one associated sandbox root entity PKs
  COUNT(DISTINCT sandbox_{{root_entity_id_col}}) OVER (PARTITION BY reference_{{root_entity_id_col}}) = 1
"""

MULTIPLE_DATASET_1_PEOPLE_FOR_DATASET_2_PERSON_TEMPLATE = """
WITH {root_entity_id_links_clause}
SELECT 
  state_code, {dataset_2_name}_{{root_entity_id_col}},
  ARRAY_AGG(
    DISTINCT {dataset_1_name}_{{root_entity_id_col}}
  ) AS associated_{dataset_1_name}_{{root_entity_id_col}}s
FROM links
WHERE {dataset_2_name}_{{root_entity_id_col}} IS NOT NULL 
  AND {dataset_1_name}_{{root_entity_id_col}} IS NOT NULL
GROUP BY state_code, {dataset_2_name}_{{root_entity_id_col}}
HAVING COUNT(DISTINCT {dataset_1_name}_{{root_entity_id_col}}) > 1
"""

MULTIPLE_SANDBOX_PEOPLE_FOR_REFERENCE_PERSON_TEMPLATE = StrictStringFormatter().format(
    MULTIPLE_DATASET_1_PEOPLE_FOR_DATASET_2_PERSON_TEMPLATE,
    root_entity_id_links_clause=ROOT_ENTITY_ID_LINKS_CLAUSE_TEMPLATE,
    dataset_1_name=SANDBOX_DATASET_NAME,
    dataset_2_name=REFERENCE_DATASET_NAME,
)

MULTIPLE_REFERENCE_PEOPLE_FOR_SANDBOX_PERSON_TEMPLATE = StrictStringFormatter().format(
    MULTIPLE_DATASET_1_PEOPLE_FOR_DATASET_2_PERSON_TEMPLATE,
    root_entity_id_links_clause=ROOT_ENTITY_ID_LINKS_CLAUSE_TEMPLATE,
    dataset_1_name=REFERENCE_DATASET_NAME,
    dataset_2_name=SANDBOX_DATASET_NAME,
)

EXTERNAL_IDS_MISSING_FROM_DATASET = """
WITH {root_entity_id_links_clause}
SELECT *
FROM links
WHERE {dataset_name}_{{root_entity_id_col}} IS NULL
"""

EXTERNAL_IDS_MISSING_FROM_REFERENCE = StrictStringFormatter().format(
    EXTERNAL_IDS_MISSING_FROM_DATASET,
    root_entity_id_links_clause=ROOT_ENTITY_ID_LINKS_CLAUSE_TEMPLATE,
    dataset_name=REFERENCE_DATASET_NAME,
)

EXTERNAL_IDS_MISSING_FROM_SANDBOX = StrictStringFormatter().format(
    EXTERNAL_IDS_MISSING_FROM_DATASET,
    root_entity_id_links_clause=ROOT_ENTITY_ID_LINKS_CLAUSE_TEMPLATE,
    dataset_name=SANDBOX_DATASET_NAME,
)


FILTERED_TABLE_ROWS_CLAUSE_TEMPLATE = """
  SELECT *
  FROM {state_table} t
  JOIN {root_entity_ids_mapping_table}
  ON t.{root_entity_id_col} = {dataset_name}_{root_entity_id_col}
"""

COMPARABLE_TABLE_ROWS_TEMPLATE = """
SELECT FARM_FINGERPRINT(TO_JSON_STRING(t)) AS comparison_key, *
FROM (
    SELECT * EXCEPT(
        sandbox_{root_entity_id_col},
        reference_{root_entity_id_col},
        {columns_to_exclude_str}
    )
    FROM ({filtered_rows_clause})
) t
"""


def root_entity_external_ids_address(
    root_entity_cls: Type[Entity],
    dataset: DatasetReference,
) -> ProjectSpecificBigQueryAddress:
    if root_entity_cls is entities.StatePerson:
        table_id = schema.StatePersonExternalId.__tablename__
    elif root_entity_cls is entities.StateStaff:
        table_id = schema.StateStaffExternalId.__tablename__
    else:
        raise ValueError(f"Unexpected root entity type: {root_entity_cls}")

    return ProjectSpecificBigQueryAddress(
        project_id=dataset.project,
        dataset_id=dataset.dataset_id,
        table_id=table_id,
    )


class StateDatasetValidator:
    """Helper class for validating a `state` dataset against a reference `state`
    dataset.
    """

    def __init__(
        self,
        reference_state_dataset: str,
        reference_state_project_id: str,
        sandbox_state_dataset: str,
        sandbox_state_project_id: str,
        output_sandbox_prefix: str,
        state_code_filter: StateCode,
    ) -> None:
        self.state_code_filter = state_code_filter
        self.reference_dataset = DatasetReference.from_string(
            reference_state_dataset, default_project=reference_state_project_id
        )
        self.sandbox_dataset = DatasetReference.from_string(
            sandbox_state_dataset, default_project=sandbox_state_project_id
        )

        self.output_project_id = GCP_PROJECT_STAGING
        self.output_dataset_id = BigQueryAddressOverrides.format_sandbox_dataset(
            prefix=output_sandbox_prefix,
            dataset_id=f"state_validation_results_{state_code_filter.value.lower()}",
        )
        self.bq_client = BigQueryClientImpl(project_id=self.output_project_id)

    def root_entity_id_mappings_address(
        self, root_entity_cls: Type[Entity]
    ) -> ProjectSpecificBigQueryAddress:
        root_entity_id_col = root_entity_cls.get_primary_key_column_name()
        return ProjectSpecificBigQueryAddress(
            project_id=self.output_project_id,
            dataset_id=self.output_dataset_id,
            table_id=f"{root_entity_id_col}s_mapping",
        )

    def build_root_entity_id_mapping(self, root_entity_cls: Type[Entity]) -> None:
        root_entity_id_col = root_entity_cls.get_primary_key_column_name()
        mappings_address = self.root_entity_id_mappings_address(
            root_entity_cls=root_entity_cls
        )

        query = StrictStringFormatter().format(
            ONE_TO_ONE_LINKS_TEMPLATE,
            root_entity_id_col=root_entity_id_col,
            reference_root_entity_external_id_table=root_entity_external_ids_address(
                root_entity_cls, self.reference_dataset
            ).format_address_for_query(),
            sandbox_root_entity_external_id_table=root_entity_external_ids_address(
                root_entity_cls, self.sandbox_dataset
            ).format_address_for_query(),
            state_code_filter=self.state_code_filter.value,
        )
        self.bq_client.insert_into_table_from_query_async(
            destination_dataset_id=mappings_address.dataset_id,
            destination_table_id=mappings_address.table_id,
            write_disposition=WriteDisposition.WRITE_TRUNCATE,
            query=query,
            use_query_cache=False,
        ).result()

    def output_root_entity_id_mapping_errors(
        self, root_entity_cls: Type[Entity]
    ) -> Dict[ProjectSpecificBigQueryAddress, int]:
        """Outputs a set of error tables that show discrepancies in root entities
        between the two `state` datasets.
        """
        root_entity_id_col = root_entity_cls.get_primary_key_column_name()

        output_table_id_to_template = {
            f"multiple_sandbox_{root_entity_id_col}_for_reference_{root_entity_id_col}": (
                MULTIPLE_SANDBOX_PEOPLE_FOR_REFERENCE_PERSON_TEMPLATE
            ),
            f"multiple_reference_{root_entity_id_col}_for_sandbox_{root_entity_id_col}": (
                MULTIPLE_REFERENCE_PEOPLE_FOR_SANDBOX_PERSON_TEMPLATE
            ),
            f"{root_entity_id_col}s_missing_from_reference": (
                EXTERNAL_IDS_MISSING_FROM_REFERENCE
            ),
            f"{root_entity_id_col}s_missing_from_sandbox": (
                EXTERNAL_IDS_MISSING_FROM_SANDBOX
            ),
        }

        jobs = []
        for output_table_id, template in output_table_id_to_template.items():
            jobs.append(
                self.bq_client.insert_into_table_from_query_async(
                    destination_dataset_id=self.output_dataset_id,
                    destination_table_id=output_table_id,
                    write_disposition=WriteDisposition.WRITE_TRUNCATE,
                    query=StrictStringFormatter().format(
                        template,
                        root_entity_id_col=root_entity_id_col,
                        reference_root_entity_external_id_table=root_entity_external_ids_address(
                            root_entity_cls, self.reference_dataset
                        ).format_address_for_query(),
                        sandbox_root_entity_external_id_table=root_entity_external_ids_address(
                            root_entity_cls, self.sandbox_dataset
                        ).format_address_for_query(),
                        state_code_filter=self.state_code_filter.value,
                    ),
                    use_query_cache=False,
                )
            )
        self.bq_client.wait_for_big_query_jobs(jobs)

        error_table_addresses = {}
        for table_id, row_count in self.bq_client.get_row_counts_for_tables(
            self.output_dataset_id
        ).items():
            if table_id not in output_table_id_to_template:
                continue
            if row_count == 0:
                # We found no errors, delete empty error table
                self.bq_client.delete_table(
                    dataset_id=self.output_dataset_id, table_id=table_id
                )
                continue
            error_table_addresses[
                ProjectSpecificBigQueryAddress(
                    project_id=self.bq_client.project_id,
                    dataset_id=self.output_dataset_id,
                    table_id=table_id,
                )
            ] = row_count
        return error_table_addresses

    def output_validations_for_table(
        self,
        root_entity_cls: Type[Entity],
        state_entity_cls: Type[DatabaseEntity],
    ) -> CompareTablesResult:
        """For a given table in the `state` dataset (e.g. state_charge), runs a set of
        validation queries that will get materialized to tables in the output dataset,
        then returns a result with some stats about the comparison.
        """
        root_entity_id_col = root_entity_cls.get_primary_key_column_name()
        mapping_address = self.root_entity_id_mappings_address(
            root_entity_cls=root_entity_cls,
        )

        state_table_id = state_entity_cls.get_entity_name()
        columns_to_exclude = [
            state_entity_cls.get_primary_key_column_name(),
            *[
                c
                for c in state_entity_cls.get_foreign_key_names()
                if c != root_entity_cls
            ],
        ]
        columns_to_exclude_str = ",".join(columns_to_exclude)
        reference_table_address = ProjectSpecificBigQueryAddress(
            project_id=self.reference_dataset.project,
            dataset_id=self.reference_dataset.dataset_id,
            table_id=state_table_id,
        )

        reference_comparable_rows_query = StrictStringFormatter().format(
            COMPARABLE_TABLE_ROWS_TEMPLATE,
            root_entity_id_col=root_entity_id_col,
            columns_to_exclude_str=columns_to_exclude_str,
            filtered_rows_clause=StrictStringFormatter().format(
                FILTERED_TABLE_ROWS_CLAUSE_TEMPLATE,
                dataset_name=REFERENCE_DATASET_NAME,
                root_entity_id_col=root_entity_id_col,
                state_table=reference_table_address.format_address_for_query(),
                root_entity_ids_mapping_table=mapping_address.format_address_for_query(),
            ),
        )

        comparable_reference_table_address = ProjectSpecificBigQueryAddress(
            project_id=self.bq_client.project_id,
            dataset_id=self.output_dataset_id,
            table_id=f"{state_table_id}_reference_comparable",
        )

        self.bq_client.insert_into_table_from_query_async(
            destination_dataset_id=comparable_reference_table_address.dataset_id,
            destination_table_id=comparable_reference_table_address.table_id,
            write_disposition=WriteDisposition.WRITE_TRUNCATE,
            query=reference_comparable_rows_query,
            use_query_cache=False,
        ).result()

        sandbox_table_address = ProjectSpecificBigQueryAddress(
            project_id=self.sandbox_dataset.project,
            dataset_id=self.sandbox_dataset.dataset_id,
            table_id=state_table_id,
        )

        sandbox_comparable_rows_query = StrictStringFormatter().format(
            COMPARABLE_TABLE_ROWS_TEMPLATE,
            root_entity_id_col=root_entity_id_col,
            columns_to_exclude_str=columns_to_exclude_str,
            filtered_rows_clause=StrictStringFormatter().format(
                FILTERED_TABLE_ROWS_CLAUSE_TEMPLATE,
                dataset_name=SANDBOX_DATASET_NAME,
                root_entity_id_col=root_entity_id_col,
                state_table=sandbox_table_address.format_address_for_query(),
                root_entity_ids_mapping_table=mapping_address.format_address_for_query(),
            ),
        )

        comparable_sandbox_table_address = ProjectSpecificBigQueryAddress(
            project_id=self.bq_client.project_id,
            dataset_id=self.output_dataset_id,
            table_id=f"{state_table_id}_sandbox_comparable",
        )

        self.bq_client.insert_into_table_from_query_async(
            destination_dataset_id=comparable_sandbox_table_address.dataset_id,
            destination_table_id=comparable_sandbox_table_address.table_id,
            write_disposition=WriteDisposition.WRITE_TRUNCATE,
            query=sandbox_comparable_rows_query,
            use_query_cache=False,
        ).result()

        return compare_table_or_view(
            address_original=comparable_reference_table_address,
            address_new=comparable_sandbox_table_address,
            comparison_output_dataset_id=self.output_dataset_id,
            primary_keys=["comparison_key"],
            grouping_columns=None,
        )

    def _generate_table_specific_comparison_result(
        self,
        client: BigQueryClient,  # pylint: disable=unused-argument
        reference_table_address: ProjectSpecificBigQueryAddress,
    ) -> Optional[CompareTablesResult]:
        entity_name = reference_table_address.table_id
        if is_association_table(entity_name):
            # TODO(#24413): Validate association tables somehow
            return None

        db_entity_cls = get_database_entity_by_table_name(schema, entity_name)

        entity_cls = get_entity_class_in_module_with_name(
            entities, db_entity_cls.__name__
        )
        if issubclass(entity_cls, EnumEntity):
            # TODO(#24413): Handle EnumEntity checking differently
            return None

        return self.output_validations_for_table(
            root_entity_cls=cast(
                Type[Entity], get_root_entity_class_for_entity(entity_cls)
            ),
            state_entity_cls=db_entity_cls,
        )

    def run_validation(self) -> None:
        """Runs the full suite of `state` dataset validation jobs."""
        self.bq_client.create_dataset_if_necessary(
            self.bq_client.dataset_ref_for_id(self.output_dataset_id),
            default_table_expiration_ms=TEMP_DATAFLOW_DATASET_DEFAULT_TABLE_EXPIRATION_MS,
        )

        root_entity_errors = {}
        for root_entity_cls in RootEntity.__subclasses__():
            if not issubclass(root_entity_cls, Entity):
                raise ValueError(
                    f"Expected root_entity_cls to be an Entity subclass, "
                    f"found: {root_entity_cls}"
                )

            print(f"Building {root_entity_cls.__name__} mapping...")
            self.build_root_entity_id_mapping(root_entity_cls=root_entity_cls)  # type: ignore

            root_entity_errors[
                root_entity_cls
            ] = self.output_root_entity_id_mapping_errors(
                root_entity_cls=root_entity_cls  # type: ignore
            )

        print("Outputting table-specific validation results...")
        results = run_operation_for_tables(
            client=BigQueryClientImpl(project_id=self.output_project_id),
            prompt=None,
            operation=self._generate_table_specific_comparison_result,
            dataset_filter=dataset_id_to_filter_regex(
                self.reference_dataset.dataset_id
            ),
        )
        skipped_tables = [
            table.table_id for table, result in results.items() if result is None
        ]

        table_to_comparison_result = {
            table.table_id: result
            for table, result in results.items()
            if result is not None
        }

        validation_result = ValidationResult(
            root_entity_errors=root_entity_errors,  # type: ignore
            skipped_tables=skipped_tables,
            table_to_comparison_result=table_to_comparison_result,
        )

        validation_result.print()

        print(f"Results can be found in dataset `{self.output_dataset_id}`.")


@attr.define(kw_only=True)
class ValidationResult:
    """Stores the result of the `state` dataset validation."""

    root_entity_errors: Dict[Type[Entity], Dict[ProjectSpecificBigQueryAddress, int]]
    skipped_tables: List[str]
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
        for root_entity_cls, errors in self.root_entity_errors.items():
            root_entity_name = root_entity_cls.__name__
            if not errors:
                print(
                    f"✅ All {root_entity_name} have an exact corresponding "
                    f"{root_entity_name} in the reference dataset."
                )
            else:
                output_dataset_id = one(a.dataset_id for a in errors)
                print(
                    f"⚠️ Found the following {root_entity_name} inconsistencies (see "
                    f"corresponding table in the {output_dataset_id} dataset):"
                )
                for address in sorted(errors):
                    print(f"  * {address.table_id}: {errors[address]}")

        print("Skipped validation of the following tables:")
        for table in self.skipped_tables:
            print(f"  * {table}")
        print()
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
        "--reference_state_dataset",
        type=str,
        default=STATE_BASE_DATASET,
        help="Reference dataset containing tables for the STATE schema.",
    )
    parser.add_argument(
        "--reference_state_project_id",
        dest="reference_state_project_id",
        type=str,
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        default=GCP_PROJECT_STAGING,
        help="The project the |reference_state_dataset| lives in.",
    )
    parser.add_argument(
        "--sandbox_state_dataset",
        type=str,
        required=True,
        help="Dataset which should be compared to |reference_state_dataset|.",
    )
    parser.add_argument(
        "--sandbox_state_project_id",
        dest="sandbox_state_project_id",
        type=str,
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        default=GCP_PROJECT_STAGING,
        help="The project the |sandbox_state_dataset| lives in.",
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
    StateDatasetValidator(
        reference_state_dataset=args.reference_state_dataset,
        reference_state_project_id=args.reference_state_project_id,
        sandbox_state_dataset=args.sandbox_state_dataset,
        sandbox_state_project_id=args.sandbox_state_project_id,
        output_sandbox_prefix=args.output_sandbox_prefix,
        state_code_filter=args.state_code_filter,
    ).run_validation()

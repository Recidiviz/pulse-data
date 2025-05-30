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
"""Script for validating that a sandbox `state` or `normalized_state` dataset is
equivalent to a reference state dataset, even if they have different root entity id
schemes.

Usage:
    python -m recidiviz.tools.ingest.testing.validate_state_dataset \
        --output_sandbox_prefix PREFIX \
        --state_code_filter STATE_CODE \
        --reference_state_dataset DATASET_ID \
        --sandbox_state_dataset DATASET_ID \
        --schema_type {state,normalized_state} \
        [--sandbox_state_project_id PROJECT_ID] \
        [--reference_state_project_id PROJECT_ID]


Examples:
    python -m recidiviz.tools.ingest.testing.validate_state_dataset \
        --output_sandbox_prefix my_prefix \
        --state_code_filter US_CA \
        --reference_state_dataset us_ca_state \
        --sandbox_state_dataset another_prefix_us_ca_state \
        --schema_type state

    python -m recidiviz.tools.ingest.testing.validate_state_dataset \
        --output_sandbox_prefix my_prefix \
        --state_code_filter US_CA \
        --reference_state_dataset us_ca_normalized_state \
        --sandbox_state_dataset another_prefix_us_ca_normalized_state \
        --schema_type normalized_state
"""
import argparse
from types import ModuleType
from typing import Dict, List, Optional, Set, Type, cast

import attr
import pandas as pd
from google.cloud.bigquery import DatasetReference
from more_itertools import one

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_address import (
    BigQueryAddress,
    ProjectSpecificBigQueryAddress,
)
from recidiviz.big_query.big_query_client import BigQueryClient, BigQueryClientImpl
from recidiviz.common.attr_mixins import attribute_field_type_reference_for_class
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.schema_utils import is_association_table
from recidiviz.persistence.entity.base_entity import Entity, EnumEntity, RootEntity
from recidiviz.persistence.entity.entities_module_context_factory import (
    entities_module_context_for_module,
)
from recidiviz.persistence.entity.entity_field_index import EntityFieldType
from recidiviz.persistence.entity.entity_utils import (
    get_entities_by_association_table_id,
    get_entity_class_in_module_with_name,
    get_entity_class_in_module_with_table_id,
)
from recidiviz.persistence.entity.root_entity_utils import (
    get_root_entity_class_for_entity,
)
from recidiviz.persistence.entity.state import entities, normalized_entities
from recidiviz.persistence.entity.state.entities import (
    StatePerson,
    StatePersonExternalId,
    StateStaff,
    StateStaffExternalId,
)
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStatePerson,
    NormalizedStateStaff,
)
from recidiviz.source_tables.ingest_pipeline_output_table_collector import (
    build_normalized_state_output_source_table_collection,
    build_state_output_source_table_collection,
)
from recidiviz.source_tables.source_table_config import SourceTableCollection
from recidiviz.tools.calculator.compare_views import compare_table_or_view
from recidiviz.tools.calculator.create_or_update_dataflow_sandbox import (
    TEMP_DATAFLOW_DATASET_DEFAULT_TABLE_EXPIRATION_MS,
)
from recidiviz.tools.utils.bigquery_helpers import (
    dataset_id_to_filter_regex,
    run_operation_for_tables_in_datasets,
)
from recidiviz.tools.utils.compare_tables_helper import (
    STATS_NEW_ERROR_RATE_COL,
    STATS_ORIGINAL_ERROR_RATE_COL,
    CompareTablesResult,
)
from recidiviz.tools.utils.script_helpers import prompt_for_confirmation
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.string import StrictStringFormatter
from recidiviz.utils.types import assert_subclass

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


def root_entity_external_ids_address(
    root_entity_cls: Type[Entity],
    dataset: DatasetReference,
) -> ProjectSpecificBigQueryAddress:
    if root_entity_cls in {StatePerson, NormalizedStatePerson}:
        table_id = StatePersonExternalId.get_table_id()
    elif root_entity_cls in {StateStaff, NormalizedStateStaff}:
        table_id = StateStaffExternalId.get_table_id()
    else:
        raise ValueError(f"Unexpected root entity: {root_entity_cls}")

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
        entities_module: ModuleType,
        schema_type: str,
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
        self.entities_module = entities_module
        self.entities_module_context = entities_module_context_for_module(
            entities_module
        )
        self.field_index = self.entities_module_context.field_index()

        with local_project_id_override(reference_state_project_id):
            self.source_table_collection = self.get_source_table_collection(
                state_code_filter, schema_type
            )

    def get_source_table_collection(
        self, state_code: StateCode, schema_type: str
    ) -> SourceTableCollection:
        if schema_type == "state":
            return build_state_output_source_table_collection(state_code)
        if schema_type == "normalized_state":
            return build_normalized_state_output_source_table_collection(state_code)
        raise ValueError(f"Unexpected schema_type: {schema_type}")

    def _is_enum_entity(self, table_name: str) -> bool:
        if is_association_table(table_name):
            return False

        entity_cls = get_entity_class_in_module_with_table_id(
            self.entities_module, table_name
        )
        return issubclass(entity_cls, EnumEntity)

    def _primary_keys_for_differences_output(self, state_table_id: str) -> List[str]:
        """Returns the list of columns that should not be nulled out for convenience
        (when the values are the same) in the differences output for this table.
        """
        if is_association_table(state_table_id):
            return []
        state_entity_cls = get_entity_class_in_module_with_table_id(
            self.entities_module, state_table_id
        )

        # If an external_id column exists, don't null it out in the `differences`
        # output.
        primary_keys = (
            ["external_id"] if state_entity_cls.has_field("external_id") else []
        )
        return primary_keys

    def _dataset_name_for_dataset(self, dataset: DatasetReference) -> str:
        if dataset == self.reference_dataset:
            return REFERENCE_DATASET_NAME
        if dataset == self.sandbox_dataset:
            return SANDBOX_DATASET_NAME
        raise ValueError(f"Unexpected dataset {dataset}")

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
        self.bq_client.create_table_from_query(
            address=mappings_address.to_project_agnostic_address(),
            query=query,
            use_query_cache=False,
            overwrite=True,
        )

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
                self.bq_client.create_table_from_query_async(
                    address=BigQueryAddress(
                        dataset_id=self.output_dataset_id,
                        table_id=output_table_id,
                    ),
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
                    overwrite=True,
                )
            )
        self.bq_client.wait_for_big_query_jobs(jobs)

        error_table_addresses = {}
        for table_id, row_count in self.bq_client.get_row_counts_for_tables(
            self.output_dataset_id
        ).items():
            table_address = ProjectSpecificBigQueryAddress(
                project_id=self.bq_client.project_id,
                dataset_id=self.output_dataset_id,
                table_id=table_id,
            )
            if table_id not in output_table_id_to_template:
                continue
            if row_count == 0:
                # We found no errors, delete empty error table
                self.bq_client.delete_table(
                    address=table_address.to_project_agnostic_address()
                )
                continue
            error_table_addresses[table_address] = row_count
        return error_table_addresses

    def _build_comparable_entity_rows_query(
        self,
        dataset: DatasetReference,
        entity_cls: Type[Entity],
    ) -> str:
        """Returns a formatted query that queries the provided state entity in the
        provided |dataset|, which can be used to compare against the same table in
        a different dataset.
        """

        filtered_with_new_cols = self._filtered_table_rows_clause_for_entity_table(
            dataset=dataset, entity_cls=entity_cls
        )

        source_tables = [
            t
            for t in self.source_table_collection.source_tables
            if t.address.table_id == entity_cls.get_table_id()
        ]
        if len(source_tables) != 1:
            raise ValueError(
                f"Found incorrect number of source tables for {entity_cls.get_table_id()}: [{[t.address for t in self.source_table_collection.source_tables]}]"
            )
        source_table = one(
            t
            for t in self.source_table_collection.source_tables
            if t.address.table_id == entity_cls.get_table_id()
        )

        source_table_columns = {f.name for f in source_table.schema_fields}
        entity_fields = attribute_field_type_reference_for_class(entity_cls).fields

        # The only columns that live on the source table but do not match attribute
        # names on the entity are foreign key columns.
        foreign_key_columns = source_table_columns - entity_fields

        columns_to_exclude = [
            entity_cls.get_primary_key_column_name(),
            *sorted(foreign_key_columns),
        ]
        columns_to_exclude_str = ",".join(columns_to_exclude)
        return f"""SELECT * EXCEPT ({columns_to_exclude_str})
FROM ({filtered_with_new_cols})"""

    def _get_direct_enum_entity_child_classes(
        self, entity_cls: Type[Entity]
    ) -> Set[Type[EnumEntity]]:
        """Returns the set of EnumEntity classes that are direct children of the
        provided |entity_cls|.
        """
        enum_classes = set()
        class_reference = attribute_field_type_reference_for_class(entity_cls)
        for field_name in class_reference.fields:
            attr_info = class_reference.get_field_info(field_name)
            if not attr_info.referenced_cls_name:
                continue
            entity_cls = get_entity_class_in_module_with_name(
                self.entities_module, attr_info.referenced_cls_name
            )
            if issubclass(entity_cls, EnumEntity):
                enum_classes.add(entity_cls)
        return enum_classes

    def _filtered_table_rows_clause_for_entity_table(
        self, dataset: DatasetReference, entity_cls: Type[Entity]
    ) -> str:
        """Returns a formatted query clause that filters the table in |dataset| for the
        provided |state_entity_cls| down to only rows that are connected to a root
        entity present in both the reference and sandbox datasets. Adds a column that
        contains the root entity id (e.g. person_id) that can be used to compare across
        both sandbox and reference datasets. If the entity has EnumEntity child tables,
        adds a column that collapses those values into a JSON string value.
        """
        root_entity_cls = cast(
            Type[Entity],
            get_root_entity_class_for_entity(
                get_entity_class_in_module_with_name(
                    self.entities_module, entity_cls.__name__
                )
            ),
        )
        table_address = ProjectSpecificBigQueryAddress(
            project_id=dataset.project,
            dataset_id=dataset.dataset_id,
            table_id=entity_cls.get_table_id(),
        )
        root_entity_id_col = root_entity_cls.get_primary_key_column_name()
        mapping_address = self.root_entity_id_mappings_address(
            root_entity_cls=root_entity_cls,
        )
        dataset_name = self._dataset_name_for_dataset(dataset)
        state_table = table_address.format_address_for_query()
        root_entity_ids_mapping_table = mapping_address.format_address_for_query()

        enum_entity_child_join_clauses = []
        for enum_entity_cls in self._get_direct_enum_entity_child_classes(entity_cls):
            enum_table_address = ProjectSpecificBigQueryAddress(
                project_id=dataset.project,
                dataset_id=dataset.dataset_id,
                table_id=enum_entity_cls.get_table_id(),
            )
            enum_field_name = enum_entity_cls.get_enum_field_name()
            raw_text_field_name = enum_entity_cls.get_raw_text_field_name()
            enum_entity_child_join_clauses.append(
                f"""LEFT OUTER JOIN (
    SELECT 
      {entity_cls.get_primary_key_column_name()}, 
      TO_JSON_STRING(
        ARRAY_AGG(STRUCT({enum_field_name}, {raw_text_field_name})
        ORDER BY {enum_field_name}, {raw_text_field_name})
      ) AS {enum_entity_cls.get_table_id()}_json
    FROM {enum_table_address.format_address_for_query()} 
    GROUP BY {entity_cls.get_primary_key_column_name()}
  )
  USING ({entity_cls.get_primary_key_column_name()})
"""
            )
        enum_entity_column_join_clauses_str = "".join(enum_entity_child_join_clauses)
        return f"""
  SELECT 
    * EXCEPT(
     sandbox_{root_entity_id_col},
     reference_{root_entity_id_col}
    ), 
    reference_{root_entity_id_col} AS comparable_{root_entity_id_col}
  FROM {state_table} t
  JOIN {root_entity_ids_mapping_table}
  ON t.{root_entity_id_col} = {dataset_name}_{root_entity_id_col}
  {enum_entity_column_join_clauses_str}
"""

    def _build_comparable_association_table_rows_query(
        self,
        dataset: DatasetReference,
        association_table_id: str,
    ) -> str:
        """Returns a formatted query for the given association table in the provided
        |dataset|, which can be used to compare against the same association table in
        a different dataset.
        """

        if not is_association_table(association_table_id):
            raise ValueError(
                f"This function called with non-association table "
                f"[{association_table_id}]."
            )

        (
            associated_entity_1_cls,
            associated_entity_2_cls,
        ) = get_entities_by_association_table_id(
            self.entities_module_context, association_table_id
        )

        associated_entity_1_pk = associated_entity_1_cls.get_primary_key_column_name()
        associated_entity_2_pk = associated_entity_2_cls.get_primary_key_column_name()

        root_entity_cls = cast(
            Type[Entity],
            get_root_entity_class_for_entity(associated_entity_1_cls),
        )
        root_entity_id_col = root_entity_cls.get_primary_key_column_name()

        association_table_address = ProjectSpecificBigQueryAddress(
            project_id=dataset.project,
            dataset_id=dataset.dataset_id,
            table_id=association_table_id,
        )

        t1_name = associated_entity_1_cls.get_table_id()
        t1_clause = self._filtered_table_rows_clause_for_entity_table(
            dataset=dataset, entity_cls=associated_entity_1_cls
        )
        t2_name = associated_entity_2_cls.get_table_id()
        t2_clause = self._filtered_table_rows_clause_for_entity_table(
            dataset=dataset, entity_cls=associated_entity_2_cls
        )

        comparable_root_entity_id_col = f"comparable_{root_entity_id_col}"

        association_table = association_table_address.format_address_for_query()

        return f"""
SELECT
  {t1_name}.{comparable_root_entity_id_col} AS {t1_name}_{comparable_root_entity_id_col},
  {t2_name}.{comparable_root_entity_id_col} AS {t2_name}_{comparable_root_entity_id_col},
  {t1_name}.external_id AS {t1_name}_external_id,
  {t2_name}.external_id AS {t2_name}_external_id,
FROM
  {association_table} association
JOIN (
{t1_clause}
) {t1_name}
USING({associated_entity_1_pk})
JOIN (
{t2_clause}
) {t2_name}
USING({associated_entity_2_pk})
"""

    def _materialize_comparable_table(
        self,
        dataset: DatasetReference,
        state_table_id: str,
    ) -> ProjectSpecificBigQueryAddress:
        """For the given state dataset table, generates a query that produces results
        for that table that can be compared against results from another dataset, then
        materializes the results of that query and returns the results address.
        """
        table_address = ProjectSpecificBigQueryAddress(
            project_id=dataset.project,
            dataset_id=dataset.dataset_id,
            table_id=state_table_id,
        )
        if not is_association_table(state_table_id):
            entity_cls = get_entity_class_in_module_with_table_id(
                self.entities_module, state_table_id
            )
            comparable_rows_query = self._build_comparable_entity_rows_query(
                dataset=dataset,
                entity_cls=entity_cls,
            )
        else:
            comparable_rows_query = self._build_comparable_association_table_rows_query(
                dataset=dataset, association_table_id=state_table_id
            )

        dataset_name = self._dataset_name_for_dataset(dataset)
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

    def _root_is_direct_parent(self, entity_cls: Type[Entity]) -> bool:
        """Returns True if this entity is directly connected to the root entity
        in the entity tree, False if it is connected via an intermediate entity /
        intermediate entities.
        """
        back_edge_fields = self.field_index.get_all_entity_fields(
            entity_cls, EntityFieldType.BACK_EDGE
        )

        non_root_back_edges = back_edge_fields - {
            get_root_entity_class_for_entity(entity_cls).back_edge_field_name()
        }
        if non_root_back_edges:
            # If this entity has back edges other than to the root entity, then the
            # root is not this entity's direct parent.
            return False
        return True

    def _generate_table_specific_comparison_result(
        self,
        client: BigQueryClient,  # pylint: disable=unused-argument
        reference_table_address: ProjectSpecificBigQueryAddress,
    ) -> Optional[CompareTablesResult]:
        """For a given table in the `state` dataset (e.g. state_charge), runs a set of
        validation queries that will get materialized to tables in the output dataset,
        then returns a result with some stats about the comparison.

        Returns None if the table is not supported for comparison.
        """
        state_table_id = reference_table_address.table_id
        if self._is_enum_entity(table_name=state_table_id):
            entity_cls = get_entity_class_in_module_with_table_id(
                self.entities_module, state_table_id
            )
            if not self._root_is_direct_parent(entity_cls):
                # It doesn't make sense to compare enum entities that are connected to
                # non-root entities the way we compare other entities because we expect
                # to see many duplicate enum entities where the only difference is the
                # direct parent primary key value.
                return None

        comparable_reference_table_address = self._materialize_comparable_table(
            dataset=self.reference_dataset,
            state_table_id=state_table_id,
        )

        comparable_sandbox_table_address = self._materialize_comparable_table(
            dataset=self.sandbox_dataset,
            state_table_id=state_table_id,
        )

        return compare_table_or_view(
            address_original=comparable_reference_table_address,
            address_new=comparable_sandbox_table_address,
            comparison_output_dataset_id=self.output_dataset_id,
            primary_keys=self._primary_keys_for_differences_output(state_table_id),
            grouping_columns=None,
        )

    def run_validation(self) -> None:
        """Runs the full suite of `state` dataset validation jobs."""
        self.bq_client.create_dataset_if_necessary(
            self.output_dataset_id,
            default_table_expiration_ms=TEMP_DATAFLOW_DATASET_DEFAULT_TABLE_EXPIRATION_MS,
        )

        root_entity_errors = {}
        for root_entity_table_id in {
            assert_subclass(e, Entity).get_table_id()
            for e in RootEntity.__subclasses__()
        }:
            print(f"Building {root_entity_table_id} mapping...")

            root_entity_cls = get_entity_class_in_module_with_table_id(
                self.entities_module, root_entity_table_id
            )
            self.build_root_entity_id_mapping(root_entity_cls=root_entity_cls)

            root_entity_errors[
                root_entity_cls
            ] = self.output_root_entity_id_mapping_errors(
                root_entity_cls=root_entity_cls
            )

        print("Outputting table-specific validation results...")
        results = run_operation_for_tables_in_datasets(
            client=BigQueryClientImpl(project_id=self.output_project_id),
            prompt=None,
            operation=self._generate_table_specific_comparison_result,
            dataset_filter=dataset_id_to_filter_regex(
                self.reference_dataset.dataset_id
            ),
        )
        for table, result in results.items():
            if result is None and not self._is_enum_entity(table_name=table.table_id):
                raise ValueError(
                    f"Skipped validation of non-enum entity {table.table_id}"
                )

        table_to_comparison_result = {
            table.table_id: result
            for table, result in results.items()
            if result is not None
        }

        validation_result = ValidationResult(
            root_entity_errors=root_entity_errors,
            table_to_comparison_result=table_to_comparison_result,
        )

        validation_result.print()

        print(f"Results can be found in dataset `{self.output_dataset_id}`.")


@attr.define(kw_only=True)
class ValidationResult:
    """Stores the result of the `state` dataset validation."""

    root_entity_errors: Dict[Type[Entity], Dict[ProjectSpecificBigQueryAddress, int]]
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
            root_entity_table_id = root_entity_cls.get_table_id()
            if not errors:
                print(
                    f"✅ All {root_entity_table_id} have an exact corresponding row in "
                    f"the reference dataset."
                )
            else:
                output_dataset_id = one({a.dataset_id for a in errors})
                print(
                    f"⚠️ Found the following {root_entity_table_id} inconsistencies "
                    f"(see corresponding table in the {output_dataset_id} dataset):"
                )
                for address in sorted(errors):
                    print(f"  * {address.table_id}: {errors[address]}")
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
        required=True,
        help="Reference dataset containing tables for the STATE schema. For example, "
        "`us_xx_state` or `normalized_state`.",
    )
    parser.add_argument(
        "--schema_type",
        type=str,
        default="state",
        choices=["state", "normalized_state"],
        required=True,
        help="Choose 'state' if you are comparing datasets with the schema defined in "
        "state/entities.py. Choose 'normalized_state' if you are comparing datsets "
        "with the schema defined in state/normalized_entities.py",
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

    if args.state_code_filter.value.lower() not in args.sandbox_state_dataset:
        prompt_for_confirmation(
            f"Sandbox dataset [{args.sandbox_state_dataset}] does not contain state "
            f"code filter [{args.state_code_filter.value}] in its name. Are you sure "
            f"this is the correct dataset?"
        )

    if args.schema_type == "state":
        if (
            "normalized" in args.reference_state_dataset
            or "normalized" in args.sandbox_state_dataset
        ):
            prompt_for_confirmation(
                f"Found schema_type [{args.schema_type}], but one of the input "
                f"datasets has 'normalized' in the name. Are you sure you didn't mean "
                f"to set '--schema_type=normalized_state'?"
            )
        entities_definitions_module: ModuleType = entities
    elif args.schema_type == "normalized_state":
        entities_definitions_module = normalized_entities
    else:
        raise ValueError(f"Unexpected schema_type: {args.schema_type}")

    StateDatasetValidator(
        reference_state_dataset=args.reference_state_dataset,
        reference_state_project_id=args.reference_state_project_id,
        sandbox_state_dataset=args.sandbox_state_dataset,
        sandbox_state_project_id=args.sandbox_state_project_id,
        output_sandbox_prefix=args.output_sandbox_prefix,
        state_code_filter=args.state_code_filter,
        entities_module=entities_definitions_module,
        schema_type=args.schema_type,
    ).run_validation()

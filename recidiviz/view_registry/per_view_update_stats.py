# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Helpers for calculating view-level metadata about views updated by our standard view
update.
"""
import datetime
from typing import Any

import attr
import sqlglot

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_sqlglot_helpers import (
    get_state_code_literal_references,
)
from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.big_query.big_query_view_dag_walker import (
    BigQueryViewDagWalker,
    ProcessDagResult,
    ViewProcessingMetadata,
)
from recidiviz.big_query.view_update_manager import CreateOrUpdateViewResult
from recidiviz.common import attr_validators
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_helpers import get_raw_data_table_and_view_datasets
from recidiviz.source_tables.collect_all_source_table_configs import (
    build_source_table_repository_for_collected_schemata,
)
from recidiviz.utils import environment, metadata
from recidiviz.view_registry.address_to_complexity_score_mapping import (
    ParentAddressComplexityScoreMapper,
)
from recidiviz.view_registry.deployed_views import deployed_view_builders
from recidiviz.view_registry.infra_library import is_view_part_of_infra_library_2025
from recidiviz.view_registry.query_complexity_score_2025 import (
    get_query_complexity_score_2025,
)


@attr.define(kw_only=True)
class PerViewUpdateStats:
    """
    Info about one view that was updated in a job where a connected graph of views were
    updated.

    See field definitions in view_update_metadata/per_view_update_stats.yaml
    """

    success_datetime: datetime.datetime = attr.ib(validator=attr_validators.is_datetime)
    create_or_update_result: CreateOrUpdateViewResult = attr.ib(
        validator=attr.validators.instance_of(CreateOrUpdateViewResult)
    )
    view_processing_metadata: ViewProcessingMetadata = attr.ib(
        validator=attr.validators.instance_of(ViewProcessingMetadata)
    )

    @property
    def view_address(self) -> BigQueryAddress:
        return self.create_or_update_result.view.address

    @property
    def dataset_id(self) -> str:
        return self.view_address.dataset_id

    @property
    def table_id(self) -> str:
        return self.view_address.table_id

    @property
    def was_materialized(self) -> bool:
        return self.create_or_update_result.materialization_result is not None

    @property
    def update_runtime_sec(self) -> float:
        return self.view_processing_metadata.view_processing_runtime_sec

    @property
    def view_query_signature(self) -> str:
        return self.create_or_update_result.view.view_query_signature

    @property
    def clustering_fields_string(self) -> str | None:
        return self.create_or_update_result.view.clustering_fields_string

    @property
    def time_partitioning_string(self) -> str | None:
        return self.create_or_update_result.view.time_partitioning_string

    @property
    def materialized_table_num_rows(self) -> int | None:
        materialization_result = self.create_or_update_result.materialization_result
        if not materialization_result:
            return None
        return materialization_result.materialized_table_num_rows

    @property
    def materialized_table_size_bytes(self) -> int | None:
        materialization_result = self.create_or_update_result.materialization_result
        if not materialization_result:
            return None
        return materialization_result.materialized_table_size_bytes

    @property
    def slot_millis(self) -> int | None:
        materialization_result = self.create_or_update_result.materialization_result
        if not materialization_result:
            return None
        return materialization_result.slot_millis

    @property
    def total_bytes_processed(self) -> int | None:
        materialization_result = self.create_or_update_result.materialization_result
        if not materialization_result:
            return None
        return materialization_result.total_bytes_processed

    @property
    def total_bytes_billed(self) -> int | None:
        materialization_result = self.create_or_update_result.materialization_result
        if not materialization_result:
            return None
        return materialization_result.total_bytes_billed

    @property
    def job_id(self) -> str | None:
        materialization_result = self.create_or_update_result.materialization_result
        if not materialization_result:
            return None
        return materialization_result.job_id

    @property
    def graph_depth(self) -> int:
        return self.view_processing_metadata.graph_depth

    @property
    def state_code_specific_to_view(self) -> str | None:
        state_code_for_address = (
            self.create_or_update_result.view.address.state_code_for_address()
        )
        return state_code_for_address.value if state_code_for_address else None

    ancestor_view_addresses: set[BigQueryAddress] = attr.ib(
        validator=attr_validators.is_set_of(BigQueryAddress)
    )

    @property
    def num_ancestor_views(self) -> int:
        return len(self.ancestor_view_addresses)

    state_code_literal_references: set[StateCode] = attr.ib(
        validator=attr_validators.is_set_of(StateCode)
    )

    @property
    def states_referenced(self) -> list[StateCode]:
        states_referenced_set = self.state_code_literal_references | {
            state_code
            for a in self.parent_addresses
            if (state_code := a.state_code_for_address())
        }
        return sorted(states_referenced_set, key=lambda state_code: state_code.value)

    @property
    def has_state_specific_logic(self) -> bool:
        return bool(self.states_referenced)

    parent_addresses: list[BigQueryAddress] = attr.ib(
        validator=attr_validators.is_list_of(BigQueryAddress)
    )
    complexity_score_2025: int = attr.ib(validator=attr_validators.is_int)
    composite_complexity_score_2025: int = attr.ib(validator=attr_validators.is_int)
    post_infra_library_composite_complexity_score_2025: int = attr.ib(
        validator=attr_validators.is_int
    )
    referenced_raw_data_tables: list[BigQueryAddress] = attr.ib(
        validator=attr_validators.is_list_of(BigQueryAddress)
    )
    is_leaf_node: bool = attr.ib(validator=attr_validators.is_bool)

    def as_table_row(self) -> dict[str, Any]:
        """Converts this object to a dictionary that can be used to generate a single
        row in the view_update_metadata.per_view_update_stats table.
        """
        return {
            "success_timestamp": self.success_datetime.isoformat(),
            "data_platform_version": environment.get_data_platform_version(),
            "dataset_id": self.dataset_id,
            "table_id": self.table_id,
            "was_materialized": self.was_materialized,
            "update_runtime_sec": self.update_runtime_sec,
            "view_query_signature": self.view_query_signature,
            "clustering_fields_string": self.clustering_fields_string,
            "time_partitioning_string": self.time_partitioning_string,
            "materialized_table_num_rows": self.materialized_table_num_rows,
            "materialized_table_size_bytes": self.materialized_table_size_bytes,
            "slot_millis": self.slot_millis,
            "total_bytes_processed": self.total_bytes_processed,
            "total_bytes_billed": self.total_bytes_billed,
            "job_id": self.job_id,
            "graph_depth": self.graph_depth,
            "num_ancestor_views": self.num_ancestor_views,
            "parent_addresses": sorted(a.to_str() for a in self.parent_addresses),
            "complexity_score_2025": self.complexity_score_2025,
            "composite_complexity_score_2025": self.composite_complexity_score_2025,
            "post_infra_library_composite_complexity_score_2025": (
                self.post_infra_library_composite_complexity_score_2025
            ),
            "has_state_specific_logic": self.has_state_specific_logic,
            "states_referenced": sorted(s.value for s in self.states_referenced),
            "state_code_specific_to_view": self.state_code_specific_to_view,
            "referenced_raw_data_tables": sorted(
                a.to_str() for a in self.referenced_raw_data_tables
            ),
            "is_leaf_node": self.is_leaf_node,
        }


def per_view_update_stats_for_view_update_result(
    success_datetime: datetime.datetime,
    view_update_dag_walker: BigQueryViewDagWalker,
    update_views_result: ProcessDagResult[CreateOrUpdateViewResult],
) -> list["PerViewUpdateStats"]:
    """Given the outputs from a view update run, produces update statistics for each
    view involved in the update, packaging those in a class that can be easily persisted
    to the per_view_update_stats table.
    """
    source_table_repository = build_source_table_repository_for_collected_schemata(
        project_id=metadata.project_id()
    )
    address_to_table_complexity_score_mapper = ParentAddressComplexityScoreMapper(
        source_table_repository=source_table_repository,
        all_view_builders=deployed_view_builders(),
    )
    raw_data_datasets = get_raw_data_table_and_view_datasets()

    def stats_process_fn(
        v: BigQueryView, parent_results: dict[BigQueryView, PerViewUpdateStats]
    ) -> PerViewUpdateStats:
        ancestor_view_addresses = {p.address for p in parent_results}
        for p in parent_results.values():
            ancestor_view_addresses |= p.ancestor_view_addresses

        node = view_update_dag_walker.nodes_by_address[v.address]
        parent_addresses = node.parent_node_addresses | node.source_addresses

        raw_data_parent_addresses = {
            p for p in parent_addresses if p.dataset_id in raw_data_datasets
        }

        query_expression = sqlglot.parse_one(v.view_query, dialect="bigquery")
        if not isinstance(query_expression, sqlglot.expressions.Query):
            raise ValueError(
                f"Unexpected query expression type for view [{v.address.to_str()}]"
            )

        state_code_literal_references = get_state_code_literal_references(
            query_expression
        )

        complexity_score_2025 = get_query_complexity_score_2025(
            query_expression,
            address_to_table_complexity_score_mapper.get_parent_complexity_map_for_view_2025(
                v.address
            ),
        )

        composite_complexity_score_2025 = (
            sum(p.composite_complexity_score_2025 for p in parent_results.values())
            + complexity_score_2025
        )

        post_infra_library_composite_complexity_score_2025 = (
            sum(
                (
                    p.post_infra_library_composite_complexity_score_2025
                    if not is_view_part_of_infra_library_2025(p.view_address)
                    else 0
                )
                for p in parent_results.values()
            )
            + complexity_score_2025
        )

        return PerViewUpdateStats(
            success_datetime=success_datetime,
            create_or_update_result=update_views_result.view_results[v],
            view_processing_metadata=update_views_result.view_processing_stats[v],
            ancestor_view_addresses=ancestor_view_addresses,
            parent_addresses=sorted(parent_addresses, key=lambda a: a.to_str()),
            state_code_literal_references=state_code_literal_references,
            complexity_score_2025=complexity_score_2025,
            composite_complexity_score_2025=composite_complexity_score_2025,
            post_infra_library_composite_complexity_score_2025=(
                post_infra_library_composite_complexity_score_2025
            ),
            referenced_raw_data_tables=sorted(
                raw_data_parent_addresses, key=lambda a: a.to_str()
            ),
            is_leaf_node=node in update_views_result.leaf_nodes,
        )

    per_view_stats_results = view_update_dag_walker.process_dag(
        stats_process_fn,
        synchronous=False,
    )

    return sorted(
        per_view_stats_results.view_results.values(), key=lambda r: r.view_address
    )

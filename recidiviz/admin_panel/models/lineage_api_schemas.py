# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Schemas for lineage endpoints"""
import abc
from enum import Enum

import attr
from marshmallow import fields

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view_dag_walker import BigQueryViewDagNode
from recidiviz.case_triage.api_schemas_utils import CamelCaseSchema
from recidiviz.common.attr_converters import optional_enum_value_from_enum
from recidiviz.source_tables.source_table_config import SourceTableConfig


class BigQueryNodeType(Enum):
    view = "view"
    source = "source"


# --- marshmallow schema definitions ---------------------------------------------------


class LineageNodeSchema(CamelCaseSchema):
    """Base schema representing a single a node in our lineage graph."""

    urn = fields.Str(required=True)


class BigQueryNodeLimitedSchema(LineageNodeSchema):
    """Schema representing a limited set of fields and metadata about a node that
    represents a BigQuery view or table.
    """

    type = fields.Enum(BigQueryNodeType)
    view_id = fields.Str(required=True)
    dataset_id = fields.Str(required=True)
    state_code = fields.Str(required=True)


class LineageReferenceSchema(CamelCaseSchema):
    """Schema representing a reference between two LineageNodes"""

    source = fields.Str(required=True)
    target = fields.Str(required=True)


class BigQueryBetweenSchema(CamelCaseSchema):
    """Schema representing the set of urns that are between two different BigQuery
    nodes
    """

    urns = fields.List(fields.Str())


class BigQueryDownstreamSchema(CamelCaseSchema):

    urns = fields.List(fields.Str())


class LineageGraphSchema(CamelCaseSchema):
    """Schema holding the entire representation of the lineage view graph."""

    # TODO(#36174): consider making this a poly field that can return multiple different
    # node types as we add in dataflow pipelines & looker references
    nodes = fields.List(fields.Nested(BigQueryNodeLimitedSchema))
    references = fields.List(fields.Nested(LineageReferenceSchema))


class BigQueryNodeExpandedSchema(BigQueryNodeLimitedSchema):
    """Schema representing a fuller set of fields and metadata about a node that
    represents a BigQuery view or table.
    """

    description = fields.Str(required=True)
    view_query = fields.Str()
    materialized_address = fields.Str()
    # TODO(#46345): populate this w/ add'l info from bq???


class BigQuerySourceTableExpandedSchema(BigQueryNodeLimitedSchema):
    """Schema representing a fuller set of fields and metadata about a node that
    represents a source table in our big query view graph
    """

    description = fields.Str(required=True)
    # TODO(#46345): populate this w/ add'l info from bq???


# --- attr definitions that will be used to populate the marshmallow objs above --------


@attr.define(kw_only=True)
class LineageReference:
    """Attr representation of reference between two lineage nodes"""

    source: str = attr.field()
    target: str = attr.field()

    @classmethod
    def for_bq_node(cls, node: BigQueryViewDagNode) -> list["LineageReference"]:
        return [
            cls(
                source=parent.to_str(),
                target=node.view.address.to_str(),
            )
            for parent in node.parent_node_addresses
        ] + [
            cls(
                source=source.to_str(),
                target=node.view.address.to_str(),
            )
            for source in node.source_addresses
        ]


@attr.define(kw_only=True)
class LineageNode:
    """Attr representation of a node in our data lineage graph"""

    urn: str = attr.field()

    @abc.abstractmethod
    def upstream_references(self) -> list[LineageReference]:
        """A list of references that this LineageNode makes to upstream LineageNodes."""


@attr.define(kw_only=True)
class BigQueryGraphNode(LineageNode):
    """Attr version of a limited set of fields and metadata about a node that represents
    a BigQuery view or table.
    """

    type: BigQueryNodeType = attr.field()
    view_id: str = attr.field()
    dataset_id: str = attr.field()
    state_code: str | None = attr.field(converter=optional_enum_value_from_enum)

    @abc.abstractmethod
    def upstream_references(self) -> list[LineageReference]:
        """A list of references that this LineageNode makes to upstream LineageNodes."""


@attr.define(kw_only=True)
class BigQueryViewNode(BigQueryGraphNode):
    """Attr version of a limited set of fields and metadata about a node that represents
    a BigQueryView in our BigQueryViewGraph.
    """

    node: BigQueryViewDagNode = attr.field()
    type: BigQueryNodeType = attr.field(default=BigQueryNodeType.view)

    @classmethod
    def from_node(cls, node: BigQueryViewDagNode) -> "BigQueryViewNode":
        return cls(
            urn=node.view.address.to_str(),
            node=node,
            view_id=node.view.view_id,
            dataset_id=node.view.dataset_id,
            state_code=node.view.address.state_code_for_address(),
        )

    def upstream_references(self) -> list[LineageReference]:
        """A list of references that this LineageNode makes to upstream LineageNodes."""
        return LineageReference.for_bq_node(self.node)


@attr.define(kw_only=True)
class BigQuerySourceTableNode(BigQueryGraphNode):
    """Attr version of a limited set of fields and metadata about a node that represents
    a source table in our BigQueryViewGraph.
    """

    address: BigQueryAddress = attr.field()
    type: BigQueryNodeType = attr.field(default=BigQueryNodeType.source)

    @classmethod
    def from_address(cls, address: BigQueryAddress) -> "BigQuerySourceTableNode":
        return cls(
            urn=address.to_str(),
            address=address,
            view_id=address.table_id,
            dataset_id=address.dataset_id,
            state_code=address.state_code_for_address(),
        )

    def upstream_references(self) -> list[LineageReference]:
        """A list of references that this LineageNode makes to upstream LineageNodes."""
        return []


@attr.define(kw_only=True)
class BigQueryViewNodeMetadata(BigQueryViewNode):
    """Attr version of a fuller set of fields and metadata about a node that represents
    a BigQuery view or table.
    """

    description: str = attr.field()
    view_query: str = attr.field()
    materialized_address: str | None = attr.field()

    @classmethod
    def from_node(cls, node: BigQueryViewDagNode) -> "BigQueryViewNodeMetadata":
        materialized_address = (
            node.view.materialized_address.to_str()
            if node.view.materialized_address
            else None
        )
        return cls(
            urn=node.view.address.to_str(),
            node=node,
            view_id=node.view.view_id,
            dataset_id=node.view.dataset_id,
            description=node.view.description,
            view_query=node.view.view_query,
            materialized_address=materialized_address,
            state_code=node.view.address.state_code_for_address(),
        )


@attr.define(kw_only=True)
class BigQuerySourceTableMetadata(BigQuerySourceTableNode):
    """Attr version of a fuller set of fields and metadata about a node that represents
    a source table
    """

    config: SourceTableConfig = attr.field()
    description: str = attr.field()
    # TODO(#46345): populate this w/ add'l info?? like schema? or yaml?

    @classmethod
    def from_source_table_config(
        cls, config: SourceTableConfig
    ) -> "BigQuerySourceTableMetadata":
        return cls(
            urn=config.address.to_str(),
            address=config.address,
            config=config,
            view_id=config.address.table_id,
            dataset_id=config.address.dataset_id,
            description=config.description,
            state_code=config.address.state_code_for_address(),
        )

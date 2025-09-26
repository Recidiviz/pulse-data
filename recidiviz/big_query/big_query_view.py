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
"""An implementation of bigquery.TableReference with extra functionality related to views."""
import abc
import hashlib
import json
from typing import Any, Generic, List, Optional, Set, TypeVar

from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_query_builder import BigQueryQueryBuilder
from recidiviz.big_query.big_query_query_provider import BigQueryQueryProvider
from recidiviz.big_query.big_query_utils import build_lineage_link_description
from recidiviz.big_query.big_query_view_sandbox_context import (
    BigQueryViewSandboxContext,
)
from recidiviz.big_query.constants import BQ_VIEW_QUERY_MAX_LENGTH
from recidiviz.utils import metadata

_DEFAULT_MATERIALIZED_SUFFIX = "_materialized"
BQ_TABLE_DESCRIPTION_MAX_LENGTH = 2**14


# TODO(#25330): Remove the inheritance from TableReference
class BigQueryView(bigquery.TableReference, BigQueryQueryProvider):
    """An implementation of bigquery.TableReference with extra functionality related to views."""

    def __init__(
        self,
        *,
        project_id: Optional[str] = None,
        dataset_id: str,
        view_id: str,
        bq_description: str,
        description: str,
        view_query_template: str,
        # Address this table will be materialized to, if this is a view that is
        # materialized.
        materialized_address: Optional[BigQueryAddress] = None,
        materialized_table_schema: Optional[list[bigquery.SchemaField]] = None,
        sandbox_context: BigQueryViewSandboxContext | None = None,
        clustering_fields: Optional[List[str]] = None,
        time_partitioning: bigquery.TimePartitioning | None = None,
        **query_format_kwargs: Any,
    ) -> None:
        if sandbox_context:
            sandbox_view_address = sandbox_context.sandbox_view_address(
                original_view_address=BigQueryAddress(
                    dataset_id=dataset_id, table_id=view_id
                )
            )
            sandbox_materialized_address = sandbox_context.sandbox_materialized_address(
                original_materialized_address=materialized_address
            )
            dataset_id = sandbox_view_address.dataset_id
            view_id = sandbox_view_address.table_id
            materialized_address = sandbox_materialized_address

        if materialized_table_schema is not None and materialized_address is None:
            raise ValueError(
                "Cannot set materialized_table_schema if materialized_address is not set."
            )

        if project_id is None:
            project_id = metadata.project_id()

            if not project_id:
                raise ValueError(
                    f"Found no project_id set instantiating view [{view_id}]"
                )

        dataset_ref = bigquery.DatasetReference.from_string(
            dataset_id, default_project=project_id
        )
        super().__init__(dataset_ref, view_id)
        self.query_format_kwargs = query_format_kwargs
        self.query_builder = BigQueryQueryBuilder(
            parent_address_overrides=(
                sandbox_context.parent_address_overrides if sandbox_context else None
            ),
            parent_address_formatter_provider=(
                sandbox_context.parent_address_formatter_provider
                if sandbox_context
                else None
            ),
        )

        self._bq_description = bq_description + build_lineage_link_description(
            view_id=view_id,
            dataset_id=dataset_id,
            non_empty_description=bool(bq_description),
        )
        self._description = description + build_lineage_link_description(
            view_id=view_id,
            dataset_id=dataset_id,
            non_empty_description=bool(description),
        )
        self._view_query_template = view_query_template

        try:
            self._view_query = self.query_builder.build_query(
                project_id=self.project,
                query_template=self.view_query_template,
                query_format_kwargs=query_format_kwargs,
            )
        except ValueError as e:
            raise ValueError(f"Unable to format view query for {self.address}") from e

        if len(self._view_query) > BQ_VIEW_QUERY_MAX_LENGTH:
            raise ValueError(
                f"Query for view [{dataset_id}.{view_id}] is [{len(self._view_query)}] "
                f"chars long which is more than the max allowed limit of "
                f"{BQ_VIEW_QUERY_MAX_LENGTH} chars."
            )

        self._view_query_signature: str | None = None

        if materialized_address == self.address:
            raise ValueError(
                f"Materialized address [{materialized_address}] cannot be same as view "
                f"itself."
            )
        self._materialized_address = materialized_address
        self._clustering_fields = clustering_fields
        self._time_partitioning = time_partitioning
        self.materialized_table_schema = materialized_table_schema

    @property
    def view_id(self) -> str:
        return self.table_id

    @property
    def description(self) -> str:
        """The full description for this view which will be deployed to Gitbook."""
        return self._description

    @property
    def bq_description(self) -> str:
        """The BigQuery-compatible description for this view which is short enough to
        be saved as a table description in BigQuery. This will generally be the same
        as the description returned by description() above, but users may choose to
        provide an abbreviated description for this field where necessary.
        """
        if len(self._bq_description) > BQ_TABLE_DESCRIPTION_MAX_LENGTH:
            raise ValueError(
                f"Description for view [{self.address.to_str()}] is too long to deploy to"
                f"BigQuery. Consider setting a shorter, alternate description via the "
                f"`bq_description` field."
            )
        return self._bq_description

    @property
    def materialized_table_bq_description(self) -> str:
        """The BigQuery-compatible description for this view's materialized table."""
        if not self.materialized_address:
            raise ValueError(
                f"Can only get the materialized table BQ address for materialized "
                f"views. No materialized address found for view "
                f"[{self.address.to_str()}]"
            )
        description_prefix = (
            f"Materialized data from view [{self.address.to_str()}]. "
            f"View description:\n"
        )
        table_description = f"{description_prefix}{self._bq_description}"
        if len(table_description) > BQ_TABLE_DESCRIPTION_MAX_LENGTH:
            raise ValueError(
                f"Materialized table description for view [{self.address.to_str()}] is "
                f"too long to deploy to BigQuery. Consider setting a shorter, "
                f"alternate description via the `bq_description` field."
            )
        return table_description

    # BigQueryQueryProvider implementation
    def get_query(self) -> str:
        return self.view_query

    @property
    def view_query(self) -> str:
        return self._view_query

    @property
    def view_query_signature(self) -> str:
        """Returns the SHA256 hash of this view's view_query. Can be used to determine
        if a view query has changed across view update runs.
        """
        if not self._view_query_signature:
            self._view_query_signature = hashlib.sha256(
                self.view_query.encode("utf-8")
            ).hexdigest()
        return self._view_query_signature

    @property
    def view_query_template(self) -> str:
        return self._view_query_template

    @property
    def direct_select_query(self) -> str:
        """Returns a SELECT * query that queries the view directly. Should be used only
        when we need 100% live data from this view. In most cases, you should use
        |select_query| instead, which will query from the materialized table if there
        is one.
        """
        return self.address.to_project_specific_address(self.project).select_query()

    @property
    def select_query(self) -> str:
        return self.table_for_query.to_project_specific_address(
            self.project
        ).select_query()

    @property
    def address(self) -> BigQueryAddress:
        """The (dataset_id, table_id) address for this view"""
        return BigQueryAddress(dataset_id=self.dataset_id, table_id=self.view_id)

    @property
    def materialized_address(self) -> Optional[BigQueryAddress]:
        """The (dataset_id, table_id) for a table that contains the result of the
        view_query if this view were to be materialized.
        """
        return self._materialized_address

    @property
    def table_for_query(self) -> BigQueryAddress:
        """The (dataset_id, table_id) to use when querying from this view.

        Will return the materialized view address when available, otherwise the plain
        view address."""

        if self.materialized_address:
            return self.materialized_address
        return self.address

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"view={self.project}.{self.dataset_id}.{self.view_id}, "
            f"view_query='{self.view_query}')"
        )

    def __lt__(self, other: Any) -> bool:
        if other.__class__ is not self.__class__:
            return NotImplemented
        return self.address < other.address

    @property
    def clustering_fields(self) -> Optional[List[str]]:
        return self._clustering_fields

    @property
    def clustering_fields_string(self) -> str | None:
        """String representation of clustering_fields which can be easily saved as a
        STRING BigQuery value.
        """
        if not self._clustering_fields:
            return None

        return ",".join(self._clustering_fields)

    @property
    def time_partitioning(self) -> bigquery.TimePartitioning | None:
        return self._time_partitioning

    @property
    def time_partitioning_string(self) -> str | None:
        """String representation of time_partitioning which can be easily saved as a
        STRING BigQuery value.
        """
        if not self._time_partitioning:
            return None

        time_partitioning_dict = self._time_partitioning.to_api_repr()
        return json.dumps(time_partitioning_dict, sort_keys=True)


BigQueryViewType = TypeVar("BigQueryViewType", bound=BigQueryView)


class BigQueryViewBuilder(Generic[BigQueryViewType]):
    """Abstract interface for a class that builds a BigQueryView."""

    dataset_id: str
    view_id: str
    materialized_address: Optional[BigQueryAddress]

    # The set of project ids to deploy this view in. If None, deploys this in all
    # projects. If an empty set, does not deploy in any projects.
    projects_to_deploy: Optional[Set[str]]

    # TODO(#7453): This functionality should be shared between *View and *ViewBuilder classes.
    @property
    def address(self) -> BigQueryAddress:
        """Returns the address of this view, with no dataset overrides applied."""
        return BigQueryAddress(dataset_id=self.dataset_id, table_id=self.view_id)

    # TODO(#7453): This functionality should be shared between *View and *ViewBuilder classes.
    @property
    def table_for_query(self) -> BigQueryAddress:
        """The (dataset_id, table_id) to use when querying from this view.

        Will return the materialized view address when available, otherwise the plain
        view address."""

        if self.materialized_address:
            return self.materialized_address
        return self.address

    def build(
        self, *, sandbox_context: BigQueryViewSandboxContext | None = None
    ) -> BigQueryViewType:
        """Builds and returns the view object."""
        return self._build(sandbox_context=sandbox_context)

    @abc.abstractmethod
    def _build(
        self, *, sandbox_context: BigQueryViewSandboxContext | None
    ) -> BigQueryViewType:
        """Should be implemented by subclasses to build the view."""

    def build_and_print(self) -> None:
        """Builds the view and prints it to stdout."""
        view = self.build()
        print(view.view_query)

    def should_deploy_in_project(self, project_id: str) -> bool:
        return self.projects_to_deploy is None or project_id in self.projects_to_deploy

    @classmethod
    def _build_materialized_address(
        cls,
        dataset_id: str,
        view_id: str,
        should_materialize: bool,
        materialized_address_override: Optional[BigQueryAddress],
    ) -> Optional[BigQueryAddress]:
        """The (dataset_id, table_id) for a table that contains the result of the
        view_query if this view were to be materialized.
        """

        if not should_materialize and materialized_address_override:
            raise ValueError(
                f"Found nonnull materialized_address_override "
                f"[{materialized_address_override}] when `should_materialize` is not "
                f"True. Must set `should_materialize` to set address override."
            )

        if not should_materialize:
            return None

        if materialized_address_override:
            return materialized_address_override

        return cls.build_standard_materialized_address(
            dataset_id=dataset_id, view_id=view_id
        )

    @classmethod
    def build_standard_materialized_address(
        cls, *, dataset_id: str, view_id: str
    ) -> BigQueryAddress:
        """Returns the standard materialized location for a given BigQueryView address.
        This address is what would be used if no materialized_address_override is used
        when building a view.
        """
        return BigQueryAddress(
            dataset_id=dataset_id,
            table_id=view_id + _DEFAULT_MATERIALIZED_SUFFIX,
        )


BigQueryViewBuilderType = TypeVar("BigQueryViewBuilderType", bound=BigQueryViewBuilder)


class SimpleBigQueryViewBuilder(BigQueryViewBuilder[BigQueryView]):
    """Class that builds a BigQueryView. Can be instantiated as a top-level variable, unlike a BigQueryView, whose
    constructor requires that a project_id has been properly set in the metadata package - something that often happens
    after a file is imported."""

    def __init__(
        self,
        *,
        dataset_id: str,
        view_id: str,
        description: str,
        view_query_template: str,
        bq_description: Optional[str] = None,
        should_materialize: bool = False,
        projects_to_deploy: Optional[Set[str]] = None,
        materialized_address_override: Optional[BigQueryAddress] = None,
        materialized_table_schema: Optional[list[bigquery.SchemaField]] = None,
        clustering_fields: Optional[List[str]] = None,
        time_partitioning: bigquery.TimePartitioning | None = None,
        # All query format kwargs args must have string values
        **query_format_kwargs: str,
    ):
        if materialized_table_schema is not None and not should_materialize:
            raise ValueError(
                "Cannot set materialized_table_schema if should_materialize is False."
            )

        self.dataset_id = dataset_id
        self.view_id = view_id
        self.projects_to_deploy = projects_to_deploy
        self.description = description
        self.bq_description = bq_description or description
        self.view_query_template = view_query_template
        self.clustering_fields = clustering_fields
        self.time_partitioning = time_partitioning
        self.materialized_table_schema = materialized_table_schema
        self.query_format_kwargs = query_format_kwargs
        self.materialized_address = self._build_materialized_address(
            dataset_id=dataset_id,
            view_id=view_id,
            materialized_address_override=materialized_address_override,
            should_materialize=should_materialize,
        )

    def _build(
        self, *, sandbox_context: BigQueryViewSandboxContext | None
    ) -> BigQueryView:
        return BigQueryView(
            dataset_id=self.dataset_id,
            view_id=self.view_id,
            description=self.description,
            bq_description=self.bq_description,
            view_query_template=self.view_query_template,
            materialized_address=self.materialized_address,
            clustering_fields=self.clustering_fields,
            time_partitioning=self.time_partitioning,
            sandbox_context=sandbox_context,
            materialized_table_schema=self.materialized_table_schema,
            **self.query_format_kwargs,
        )

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
import re
from typing import Any, Callable, Dict, Generic, List, Optional, Set, TypeVar

from google.cloud import bigquery

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.utils import metadata
from recidiviz.utils.string import StrictStringFormatter

PROJECT_ID_KEY = "project_id"
_DEFAULT_MATERIALIZED_SUFFIX = "_materialized"


class BigQueryView(bigquery.TableReference):
    """An implementation of bigquery.TableReference with extra functionality related to views."""

    # Description and project_id are required arguments to BigQueryView itself and may
    # also, but not always, be used in the query text. The BigQueryView class does not
    # know whether they will be used so it always makes them available to the query. If
    # the query does not use them this is not indicative of a bug.
    QUERY_FORMATTER = StrictStringFormatter(
        allowed_unused_keywords=frozenset({"description", "project_id"})
    )

    def __init__(
        self,
        *,
        project_id: Optional[str] = None,
        dataset_id: str,
        view_id: str,
        description: str,
        view_query_template: str,
        # Address this table will be materialized to, if this is a view that is
        # materialized.
        materialized_address: Optional[BigQueryAddress] = None,
        address_overrides: Optional[BigQueryAddressOverrides] = None,
        clustering_fields: Optional[List[str]] = None,
        should_deploy_predicate: Optional[Callable[[], bool]] = None,
        **query_format_kwargs: Any,
    ) -> None:
        if address_overrides:
            original_address = BigQueryAddress(dataset_id=dataset_id, table_id=view_id)
            sandbox_address = address_overrides.get_sandbox_address(original_address)
            if sandbox_address:
                dataset_id = sandbox_address.dataset_id

                if materialized_address:
                    sandbox_materialized_address = (
                        address_overrides.get_sandbox_address(materialized_address)
                    )
                    if not sandbox_materialized_address:
                        raise ValueError(
                            f"Found override set for [{original_address}] but no override"
                            f"set for the materialized address [{materialized_address}]"
                        )
                    materialized_address = sandbox_materialized_address

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
        self.query_format_kwargs = {
            **query_format_kwargs,
            "description": description,
        }
        self._view_id = view_id
        self._description = description
        self._view_query_template = view_query_template
        view_query_no_overrides = self._format_view_query(
            view_query_template, inject_project_id=True, **self.query_format_kwargs
        )
        self._view_query = (
            self._apply_overrides_to_view_query(
                view_query_no_overrides, address_overrides
            )
            if address_overrides
            else view_query_no_overrides
        )
        self._parent_tables: Set[BigQueryAddress] = self._parse_parent_tables(
            self._view_query
        )
        if materialized_address == self.address:
            raise ValueError(
                f"Materialized address [{materialized_address}] cannot be same as view "
                f"itself."
            )
        self._materialized_address = materialized_address
        self._clustering_fields = clustering_fields

        # Returns whether it's safe to deploy this view.
        self._should_deploy_predicate = should_deploy_predicate

        # Cached result of self._should_deploy_predicate
        self._should_deploy: Optional[bool] = None

    def should_deploy(self) -> bool:
        """Returns whether it is safe to deploy this view. This may be an expensive
        call (e.g. it might query against BQ).
        """
        if self._should_deploy is None:
            if self._should_deploy_predicate:
                self._should_deploy = self._should_deploy_predicate()
            else:
                self._should_deploy = True
        return self._should_deploy

    @classmethod
    def _format_view_query_without_project_id(
        cls, view_query_template: str, **query_format_kwargs: Any
    ) -> str:
        """Formats the given |view_query_template| string with the given arguments, without injecting a value for the
        PROJECT_ID_KEY."""
        query_format_args = {
            PROJECT_ID_KEY: f"{{{PROJECT_ID_KEY}}}",
            **query_format_kwargs,
        }

        return cls.QUERY_FORMATTER.format(view_query_template, **query_format_args)

    def _format_view_query(
        self,
        view_query_template: str,
        inject_project_id: bool,
        **query_format_kwargs: Any,
    ) -> str:
        """This builds the view_query with the given query_format_kwargs. If |inject_project_id| is set to True, sets
        the PROJECT_ID_KEY value in the template to the current project. Else, returns the formatted view_query with the
        PROJECT_ID_KEY as {PROJECT_ID_KEY}."""

        try:

            view_query_no_project_id = self._format_view_query_without_project_id(
                view_query_template, **query_format_kwargs
            )

            if not inject_project_id:
                return view_query_no_project_id

            project_id_format_args = {PROJECT_ID_KEY: self.project}

            return self.QUERY_FORMATTER.format(
                view_query_no_project_id, **project_id_format_args
            )
        except ValueError as e:
            raise ValueError(f"Unable to format view query for {self.address}") from e

    def _apply_overrides_to_view_query(
        self, view_query: str, address_overrides: BigQueryAddressOverrides
    ) -> str:
        query_with_overrides = view_query
        referenced_parent_tables = self._parse_parent_tables(view_query)
        for parent_table in referenced_parent_tables:
            if override := address_overrides.get_sandbox_address(address=parent_table):
                query_with_overrides = query_with_overrides.replace(
                    f"`{self.project}.{parent_table.dataset_id}.{parent_table.table_id}`",
                    f"`{self.project}.{override.dataset_id}.{override.table_id}`",
                )
        return query_with_overrides

    def _query_format_args_with_project_id(
        self, **query_format_kwargs: Any
    ) -> Dict[str, str]:
        return {PROJECT_ID_KEY: self.project, **query_format_kwargs}

    @property
    def view_id(self) -> str:
        return self.table_id

    @property
    def description(self) -> str:
        return self._description

    @property
    def view_query(self) -> str:
        return self._view_query

    @property
    def view_query_template(self) -> str:
        return self._view_query_template

    @property
    def parent_tables(self) -> Set[BigQueryAddress]:
        """Returns a set of set of addresses for tables/views referenced in the fully
        formatted view query.
        """
        return self._parent_tables

    @staticmethod
    def _parse_parent_tables(view_query: str) -> Set[BigQueryAddress]:
        """Returns a set of set of addresses for tables/views referenced in the provided
        |view_query|.
        """
        parents = re.findall(r"`[\w-]*\.([\w-]*)\.([\w-]*)`", view_query)
        return {
            BigQueryAddress(dataset_id=candidate[0], table_id=candidate[1])
            for candidate in parents
        }

    @property
    def direct_select_query(self) -> str:
        """Returns a SELECT * query that queries the view directly. Should be used only
        when we need 100% live data from this view. In most cases, you should use
        |select_query| instead, which will query from the materialized table if there
        is one.
        """
        return f"SELECT * FROM `{self.project}.{self.address.dataset_id}.{self.address.table_id}`"

    @property
    def select_query(self) -> str:
        t = self.table_for_query
        return f"SELECT * FROM `{self.project}.{t.dataset_id}.{t.table_id}`"

    @property
    def select_query_uninjected_project_id(self) -> str:
        """This should be used when building another view template that will ultimately be passed to another
        BigQueryView."""
        return f"SELECT * FROM `{{project_id}}.{self.dataset_id}.{self.view_id}`"

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

    @property
    def clustering_fields(self) -> Optional[List[str]]:
        return self._clustering_fields


BigQueryViewType = TypeVar("BigQueryViewType", bound=BigQueryView)


class BigQueryViewBuilder(Generic[BigQueryViewType]):
    """Abstract interface for a class that builds a BigQueryView."""

    dataset_id: str
    view_id: str
    materialized_address: Optional[BigQueryAddress]

    # The set of project ids to deploy this view in. If None, deploys this in all
    # projects. If an empty set, does not deploy in any projects.
    projects_to_deploy: Optional[Set[str]]

    def build(
        self,
        *,
        address_overrides: Optional[BigQueryAddressOverrides] = None,
    ) -> BigQueryViewType:
        """Builds and returns the view object."""
        return self._build(address_overrides=address_overrides)

    @abc.abstractmethod
    def _build(
        self, *, address_overrides: Optional[BigQueryAddressOverrides] = None
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
        should_materialize: bool = False,
        projects_to_deploy: Optional[Set[str]] = None,
        materialized_address_override: Optional[BigQueryAddress] = None,
        should_deploy_predicate: Optional[Callable[[], bool]] = None,
        clustering_fields: Optional[List[str]] = None,
        # All query format kwargs args must have string values
        **query_format_kwargs: str,
    ):
        self.dataset_id = dataset_id
        self.view_id = view_id
        self.projects_to_deploy = projects_to_deploy
        self.description = description
        self.view_query_template = view_query_template
        self.should_deploy_predicate = should_deploy_predicate
        self.clustering_fields = clustering_fields
        self.query_format_kwargs = query_format_kwargs
        self.materialized_address = self._build_materialized_address(
            dataset_id=dataset_id,
            view_id=view_id,
            materialized_address_override=materialized_address_override,
            should_materialize=should_materialize,
        )

    def _build(
        self, *, address_overrides: BigQueryAddressOverrides = None
    ) -> BigQueryView:
        return BigQueryView(
            dataset_id=self.dataset_id,
            view_id=self.view_id,
            description=self.description,
            view_query_template=self.view_query_template,
            materialized_address=self.materialized_address,
            clustering_fields=self.clustering_fields,
            address_overrides=address_overrides,
            should_deploy_predicate=self.should_deploy_predicate,
            **self.query_format_kwargs,
        )

    # TODO(#7453): This functionality should be shared between *View and *ViewBuilder classes.
    @property
    def address(self) -> BigQueryAddress:
        """The (dataset_id, table_id) address for this view"""
        return BigQueryAddress(dataset_id=self.dataset_id, table_id=self.view_id)

    @property
    def table_for_query(self) -> BigQueryAddress:
        """The (dataset_id, table_id) to use when querying from this view.

        Will return the materialized view address when available, otherwise the plain
        view address."""

        if self.materialized_address:
            return self.materialized_address
        return self.address

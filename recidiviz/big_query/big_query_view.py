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
        dataset_overrides: Optional[
            Dict[str, str]
        ] = None,  # 'original_name' -> 'prefix_original_name'
        clustering_fields: Optional[List[str]] = None,
        **query_format_kwargs: Any,
    ) -> None:

        override_kwargs = {}
        if dataset_overrides:
            override_kwargs = self._get_dataset_override_kwargs(
                dataset_overrides=dataset_overrides, **query_format_kwargs
            )

            if override_kwargs and dataset_id not in dataset_overrides:
                raise ValueError(
                    f"Dataset [{dataset_id}] for view [{view_id}] not found in dataset_overrides even though dependent "
                    f"table datasets are overwritten: [{override_kwargs}]. You cannot write a view with "
                    f"overwritten kwargs to its standard dataset."
                )

            if dataset_id in dataset_overrides:
                dataset_id = dataset_overrides[dataset_id]

            if (
                materialized_address
                and materialized_address.dataset_id in dataset_overrides
            ):
                updated_materialized_dataset_override = dataset_overrides[
                    materialized_address.dataset_id
                ]
                materialized_address = BigQueryAddress(
                    dataset_id=updated_materialized_dataset_override,
                    table_id=materialized_address.table_id,
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
        self.query_format_kwargs = {
            **query_format_kwargs,
            **override_kwargs,
            "description": description,
        }
        self._view_id = view_id
        self._description = description
        self._view_query_template = view_query_template
        self._view_query = self._format_view_query(
            view_query_template, inject_project_id=True, **self.query_format_kwargs
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

    @classmethod
    def _get_dataset_override_kwargs(
        cls, *, dataset_overrides: Dict[str, str], **query_format_kwargs: Any
    ) -> Dict[str, str]:
        overrides = {}
        for key, original_value in query_format_kwargs.items():
            if original_value in dataset_overrides:
                if not key.endswith("_dataset") and not key.endswith("_dataset_id"):
                    raise ValueError(
                        f"Keyword arg key [{key}] with dataset overridden value [{original_value}] "
                        f"does not have an expected suffix."
                    )
                new_value = dataset_overrides[original_value]
                overrides[key] = new_value

        return overrides

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
        dataset_overrides: Optional[Dict[str, str]] = None,
        override_should_build_predicate: bool = False,
    ) -> BigQueryViewType:
        """Builds and returns the view object. Throws an exception of type  `BigQueryViewBuilderShouldNotBuildError`
        if `should_build()` is false for this view."""
        if not override_should_build_predicate and not self.should_build():
            raise BigQueryViewBuilderShouldNotBuildError()
        return self._build(dataset_overrides=dataset_overrides)

    @abc.abstractmethod
    def _build(
        self, *, dataset_overrides: Optional[Dict[str, str]] = None
    ) -> BigQueryViewType:
        """Should be implemented by subclasses to build the view. Will only be called if should_build() returns True."""

    def build_and_print(self) -> None:
        """Builds the view and prints it to stdout."""
        view = self.build()
        print(view.view_query)

    @abc.abstractmethod
    def should_build(self) -> bool:
        """Should be implemented by subclasses to return True if this view can be built
        (e.g. if all dependent tables /columns exist). Returns False otherwise."""

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
        should_build_predicate: Optional[Callable[[], bool]] = None,
        clustering_fields: Optional[List[str]] = None,
        # All query format kwargs args must have string values
        **query_format_kwargs: str,
    ):
        self.dataset_id = dataset_id
        self.view_id = view_id
        self.projects_to_deploy = projects_to_deploy
        self.description = description
        self.view_query_template = view_query_template
        self.should_build_predicate = should_build_predicate
        self.clustering_fields = clustering_fields
        self.query_format_kwargs = query_format_kwargs
        self.materialized_address = self._build_materialized_address(
            dataset_id=dataset_id,
            view_id=view_id,
            materialized_address_override=materialized_address_override,
            should_materialize=should_materialize,
        )

    def _build(self, *, dataset_overrides: Dict[str, str] = None) -> BigQueryView:
        return BigQueryView(
            dataset_id=self.dataset_id,
            view_id=self.view_id,
            description=self.description,
            view_query_template=self.view_query_template,
            materialized_address=self.materialized_address,
            clustering_fields=self.clustering_fields,
            dataset_overrides=dataset_overrides,
            **self.query_format_kwargs,
        )

    def should_build(self) -> bool:
        return not self.should_build_predicate or self.should_build_predicate()

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


class BigQueryViewBuilderShouldNotBuildError(Exception):
    """Error thrown when the should_build() check for a BigQueryView fails."""

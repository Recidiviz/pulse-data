# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
from typing import Any, Callable, Generic, Optional, Dict, TypeVar

from google.cloud import bigquery

from recidiviz.utils import metadata
from recidiviz.utils.environment import GCP_PROJECTS

PROJECT_ID_KEY = "project_id"
MATERIALIZED_SUFFIX = "_materialized"


class BigQueryView(bigquery.TableReference):
    """An implementation of bigquery.TableReference with extra functionality related to views."""

    def __init__(
        self,
        *,
        project_id: Optional[str] = None,
        dataset_id: str,
        view_id: str,
        description: str,
        view_query_template: str,
        should_materialize: bool = False,
        dataset_overrides: Optional[
            Dict[str, str]
        ] = None,  # 'original_name' -> 'prefix_original_name'
        **query_format_kwargs: Any,
    ):
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

        if project_id is None:
            project_id = metadata.project_id()

            if not project_id:
                raise ValueError(
                    f"Found no project_id set instantiating view [{view_id}]"
                )

        _validate_view_query_template(dataset_id, view_id, view_query_template)

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
        self._view_query = self._format_view_query(
            view_query_template, inject_project_id=True, **self.query_format_kwargs
        )
        self._should_materialize = should_materialize

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

        return view_query_template.format(**query_format_args)

    def _format_view_query(
        self,
        view_query_template: str,
        inject_project_id: bool,
        **query_format_kwargs: Any,
    ) -> str:
        """This builds the view_query with the given query_format_kwargs. If |inject_project_id| is set to True, sets
        the PROJECT_ID_KEY value in the template to the current project. Else, returns the formatted view_query with the
        PROJECT_ID_KEY as {PROJECT_ID_KEY}."""

        view_query_no_project_id = self._format_view_query_without_project_id(
            view_query_template, **query_format_kwargs
        )

        if not inject_project_id:
            return view_query_no_project_id

        project_id_format_args = {PROJECT_ID_KEY: self.project}

        return view_query_no_project_id.format(**project_id_format_args)

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
    def select_query(self) -> str:
        return f"SELECT * FROM `{self.project}.{self.dataset_id}.{self.view_id}`"

    @property
    def select_query_uninjected_project_id(self) -> str:
        """This should be used when building another view template that will ultimately be passed to another
        BigQueryView."""
        return f"SELECT * FROM `{{project_id}}.{self.dataset_id}.{self.view_id}`"

    @property
    def materialized_view_table_id(self) -> Optional[str]:
        """The table_id for a table that contains the result of the view_query if this view were to be materialized."""
        return self.view_id + MATERIALIZED_SUFFIX if self._should_materialize else None

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"view={self.project}.{self.dataset_id}.{self.view_id}, "
            f"view_query='{self.view_query}')"
        )


def _validate_view_query_template(
    dataset_id: str, view_id: str, view_query_template: str
) -> None:
    """Validates that the view_query_template does not contain any raw GCP project_id values. Note that this prevents
    views from referencing project IDs directly in any comments or view descriptions."""
    for project_id in GCP_PROJECTS:
        if project_id in view_query_template:
            raise ValueError(
                f"view_query_template for view {dataset_id}.{view_id} cannot contain raw"
                f" value: {project_id}."
            )


BigQueryViewType = TypeVar("BigQueryViewType", bound=BigQueryView)


class BigQueryViewBuilder(Generic[BigQueryViewType]):
    """Abstract interface for a class that builds a BigQueryView."""

    dataset_id: str
    view_id: str

    def build(
        self, *, dataset_overrides: Optional[Dict[str, str]] = None
    ) -> BigQueryViewType:
        """Builds and returns the view object. Throws an exception of type  `BigQueryViewBuilderShouldNotBuildError`
        if `should_build()` is false for this view."""
        if not self.should_build():
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


BigQueryViewBuilderType = TypeVar("BigQueryViewBuilderType", bound=BigQueryViewBuilder)


class SimpleBigQueryViewBuilder(BigQueryViewBuilder):
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
        should_build_predicate: Optional[Callable[[], bool]] = None,
        # All query format kwargs args must have string values
        **query_format_kwargs: str,
    ):
        self.dataset_id = dataset_id
        self.view_id = view_id
        self.description = description
        self.view_query_template = view_query_template
        self.should_materialize = should_materialize
        self.should_build_predicate = should_build_predicate
        self.query_format_kwargs = query_format_kwargs

    def _build(self, *, dataset_overrides: Dict[str, str] = None) -> BigQueryView:
        return BigQueryView(
            dataset_id=self.dataset_id,
            view_id=self.view_id,
            description=self.description,
            view_query_template=self.view_query_template,
            should_materialize=self.should_materialize,
            dataset_overrides=dataset_overrides,
            **self.query_format_kwargs,
        )

    def should_build(self) -> bool:
        return not self.should_build_predicate or self.should_build_predicate()


class BigQueryViewBuilderShouldNotBuildError(Exception):
    """Error thrown when the should_build() check for a BigQueryView fails."""

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
from typing import Optional, Dict, TypeVar, Generic

from google.cloud import bigquery

from recidiviz.utils import metadata
from recidiviz.utils.environment import GCP_PROJECTS

PROJECT_ID_KEY = 'project_id'


class BigQueryView(bigquery.TableReference):
    """An implementation of bigquery.TableReference with extra functionality related to views."""
    def __init__(self,
                 *,
                 project_id: Optional[str] = None,
                 dataset_id: str,
                 view_id: str,
                 view_query_template: str,
                 should_materialize: bool = False,
                 **query_format_kwargs):

        if project_id is None:
            project_id = metadata.project_id()

            if not project_id:
                raise ValueError(f'Found no project_id set instantiating view [{view_id}]')

        _validate_view_query_template(dataset_id, view_id, view_query_template)

        dataset_ref = bigquery.DatasetReference.from_string(dataset_id,
                                                            default_project=project_id)
        super().__init__(dataset_ref, view_id)
        self._view_id = view_id
        self._view_query = self._format_view_query(view_query_template, inject_project_id=True, **query_format_kwargs)
        self._should_materialize = should_materialize

    @classmethod
    def _format_view_query_without_project_id(cls, view_query_template: str, **query_format_kwargs):
        """Formats the given |view_query_template| string with the given arguments, without injecting a value for the
        PROJECT_ID_KEY."""
        query_format_args = {
            PROJECT_ID_KEY: f'{{{PROJECT_ID_KEY}}}',
            **query_format_kwargs
        }

        return view_query_template.format(**query_format_args)

    def _format_view_query(self, view_query_template: str, inject_project_id: bool, **query_format_kwargs) -> str:
        """This builds the view_query with the given query_format_kwargs. If |inject_project_id| is set to True, sets
        the PROJECT_ID_KEY value in the template to the current project. Else, returns the formatted view_query with the
        PROJECT_ID_KEY as {PROJECT_ID_KEY}."""

        view_query_no_project_id = self._format_view_query_without_project_id(
            view_query_template, **query_format_kwargs)

        if not inject_project_id:
            return view_query_no_project_id

        project_id_format_args = {
            PROJECT_ID_KEY: self.project
        }

        return view_query_no_project_id.format(**project_id_format_args)

    def _query_format_args_with_project_id(self, **query_format_kwargs) -> Dict[str, str]:
        return {
            PROJECT_ID_KEY: self.project,
            **query_format_kwargs
        }

    @property
    def view_id(self) -> str:
        return self.table_id

    @property
    def view_query(self) -> str:
        return self._view_query

    @property
    def select_query(self) -> str:
        return f'SELECT * FROM `{self.project}.{self.dataset_id}.{self.view_id}`'

    @property
    def select_query_uninjected_project_id(self) -> str:
        """This should be used when building another view template that will ultimately be passed to another
        BigQueryView."""
        return f'SELECT * FROM `{{project_id}}.{self.dataset_id}.{self.view_id}`'

    @property
    def materialized_view_table_id(self) -> Optional[str]:
        """The table_id for a table that contains the result of the view_query if this view were to be materialized."""
        return self.view_id + '_materialized' if self._should_materialize else None

    def __repr__(self):
        return f'{self.__class__.__name__}(' \
            f'view={self.project}.{self.dataset_id}.{self.view_id}, ' \
            f'view_query=\'{self.view_query}\')'


def _validate_view_query_template(dataset_id: str, view_id: str, view_query_template: str):
    """Validates that the view_query_template does not contain any raw GCP project_id values. Note that this prevents
    views from referencing project IDs directly in any comments or view descriptions."""
    for project_id in GCP_PROJECTS:
        if project_id in view_query_template:
            raise ValueError(f"view_query_template for view {dataset_id}.{view_id} cannot contain raw"
                             f" value: {project_id}.")


BigQueryViewType = TypeVar('BigQueryViewType', bound=BigQueryView)


class BigQueryViewBuilder(Generic[BigQueryViewType]):
    """Abstract interface for a class that builds a BigQueryView."""

    @abc.abstractmethod
    def build(self) -> BigQueryViewType:
        pass

    @abc.abstractmethod
    def build_and_print(self) -> None:
        pass


class SimpleBigQueryViewBuilder(BigQueryViewBuilder):
    """Class that builds a BigQueryView. Can be instantiated as a top-level variable, unlike a BigQueryView, whose
    constructor requires that a project_id has been properly set in the metadata package - something that often happens
    after a file is imported."""

    def __init__(self,
                 *,
                 dataset_id: str,
                 view_id: str,
                 view_query_template: str,
                 **kwargs):
        self.dataset_id = dataset_id
        self.view_id = view_id
        self.view_query_template = view_query_template
        self.kwargs = kwargs

    def build(self) -> BigQueryView:
        return BigQueryView(
            dataset_id=self.dataset_id,
            view_id=self.view_id,
            view_query_template=self.view_query_template,
            **self.kwargs)

    def build_and_print(self):
        """Builds the BigQueryView and prints the view's view_query."""
        view = self.build()
        print(view.view_query)

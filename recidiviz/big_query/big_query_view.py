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

from recidiviz.utils import metadata, environment

PROJECT_ID_KEY = 'project_id'


@environment.test_only
def test_only_project_id() -> str:
    return 'test-only-project'


class BigQueryView(bigquery.TableReference):
    """An implementation of bigquery.TableReference with extra functionality related to views."""
    def __init__(self,
                 *,
                 project_id: Optional[str] = None,
                 dataset_id: str,
                 view_id: str,
                 view_query_template: str,
                 materialized_view_table_id: Optional[str] = None,
                 **query_format_kwargs):

        if project_id is None:
            project_id = metadata.project_id()

            if not project_id:
                # BigQueryViews are sometimes declared as top-level objects that are instantiated with file load. This
                # means this constructor might execute inside of an import and before a test has a chance to mock the
                # project id. This keeps us from constructing a DatasetReference with a None project_id, which will
                # throw.
                project_id = test_only_project_id()

        dataset_ref = bigquery.DatasetReference.from_string(dataset_id,
                                                            default_project=project_id)
        super().__init__(dataset_ref, view_id)
        self._view_id = view_id
        self._view_query = view_query_template.format(**self._query_format_args(**query_format_kwargs))
        self._materialized_view_table_id = materialized_view_table_id

    def _query_format_args(self, **query_format_kwargs) -> Dict[str, str]:
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
    def materialized_view_table_id(self) -> Optional[str]:
        """The table_id for a table that contains the result of the view_query if this view were to be materialized."""
        return self._materialized_view_table_id

    def __repr__(self):
        return f'{self.__class__.__name__}(' \
            f'view={self.project}.{self.dataset_id}.{self.view_id}, ' \
            f'view_query=\'{self.view_query}\')'


BigQueryViewType = TypeVar('BigQueryViewType', bound=BigQueryView)


class BigQueryViewBuilder(Generic[BigQueryViewType]):
    """Abstract interface for a class that builds a BigQueryView."""

    @abc.abstractmethod
    def build(self) -> BigQueryViewType:
        pass

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
"""Defines a BigQueryView that enforces that the output has the required columns."""
from typing import List, Dict, Optional

from recidiviz.big_query.big_query_view import BigQueryView, BigQueryViewBuilder


class SelectedColumnsBigQueryView(BigQueryView):
    """An extension of BigQueryView that adds the `columns` parameter, which provides
    the in-order name of the columns returned by the view.

    This field is, in particular, useful for CSV to Postgres exports in instances where migrations mean
    that our views and the Postgres schema may be slightly out of sync.
    """

    def __init__(self,
                 *,
                 dataset_id: str,
                 view_id: str,
                 view_query_template: str,
                 columns: List[str],
                 should_materialize: bool,
                 dataset_overrides: Optional[Dict[str, str]],
                 **query_format_kwargs: str):
        query_format_kwargs['columns'] = ',\n'.join(columns)
        super().__init__(dataset_id=dataset_id,
                         view_id=view_id,
                         view_query_template=view_query_template,
                         should_materialize=should_materialize,
                         dataset_overrides=dataset_overrides,
                         **query_format_kwargs)
        self.columns = columns

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}(' \
            f'view={self.project}.{self.dataset_id}.{self.view_id}, ' \
            f'view_query=\'{self.view_query}\', ' \
            f'columns={self.columns})'


class SelectedColumnsBigQueryViewBuilder(BigQueryViewBuilder[SelectedColumnsBigQueryView]):
    """Class that builds a SelectedColumnsBigQueryView. Can be instantiated as a top-level variable."""

    def __init__(self,
                 *,
                 dataset_id: str,
                 view_id: str,
                 view_query_template: str,
                 columns: List[str],
                 should_materialize: bool = False,
                 # All keyword args must have string values
                 **query_format_kwargs: str):
        self.dataset_id = dataset_id
        self.view_id = view_id
        self.view_query_template = view_query_template
        self.columns = columns
        self.should_materialize = should_materialize
        self.query_format_kwargs = query_format_kwargs

    def build(self, *, dataset_overrides: Optional[Dict[str, str]] = None) -> SelectedColumnsBigQueryView:
        return SelectedColumnsBigQueryView(
            dataset_id=self.dataset_id,
            view_id=self.view_id,
            view_query_template=self.view_query_template,
            columns=self.columns,
            should_materialize=self.should_materialize,
            dataset_overrides=dataset_overrides,
            **self.query_format_kwargs)

    def build_and_print(self) -> None:
        """Builds the SelectedColumnsBigQueryView and prints the view's view_query."""
        view = self.build()
        print(view.view_query)

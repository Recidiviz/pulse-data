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

"""An extension of BigQueryView with extra functionality related to metric views specifically."""

from typing import List, Dict, Optional

from recidiviz.big_query.big_query_view import BigQueryView, BigQueryViewBuilder

PROJECT_ID_KEY = 'project_id'


class MetricBigQueryView(BigQueryView):
    """An extension of BigQueryView with extra functionality related to metric views specifically."""

    def __init__(self,
                 *,
                 dataset_id: str,
                 view_id: str,
                 view_query_template: str,
                 dimensions: List[str],
                 should_materialize: bool,
                 dataset_overrides: Optional[Dict[str, str]],
                 **query_format_kwargs: str):
        super().__init__(dataset_id=dataset_id,
                         view_id=view_id,
                         view_query_template=view_query_template,
                         should_materialize=should_materialize,
                         dataset_overrides=dataset_overrides,
                         **query_format_kwargs)
        self.dimensions = dimensions

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}(' \
            f'view={self.project}.{self.dataset_id}.{self.view_id}, ' \
            f'view_query=\'{self.view_query}\', ' \
            f'dimensions={self.dimensions})'


class MetricBigQueryViewBuilder(BigQueryViewBuilder[MetricBigQueryView]):
    """Class that builds a MetricBigQueryView. Can be instantiated as a top-level variable, unlike a BigQueryView, whose
    constructor requires that a project_id has been properly set in the metadata package - something that often happens
    after a file is imported."""

    def __init__(self,
                 *,
                 dataset_id: str,
                 view_id: str,
                 view_query_template: str,
                 dimensions: List[str],
                 should_materialize: bool = False,
                 # All keyword args must have string values
                 **query_format_kwargs: str):
        self.dataset_id = dataset_id
        self.view_id = view_id
        self.view_query_template = view_query_template
        self.dimensions = dimensions
        self.should_materialize = should_materialize
        self.query_format_kwargs = query_format_kwargs

    def build(self, *, dataset_overrides: Optional[Dict[str, str]] = None) -> MetricBigQueryView:
        return MetricBigQueryView(
            dataset_id=self.dataset_id,
            view_id=self.view_id,
            view_query_template=self.view_query_template,
            dimensions=self.dimensions,
            should_materialize=self.should_materialize,
            dataset_overrides=dataset_overrides,
            **self.query_format_kwargs)

    def build_and_print(self) -> None:
        """Builds the MetricBigQueryView and prints the view's view_query."""
        view = self.build()
        print(view.view_query)

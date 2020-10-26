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
"""Config classes for exporting metric views to Google Cloud Storage."""
import re
from typing import List, Optional, Sequence

import attr

from recidiviz.big_query.export.export_query_config import ExportBigQueryViewConfig
from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath
from recidiviz.metrics.metric_big_query_view import MetricBigQueryView, MetricBigQueryViewBuilder


def is_state_code(export_job_filter: str) -> bool:
    return re.search(r'^US_.{2}$', export_job_filter) is not None


class ExportMetricBigQueryViewConfig(ExportBigQueryViewConfig[MetricBigQueryView]):
    """Extends the ExportBigQueryViewConfig specifically for MetricBigQueryView."""


@attr.s(frozen=True)
class ExportMetricDatasetConfig:
    """Stores information necessary for exporting metric data from a list of views in a dataset to a Google Cloud
    Storage Bucket."""

    # The dataset_id where the views live
    dataset_id: str = attr.ib()

    # The list of metric views to be exported
    metric_view_builders_to_export: List[MetricBigQueryViewBuilder] = attr.ib()

    # A string template defining the output URI path for the destination directory of the export
    output_directory_uri_template: str = attr.ib()

    # If this export should filter tables by state-specific output, this is the state_code that should be exported
    state_code_filter: Optional[str] = attr.ib()

    # The name of the export config, used to filter to exports with specific names
    export_name: Optional[str] = attr.ib()

    def matches_filter(self, export_job_filter: Optional[str] = None) -> bool:
        if export_job_filter is None:
            return True
        return self.export_name == export_job_filter or (
                    is_state_code(export_job_filter) and self.state_code_filter == export_job_filter)

    def export_configs_for_views_to_export(self, project_id: str) -> Sequence[ExportMetricBigQueryViewConfig]:
        """Builds a list of ExportMetricBigQueryViewConfigs that define how all metric views in
        metric_view_builders_to_export should be exported to Google Cloud Storage."""
        view_filter_clause = (f" WHERE state_code = '{self.state_code_filter}'"
                              if self.state_code_filter else None)

        intermediate_table_name = "{export_view_name}_table"
        output_directory = self.output_directory_uri_template.format(
            project_id=project_id
        )

        if self.state_code_filter:
            intermediate_table_name += f"_{self.state_code_filter}"
            output_directory += f"/{self.state_code_filter}"

        return [
            ExportMetricBigQueryViewConfig(
                view=view,
                view_filter_clause=view_filter_clause,
                intermediate_table_name=intermediate_table_name.format(
                    export_view_name=view.view_id
                ),
                output_directory=GcsfsDirectoryPath.from_absolute_path(output_directory),
            )
            for view in [vb.build() for vb in self.metric_view_builders_to_export]
        ]

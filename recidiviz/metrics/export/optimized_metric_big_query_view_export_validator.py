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

"""View Validator implementation for the optimized metric file representation, checking that the metric file's
total number of data points is greater than 0."""

from recidiviz.big_query.export.big_query_view_export_validator import BigQueryViewExportValidator
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath


class OptimizedMetricBigQueryViewExportValidator(BigQueryViewExportValidator):
    """View exporter which checks the metadata for the object at the given path and ensures that there is a
    total_data_points key with an integer value greater than 0."""

    def validate(self, path: GcsfsFilePath) -> bool:
        metadata = self.fs.get_metadata(path)
        if not metadata:
            return False

        data_points = metadata.get('total_data_points')
        try:
            total_data_points = int(data_points)
            return total_data_points > 0
        except ValueError:
            return False

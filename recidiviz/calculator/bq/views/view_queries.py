# Recidiviz - a platform for tracking granular recidivism metrics in real time
# Copyright (C) 2019 Recidiviz, Inc.
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

"""BigQuery View query definitions.

Views should reference the raw data from the base tables Dataset:
    export_config.BASE_TABLES_BQ_DATASET

To have a View exported to BigQuery, list the view in:
recidiviz.calculator.bq.views.view_manager.VIEWS_TO_UPDATE

If views are derived from other views, they must be listed after the views
they rely on in
recidiviz.calculator.bq.views.view_manager.VIEWS_TO_UPDATE
"""
from recidiviz.calculator.bq.views.bqview import BigQueryView

from recidiviz.calculator.bq import export_config
from recidiviz.utils import metadata


PROJECT_ID = metadata.project_id()
BASE_DATASET = export_config.BASE_TABLES_BQ_DATASET

# pylint: disable=pointless-string-statement
# We want pointless string statements in this file to allow for long comments.


"""Example View that computes a count of all unique people in the database."""
PERSON_COUNT_VIEW = BigQueryView(
    view_id='person_count_view',
    view_query=(
        'SELECT COUNT(DISTINCT(person_id)) AS person_count '
        'FROM `{project_id}.{base_dataset}.person`'.format(
            project_id=PROJECT_ID,
            base_dataset=BASE_DATASET
        )
    )
)

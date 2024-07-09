# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Script to write Case Insights data to BigQuery
to be called only within the Airflow DAG's KubernetesPodOperator."""
from recidiviz.entrypoints.sentencing.write_case_insights_data_to_bq import (
    write_case_insights_data_to_bq,
)
from recidiviz.utils import metadata
from recidiviz.utils.metadata import local_project_id_override

if __name__ == "__main__":
    with local_project_id_override("recidiviz-staging"):
        write_case_insights_data_to_bq(project_id=metadata.project_id())

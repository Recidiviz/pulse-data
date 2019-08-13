# Recidiviz - a data platform for criminal justice reform
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
"""Most recent calculate job_id."""
# pylint: disable=trailing-whitespace, line-too-long
from recidiviz.calculator.bq import bqview
from recidiviz.calculator.bq.dashboard.views import view_config
from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
METRICS_DATASET = view_config.DATAFLOW_METRICS_DATASET

MOST_RECENT_CALCULATE_JOB_VIEW_NAME = 'most_recent_calculate_job'

MOST_RECENT_CALCULATE_JOB_DESCRIPTION = \
    """ Job ID of the most recent calculate job
    
    All job_ids begin with the format: 'YYYY-MM-DD_HH_MM_SS', so ordering
    by job_id gives us the most recent job. This format is true of both jobs
    run locally and run on Dataflow.
    """

MOST_RECENT_CALCULATE_JOB_QUERY = \
    """
    /*{description}*/

    SELECT state_code, job_id FROM `{project_id}.{metrics_dataset}.recidivism_rate_metrics`
    GROUP BY state_code, job_id
    ORDER BY state_code, job_id DESC
    LIMIT 1
    """.format(
        description=MOST_RECENT_CALCULATE_JOB_DESCRIPTION,
        project_id=PROJECT_ID,
        metrics_dataset=METRICS_DATASET,
    )

MOST_RECENT_CALCULATE_JOB_VIEW = bqview.BigQueryView(
    view_id=MOST_RECENT_CALCULATE_JOB_VIEW_NAME,
    view_query=MOST_RECENT_CALCULATE_JOB_QUERY
)

if __name__ == '__main__':
    print(MOST_RECENT_CALCULATE_JOB_VIEW.view_id)
    print(MOST_RECENT_CALCULATE_JOB_VIEW.view_query)

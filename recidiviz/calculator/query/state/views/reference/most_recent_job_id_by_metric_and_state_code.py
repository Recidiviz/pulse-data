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
"""Most recent calculate job_id by metric and state code."""
# pylint: disable=trailing-whitespace, line-too-long
from recidiviz.calculator.query import bqview
from recidiviz.calculator.query.state import view_config
from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
METRICS_DATASET = view_config.DATAFLOW_METRICS_DATASET

MOST_RECENT_JOB_ID_BY_METRIC_AND_STATE_CODE_VIEW_NAME = \
    'most_recent_job_id_by_metric_and_state_code'

MOST_RECENT_JOB_ID_BY_METRIC_AND_STATE_CODE_DESCRIPTION = \
    """ Job ID of the most recent calculate job by metric and state code.
    
    All job_ids begin with the format: 'YYYY-MM-DD_HH_MM_SS', so ordering
    by job_id gives us the most recent job. This format is true of both jobs
    run locally and run on Dataflow.
    """

MOST_RECENT_JOB_ID_BY_METRIC_AND_STATE_CODE_QUERY = \
    """
    /*{description}*/
    SELECT metric_type, state_code, job_id FROM
    (SELECT *, row_number() OVER (PARTITION BY state_code, metric_type ORDER BY job_id DESC) AS recency_rank FROM
    ((SELECT job_id, state_code, 'RECIDIVISM_COUNT' as metric_type FROM `{project_id}.{metrics_dataset}.recidivism_count_metrics`
    GROUP BY job_id, state_code)
    UNION ALL
    (SELECT job_id, state_code, 'RECIDIVISM_LIBERTY' as metric_type FROM `{project_id}.{metrics_dataset}.recidivism_liberty_metrics` 
    GROUP BY job_id, state_code)
    UNION ALL
    (SELECT job_id, state_code, 'RECIDIVISM_RATE' as metric_type FROM `{project_id}.{metrics_dataset}.recidivism_rate_metrics`  
    GROUP BY job_id, state_code)
    UNION ALL
    (SELECT job_id, state_code, 'INCARCERATION_ADMISSION' as metric_type FROM `{project_id}.{metrics_dataset}.incarceration_admission_metrics`   
    GROUP BY job_id, state_code)
    UNION ALL
    (SELECT job_id, state_code, 'INCARCERATION_POPULATION' as metric_type FROM `{project_id}.{metrics_dataset}.incarceration_population_metrics`    
    GROUP BY job_id, state_code)
    UNION ALL
    (SELECT job_id, state_code, 'INCARCERATION_RELEASE' as metric_type FROM `{project_id}.{metrics_dataset}.incarceration_release_metrics`     
    GROUP BY job_id, state_code)
    UNION ALL
    (SELECT job_id, state_code, 'SUPERVISION_ASSESSMENT_CHANGE' as metric_type FROM `{project_id}.{metrics_dataset}.terminated_supervision_assessment_score_change_metrics`      
    GROUP BY job_id, state_code)
    UNION ALL
    (SELECT job_id, state_code, 'SUPERVISION_POPULATION' as metric_type FROM `{project_id}.{metrics_dataset}.supervision_population_metrics`       
    GROUP BY job_id, state_code)
    UNION ALL
    (SELECT job_id, state_code, 'SUPERVISION_REVOCATION' as metric_type FROM `{project_id}.{metrics_dataset}.supervision_revocation_metrics`        
    GROUP BY job_id, state_code)
    UNION ALL
    (SELECT job_id, state_code, 'SUPERVISION_SUCCESS' as metric_type FROM `{project_id}.{metrics_dataset}.supervision_success_metrics`         
    GROUP BY job_id, state_code)
    UNION ALL
    (SELECT job_id, state_code, 'PROGRAM_REFERRAL' as metric_type FROM `{project_id}.{metrics_dataset}.program_referral_metrics`          
    GROUP BY job_id, state_code)))
    WHERE recency_rank = 1
    ORDER BY metric_type, state_code
    """.format(
        description=MOST_RECENT_JOB_ID_BY_METRIC_AND_STATE_CODE_DESCRIPTION,
        project_id=PROJECT_ID,
        metrics_dataset=METRICS_DATASET,
    )

MOST_RECENT_JOB_ID_BY_METRIC_AND_STATE_CODE_VIEW = bqview.BigQueryView(
    view_id=MOST_RECENT_JOB_ID_BY_METRIC_AND_STATE_CODE_VIEW_NAME,
    view_query=MOST_RECENT_JOB_ID_BY_METRIC_AND_STATE_CODE_QUERY
)

if __name__ == '__main__':
    print(MOST_RECENT_JOB_ID_BY_METRIC_AND_STATE_CODE_VIEW.view_id)
    print(MOST_RECENT_JOB_ID_BY_METRIC_AND_STATE_CODE_VIEW.view_query)

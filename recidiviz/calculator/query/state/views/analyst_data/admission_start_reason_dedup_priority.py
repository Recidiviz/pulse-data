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
"""Dedup priority for session start reasons"""
# pylint: disable=trailing-whitespace
# pylint: disable=line-too-long
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

ADMISSION_START_REASON_DEDUP_PRIORITY_VIEW_NAME = 'admission_start_reason_dedup_priority'

ADMISSION_START_REASON_DEDUP_PRIORITY_VIEW_DESCRIPTION = \
    """Dedup priority for session start reasons"""

ADMISSION_START_REASON_DEDUP_PRIORITY_QUERY_TEMPLATE = \
    """
    /*{description}*/
    SELECT 
        'SUPERVISION_START' AS metric_source,
        * 
    FROM UNNEST([
        'CONDITIONAL_RELEASE',
        'ABSCONSION',
        'COURT_SENTENCE',
        'RETURN_FROM_ABSCONSION',
        'TRANSFER_WITHIN_STATE',
        'TRANSFER_OUT_OF_STATE']) AS start_reason
    WITH OFFSET AS priority
    
    UNION ALL
    
    SELECT 
        'INCARCERATION_ADMISSION' AS metric_source,
        *
    FROM UNNEST([
        'PAROLE_REVOCATION',
        'PROBATION_REVOCATION',
        'RETURN_FROM_SUPERVISION', 
        'NEW_ADMISSION',
        'RETURN_FROM_ESCAPE',
        'TRANSFER',
        'TRANSFERRED_FROM_OUT_OF_STATE',
        'RETURN_FROM_ERRONEOUS_RELEASE',
        'ADMITTED_IN_ERROR',
        'INTERNAL_UNKNOWN',
        'EXTERNAL_UNKNOWN']) AS start_reason
    WITH OFFSET AS priority  
    """

ADMISSION_START_REASON_DEDUP_PRIORITY_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=ADMISSION_START_REASON_DEDUP_PRIORITY_VIEW_NAME,
    view_query_template=ADMISSION_START_REASON_DEDUP_PRIORITY_QUERY_TEMPLATE,
    description=ADMISSION_START_REASON_DEDUP_PRIORITY_VIEW_DESCRIPTION,
    should_materialize=False
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        ADMISSION_START_REASON_DEDUP_PRIORITY_VIEW_BUILDER.build_and_print()

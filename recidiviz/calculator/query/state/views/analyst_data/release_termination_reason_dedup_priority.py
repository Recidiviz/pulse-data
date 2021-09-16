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
"""Dedup priority for session end reasons"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

RELEASE_TERMINATION_REASON_DEDUP_PRIORITY_VIEW_NAME = (
    "release_termination_reason_dedup_priority"
)

RELEASE_TERMINATION_REASON_DEDUP_PRIORITY_VIEW_DESCRIPTION = (
    """Dedup priority for session end reasons"""
)

RELEASE_TERMINATION_REASON_DEDUP_PRIORITY_QUERY_TEMPLATE = """
    /*{description}*/
    SELECT 
        'INCARCERATION_RELEASE' AS metric_source,
        * 
    FROM UNNEST([
        'CONDITIONAL_RELEASE',
        'SENTENCE_SERVED',
        'RELEASED_FROM_TEMPORARY_CUSTODY',
        'ESCAPE',
        'DEATH',
        'TRANSFER_TO_OTHER_JURISDICTION',
        'COURT_ORDER',
        'COMMUTED',
        'STATUS_CHANGE',
        'TRANSFER',
        'RELEASED_IN_ERROR',
        'INTERNAL_UNKNOWN',
        'EXTERNAL_UNKNOWN']) AS end_reason
    WITH OFFSET AS priority
    UNION ALL 
    SELECT 
        'SUPERVISION_TERMINATION' AS metric_source,
        * 
    FROM UNNEST([
        'DISCHARGE',
        'EXPIRATION',
        'RETURN_TO_INCARCERATION',
        'REVOCATION',
        'ABSCONSION',
        'TRANSFER_TO_OTHER_JURISDICTION',
        'DEATH',
        'SUSPENSION',
        'PARDONED',
        'COMMUTED',
        'TRANSFER_WITHIN_STATE',
        'RETURN_FROM_ABSCONSION',
        'INTERNAL_UNKNOWN',
        'EXTERNAL_UNKNOWN']) AS end_reason
    WITH OFFSET AS priority
    """

RELEASE_TERMINATION_REASON_DEDUP_PRIORITY_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=RELEASE_TERMINATION_REASON_DEDUP_PRIORITY_VIEW_NAME,
    view_query_template=RELEASE_TERMINATION_REASON_DEDUP_PRIORITY_QUERY_TEMPLATE,
    description=RELEASE_TERMINATION_REASON_DEDUP_PRIORITY_VIEW_DESCRIPTION,
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        RELEASE_TERMINATION_REASON_DEDUP_PRIORITY_VIEW_BUILDER.build_and_print()

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
"""Dedup priority for supervision levels"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_LEVEL_DEDUP_PRIORITY_VIEW_NAME = "supervision_level_dedup_priority"

SUPERVISION_LEVEL_DEDUP_PRIORITY_VIEW_DESCRIPTION = (
    """Dedup priority for supervision levels"""
)

SUPERVISION_LEVEL_DEDUP_PRIORITY_QUERY_TEMPLATE = """
    /*{description}*/
    SELECT 
        'SUPERVISION' AS metric_source,
        correctional_level,
        correctional_level_priority,
        -- Indicator for whether supervision level can be assigned based on risk level/PO discretion, to determine inclusion in downgrade/upgrade counts
        correctional_level IN ('MAXIMUM', 'HIGH', 'MEDIUM', 'MINIMUM', 'LIMITED') AS is_discretionary_level
    FROM UNNEST([
        'INCARCERATED',
        'IN CUSTODY',
        'MAXIMUM',
        'HIGH',
        'MEDIUM',
        'MINIMUM',
        'LIMITED',
        'UNSUPERVISED',
        'DIVERSION',
        'INTERNAL_UNKNOWN',
        'EXTERNAL_UNKNOWN']) AS correctional_level
    WITH OFFSET AS correctional_level_priority 
    """

SUPERVISION_LEVEL_DEDUP_PRIORITY_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=SUPERVISION_LEVEL_DEDUP_PRIORITY_VIEW_NAME,
    view_query_template=SUPERVISION_LEVEL_DEDUP_PRIORITY_QUERY_TEMPLATE,
    description=SUPERVISION_LEVEL_DEDUP_PRIORITY_VIEW_DESCRIPTION,
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_LEVEL_DEDUP_PRIORITY_VIEW_BUILDER.build_and_print()

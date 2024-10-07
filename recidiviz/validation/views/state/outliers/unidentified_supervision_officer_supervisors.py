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
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.    See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.    If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""
View that returns information for supervision officer supervisors who do not have a corresponding record in
the outliers supervision_officer_supervisor product view, but are supervisors for officer with metrics in the current
12-month reporting period (via supervision_officer_aggregated_metrics).
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import OUTLIERS_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views.dataset_config import VIEWS_DATASET

_VIEW_NAME = "unidentified_supervision_officer_supervisors"

_VIEW_DESCRIPTION = (
    "View that returns information for supervision officer supervisors who do not have a corresponding record in "
    "the outliers supervision_officer_supervisor product view, but are supervisors for officer with "
    "metrics in the 12-month reporting period with an end date as the first of the current month."
)

_QUERY_TEMPLATE = """
WITH all_supervisors_from_metrics AS (
    SELECT
        o.state_code,
        o.supervisor_external_id as external_id,
        o.supervision_district,
        COUNT(DISTINCT o.external_id) AS num_officers_supervised_with_metrics,
    FROM `{project_id}.{outliers_dataset}.supervision_officers_materialized` o
    INNER JOIN `{project_id}.{outliers_dataset}.supervision_officer_metrics_materialized` m 
        ON o.state_code = m.state_code
        AND o.external_id = m.officer_id
    WHERE 
        m.period = "YEAR"
        AND m.end_date = DATE_TRUNC(CURRENT_DATE("US/Eastern"), MONTH)
    GROUP BY 1, 2, 3
)

SELECT
    asfm.*,
    asfm.state_code AS region_code
FROM all_supervisors_from_metrics asfm
LEFT JOIN `{project_id}.{outliers_dataset}.supervision_officer_supervisors_materialized` sos
    ON asfm.state_code = sos.state_code AND asfm.external_id = sos.external_id
WHERE sos.staff_id IS NULL AND asfm.external_id IS NOT NULL
"""

UNIDENTIFIED_SUPERVISION_OFFICER_SUPERVISORS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=VIEWS_DATASET,
    view_id=_VIEW_NAME,
    view_query_template=_QUERY_TEMPLATE,
    description=_VIEW_DESCRIPTION,
    should_materialize=True,
    outliers_dataset=OUTLIERS_VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        UNIDENTIFIED_SUPERVISION_OFFICER_SUPERVISORS_VIEW_BUILDER.build_and_print()

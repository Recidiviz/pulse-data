# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Creates a view that surfaces sessions during which clients are ineligible for
Compliant Reporting due to sanctions."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    STATE_BASE_DATASET,
    US_TN_RAW_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TN_COMPLIANT_REPORTING_SANCTION_INELIGIBLE_VIEW_NAME = (
    "us_tn_compliant_reporting_sanction_ineligible"
)

US_TN_COMPLIANT_REPORTING_SANCTION_INELIGIBLE_VIEW_DESCRIPTION = (
    "Creates a view that surfaces sessions during which clients are ineligible for "
    "Compliant Reporting due to sanctions."
)

US_TN_COMPLIANT_REPORTING_SANCTION_INELIGIBLE_QUERY_TEMPLATE = """

-- serious sanctions criteria
WITH sanction_sessions AS (
    SELECT DISTINCT
        person_id,
        CAST(CAST(ProposedDate AS DATETIME) AS DATE) AS start_date,
        DATE_ADD(CAST(CAST(ProposedDate AS DATETIME) AS DATE), INTERVAL 12 MONTH) AS end_date,
    FROM
        `{project_id}.{raw_dataset}.Violations_latest` a
    INNER JOIN
        `{project_id}.{raw_dataset}.Sanctions_latest` 
    USING
        (TriggerNumber)
    INNER JOIN
        `{project_id}.{state_base_dataset}.state_person_external_id` pei            
    ON
        pei.external_id = a.OffenderID
        AND id_type = "US_TN_DOC"
    WHERE
        CAST(SanctionLevel AS INT) > 1
)

-- handle overlapping periods
SELECT
    person_id,
    "US_TN" AS state_code,
    sanction_session_id,
    MIN(start_date) AS start_date,
    MAX(end_date) AS end_date,
    FALSE AS sanction_eligible,
FROM (
    SELECT
        * EXCEPT(new_session),
        SUM(new_session) OVER (PARTITION BY person_id ORDER BY start_date) AS sanction_session_id,
    FROM (
        SELECT
            person_id,
            start_date,
            end_date,
            IF(
                LAG(end_date) OVER (PARTITION BY person_id ORDER BY start_date) < start_date,
                1, 0
            ) AS new_session
        FROM
            sanction_sessions
    )
)
GROUP BY 1,2,3
"""

US_TN_COMPLIANT_REPORTING_SANCTION_INELIGIBLE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_TN_COMPLIANT_REPORTING_SANCTION_INELIGIBLE_VIEW_NAME,
    description=US_TN_COMPLIANT_REPORTING_SANCTION_INELIGIBLE_VIEW_DESCRIPTION,
    view_query_template=US_TN_COMPLIANT_REPORTING_SANCTION_INELIGIBLE_QUERY_TEMPLATE,
    state_base_dataset=STATE_BASE_DATASET,
    raw_dataset=US_TN_RAW_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_COMPLIANT_REPORTING_SANCTION_INELIGIBLE_VIEW_BUILDER.build_and_print()

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
"""Sessionized table with spans of weighted justice impact"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.calculator.query.state.views.sessions.compartment_sub_sessions import (
    COMPARTMENT_SUB_SESSIONS_VIEW_BUILDER,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# below we define relative weights for different types of justice involvement, each
# scaled relative to minimum security incarceration = 1. These weights are determined
# by UXR with JIIs.
MAX_SECURITY_WEIGHT = 30
SOLITARY_CONFINEMENT_WEIGHT = 7.5
MEDIUM_SECURITY_WEIGHT = 2.5
SUPERVISION_WEIGHT = 1 / 25
LIMITED_UNSUPERVISED_WEIGHT = 1 / 50

_VIEW_NAME = "justice_impact_sessions"

_VIEW_DESCRIPTION = f"""
This table contains spans of justice impact weighted by the relative impact of each
compartment (as determined by UXR with JIIs). The weights are relative to
min. security incarceration such that a reduction in a person-year of (weighted) justice
impact is equally valued, on average, by JIIs. Metrics calculated using this table
facilitate apples-to-apples comparisons of the impact of Recidiviz tools on different
justice compartments.

Weights are defined as follows:
- Max Security Incarceration: {MAX_SECURITY_WEIGHT}
- Solitary confinement: {SOLITARY_CONFINEMENT_WEIGHT}
- Medium Security Incarceration: {MEDIUM_SECURITY_WEIGHT}
- Min Security Incarceration: 1
- Supervision: {SUPERVISION_WEIGHT}
- Limited/unsupervised: {LIMITED_UNSUPERVISED_WEIGHT}
"""

_QUERY_TEMPLATE = f"""
WITH weights AS (
    SELECT
        state_code,
        person_id,
        start_date,
        end_date_exclusive,
        -- Below we weight every compartment level 1 in compartment_sub_sessions
        -- with exceptions for more specific experiences (e.g. solitary confinement)
        CASE
            WHEN compartment_level_1 IN (
                "DEATH", "ERRONEOUS_RELEASE", "INTERNAL_UNKNOWN", "LIBERTY", "SUSPENSION"
            ) THEN 0
            WHEN housing_unit_type IN (
                "TEMPORARY_SOLITARY_CONFINEMENT",
                "PERMANENT_SOLITARY_CONFINEMENT"
            ) THEN {SOLITARY_CONFINEMENT_WEIGHT}
            WHEN compartment_level_1 IN (
                "INCARCERATION", "INCARCERATION_OUT_OF_STATE", "PENDING_CUSTODY"
            ) THEN
                -- determine security level of incarceration
                CASE
                    WHEN correctional_level IN (
                        "MAXIMUM"
                    ) THEN {MAX_SECURITY_WEIGHT}
                    WHEN correctional_level IN (
                        "MEDIUM", "CLOSE"
                    ) THEN {MEDIUM_SECURITY_WEIGHT}
                    -- the rest is assumed minimum security or equivalent
                    -- this includes MINIMUM, RESTRICTIVE_MINIMUM, and INTERNAL_UNKNOWN
                    ELSE 1 END
            -- all remaining compartments supervision or investigation
            -- first, handle unconventional supervision (ignore qualitative factors)
            WHEN compartment_level_1 IN (
                "INVESTIGATION", "PENDING_SUPERVISION"
            ) THEN {SUPERVISION_WEIGHT}
            -- second, weights for unsupervised parole/probation
            WHEN correctional_level IN (
                "LIMITED", "UNSUPERVISED"
            ) THEN {LIMITED_UNSUPERVISED_WEIGHT}
            -- finally, supervised parole/probation
            WHEN compartment_level_1 IN (
                "SUPERVISION", "SUPERVISION_OUT_OF_STATE"
            ) THEN {SUPERVISION_WEIGHT}
            -- the above should cover all cases
            ELSE NULL END AS justice_impact_weight,
    FROM
        `{{project_id}}.{{compartment_sub_sessions}}`
)

-- sessionize and return
, sessionized_cte AS (
{aggregate_adjacent_spans(
    table_name="weights",
    attribute="justice_impact_weight",
    session_id_output_name="session_id",
    end_date_field_name="end_date_exclusive",
)}
)

SELECT
    state_code,
    person_id,
    session_id,
    start_date,
    end_date_exclusive,
    justice_impact_weight,
    DATE_DIFF(
        end_date_exclusive, start_date, DAY
    ) AS unweighted_days_justice_impacted,
    DATE_DIFF(
        end_date_exclusive, start_date, DAY
    ) * justice_impact_weight AS weighted_days_justice_impacted,
FROM
    sessionized_cte
WHERE
    -- must have at least one complete day to include session
    end_date_exclusive > start_date OR end_date_exclusive IS NULL
"""

JUSTICE_IMPACT_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=_VIEW_NAME,
    description=_VIEW_DESCRIPTION,
    view_query_template=_QUERY_TEMPLATE,
    compartment_sub_sessions=COMPARTMENT_SUB_SESSIONS_VIEW_BUILDER.table_for_query.to_str(),
    clustering_fields=["state_code", "person_id"],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        JUSTICE_IMPACT_SESSIONS_VIEW_BUILDER.build_and_print()

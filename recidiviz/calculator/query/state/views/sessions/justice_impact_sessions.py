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
"""
Sessionized table with spans of weighted justice impact

We define relative weights for different types of justice involvement, each
scaled relative to minimum security incarceration = 1. These weights were determined
via UXR with JIIs.
"""

from enum import Enum
from typing import Union

import attr

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.calculator.query.state.views.sessions.compartment_sub_sessions import (
    COMPARTMENT_SUB_SESSIONS_VIEW_BUILDER,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


# flags for compartments corresponding to justice impact weights
class JusticeImpactType(Enum):
    LIMITED_SUPERVISION = "LIMITED_SUPERVISION"
    MAXIMUM_CUSTODY = "MAXIMUM_CUSTODY"
    MEDIUM_CUSTODY = "MEDIUM_CUSTODY"
    MINIMUM_CUSTODY = "MINIMUM_CUSTODY"
    NONLIMITED_SUPERVISION = "NONLIMITED_SUPERVISION"
    SOLITARY_CONFINEMENT = "SOLITARY_CONFINEMENT"


@attr.define(frozen=True, kw_only=True)
class JusticeImpact:
    """
    Class that stores information about a justice impact type.
    """

    justice_impact_type: JusticeImpactType
    weight: float

    @property
    def name(self) -> str:
        return self.justice_impact_type.value


JUSTICE_IMPACT_TYPE_LIST = [
    JusticeImpact(
        justice_impact_type=JusticeImpactType.MAXIMUM_CUSTODY,
        weight=30.0,
    ),
    JusticeImpact(
        justice_impact_type=JusticeImpactType.SOLITARY_CONFINEMENT,
        weight=7.5,
    ),
    JusticeImpact(
        justice_impact_type=JusticeImpactType.MEDIUM_CUSTODY,
        weight=2.5,
    ),
    JusticeImpact(
        justice_impact_type=JusticeImpactType.MINIMUM_CUSTODY,
        weight=1.0,
    ),
    JusticeImpact(
        justice_impact_type=JusticeImpactType.NONLIMITED_SUPERVISION,
        weight=1 / 25,
    ),
    JusticeImpact(
        justice_impact_type=JusticeImpactType.LIMITED_SUPERVISION,
        weight=1 / 50,
    ),
]
JUSTICE_IMPACT_TYPES = {j.justice_impact_type: j for j in JUSTICE_IMPACT_TYPE_LIST}


_VIEW_NAME = "justice_impact_sessions"

_WEIGHT_DOCSTRING = "\n".join(
    [f"- {ji_type.name}: {ji_type.weight}" for ji_type in JUSTICE_IMPACT_TYPES.values()]
)
_VIEW_DESCRIPTION = f"""
This table contains spans of justice impact weighted by the relative impact of each
compartment (as determined by UXR with JIIs). The weights are relative to
min. security incarceration such that a reduction in a person-year of (weighted) justice
impact is equally valued, on average, by JIIs. Metrics calculated using this table
facilitate apples-to-apples comparisons of the impact of Recidiviz tools on different
justice compartments.

Weights are defined as follows:
{_WEIGHT_DOCSTRING}
"""


# helper function for getting justice impact type name or weight for a single entity
def get_ji_type_or_weight(
    ji_type: JusticeImpactType,
    column: str,
) -> Union[str, float]:
    """
    Returns a string justice impact type name or weight for a given
    JusticeImpact object.
    """
    if column not in [
        "type",
        "weight",
    ]:
        raise ValueError(
            f"Column {column} is not a valid column for justice impact sessions."
        )

    if ji_type not in JUSTICE_IMPACT_TYPES:
        raise ValueError(
            f"Justice impact type {ji_type} is not a valid justice impact type."
        )
    justice_impact_object = JUSTICE_IMPACT_TYPES[ji_type]

    if column == "weight":
        return justice_impact_object.weight
    # if type, return the name with quotes
    return f'"{justice_impact_object.name}"'


# helper function for getting compartment-specific levels or weights
def get_justice_impact_type_or_weight_column(column: str) -> str:
    """
    Returns the column SQL for the given justice impact type or weight.

    `column` can be one of:
    - "type": the justice impact type (e.g. "MAXIMUM_CUSTODY")
    - "weight": the justice impact weight (e.g. 30)
    """

    if column not in [
        "type",
        "weight",
    ]:
        raise ValueError(
            f"Column {column} is not a valid column for justice impact sessions."
        )

    return f"""CASE
            WHEN compartment_level_1 IN (
                "DEATH", "ERRONEOUS_RELEASE", "INTERNAL_UNKNOWN", "LIBERTY", "SUSPENSION"
            ) THEN {0 if column == "weight" else "NULL"}
            -- TODO(#22252): Delete this reference to `housing_unit_type` once
            -- `housing_unit_category` replaces it as the super-type for housing placement
            WHEN housing_unit_type IN (
                "TEMPORARY_SOLITARY_CONFINEMENT",
                "PERMANENT_SOLITARY_CONFINEMENT"
            )
            -- OR housing_unit_category = "SOLITARY_CONFINEMENT"
            THEN {get_ji_type_or_weight(JusticeImpactType.SOLITARY_CONFINEMENT, column)}
            WHEN compartment_level_1 IN (
                "INCARCERATION", "INCARCERATION_OUT_OF_STATE", "PENDING_CUSTODY"
            ) THEN
                -- determine security level of incarceration
                CASE
                    WHEN correctional_level IN (
                        "MAXIMUM"
                    ) THEN {get_ji_type_or_weight(JusticeImpactType.MAXIMUM_CUSTODY, column)}
                    WHEN correctional_level IN (
                        "MEDIUM", "CLOSE"
                    ) THEN {get_ji_type_or_weight(JusticeImpactType.MEDIUM_CUSTODY, column)}
                    -- the rest is assumed minimum security or equivalent
                    -- this includes MINIMUM, RESTRICTIVE_MINIMUM, and INTERNAL_UNKNOWN
                    ELSE {get_ji_type_or_weight(JusticeImpactType.MINIMUM_CUSTODY, column)} END
            -- all remaining compartments supervision or investigation
            -- first, handle unconventional supervision (ignore qualitative factors)
            WHEN compartment_level_1 IN (
                "INVESTIGATION", "PENDING_SUPERVISION"
            ) THEN {get_ji_type_or_weight(JusticeImpactType.NONLIMITED_SUPERVISION, column)}
            -- second, weights for unsupervised parole/probation
            WHEN correctional_level IN (
                "LIMITED", "UNSUPERVISED"
            ) THEN {get_ji_type_or_weight(JusticeImpactType.LIMITED_SUPERVISION, column)}
            -- finally, supervised parole/probation
            WHEN compartment_level_1 IN (
                "SUPERVISION", "SUPERVISION_OUT_OF_STATE"
            ) THEN {get_ji_type_or_weight(JusticeImpactType.NONLIMITED_SUPERVISION, column)}
            -- the above should cover all cases
            ELSE NULL END"""


_QUERY_TEMPLATE = f"""
WITH weights AS (
    SELECT
        state_code,
        person_id,
        start_date,
        end_date_exclusive,
        -- label each period with a mutually exclusive type of justice impact
        {get_justice_impact_type_or_weight_column("type")} AS justice_impact_type,
        -- assign a weight to each period of justice impact
        {get_justice_impact_type_or_weight_column("weight")} AS justice_impact_weight,
    FROM
        `{{project_id}}.{{compartment_sub_sessions}}`
)

-- sessionize and return
, sessionized_cte AS (
{aggregate_adjacent_spans(
    table_name="weights",
    attribute=["justice_impact_type", "justice_impact_weight"],
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
    justice_impact_type,
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

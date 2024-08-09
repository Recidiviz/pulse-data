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
"""Query containing MDOC supervision violation information from OMNI."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# In MI, there is a ADH_SUPERVISION_VIOLATION table which holds reports of supervision violation
# as well as a ADH_SUPV_VIOLATION_INCIDENT table which holds data on each incident.  Multiple incidents
# can be attached to the same record in ADH_SUPERVISION_VIOLATION so for our schema, we're going to count
# each incident as a single StateSupervisionViolation and each record in ADH_SUPERVISION_VIOLATION as a
# single StateSupervisionViolationResponse. There are cases where there's a record in the ADH_SUPERVISION_VIOLATION
# table but no associated incidents, and I think these are largely probation violations which MI research
# has said we should expect to be less documented in their data.  In those cases, we'll still have a StateSupervisionViolation
# and StateSupervisionViolationResponse constructed for them but leave many fields null.
#
# Then there's the ADH_MANAGEMENT_DECISION table which holds supervision officer level decisions/response while the
# ADH_SANCTION table holds other sanction responses.  Each row in either of these tables attaches to a record
# in ADH_SUPERVISION_VIOLATION and so we're going to count each as a StateSupervisionViolationResponse with an
# attached StateSupervisionViolationResponseDecision
#
# And finally the ADH_SUPER_COND_VIOLATION tells us about the violated condition, and each record attaches to a violation incident,
# which aligns with our schema.

VIEW_QUERY_TEMPLATE = """,
    -- grab all the relevant StateSupervisionViolationResponseDecision from ADH_MANAGEMENT_DECISION
    management_decisions as (
    SELECT
        supervision_violation_id,
        'DECISION' as _response_type,
        management_decision_id as _response_external_id,
        (DATE(decision_date)) as _response_date,
        decision_ref.description as _decision,
        decide_employee_id as _decision_agent_id,
        UPPER(decide_employee_position) as _deciding_body_role
    FROM {ADH_MANAGEMENT_DECISION} m
    LEFT JOIN {ADH_REFERENCE_CODE} decision_ref ON m.decision_type_id = decision_ref.reference_code_id
    ),
    -- grab all the relevant StateSupervisionViolationResponseDecision from ADH_SANCTION
    sanction_decisions as (
    SELECT
        supervision_violation_id,
        'SANCTION' as _response_type,
        sanction_id as _response_external_id,
        (DATE(sanction_date)) as _response_date,
        sanction_ref.description as _decision,
        CAST(NULL as string) as _decision_agent_id,
        approval_authority_id as _deciding_body_role
    from {ADH_SANCTION} s
    LEFT JOIN {ADH_REFERENCE_CODE} sanction_ref ON s.sanction_type_id = sanction_ref.reference_code_id
    ),
    -- stack the DECISION and SANCTION data and then transform it to be one line per supervision_violation_id
    all_decisions as (
        SELECT 
            supervision_violation_id,
            TO_JSON_STRING(
                ARRAY_AGG(STRUCT<_response_type string,
                                _response_external_id string,
                                _response_date DATE,
                                _decision string,
                                _deciding_body_role string,
                                _decision_agent_id string,
                                first_name string,
                                middle_name string,
                                last_name string,
                                name_suffix string,
                                position string>
                        (_response_type,
                        _response_external_id,
                        _response_date,
                        _decision,
                        _deciding_body_role,
                    _decision_agent_id,
                        first_name,
                    middle_name,
                        last_name,
                        name_suffix,
                        position) ORDER BY _response_type, _response_external_id)
            ) as decision_info
        FROM (
            (select * from management_decisions) 
            union all 
            (select * from sanction_decisions)
        ) stacked
        LEFT JOIN {ADH_EMPLOYEE} e on stacked._decision_agent_id = e.employee_id
        GROUP BY supervision_violation_id
    ),
    -- grab all the relevant violated conditions info and transform it to one row per supv_violation_incident_id
    violated_conditions as (
        SELECT             
            cv.supv_violation_incident_id,
            TO_JSON_STRING(
                ARRAY_AGG(STRUCT<special_condition_id string, short_description string, description string>(c.special_condition_id,short_description,description) ORDER BY c.special_condition_id)
            ) as violated_conditions
        FROM {ADH_SUPER_COND_VIOLATION} cv
        LEFT JOIN {ADH_SUPERVISION_CONDITION} c on cv.supervision_condition_id = c.supervision_condition_id
        LEFT JOIN {ADH_SPECIAL_CONDITION} s on c.special_condition_id = s.special_condition_id
        GROUP BY supv_violation_incident_id
    )

    -- grab the remaining info for each violation incidient (from ADH_SUPV_VIOLATION_INCIDENT) and 
    -- the supervision violation record (from ADH_SUPERVSION_VIOLATION), and then join on the violated conditions
    -- and response decisions from above
    SELECT
        v.offender_booking_id,
        v.supervision_violation_id,
        i.supv_violation_incident_id,
        (DATE(i.violation_date)) as violation_date,
        i.incident_summary_notes,
        ref_type.description as violation_type,
        (DATE(v.violation_invest_begin_date)) as violation_invest_begin_date,
        decision_info,
        violated_conditions
    FROM {ADH_SUPERVISION_VIOLATION} v
    LEFT JOIN {ADH_SUPV_VIOLATION_INCIDENT} i on i.supervision_violation_id = v.supervision_violation_id
    LEFT JOIN all_decisions d on v.supervision_violation_id = d.supervision_violation_id
    LEFT JOIN violated_conditions c on i.supv_violation_incident_id = c.supv_violation_incident_id
    LEFT JOIN {ADH_REFERENCE_CODE} ref_type on v.violation_type_id = ref_type.reference_code_id
    INNER JOIN {ADH_OFFENDER_BOOKING} b on v.offender_booking_id = b.offender_booking_id
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_mi",
    ingest_view_name="supervision_violations",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()

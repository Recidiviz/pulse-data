# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Helper SQL queries and constants for Idaho Disciplinary Offense Reports (DORs)."""

from recidiviz.calculator.query.bq_utils import list_to_query_string

# Nested lookback windows the COPP Class A and Class B DOR questions count DORs in. Each
# window is meant to be worth 1 point for one or more DORs, so a DOR within 12 months
# scores 3, one 12–24 months old scores 2, and one 24–36 months old scores 1.
# TODO(ID-387): Give those points in a score table once a policy scores these questions.
US_IX_CLASSIFICATION_DOR_CLASS_A_OR_B_LOOKBACK_MONTH_RANGES: list[tuple[int, int]] = [
    (0, 12),
    (0, 24),
    (0, 36),
]


def us_ix_dors_query(*, dor_classes: list[str]) -> str:
    """Returns a SQL query for the US_IX DORs of the given classes, one row per DOR, read
    from normalized state. Dismissed and not-guilty DORs are left out; pending DORs and
    informal resolutions are kept. Reports with no DA case are also left out, since
    nothing records whether they were decided. The offense type name and the description
    come from incident_metadata, which keeps IDOC's original casing for display.

    For example, a resident with a pending class B DOR and a dismissed class A DOR gets
    one row (some columns abridged):

        person_id,dor_class,offense_type_name,dor_description,incident_date,hearing_date,is_pending
        123,B,Disobedience to Orders 2,Resident refused to return to the assigned cell.,2026-08-01,,true
    """
    return f"""
    SELECT
        incident.state_code,
        incident.person_id,
        incident.incarceration_incident_id,
        -- Report-backed DORs use the report id as their external_id.
        incident.external_id AS incident_external_id,
        NULLIF(JSON_EXTRACT_SCALAR(incident.incident_metadata, '$.DACaseId'), '') AS da_case_id,
        incident.incident_severity_raw_text AS dor_class,
        JSON_EXTRACT_SCALAR(incident.incident_metadata, '$.OffenseTypeName') AS offense_type_name,
        JSON_EXTRACT_SCALAR(incident.incident_metadata, '$.IncidentDetails') AS dor_description,
        incident.incident_date,
        CAST(
            LEFT(NULLIF(JSON_EXTRACT_SCALAR(incident.incident_metadata, '$.HearingDate'), ''), 10)
            AS DATE
        ) AS hearing_date,
        incident.facility,
        incident.incarceration_incident_id IN (
            SELECT incarceration_incident_id
            FROM `{{project_id}}.{{normalized_state_dataset}}.state_incarceration_incident_outcome`
            WHERE state_code = 'US_IX'
                AND outcome_type = 'PENDING'
        ) AS is_pending,
    FROM `{{project_id}}.{{normalized_state_dataset}}.state_incarceration_incident` incident
    WHERE incident.state_code = 'US_IX'
        AND incident.incident_severity_raw_text
            IN ({list_to_query_string(dor_classes, quoted=True, single_quote=True)})
        AND NULLIF(JSON_EXTRACT_SCALAR(incident.incident_metadata, '$.DACaseId'), '') IS NOT NULL
        AND incident.incarceration_incident_id NOT IN (
            SELECT incarceration_incident_id
            FROM `{{project_id}}.{{normalized_state_dataset}}.state_incarceration_incident_outcome`
            WHERE state_code = 'US_IX'
                AND outcome_type IN ('DISMISSED', 'NOT_GUILTY')
        )"""

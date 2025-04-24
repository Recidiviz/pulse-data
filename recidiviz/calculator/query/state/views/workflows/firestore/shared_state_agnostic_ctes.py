# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Query logic shared between client_record and resident_record views."""

CLIENT_OR_RESIDENT_RECORD_STABLE_PERSON_EXTERNAL_IDS_CTE_TEMPLATE = """
    stable_person_external_ids AS (
        # Gives the "stable" external id value for a given person (one that we expect to
        # ideally never change over time). This ID is persisted to analytics events and 
        # browser session caches, so we'll lose user session information (e.g. snooze 
        # information) about this person if this ID ever changes. Also, as of 4/23/25,
        # this ID is used to connect the client / resident records to opportunity
        # records, so the two ids must agree.
        SELECT 
            pei.person_id, pei.person_external_id
        FROM (
            SELECT person_id, person_external_id 
            FROM `{{project_id}}.workflows_views.person_id_to_external_id_materialized`
            WHERE system_type = "{system_type}"
        ) pei
        LEFT JOIN (
            SELECT person_id, opportunity_person_external_id
            FROM opportunities_aggregated, UNNEST(
                all_referenced_person_external_ids
            ) AS opportunity_person_external_id
        ) opp_pei
        ON pei.person_id = opp_pei.person_id 
            AND pei.person_external_id = opp_pei.opportunity_person_external_id
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY person_id 
            ORDER BY
                -- Choose external_ids that appear in an opportunity record over 
                -- those that do not. Otherwise, choose the alphabetically lowest
                -- id (this is often the oldest one).
                IF(opportunity_person_external_id IS NOT NULL, 0, 1),
                person_external_id
        ) = 1
    ),
"""

#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2022 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""View logic to prepare US_ND Workflows supervision clients."""
from recidiviz.calculator.query.state.views.workflows.firestore.client_record_ctes import (
    client_record_join_clients_cte,
    client_record_supervision_cte,
    client_record_supervision_level_cte,
    client_record_supervision_super_sessions_cte,
    clients_cte,
)

# This template returns a CTEs to be used in the `client_record.py` firestore ETL query
US_ND_SUPERVISION_CLIENTS_QUERY_TEMPLATE = f"""
    {client_record_supervision_cte("US_ND")}
    {client_record_supervision_level_cte("US_ND")}
    {client_record_supervision_super_sessions_cte("US_ND")}
    us_nd_phone_numbers AS (
        # TODO(#14676): Pull from state_person.phone_number once hydrated
        SELECT 
            state_code, 
            external_id AS person_external_id, 
            PHONE AS phone_number
        FROM `{{project_id}}.{{us_nd_raw_data}}.docstars_offenders_latest` doc
        INNER JOIN `{{project_id}}.{{state_dataset}}.state_person_external_id` pei
        ON doc.SID = pei.external_id
        AND pei.id_type = "US_ND_SID"
    ),
    {client_record_join_clients_cte("US_ND")}
    {clients_cte("US_ND", ["nd_early_termination_eligibility"])}
"""

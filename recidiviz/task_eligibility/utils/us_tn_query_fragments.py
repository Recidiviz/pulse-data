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
Helper SQL queries for Tennessee
"""


def detainers_cte() -> str:
    """Helper method that returns a CTE getting detainer information in TN"""

    return """
        -- As discussed with TTs in TN, a detainer is "relevant" until it has been lifted, so we use that as
        -- our end date
        SELECT
            state_code,
            person_id,
            DATE(DetainerReceivedDate) AS start_date,
            DATE(DetainerLiftDate) AS end_date,
            DetainerFelonyFlag AS detainer_felony_flag,
            DetainerMisdemeanorFlag AS detainer_misdemeanor_flag,
            CASE WHEN DetainerFelonyFlag = 'X' THEN 5
                 WHEN DetainerMisdemeanorFlag = 'X' THEN 3
                 END AS detainer_score
        FROM 
            `{project_id}.{us_tn_raw_data_up_to_date_dataset}.Detainer_latest` dis
        INNER JOIN
            `{project_id}.{normalized_state_dataset}.state_person_external_id` pei
        ON
            dis.OffenderID = pei.external_id
        AND
            pei.state_code = 'US_TN'
        WHERE
            DetainerFelonyFlag = 'X' OR DetainerMisdemeanorFlag = 'X'
        
        """

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
"""Provides raw ME case notes contain snoozed opportunity metadata.
"""
from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_query_builder import BigQueryQueryBuilder
from recidiviz.big_query.big_query_query_provider import (
    BigQueryQueryProvider,
    SimpleBigQueryQueryProvider,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.pipelines.ingest.dataset_config import (
    normalized_state_dataset_for_state_code,
)

US_ME_SNOOZED_OPPORTUNITY_NOTES_QUERY_NAME = "us_me_snoozed_opportunity_notes"

US_ME_SNOOZED_OPPORTUNITY_NOTES_QUERY_TEMPLATE = """
    WITH person_id_with_external_ids AS (
        SELECT
            person_id,
            external_id AS person_external_id,
            state_code
        FROM `{project_id}.{normalized_state_dataset}.state_person_external_id`
        WHERE state_code = 'US_ME' AND id_type = 'US_ME_DOC'
    )
    SELECT
       person_id,
       Cis_100_Client_Id,
       Note_Id,
       Note_Tx,
       Note_Date,
       'US_ME' as state_code
    FROM person_id_with_external_ids
    JOIN  `{project_id}.{us_me_raw_data_up_to_date_dataset}.CIS_204_GEN_NOTE_latest` n
    ON person_id_with_external_ids.person_external_id = n.Cis_100_Client_Id
    WHERE
        Note_Tx like '%is_recidiviz_snooze_note%'
"""


def get_us_me_snoozed_opportunity_notes_query_provider(
    project_id: str,
    address_overrides: BigQueryAddressOverrides | None,
) -> BigQueryQueryProvider:
    query_builder = BigQueryQueryBuilder(
        parent_address_overrides=address_overrides,
        parent_address_formatter_provider=None,
    )

    formatted_query = query_builder.build_query(
        project_id=project_id,
        query_template=US_ME_SNOOZED_OPPORTUNITY_NOTES_QUERY_TEMPLATE,
        query_format_kwargs={
            "normalized_state_dataset": normalized_state_dataset_for_state_code(
                StateCode.US_ME
            ),
            "us_me_raw_data_up_to_date_dataset": raw_latest_views_dataset_for_region(
                state_code=StateCode.US_ME, instance=DirectIngestInstance.PRIMARY
            ),
        },
    )

    return SimpleBigQueryQueryProvider(query=formatted_query)

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
"""Queries information needed to fill out the SLD form in CA
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.workflows.firestore.opportunity_record_query_fragments import (
    join_current_task_eligibility_spans_with_external_id,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_spans_state_specific_dataset,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_CA_SUPERVISION_LEVEL_DOWNGRADE_VIEW_NAME = (
    "us_ca_supervision_level_downgrade_form_record"
)

US_CA_SUPERVISION_LEVEL_DOWNGRADE_DESCRIPTION = """
    Queries information needed to fill out the SLD form in CA
    """

US_CA_SUPERVISION_LEVEL_DOWNGRADE_QUERY_TEMPLATE = f"""
WITH current_parole_pop_cte AS (
    -- Keep only people in Parole
    {join_current_task_eligibility_spans_with_external_id(
        state_code= "'US_CA'", 
        tes_task_query_view = 'supervision_level_downgrade_materialized',
        id_type = "'US_CA_DOC'")}
),
offender_id_by_person AS (
    SELECT person_id, external_id AS OffenderId
    FROM `{{project_id}}.us_ca_normalized_state.state_person_external_id`
    WHERE id_type = 'US_CA_DOC'
    -- It's very rare but possible for a person to have multiple OffenderId
    -- TODO(#41554): Use the stable product ids view to properly choose OffenderId when
    -- there are multiple here.
    QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY OffenderId) = 1
),
cdcno_by_person AS (
    -- TODO(#41541): Replace this logic with logic that pulls US_CA_CDCNO ids from 
    -- state_person_external_id once those are ingested
    SELECT person_id, pp.Cdcno
    FROM current_parole_pop_cte
    LEFT JOIN `{{project_id}}.{{us_ca_raw_data_up_to_date_dataset}}.PersonParole_latest` pp
        ON external_id=pp.OffenderId
    -- It's very rare but possible for a person to have multiple OffenderId. For now, 
    -- pick the CDC number associated with the lowest OffenderId.
    QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY OffenderId) = 1
),
additional_information AS (
  SELECT 
    person_id,
    offender_id_by_person.OffenderId,
    ssp.supervision_site AS form_information_parole_unit,
    CASE ssp.supervision_level_raw_text 
        -- Abbreviate SO main categories
        WHEN "SO CATEGORY A" THEN "SA"
        WHEN "SO CATEGORY B" THEN "SB"
        WHEN "SO CATEGORY C" THEN "SC"
        WHEN "SO CATEGORY D" THEN "SD"

        -- Abbreviate other categories
        WHEN "CATEGORY A" THEN "CA"
        WHEN "CATEGORY B" THEN "CB"
        WHEN "CATEGORY C" THEN "CC"
        WHEN "CATEGORY D" THEN "CD"

        ELSE ssp.supervision_level_raw_text
    END AS form_information_supervision_level,
    scte.case_type,
  FROM `{{project_id}}.{{normalized_state_dataset}}.state_supervision_period` ssp
  LEFT JOIN offender_id_by_person USING (person_id)
  LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_supervision_case_type_entry` scte USING (person_id, supervision_period_id)
  WHERE ssp.state_code = 'US_CA' AND ssp.termination_date IS NULL
)

SELECT 
    person_id,
    OffenderId AS external_id,
    state_code,
    reasons,
    ineligible_criteria,
    is_eligible,
    is_almost_eligible,
    ai.case_type,
    cdcno_by_person.Cdcno as form_information_cdcno,
    ai.form_information_parole_unit,
    ai.form_information_supervision_level
FROM current_parole_pop_cte
LEFT JOIN cdcno_by_person
USING (person_id)
LEFT JOIN additional_information ai USING (person_id)
WHERE is_eligible
"""

US_CA_SUPERVISION_LEVEL_DOWNGRADE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=US_CA_SUPERVISION_LEVEL_DOWNGRADE_VIEW_NAME,
    view_query_template=US_CA_SUPERVISION_LEVEL_DOWNGRADE_QUERY_TEMPLATE,
    description=US_CA_SUPERVISION_LEVEL_DOWNGRADE_DESCRIPTION,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    task_eligibility_dataset=task_eligibility_spans_state_specific_dataset(
        StateCode.US_CA
    ),
    us_ca_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_CA, instance=DirectIngestInstance.PRIMARY
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_CA_SUPERVISION_LEVEL_DOWNGRADE_VIEW_BUILDER.build_and_print()

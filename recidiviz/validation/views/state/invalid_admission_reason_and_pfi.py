# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""A view revealing when we see invalid combinations of admission_reason and
specialized_purpose_for_incarceration.
Existence of any rows indicates a bug in IP normalization logic.
"""
# pylint: disable=trailing-whitespace
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateIncarcerationPeriod,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config
from recidiviz.validation.views.utils.entities_validation_utils import (
    validation_query_for_normalized_entity,
)

INVALID_ADMISSION_REASON_AND_PFI_VIEW_NAME = "invalid_admission_reason_and_pfi"

INVALID_ADMISSION_REASON_AND_PFI_DESCRIPTION = """Normalized incarceration periods with 
invalid combinations of admission_reason and specialized_purpose_for_incarceration."""

INVALID_ROWS_FILTER_CLAUSE = f"""WHERE (admission_reason =
'{StateIncarcerationPeriodAdmissionReason.REVOCATION.value}'
    -- Any REVOCATION admission_reason should have a purpose of GENERAL, except for in US_AR,
    -- where they should have a purpose of either GENERAL or SHOCK_INCARCERATION.
        AND (
                (
                    state_code != 'US_AR' AND
                    specialized_purpose_for_incarceration NOT IN (
                        '{StateSpecializedPurposeForIncarceration.GENERAL.value}'
                ))
                OR (
                    state_code = 'US_AR' AND
                    specialized_purpose_for_incarceration NOT IN (
                        '{StateSpecializedPurposeForIncarceration.GENERAL.value}',
                        '{StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION.value}'
                ))
            )
        )
    -- Any NEW_ADMISSION should have a purpose of GENERAL, TREATMENT_IN_PRISON, INTERNAL_UNKNOWN, or SHOCK_INCARCERATION
    OR (admission_reason = '{StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION.value}'
        AND specialized_purpose_for_incarceration NOT IN (
              '{StateSpecializedPurposeForIncarceration.GENERAL.value}',
              '{StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON.value}',
              '{StateSpecializedPurposeForIncarceration.INTERNAL_UNKNOWN.value}',
              '{StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION.value}'
          ))
    -- ANY SANCTION_ADMISSION should have a purpose of TREATMENT_IN_PRISON or SHOCK_INCARCERATION
    OR (admission_reason = '{StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION.value}'
        AND specialized_purpose_for_incarceration NOT IN (
              '{StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON.value}',
              '{StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION.value}'
         ))
"""

INVALID_ADMISSION_REASON_AND_PFI_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=INVALID_ADMISSION_REASON_AND_PFI_VIEW_NAME,
    view_query_template=validation_query_for_normalized_entity(
        normalized_entity_class=NormalizedStateIncarcerationPeriod,
        additional_columns_to_select=[
            "person_id",
            "admission_reason",
            "specialized_purpose_for_incarceration",
            "admission_date",
        ],
        invalid_rows_filter_clause=INVALID_ROWS_FILTER_CLAUSE,
        validation_description=INVALID_ADMISSION_REASON_AND_PFI_DESCRIPTION,
    ),
    description=INVALID_ADMISSION_REASON_AND_PFI_DESCRIPTION,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        INVALID_ADMISSION_REASON_AND_PFI_VIEW_BUILDER.build_and_print()

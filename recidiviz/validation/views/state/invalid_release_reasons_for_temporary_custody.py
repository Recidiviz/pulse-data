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
"""A view revealing when normalized incarceration periods have invalid release reasons
for periods of temporary custody (either PAROLE_BOARD_HOLD or TEMPORARY_CUSTODY
purpose_for_incarceration values).

Existence of any rows indicates a bug in IP normalization logic.
"""
from typing import List

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.pipeline.normalization.utils.normalized_entities import (
    NormalizedStateIncarcerationPeriod,
)
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodReleaseReason,
    StateSpecializedPurposeForIncarceration,
    release_reason_overrides_released_from_temporary_custody,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config
from recidiviz.validation.views.utils.normalized_entities_validation_utils import (
    validation_query_for_normalized_entity,
)

INVALID_RELEASE_REASONS_FOR_TEMPORARY_CUSTODY_VIEW_NAME = (
    "invalid_release_reasons_for_temporary_custody"
)

INVALID_RELEASE_REASONS_FOR_TEMPORARY_CUSTODY_DESCRIPTION = """ Incarceration release metrics with invalid release reasons for periods of
    temporary custody or parole board holds."""


def _valid_release_reasons_for_temporary_custody() -> str:
    # All releases with PAROLE_BOARD_HOLD or TEMPORARY_CUSTODY pfi values should have
    # one of the following release reasons
    valid_release_reason_values: List[str] = [
        f"'{release_reason.value}'"
        for release_reason in StateIncarcerationPeriodReleaseReason
        if release_reason_overrides_released_from_temporary_custody(release_reason)
        or release_reason == StateIncarcerationPeriodReleaseReason.TRANSFER
    ]

    return ", ".join(valid_release_reason_values)


INVALID_ROWS_FILTER_CLAUSE = f"""WHERE specialized_purpose_for_incarceration IN
('{StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD.value}', 
'{StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY.value}')
AND release_reason NOT IN
({_valid_release_reasons_for_temporary_custody()})
"""


INVALID_RELEASE_REASONS_FOR_TEMPORARY_CUSTODY_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=INVALID_RELEASE_REASONS_FOR_TEMPORARY_CUSTODY_VIEW_NAME,
    view_query_template=validation_query_for_normalized_entity(
        normalized_entity_class=NormalizedStateIncarcerationPeriod,
        additional_columns_to_select=[
            "person_id",
            "release_reason",
            "specialized_purpose_for_incarceration",
            "release_date",
        ],
        invalid_rows_filter_clause=INVALID_ROWS_FILTER_CLAUSE,
        validation_description=INVALID_RELEASE_REASONS_FOR_TEMPORARY_CUSTODY_DESCRIPTION,
    ),
    description=INVALID_RELEASE_REASONS_FOR_TEMPORARY_CUSTODY_DESCRIPTION,
    normalized_state_dataset=state_dataset_config.NORMALIZED_STATE_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        INVALID_RELEASE_REASONS_FOR_TEMPORARY_CUSTODY_VIEW_BUILDER.build_and_print()

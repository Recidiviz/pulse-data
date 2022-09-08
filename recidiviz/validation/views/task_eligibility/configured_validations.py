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
"""Contains configured data validations to perform against task eligibility views."""
from typing import List

from recidiviz.validation.checks.existence_check import ExistenceDataValidationCheck
from recidiviz.validation.validation_models import (
    DataValidationCheck,
    ValidationCategory,
)
from recidiviz.validation.views.task_eligibility.null_start_date_task_eligibility_spans import (
    NULL_START_DATE_TASK_ELIGIBILITY_SPANS_VIEW_BUILDER,
)
from recidiviz.validation.views.task_eligibility.null_start_date_tes_candidate_population_spans import (
    NULL_START_DATE_TES_CANDIDATE_POPULATION_SPANS_VIEW_BUILDER,
)
from recidiviz.validation.views.task_eligibility.null_start_date_tes_criteria_spans import (
    NULL_START_DATE_TES_CRITERIA_SPANS_VIEW_BUILDER,
)
from recidiviz.validation.views.task_eligibility.overlapping_task_eligibility_spans import (
    OVERLAPPING_TASK_ELIGIBILITY_SPANS_VIEW_BUILDER,
)
from recidiviz.validation.views.task_eligibility.overlapping_tes_candidate_population_spans import (
    OVERLAPPING_TES_CANDIDATE_POPULATION_SPANS_VIEW_BUILDER,
)
from recidiviz.validation.views.task_eligibility.overlapping_tes_criteria_spans import (
    OVERLAPPING_TES_CRITERIA_SPANS_VIEW_BUILDER,
)
from recidiviz.validation.views.task_eligibility.start_after_end_date_task_eligibility_spans import (
    START_AFTER_END_DATE_TASK_ELIGIBILITY_SPANS_VIEW_BUILDER,
)
from recidiviz.validation.views.task_eligibility.start_after_end_date_tes_candidate_population_spans import (
    START_AFTER_END_DATE_TES_CANDIDATE_POPULATION_SPANS_VIEW_BUILDER,
)
from recidiviz.validation.views.task_eligibility.start_after_end_date_tes_criteria_spans import (
    START_AFTER_END_DATE_TES_CRITERIA_SPANS_VIEW_BUILDER,
)
from recidiviz.validation.views.task_eligibility.unique_task_eligiblity_span_ids import (
    UNIQUE_TASK_ELIGIBILITY_SPAN_IDS_VIEW_BUILDER,
)
from recidiviz.validation.views.task_eligibility.zero_day_task_eligibility_spans import (
    ZERO_DAY_TASK_ELIGIBILITY_SPANS_VIEW_BUILDER,
)
from recidiviz.validation.views.task_eligibility.zero_day_tes_candidate_population_spans import (
    ZERO_DAY_TES_CANDIDATE_POPULATION_SPANS_VIEW_BUILDER,
)
from recidiviz.validation.views.task_eligibility.zero_day_tes_criteria_spans import (
    ZERO_DAY_TES_CRITERIA_SPANS_VIEW_BUILDER,
)


def get_all_task_eligibility_validations() -> List[DataValidationCheck]:
    """Returns the full list of configured validations to perform on TES views."""
    return [
        ExistenceDataValidationCheck(
            view_builder=OVERLAPPING_TES_CRITERIA_SPANS_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=ZERO_DAY_TES_CRITERIA_SPANS_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=NULL_START_DATE_TES_CRITERIA_SPANS_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=START_AFTER_END_DATE_TES_CRITERIA_SPANS_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=OVERLAPPING_TES_CANDIDATE_POPULATION_SPANS_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=ZERO_DAY_TES_CANDIDATE_POPULATION_SPANS_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=NULL_START_DATE_TES_CANDIDATE_POPULATION_SPANS_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=START_AFTER_END_DATE_TES_CANDIDATE_POPULATION_SPANS_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=OVERLAPPING_TASK_ELIGIBILITY_SPANS_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=ZERO_DAY_TASK_ELIGIBILITY_SPANS_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=NULL_START_DATE_TASK_ELIGIBILITY_SPANS_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=START_AFTER_END_DATE_TASK_ELIGIBILITY_SPANS_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=UNIQUE_TASK_ELIGIBILITY_SPAN_IDS_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
    ]

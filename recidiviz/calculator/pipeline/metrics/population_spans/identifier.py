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
"""Identifier class for events related to incarceration."""
from typing import Any, Dict, List, Optional

from recidiviz.calculator.pipeline.metrics.base_identifier import (
    BaseIdentifier,
    IdentifierContext,
)
from recidiviz.calculator.pipeline.metrics.population_spans.spans import (
    IncarcerationPopulationSpan,
)
from recidiviz.calculator.pipeline.normalization.utils.normalized_entities import (
    NormalizedStateIncarcerationPeriod,
)
from recidiviz.calculator.pipeline.utils.entity_normalization.normalized_incarceration_period_index import (
    NormalizedIncarcerationPeriodIndex,
)
from recidiviz.calculator.pipeline.utils.execution_utils import (
    list_of_dicts_to_dict_with_keys,
)
from recidiviz.calculator.pipeline.utils.identifier_models import Span
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_incarceration_delegate import (
    StateSpecificIncarcerationDelegate,
)
from recidiviz.calculator.query.state.views.reference.incarceration_period_judicial_district_association import (
    INCARCERATION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_NAME,
)
from recidiviz.persistence.entity.entity_utils import CoreEntityFieldIndex
from recidiviz.persistence.entity.state.entities import StatePerson


class PopulationSpanIdentifier(BaseIdentifier[List[Span]]):
    """Identifier class for events related to incarceration."""

    def __init__(self) -> None:
        self.identifier_result_class = Span
        self.field_index = CoreEntityFieldIndex()

    def identify(
        self, _person: StatePerson, identifier_context: IdentifierContext
    ) -> List[Span]:

        return self._find_incarceration_spans(
            incarceration_delegate=identifier_context[
                StateSpecificIncarcerationDelegate.__name__
            ],
            incarceration_periods=identifier_context[
                NormalizedStateIncarcerationPeriod.base_class_name()
            ],
            incarceration_period_judicial_district_association=identifier_context[
                INCARCERATION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_NAME
            ],
        )

    def _find_incarceration_spans(
        self,
        incarceration_delegate: StateSpecificIncarcerationDelegate,
        incarceration_periods: List[NormalizedStateIncarcerationPeriod],
        incarceration_period_judicial_district_association: List[Dict[str, Any]],
    ) -> List[Span]:
        """Finds instances of various events related to incarceration.
        Transforms the person's StateIncarcerationPeriods into IncarcerationPopulationSpans.

        Returns:
            A list of IncarcerationPopulationSpans for the person.
        """
        incarceration_spans: List[Span] = []

        if not incarceration_periods:
            return incarceration_spans

        # Convert the list of dictionaries into one dictionary where the keys are the
        # incarceration_period_id values
        incarceration_period_to_judicial_district: Dict[
            int, Dict[str, Any]
        ] = list_of_dicts_to_dict_with_keys(
            incarceration_period_judicial_district_association,
            key=NormalizedStateIncarcerationPeriod.get_class_id_name(),
        )

        ip_index = NormalizedIncarcerationPeriodIndex(
            sorted_incarceration_periods=incarceration_periods,
            incarceration_delegate=incarceration_delegate,
        )

        for incarceration_period in ip_index.sorted_incarceration_periods:
            if incarceration_period.admission_date is None:
                continue

            if not incarceration_period.incarceration_period_id:
                raise ValueError(
                    "Unexpected incarceration period without an incarceration_period_id."
                )
            incarceration_spans.append(
                IncarcerationPopulationSpan(
                    state_code=incarceration_period.state_code,
                    start_date_inclusive=incarceration_period.admission_date,
                    end_date_exclusive=incarceration_period.release_date,
                    included_in_state_population=incarceration_delegate.is_period_included_in_state_population(
                        incarceration_period
                    ),
                    facility=incarceration_period.facility,
                    purpose_for_incarceration=incarceration_period.specialized_purpose_for_incarceration,
                    custodial_authority=incarceration_period.custodial_authority,
                    judicial_district_code=self._get_judicial_district_code(
                        incarceration_period, incarceration_period_to_judicial_district
                    ),
                )
            )

        return incarceration_spans

    def _get_judicial_district_code(
        self,
        incarceration_period: NormalizedStateIncarcerationPeriod,
        incarceration_period_to_judicial_district: Dict[int, Dict[str, Any]],
    ) -> Optional[str]:
        """Retrieves the judicial_district_code corresponding to the incarceration period, if one exists."""
        incarceration_period_id: Optional[
            int
        ] = incarceration_period.incarceration_period_id

        if incarceration_period_id is None:
            raise ValueError("Unexpected unset incarceration_period_id.")

        ip_info: Optional[
            Dict[Any, Any]
        ] = incarceration_period_to_judicial_district.get(incarceration_period_id)

        if ip_info is not None:
            return ip_info.get("judicial_district_code")

        return None

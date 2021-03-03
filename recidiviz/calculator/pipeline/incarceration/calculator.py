# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Calculates incarceration metrics from incarceration events.

This contains the core logic for calculating incarceration metrics on a person-by-person basis. It transforms
IncarcerationEvents into incarceration metrics, key-value pairs where the key represents all of the dimensions
represented in the data point, and the value represents the value the person should contribute to that metric.
"""
from typing import List, Dict, Tuple, Any, Type, Optional

from recidiviz.calculator.pipeline.incarceration.incarceration_event import (
    IncarcerationEvent,
    IncarcerationAdmissionEvent,
    IncarcerationReleaseEvent,
    IncarcerationStayEvent,
)
from recidiviz.calculator.pipeline.incarceration.metrics import (
    IncarcerationMetricType,
    IncarcerationMetric,
    IncarcerationAdmissionMetric,
    IncarcerationPopulationMetric,
    IncarcerationReleaseMetric,
)
from recidiviz.calculator.pipeline.utils.calculator_utils import (
    produce_standard_metric_combinations,
)
from recidiviz.calculator.pipeline.utils.person_utils import PersonMetadata
from recidiviz.persistence.entity.state.entities import StatePerson


EVENT_TO_METRIC_TYPES: Dict[Type[IncarcerationEvent], IncarcerationMetricType] = {
    IncarcerationAdmissionEvent: IncarcerationMetricType.INCARCERATION_ADMISSION,
    IncarcerationStayEvent: IncarcerationMetricType.INCARCERATION_POPULATION,
    IncarcerationReleaseEvent: IncarcerationMetricType.INCARCERATION_RELEASE,
}

EVENT_TO_METRIC_CLASSES: Dict[Type[IncarcerationEvent], Type[IncarcerationMetric]] = {
    IncarcerationAdmissionEvent: IncarcerationAdmissionMetric,
    IncarcerationStayEvent: IncarcerationPopulationMetric,
    IncarcerationReleaseEvent: IncarcerationReleaseMetric,
}


def map_incarceration_combinations(
    person: StatePerson,
    incarceration_events: List[IncarcerationEvent],
    metric_inclusions: Dict[IncarcerationMetricType, bool],
    calculation_end_month: Optional[str],
    calculation_month_count: int,
    person_metadata: PersonMetadata,
) -> List[Tuple[Dict[str, Any], Any]]:
    """Transforms IncarcerationEvents and a StatePerson into combinations representing IncarcerationMetrics.

    Takes in a StatePerson and all of their IncarcerationEvent and returns an array of "incarceration combinations".
    These are key-value pairs where the key represents a specific metric and the value corresponding to that metric.

    This translates a particular incarceration event, e.g. admission or release, into an incarceration metric.

    Args:
        person: the StatePerson
        incarceration_events: A list of IncarcerationEvents for the given StatePerson.
        metric_inclusions: A dictionary where the keys are each IncarcerationMetricType, and the values are boolean
            flags for whether or not to include that metric type in the calculations
        calculation_end_month: The year and month in YYYY-MM format of the last month for which metrics should be
            calculated. If unset, ends with the current month.
        calculation_month_count: The number of months (including the month of the calculation_month_upper_bound) to
            limit the monthly calculation output to. If set to -1, does not limit the calculations.
        person_metadata: Contains information about the StatePerson that is necessary for the metrics.
    Returns:
        A list of key-value tuples representing specific metric combinations and the value corresponding to that metric.
    """
    return produce_standard_metric_combinations(
        pipeline="incarceration",
        person=person,
        identifier_events=incarceration_events,
        metric_inclusions=metric_inclusions,
        calculation_end_month=calculation_end_month,
        calculation_month_count=calculation_month_count,
        person_metadata=person_metadata,
        event_to_metric_types=EVENT_TO_METRIC_TYPES,
        event_to_metric_classes=EVENT_TO_METRIC_CLASSES,
    )

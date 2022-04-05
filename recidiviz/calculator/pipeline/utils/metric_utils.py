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
"""Base class for metrics we calculate."""

import datetime
from datetime import date
from typing import Any, Dict, Optional, cast

from enum import Enum
import attr

from recidiviz.common.attr_mixins import BuildableAttr
from recidiviz.common.constants.person_characteristics import Gender, Race, \
    Ethnicity


class MetricMethodologyType(Enum):
    """Methods for counting criminal justice metrics.

     Event-Based: a method for measuring wherein the event, such as
        release from a facility, is the unit of analysis. That is, if Allison
        is released from and returned to a facility twice in a given window,
        then that counts as two separate instances of recidivism for
        measurement, even though it includes only a single individual.
     Person-based: a method for measuring wherein the person is the
        unit of analysis. That is, if Allison is revoked from supervision twice
        in one window, then that counts as only one instance for measurement.
     """

    EVENT = 'EVENT'
    PERSON = 'PERSON'


@attr.s
class RecidivizMetric(BuildableAttr):
    """Base class for modeling a single metric.

    Contains all of the identifying characteristics of the metric, including
    required characteristics for normalization as well as optional
    characteristics for slicing the data.
    """
    # Required characteristics

    # The string id of the calculation pipeline job that produced this metric.
    job_id: str = attr.ib()  # non-nullable

    # The state code of the metric this describes
    state_code: str = attr.ib()

    # MetricMethodologyType enum for the calculation of the metric
    methodology: MetricMethodologyType = \
        attr.ib(default=None)  # non-nullable

    # Optional characteristics

    # The age bucket string of the persons the metric describes, e.g. '<25' or
    # '35-39'
    age_bucket: Optional[str] = attr.ib(default=None)

    # The race of the persons the metric describes
    race: Optional[Race] = attr.ib(default=None)

    # The ethnicity of the persons the metric describes
    ethnicity: Optional[Ethnicity] = attr.ib(default=None)

    # The gender of the persons the metric describes
    gender: Optional[Gender] = attr.ib(default=None)

    # Record keeping fields

    # A date for when this metric was created
    created_on: Optional[date] = attr.ib(default=None)

    # A date for when this metric was last updated
    updated_on: Optional[date] = attr.ib(default=None)

    @staticmethod
    def build_from_metric_key_group(metric_key: Dict[str, Any],
                                    job_id: str) -> \
            Optional['RecidivizMetric']:
        """Builds a RecidivizMetric object from the given arguments.
        """

        if not metric_key:
            raise ValueError("The metric_key is empty.")

        metric_key['job_id'] = job_id
        metric_key['created_on'] = date.today()

        recidiviz_metric = cast(RecidivizMetric,
                                RecidivizMetric.
                                build_from_dictionary(metric_key))

        return recidiviz_metric


@attr.s
class PersonLevelMetric(BuildableAttr):
    """Base class for modeling a person-level metric."""
    # The external_id of StatePerson for person-specific metrics
    person_id: Optional[int] = attr.ib(default=None)

    # The external_id of StatePerson for person-specific metrics
    person_external_id: Optional[str] = attr.ib(default=None)


def json_serializable_metric_key(metric_key: Dict[str, Any]) -> Dict[str, Any]:
    """Converts a metric key into a format that is JSON serializable.

    For values that are of type Enum, converts to their raw values. For values
    that are dates, converts to a string representation.
    """
    serializable_dict = {}

    for key, v in metric_key.items():
        if isinstance(v, Enum) and v is not None:
            serializable_dict[key] = v.value
        elif isinstance(v, datetime.date) and v is not None:
            serializable_dict[key] = v.strftime('%Y-%m-%d')
        else:
            serializable_dict[key] = v

    return serializable_dict

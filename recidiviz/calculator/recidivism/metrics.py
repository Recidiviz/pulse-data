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

"""Recidivism metrics we calculate."""

from datetime import date
from enum import Enum
from typing import Any, Dict, List, Optional, cast

import attr

from recidiviz.calculator.recidivism.release_event import \
    ReincarcerationReturnType, ReincarcerationReturnFromSupervisionType
from recidiviz.common.attr_mixins import BuildableAttr
from recidiviz.common.constants.person_characteristics import Gender, Race, \
    Ethnicity


class RecidivismMethodologyType(Enum):
    """Methods for counting recidivism.

     Event-Based: a method for measuring recidivism wherein the event, such as
        release from a facility, is the unit of analysis. That is, if Allison
        is released from and returned to a facility twice in a given window,
        then that counts as two separate instances of recidivism for
        measurement, even though it includes only a single individual.
     Person-based: a method for measuring recidivism wherein the person is the
        unit of analysis. That is, if Allison is released from and returned to
        a facility twice in a given window, then that counts as only one
        instance of recidivism for measurement.
     """

    EVENT = 'EVENT'
    PERSON = 'PERSON'


# TODO: Implement rearrest and reconviction recidivism metrics
#  (Issues #1841 and #1842)
@attr.s
class ReincarcerationRecidivismMetric(BuildableAttr):
    """Models a single recidivism metric.

    Contains all of the identifying characteristics of the metric, including
    required characteristics for normalization as well as optional
    characteristics for slicing the data.
    """

    # Required characteristics

    # The string id of the calculation pipeline job that produced this metric.
    job_id: str = attr.ib()  # non-nullable

    # RecidivismMethodologyType enum for the calculation of the metric
    methodology: RecidivismMethodologyType = \
        attr.ib(default=None)  # non-nullable

    # Optional characteristics

    # The age bucket string of the persons the metric describes, e.g. '<25' or
    # '35-39'
    age_bucket: Optional[str] = attr.ib(default=None)

    # The bucket string of the persons' incarceration stay length (in months),
    # e.g., '<12' or '36-48'
    stay_length_bucket: Optional[str] = attr.ib(default=None)

    # The race of the persons the metric describes
    race: Optional[Race] = attr.ib(default=None)

    # The ethnicity of the persons the metric describes
    ethnicity: Optional[Ethnicity] = attr.ib(default=None)

    # The gender of the persons the metric describes
    gender: Optional[Gender] = attr.ib(default=None)

    # The facility the persons were released from prior to recidivating
    release_facility: Optional[str] = attr.ib(default=None)

    # ReincarcerationReturnType enum indicating whether the persons returned to
    # incarceration because of a revocation of supervision or because of a
    # new admission
    return_type: ReincarcerationReturnType = attr.ib(default=None)

    # ReincarcerationReturnFromSupervisionType enum for the type of
    # supervision the persons were on before they returned to incarceration.
    from_supervision_type: ReincarcerationReturnFromSupervisionType = \
        attr.ib(default=None)

    # TODO(1793) Track whether revocation of supervision was for a purely
    #  technical violation

    # Record keeping fields

    # A date for when this metric was created
    created_on: Optional[date] = attr.ib(default=None)

    # A date for when this metric was last updated
    updated_on: Optional[date] = attr.ib(default=None)

    @staticmethod
    def build_from_metric_key_group(metric_key: Dict[str, Any],
                                    group: List[int],
                                    job_id: str) -> \
            Optional['ReincarcerationRecidivismMetric']:
        """Builds a ReincarcerationRecidivismMetric object from the given
         arguments.
        """

        if not metric_key:
            raise ValueError("The metric_key is empty.")

        if not group:
            raise ValueError("The group is empty.")

        metric_key['job_id'] = job_id
        metric_key['created_on'] = date.today()

        recidivism_metric = cast(ReincarcerationRecidivismMetric,
                                 ReincarcerationRecidivismMetric.
                                 build_from_dictionary(metric_key))

        return recidivism_metric


@attr.s
class ReincarcerationRecidivismCountMetric(ReincarcerationRecidivismMetric):
    """Subclass of ReincarcerationRecidivismMetric that contains count data.

    A recidivism count metric contains the number of reincarceration returns in
    a given window, with a start and end date.
    """
    # Required characteristics

    # Starting date of the count window
    start_date: int = attr.ib(default=None)

    # Ending date of the count window
    end_date: int = attr.ib(default=None)

    # Number of reincarceration returns
    returns: int = attr.ib(default=None)

    @staticmethod
    def build_from_metric_key_group(metric_key: Dict[str, Any],
                                    group: List[int],
                                    job_id: str) -> \
            Optional['ReincarcerationRecidivismCountMetric']:

        if not metric_key:
            raise ValueError("The metric_key is empty.")

        if not group:
            raise ValueError("The group is empty.")

        # TODO(2036): Look into using Combine transform instead of sum
        returns = sum(group)

        # Don't write metrics with no returns
        if returns == 0:
            return None

        metric_key['returns'] = returns
        metric_key['job_id'] = job_id
        metric_key['created_on'] = date.today()

        recidivism_metric = cast(ReincarcerationRecidivismCountMetric,
                                 ReincarcerationRecidivismCountMetric.
                                 build_from_dictionary(metric_key))

        return recidivism_metric


@attr.s
class ReincarcerationRecidivismRateMetric(ReincarcerationRecidivismMetric):
    """Subclass of ReincarcerationRecidivismMetric that contains rate data.

    A recidivism rate metric contains a recidivism rate, including the numerator
    of total instances of recidivism and a denominator of total instances of
    release from incarceration.
    """
    # Required characteristics

    # The integer year during which the persons were released
    release_cohort: int = attr.ib(default=None)  # non-nullable

    # The integer number of years after date of release during which
    # recidivism was measured
    follow_up_period: int = attr.ib(default=None)  # non-nullable

    # Required metric values

    # The integer number of releases from incarceration that the
    # characteristics in this metric describe
    total_releases: int = attr.ib(default=None)  # non-nullable

    # The total number of releases from incarceration that led to recidivism
    # that the characteristics in this metric describe
    recidivated_releases: float = attr.ib(default=None)  # non-nullable

    # The float rate of recidivism for the releases that the characteristics in
    # this metric describe
    recidivism_rate: float = attr.ib(default=None)  # non-nullable

    @staticmethod
    def build_from_metric_key_group(metric_key: Dict[str, Any],
                                    group: List[int],
                                    job_id: str) -> \
            Optional['ReincarcerationRecidivismRateMetric']:
        """Constructs a RecidivismMetric object from a dictionary containing all
        required values and the corresponding group of ReleaseEvents, with 1s
        representing RecidivismReleaseEvents, and 0s representing
        NonRecidivismReleaseEvents.
        """

        if not metric_key:
            raise ValueError("The metric_key is empty.")

        metric_key['job_id'] = job_id
        metric_key['created_on'] = date.today()
        metric_key['total_releases'] = len(list(group))

        if metric_key['total_releases'] == 0:
            raise ValueError("The group is empty.")

        # TODO(2036): Look into using Combine transform instead of sum
        metric_key['recidivated_releases'] = sum(group)
        metric_key['recidivism_rate'] = ((metric_key['recidivated_releases'] +
                                          0.0) / metric_key['total_releases'])

        recidivism_metric = cast(ReincarcerationRecidivismRateMetric,
                                 ReincarcerationRecidivismRateMetric.
                                 build_from_dictionary(metric_key))

        return recidivism_metric

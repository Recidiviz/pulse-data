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
from enum import Enum, auto
from typing import Any, Dict, List, Optional

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

    EVENT = auto()
    PERSON = auto()


# TODO: Implement rearrest and reconviction recidivism metrics
#  (Issues #1841 and #1842)
@attr.s
class ReincarcerationRecidivismMetric(BuildableAttr):
    """Models a single recidivism metric.

    A recidivism metric contains a recidivism rate, including the numerator of
    total instances of recidivism and a denominator of total instances of
    release from incarceration. It also contains all of the identifying
    characteristics of the metric, including required characteristics for
    normalization as well as optional characteristics for slicing the data.
    """

    # Required characteristics

    # The string id of the calculation pipeline that produced this metric.
    execution_id: int = attr.ib()  # non-nullable

    # The integer year during which the persons were released
    release_cohort: int = attr.ib()  # non-nullable

    # The integer number of years after date of release during which
    # recidivism was measured
    follow_up_period: int = attr.ib()  # non-nullable

    # RecidivismMethodologyType enum for the calculation of the metric
    methodology: RecidivismMethodologyType = attr.ib()  # non-nullable

    # Required metric values

    # The integer number of releases from incarceration that the
    # characteristics in this metric describe
    total_releases: int = attr.ib()  # non-nullable

    # The total number of releases from incarceration that led to recidivism
    # that the characteristics in this metric describe
    recidivated_releases: float = attr.ib()  # non-nullable

    # The float rate of recidivism for the releases that the characteristics in
    # this metric describe
    recidivism_rate: float = attr.ib()  # non-nullable

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
    def build_from_metric_key_release_group(metric_key: Dict[str, Any],
                                            release_group: List[int]) -> \
            Optional['ReincarcerationRecidivismMetric']:
        """Builds a RecidivismMetric object from the given arguments.

        Constructs a RecidivismMetric object from a dictionary containing all
        required values and the corresponding group of ReleaseEvents, with 1s
        representing RecidivismReleaseEvents, and 0s representing
        NonRecidivismReleaseEvents.
        """

        if not metric_key:
            raise ValueError("The metric_key is empty.")

        recidivism_metric = ReincarcerationRecidivismMetric.builder()

        # Calculate number of releases and number resulting in recidivism
        recidivism_metric.total_releases = len(list(release_group))
        recidivism_metric.recidivated_releases = sum(release_group)

        if recidivism_metric.total_releases == 0:
            raise ValueError("The release_group is empty.")

        # Calculate recidivism rate
        recidivism_metric.recidivism_rate = \
            ((recidivism_metric.recidivated_releases + 0.0) /
             recidivism_metric.total_releases)

        # TODO(1789): Implement pipeline execution_id
        recidivism_metric.execution_id = 12345
        recidivism_metric.release_cohort = metric_key.get('release_cohort')
        recidivism_metric.follow_up_period = metric_key.get('follow_up_period')
        recidivism_metric.methodology = metric_key.get('methodology')
        recidivism_metric.created_on = date.today()

        if 'age' in metric_key:
            recidivism_metric.age_bucket = metric_key.get('age')
        if 'ethnicity' in metric_key:
            recidivism_metric.ethnicity = metric_key.get('ethnicity')
        if 'race' in metric_key:
            recidivism_metric.race = metric_key.get('race')
        if 'gender' in metric_key:
            recidivism_metric.gender = metric_key.get('gender')
        if 'release_facility' in metric_key:
            recidivism_metric.release_facility = \
                metric_key.get('release_facility')
        if 'stay_length' in metric_key:
            recidivism_metric.stay_length_bucket = metric_key.get('stay_length')
        if 'return_type' in metric_key:
            recidivism_metric.return_type = metric_key.get('return_type')
        if 'from_supervision_type' in metric_key:
            recidivism_metric.from_supervision_type = \
                metric_key['from_supervision_type']

        return recidivism_metric.build()

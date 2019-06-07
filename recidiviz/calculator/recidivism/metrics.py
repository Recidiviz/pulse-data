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

from recidiviz.calculator.recidivism.recidivism_event import \
    IncarcerationReturnType
from recidiviz.common.constants.entity_enum import EntityEnum, EntityEnumMeta
from recidiviz.common.constants.person_characteristics import Gender, Race


class Methodology(EntityEnum, metaclass=EntityEnumMeta):

    BOTH = 'BOTH'
    EVENT = 'EVENT'
    PERSON = 'PERSON'

    @staticmethod
    def _get_default_map():
        return _METHODOLOGY_MAP


class RecidivismMetric:
    """Models a single recidivism metric.

    A recidivism metric contains a recidivism rate, including the numerator of
    total instances of recidivism and a denominator of total instances of
    release from incarceration. It also contains all of the identifying
    characteristics of the metric, including required characteristics for
    normalization as well as optional characteristics for slicing the data.

    Attributes:
        execution_id: the string id of the calculation pipeline that produced
            this metric. Required.
        release_cohort: the integer year during which the person was released.
            Required.
        follow_up_period: the integer number of years after date of release
            during which recidivism was measured, e.g. the recidivism rate
            within 5 years of release. Required.
        methodology: Methodology enum for the calculation for the metric,
            i.e. Methodology.EVENT or Methodology.PERSON. Required.
        age_bucket: the age bucket string of the person the metric describes,
            e.g. '<25' or '35-39'.
        stay_length_bucket: the bucket string of the person's incarceration stay
            length (in months), e.g., '<12' or '36-48'.
        race: the race of the person the metric describes.
        gender: the gender of the person the metric describes.
        release_facility: the facility the person was released from prior to
            recidivating.
        # TODO(1809): Handle all potential recidivism types
        return_type: IncarcerationReturnType enum indicating whether the person
             returned to incarceration because of a revocation of supervision or
             because of a reconviction.
        total_releases: the integer number of releases from incarceration that
            the characteristics in this metric describe.
        total_returns: the total number of releases from incarceration that
            led to recidivism that the characteristics in this metric describe.
            This is a float because the PERSON methodology calculates a
            weighted recidivism number based on total recidivating instances
            for the person.
        recidivism_rate: the float rate of recidivism for the releases that the
            characteristics in this metric describe.
        created_on: a date for when this metric was created.
        updated_on: a date for when this metric was last updated.

    """

    def __init__(self, execution_id: int = None, release_cohort: int = None,
                 follow_up_period: int = None,
                 methodology: Methodology = None, total_releases: int = None,
                 total_returns: float = None,
                 recidivism_rate: float = None, age_bucket: str = None,
                 stay_length_bucket: str = None,
                 race: Race = None, gender: Gender = None,
                 release_facility: str = None,
                 return_type: IncarcerationReturnType = None,
                 created_on: date = None,
                 updated_on: date = None):

        # Id of the calculation pipeline that created this metric
        self.execution_id = execution_id

        # Required characteristics
        self.release_cohort = release_cohort
        self.follow_up_period = follow_up_period
        self.return_type = return_type
        self.methodology = methodology

        # Required Metric values
        self.total_releases = total_releases
        self.total_returns = total_returns
        self.recidivism_rate = recidivism_rate

        # Optional characteristics
        self.age_bucket = age_bucket
        self.stay_length_bucket = stay_length_bucket
        self.race = race
        self.gender = gender
        self.release_facility = release_facility

        # Record keeping fields
        self.created_on = created_on
        self.updated_on = updated_on

    def __eq__(self, other):
        if other is None:
            return False
        return self.__dict__ == other.__dict__

    def __str__(self):
        return to_string(self)


def to_string(obj):
    out = [obj.__class__.__name__ + ':']
    for key, val in vars(obj).items():
        if isinstance(val, list):
            for index, elem in enumerate(val):
                out += '{}[{}]: {}'.format(key, index, elem).split('\n')
        elif val or val == 0:
            out += '{}: {}'.format(key, val).split('\n')
    return '\n   '.join(out)


_METHODOLOGY_MAP = {
    'BOTH': Methodology.BOTH,
    'EVENT': Methodology.EVENT,
    'PERSON': Methodology.PERSON
}

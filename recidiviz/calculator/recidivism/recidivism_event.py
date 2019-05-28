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

"""Events of recidivism and non-recidivism for calculation."""

from typing import Optional

from datetime import date

from recidiviz.common.constants.entity_enum import EntityEnum, EntityEnumMeta

# TODO(1809): Update this enum to cover all potential recidivism types
class IncarcerationReturnType(EntityEnum, metaclass=EntityEnumMeta):

    RECONVICTION = 'RECONVICTION'
    PAROLE_REVOCATION = 'PAROLE_REVOCATION'
    PROBATION_REVOCATION = 'PROBATION_REVOCATION'

    @staticmethod
    def _get_default_map():
        return _INCARCERATION_RETURN_TYPE_MAP


class RecidivismEvent:
    """Models details related to a recidivism or non-recidivism event.

    This includes the information pertaining to a release from incarceration
    that we will want to track when calculating recidivism metrics, and whether
    or not recidivism later took place.

    Attributes:
        recidivated: A boolean indicating whether or not the person actually
            recidivated for this release event.
        original_admission_date: A Date for when the person first was admitted
            for this period of incarceration.
        release_date: A Date for when the person was last released from this
            period of incarceration.
        release_facility: The facility that the person was last released from
            for this period of incarceration.
        reincarceration_date: A Date for when the person was re-incarcerated.
        reincarceration_facility: The facility that the person entered into upon
            first return to incarceration after the release.
        return_type: IncarcerationReturnType enum describing the type of return
            to incarceration this recidivism event describes.
    """

    def __init__(self,
                 recidivated: bool,
                 original_admission_date: date,
                 release_date: date,
                 release_facility: Optional[str],
                 reincarceration_date: Optional[date] = None,
                 reincarceration_facility: Optional[str] = None,
                 return_type: IncarcerationReturnType = None):

        self.recidivated = recidivated
        self.original_admission_date = original_admission_date
        self.release_date = release_date
        self.release_facility = release_facility
        self.reincarceration_date = reincarceration_date
        self.reincarceration_facility = reincarceration_facility
        self.return_type = return_type

    @staticmethod
    def recidivism_event(original_admission_date: date, release_date: date,
                         release_facility: Optional[str],
                         reincarceration_date: date,
                         reincarceration_facility: Optional[str],
                         return_type: IncarcerationReturnType):
        """Creates a RecidivismEvent instance for an event where reincarceration
        occurred.

        Args:
            original_admission_date: A Date for when the person first was
                admitted for this period of incarceration.
            release_date: A Date for when the person was last released from this
                period of incarceration.
            release_facility: The facility that the person was released
                from for this period of incarceration.
            reincarceration_date: A Date for when the person was
                re-incarcerated.
            reincarceration_facility: The facility that the person entered into
                upon first return to incarceration after the release.
            return_type: IncarcerationReturnType enum describing the type of
                return to incarceration this recidivism event describes.

        Returns:
            A RecidivismEvent for an instance of reincarceration.
        """
        return RecidivismEvent(True, original_admission_date, release_date,
                               release_facility, reincarceration_date,
                               reincarceration_facility, return_type)

    @staticmethod
    def non_recidivism_event(original_admission_date: date, release_date: date,
                             release_facility: Optional[str]):
        """Creates a RecidivismEvent instance for an event where reincarceration
        did not occur.

        Args:
            original_admission_date: A Date for when this period of
                incarceration started.
            release_date: A Date for when the person was released from this
                period of incarceration.
            release_facility: The facility that the person was released
                from for period of incarceration.

        Returns:
            A RecidivismEvent for an instance where recidivism did not occur.
        """
        return RecidivismEvent(False, original_admission_date, release_date,
                               release_facility)

    def __repr__(self):
        return "<RecidivismEvent recidivated: %s, " \
               "original_admission_date: %s, " \
               "release_date: %s, release_facility: %s, " \
               "reincarceration_date: %s, reincarceration_facility: %s, " \
               "return_type: %s>" \
               % (self.recidivated, self.original_admission_date,
                  self.release_date,
                  self.release_facility, self.reincarceration_date,
                  self.reincarceration_facility, self.return_type)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.recidivated == other.recidivated \
                and self.original_admission_date == \
                other.original_admission_date \
                and self.release_date == other.release_date \
                and self.release_facility == other.release_facility \
                and self.reincarceration_facility == \
                other.reincarceration_facility \
                and self.return_type == other.return_type
        return False


_INCARCERATION_RETURN_TYPE_MAP = {
    'RECONVICTION': IncarcerationReturnType.RECONVICTION,
    'PAROLE REVOCATION': IncarcerationReturnType.PAROLE_REVOCATION,
    'PROBATION REVOCATION': IncarcerationReturnType.PROBATION_REVOCATION
}

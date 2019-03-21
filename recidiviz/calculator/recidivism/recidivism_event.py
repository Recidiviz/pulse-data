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


class RecidivismEvent:
    """Models details related to a recidivism or non-recidivism event.

    This includes the information pertaining to a release from prison that we
    will want to track when calculating recidivism metrics, and whether or not
    recidivism later took place.

    Attributes:
        recidivated: A boolean indicating whether or not the person actually
            recidivated for this release event.
        original_entry_date: A Date for when the person first entered prison
            for this record.
        release_date: A Date for when the person was last released from prison
            for this record.
        release_facility: The facility that the person was last released from
            for this record.
        reincarceration_date: A Date for when the person re-entered prison.
        reincarceration_facility: The facility that the person entered into upon
            first return to prison after the release.
        was_conditional: A boolean indicating whether or not the recidivism,
            if it occurred, was due to a conditional violation, as opposed to a
            new incarceration.
    """

    def __init__(self,
                 recidivated,
                 original_entry_date,
                 release_date,
                 release_facility,
                 reincarceration_date=None,
                 reincarceration_facility=None,
                 was_conditional=False):

        self.recidivated = recidivated
        self.original_entry_date = original_entry_date
        self.release_date = release_date
        self.release_facility = release_facility
        self.reincarceration_date = reincarceration_date
        self.reincarceration_facility = reincarceration_facility
        self.was_conditional = was_conditional

    @staticmethod
    def recidivism_event(original_entry_date, release_date, release_facility,
                         reincarceration_date, reincarceration_facility,
                         was_conditional):
        """Creates a RecidivismEvent instance for an event where reincarceration
        occurred.

        Args:
            original_entry_date: A Date for when the person first entered prison
                for this record.
            release_date: A Date for when the person was last released from
                prison for this record.
            release_facility: The facility that the person was last released
                from for this record.
            reincarceration_date: A Date for when the person re-entered prison.
            reincarceration_facility: The facility that the person entered into
                upon first return to prison after the release.
            was_conditional: A boolean indicating whether or not the recidivism,
                if it occurred, was due to a conditional violation, as opposed
                to a new incarceration.

        Returns:
            A RecidivismEvent for an instance of reincarceration.
        """
        return RecidivismEvent(True, original_entry_date, release_date,
                               release_facility, reincarceration_date,
                               reincarceration_facility, was_conditional)

    @staticmethod
    def non_recidivism_event(original_entry_date, release_date,
                             release_facility):
        """Creates a RecidivismEvent instance for an event where reincarceration
        did not occur.

        Args:
            original_entry_date: A Date for when the person first entered
                prison for this record.
            release_date: A Date for when the person was last released from
                prison for this record.
            release_facility: The facility that the person was last released
                from for this record.

        Returns:
            A RecidivismEvent for an instance where recidivism did not occur.
        """
        return RecidivismEvent(False, original_entry_date, release_date,
                               release_facility)

    def __repr__(self):
        return "<RecidivismEvent recidivated: %s, original_entry_date: %s, " \
               "release_date: %s, release_facility: %s, " \
               "reincarceration_date: %s, reincarceration_facility: %s, " \
               "was_conditional: %s>" \
               % (self.recidivated, self.original_entry_date, self.release_date,
                  self.release_facility, self.reincarceration_date,
                  self.reincarceration_facility, self.was_conditional)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.recidivated == other.recidivated \
                   and self.original_entry_date == other.original_entry_date \
                   and self.release_date == other.release_date \
                   and self.release_facility == other.release_facility \
                   and self.reincarceration_facility == \
                   other.reincarceration_facility \
                   and self.was_conditional == other.was_conditional
        return False

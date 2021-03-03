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
"""County schema-specific implementation of BaseHistoricalSnapshotUpdater"""

from types import ModuleType
from typing import List, Any, Optional

from datetime import date, datetime

from recidiviz.common.constants.county.booking import CustodyStatus
from recidiviz.common.constants.county.sentence import SentenceStatus
from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.persistence.database.history.base_historical_snapshot_updater import (
    BaseHistoricalSnapshotUpdater,
    _SnapshotContextRegistry,
    _SnapshotContext,
)
from recidiviz.persistence.database.schema.county import schema


_BOOKING_DESCENDANT_START_DATE_FIELD = {schema.Sentence.__name__: "date_imposed"}

_BOOKING_DESCENDANT_END_DATE_FIELD = {schema.Sentence.__name__: "completion_date"}


class CountyHistoricalSnapshotUpdater(BaseHistoricalSnapshotUpdater[schema.Person]):
    """County schema-specific implementation of BaseHistoricalSnapshotUpdater"""

    def get_system_level(self) -> SystemLevel:
        return SystemLevel.COUNTY

    def get_schema_module(self) -> ModuleType:
        return schema

    def post_process_initial_snapshot(
        self, snapshot_context: _SnapshotContext, initial_snapshot: Any
    ) -> None:
        snapshot_entity = snapshot_context.schema_object
        if isinstance(snapshot_entity, schema.Booking):
            initial_snapshot.custody_status = CustodyStatus.IN_CUSTODY.value
        elif isinstance(snapshot_entity, schema.Sentence):
            initial_snapshot.status = SentenceStatus.PRESENT_WITHOUT_INFO.value
        else:
            raise NotImplementedError(
                "Snapshot backdating not supported for "
                "type {}".format(type(snapshot_entity))
            )

    def set_provided_start_and_end_times(
        self,
        root_people: List[schema.Person],
        context_registry: _SnapshotContextRegistry,
    ) -> None:

        for person in root_people:
            earliest_booking_date = None

            for booking in person.bookings:
                admission_date = None
                release_date = None
                # Don't include case where admission_date_inferred is None
                if booking.admission_date and booking.admission_date_inferred is False:
                    admission_date = booking.admission_date
                    context_registry.snapshot_context(
                        booking
                    ).provided_start_time = self._date_to_datetime(admission_date)
                    if (
                        earliest_booking_date is None
                        or admission_date < earliest_booking_date
                    ):
                        earliest_booking_date = admission_date

                # Don't include case where release_date_inferred is None
                if booking.release_date and booking.release_date_inferred is False:
                    release_date = booking.release_date
                    context_registry.snapshot_context(
                        booking
                    ).provided_end_time = self._date_to_datetime(release_date)
                    if (
                        earliest_booking_date is None
                        or release_date < earliest_booking_date
                    ):
                        earliest_booking_date = release_date

                # If no booking start date is provided or if start date is after
                # end date, end date should be taken as booking start date
                # instead
                booking_start_date = None
                if admission_date is not None:
                    booking_start_date = admission_date
                if release_date is not None and (
                    booking_start_date is None or booking_start_date > release_date
                ):
                    booking_start_date = release_date

                # Only provide start date to descendants if booking is new
                # (otherwise new entities added to an existing booking would be
                # incorrectly backdated)
                booking_start_date_for_descendants = None
                if (
                    context_registry.snapshot_context(booking).most_recent_snapshot
                    is None
                ):
                    booking_start_date_for_descendants = booking_start_date

                self._execute_action_for_all_entities(
                    self._get_related_entities(booking),
                    self._set_provided_start_and_end_time_for_booking_descendant,
                    booking_start_date_for_descendants,
                    context_registry,
                )

            if earliest_booking_date:
                context_registry.snapshot_context(
                    person
                ).provided_start_time = self._date_to_datetime(earliest_booking_date)

    def _set_provided_start_and_end_time_for_booking_descendant(
        self,
        entity: Any,
        parent_booking_start_date: date,
        context_registry: "_SnapshotContextRegistry",
    ) -> None:
        """Sets |entity| provided start time and/or provided end time on
        |context_registry| according to type of |entity|
        """

        # If this method is called during graph exploration on parent booking or
        # parent person, do nothing.
        if isinstance(entity, (schema.Booking, schema.Person)):
            return

        start_time = None
        end_time = None

        # Unless a more specific start time is provided, booking descendants
        # should be treated as having the same start time as the parent booking
        #
        # Note the inverse of this does NOT apply to end time. We cannot assume
        # child entities end when the parent booking ends.
        if parent_booking_start_date:
            start_time = self._date_to_datetime(parent_booking_start_date)

        start_date_field_name = self._get_booking_descendant_start_date_field_name(
            entity
        )
        if start_date_field_name:
            start_date = getattr(entity, start_date_field_name)
            if start_date:
                start_time = self._date_to_datetime(start_date)

        end_date_field_name = self._get_booking_descendant_end_date_field_name(entity)
        if end_date_field_name:
            end_date = getattr(entity, end_date_field_name)
            if end_date:
                end_time = self._date_to_datetime(end_date)

        if start_time:
            context_registry.snapshot_context(entity).provided_start_time = start_time

        if end_time:
            context_registry.snapshot_context(entity).provided_end_time = end_time

    @staticmethod
    def _get_booking_descendant_start_date_field_name(entity: Any) -> Optional[str]:
        """Returns field name, if one exists, on schema object that represents
        the entity's start date
        """
        return _BOOKING_DESCENDANT_START_DATE_FIELD.get(type(entity).__name__, None)

    @staticmethod
    def _get_booking_descendant_end_date_field_name(entity: Any) -> Optional[str]:
        """Returns field name, if one exists, on schema object that represents
        the entity's end date
        """
        return _BOOKING_DESCENDANT_END_DATE_FIELD.get(type(entity).__name__, None)

    @staticmethod
    def _date_to_datetime(d: date) -> datetime:
        return datetime(d.year, d.month, d.day)

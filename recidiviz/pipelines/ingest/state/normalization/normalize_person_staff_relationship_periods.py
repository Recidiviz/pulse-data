# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Normalization logic for StatePersonStaffRelationshipPeriod entities."""
from collections import defaultdict

from recidiviz.common.constants.state.state_person_staff_relationship_period import (
    StatePersonStaffRelationshipType,
)
from recidiviz.common.date import CriticalRangesBuilder, PotentiallyOpenDateRange
from recidiviz.persistence.entity.normalized_entities_utils import (
    update_entity_with_globally_unique_id,
)
from recidiviz.persistence.entity.state.entities import (
    StatePersonStaffRelationshipPeriod,
)
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStatePersonStaffRelationshipPeriod,
)
from recidiviz.pipelines.ingest.state.create_root_entity_id_to_staff_id_mapping import (
    StaffExternalIdToIdMap,
)
from recidiviz.utils.types import assert_type


def _sort_overlapping_relationship_periods(
    person_id: int,
    overlapping_periods: list[StatePersonStaffRelationshipPeriod],
    overlapping_range: PotentiallyOpenDateRange,
    has_relationship_priorities_defined: bool,
) -> list[StatePersonStaffRelationshipPeriod]:
    """Sorts the overlapping relationship periods with the primary relationship first,
    secondary after that, etc.
    """
    if not has_relationship_priorities_defined:
        # By default, prioritize by most recent start date first.
        return list(
            reversed(
                sorted(
                    overlapping_periods,
                    key=lambda p: (
                        p.relationship_start_date,
                        p.associated_staff_external_id,
                        p.relationship_type_raw_text,
                    ),
                )
            )
        )

    found_priorities = {}
    for period in overlapping_periods:
        if period.relationship_priority is None:
            raise ValueError(
                f"Person with person_id [{person_id}] has at least one "
                f"StatePersonStaffRelationshipPeriod with nonnull "
                f"relationship_priority but found [{period}] with null "
                f"relationship_priority. This field must be hydrated to be all null or "
                f"all nonnull at ingest time."
            )
        if period.relationship_priority in found_priorities:
            raise ValueError(
                f"Found multiple StatePersonStaffRelationshipPeriod for person with "
                f"person_id [{person_id}] that have the same relationship_priority "
                f"[{period.relationship_priority}] during the range "
                f"[{overlapping_range}]. If relationship_priority values are "
                f"explicitly hydrated at ingest time, they must not conflict."
            )

        found_priorities[period.relationship_priority] = period

    return sorted(
        overlapping_periods,
        key=lambda p: assert_type(p.relationship_priority, int),
    )


def _get_relationship_periods_for_critical_range(
    *,
    person_id: int,
    critical_range: PotentiallyOpenDateRange,
    critical_range_builder: CriticalRangesBuilder,
    has_relationship_priorities: bool,
    staff_external_id_to_staff_id: StaffExternalIdToIdMap,
) -> list[NormalizedStatePersonStaffRelationshipPeriod]:
    """Helper for producing NormalizedStatePersonStaffRelationshipPeriods for the given
    time range among a set of periods (stored in critical_range_builder) that all have
    the same relationship_type value.
    """
    relationship_periods: list[NormalizedStatePersonStaffRelationshipPeriod] = []
    overlapping_periods = (
        critical_range_builder.get_objects_overlapping_with_critical_range(
            critical_range, type_filter=StatePersonStaffRelationshipPeriod
        )
    )
    sorted_overlapping_periods = _sort_overlapping_relationship_periods(
        person_id=person_id,
        overlapping_range=critical_range,
        overlapping_periods=overlapping_periods,
        has_relationship_priorities_defined=has_relationship_priorities,
    )

    found_staff_id_relationship_pairs: set[
        tuple[int, StatePersonStaffRelationshipType]
    ] = set()
    for period in sorted_overlapping_periods:
        staff_external_id = period.associated_staff_external_id
        staff_external_id_type = period.associated_staff_external_id_type
        staff_id = staff_external_id_to_staff_id[
            (staff_external_id, staff_external_id_type)
        ]

        # If we find two overlapping periods with the same staff_id and relationship
        # type, we deduplicate and only produce one relationship period for the
        # overlapping time range.
        if (staff_id, period.relationship_type) in found_staff_id_relationship_pairs:
            continue

        found_staff_id_relationship_pairs.add((staff_id, period.relationship_type))

        # Relationship priorities are 1-indexed to more closely match
        # output of the BigQuery ROW_NUMBER() function.
        relationship_priority = (
            1
            if not relationship_periods
            else (relationship_periods[-1].relationship_priority + 1)
        )

        normalized_period = NormalizedStatePersonStaffRelationshipPeriod(
            # Add a dummy id to get around attr field validation checks. This
            # will be reset to a unique value below.
            person_staff_relationship_period_id=0,
            state_code=period.state_code,
            system_type=period.system_type,
            system_type_raw_text=period.system_type_raw_text,
            relationship_type=period.relationship_type,
            relationship_type_raw_text=period.relationship_type_raw_text,
            # Truncate this relationship period to the critical range
            relationship_start_date=critical_range.lower_bound_inclusive_date,
            relationship_end_date_exclusive=critical_range.upper_bound_exclusive_date,
            associated_staff_external_id=staff_external_id,
            associated_staff_external_id_type=staff_external_id_type,
            associated_staff_id=staff_id,
            location_external_id=period.location_external_id,
            relationship_priority=relationship_priority,
        )
        update_entity_with_globally_unique_id(
            root_entity_id=person_id, entity=normalized_period
        )
        relationship_periods.append(normalized_period)

    return relationship_periods


def get_normalized_person_staff_relationship_periods(
    person_id: int,
    person_staff_relationship_periods: list[StatePersonStaffRelationshipPeriod],
    staff_external_id_to_staff_id: StaffExternalIdToIdMap,
) -> list[NormalizedStatePersonStaffRelationshipPeriod]:
    """Creates NormalizedStatePersonStaffRelationshipPeriod entities.

    Hydrates relationship_priority values to each period if they are not already
    defined. Priority is given to relationships of a given type that have started more
    recently. In cases where there are multiple overlapping periods with the same start
    date we arbitrarily sort alphabetically by associated_staff_external_id.

    For the time ranges covered by the input |person_staff_relationship_periods|, this
    produces only one period per combo of (time range, relationship type, staff
    member).
    """
    relationship_periods_by_type: dict[
        StatePersonStaffRelationshipType, list[StatePersonStaffRelationshipPeriod]
    ] = defaultdict(list)

    has_relationship_priorities = False
    for period in person_staff_relationship_periods:
        if period.relationship_priority is not None:
            has_relationship_priorities = True
        relationship_periods_by_type[period.relationship_type].append(period)

    relationship_periods: list[NormalizedStatePersonStaffRelationshipPeriod] = []

    for periods_of_type in relationship_periods_by_type.values():
        critical_range_builder = CriticalRangesBuilder(periods_of_type)
        for critical_range in critical_range_builder.get_sorted_critical_ranges():
            relationship_periods.extend(
                _get_relationship_periods_for_critical_range(
                    person_id=person_id,
                    critical_range=critical_range,
                    critical_range_builder=critical_range_builder,
                    has_relationship_priorities=has_relationship_priorities,
                    staff_external_id_to_staff_id=staff_external_id_to_staff_id,
                )
            )

    return relationship_periods

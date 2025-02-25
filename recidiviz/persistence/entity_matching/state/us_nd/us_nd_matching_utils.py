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

"""Contains util methods for UsNdMatchingDelegate."""
import datetime
from typing import Union, cast, List, Tuple, Optional, Set

from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.common.constants.state.state_incarceration import \
    StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodAdmissionReason, \
    StateIncarcerationPeriodReleaseReason
from recidiviz.persistence.database.base_schema import StateBase
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.entity.entity_utils import is_placeholder, \
    get_set_entity_field_names, EntityFieldType
from recidiviz.persistence.entity_matching.entity_matching_types import \
    EntityTree
from recidiviz.persistence.entity_matching.state.state_matching_utils import \
    default_merge_flat_fields, add_child_to_entity, remove_child_from_entity, \
    _get_all_entities_of_cls, revoked_to_prison, admitted_for_revocation
from recidiviz.persistence.errors import EntityMatchingError


def add_supervising_officer_to_open_supervision_periods(
        persons: List[schema.StatePerson]):
    """For each person in the provided |persons|, adds the supervising_officer
    from the person entity onto all open StateSupervisionPeriods.
    """
    for person in persons:
        if not person.supervising_officer:
            continue

        supervision_periods = _get_all_entities_of_cls(
            [person], schema.StateSupervisionPeriod)
        for supervision_period in supervision_periods:
            if not supervision_period.termination_date:
                supervision_period.supervising_officer = \
                    person.supervising_officer


def associate_revocation_svrs_with_ips(
        merged_persons: List[schema.StatePerson]):
    """
    For each person in the provided |merged_persons|, attempts to associate
    StateSupervisionViolationResponses that result in revocation with their
    corresponding StateIncarcerationPeriod.
    """
    for person in merged_persons:
        svrs = _get_all_entities_of_cls(
            [person], schema.StateSupervisionViolationResponse)
        ips = _get_all_entities_of_cls(
            [person], schema.StateIncarcerationPeriod)

        revocation_svrs: List[schema.StateSupervisionViolationResponse] = []
        for svr in svrs:
            svr = cast(schema.StateSupervisionViolationResponse, svr)
            if revoked_to_prison(svr) and svr.response_date:
                revocation_svrs.append(svr)
        revocation_ips: List[schema.StateIncarcerationPeriod] = []
        for ip in ips:
            ip = cast(schema.StateIncarcerationPeriod, ip)
            if admitted_for_revocation(ip) and ip.admission_date:
                revocation_ips.append(ip)

        if not revocation_svrs or not revocation_ips:
            continue

        sorted_svrs = sorted(revocation_svrs, key=lambda x: x.response_date)

        seen: Set[int] = set()
        for svr in sorted_svrs:
            closest_ip = _get_closest_matching_incarceration_period(
                svr, revocation_ips)
            if closest_ip and id(closest_ip) not in seen:
                seen.add(id(closest_ip))
                closest_ip.source_supervision_violation_response = svr


def _get_closest_matching_incarceration_period(
        svr: schema.StateSupervisionViolationResponse,
        ips: List[schema.StateIncarcerationPeriod]) \
        -> Optional[schema.StateIncarcerationPeriod]:
    """Returns the StateIncarcerationPeriod whose admission_date is
    closest to, and within 90 days of, the response_date of the provided |svr|.
    90 days is an arbitrary buffer for which we accept discrepancies between
    the SupervisionViolationResponse response_date and the
    StateIncarcerationPeriod's admission_date.
    """
    closest_ip = min(
        ips, key=lambda x: abs(x.admission_date - svr.response_date))
    if abs((closest_ip.admission_date - svr.response_date).days) <= 90:
        return closest_ip
    return None


def move_incidents_onto_periods(merged_persons: List[schema.StatePerson]):
    """Moves all StateIncarcerationIncidents that have placeholder
    StateIncarcerationPeriod parents onto non-placeholder
    StateIncarcerationPeriods if appropriate.
    """
    for person in merged_persons:
        for sentence_group in person.sentence_groups:
            placeholder_periods, non_placeholder_periods = \
                _get_periods_in_sentence_group(sentence_group)
            _move_incidents_onto_periods_helper(
                placeholder_periods=placeholder_periods,
                non_placeholder_periods=non_placeholder_periods)


def _get_periods_in_sentence_group(
        sentence_group: schema.StateSentenceGroup
) -> Tuple[List[schema.StateIncarcerationPeriod],
           List[schema.StateIncarcerationPeriod]]:
    """Finds all placeholder and non-placeholder StateIncarcerationPeriods in
    the provided |sentence_group|, and returns the two lists in a tuple.
    """
    placeholder_periods = []
    non_placeholder_periods = []

    for incarceration_sentence in \
            sentence_group.incarceration_sentences:
        for incarceration_period in \
                incarceration_sentence.incarceration_periods:
            if is_placeholder(incarceration_period):
                placeholder_periods.append(incarceration_period)
            else:
                non_placeholder_periods.append(incarceration_period)
    return placeholder_periods, non_placeholder_periods


def _move_incidents_onto_periods_helper(
        *,
        placeholder_periods: List[schema.StateIncarcerationPeriod],
        non_placeholder_periods: List[schema.StateIncarcerationPeriod]):
    """Moves all StateIncarcerationIncidents on any of the provided
    |placeholder_periods| onto periods in |non_placeholder_periods|, if a
    matching non-placeholder period exists.
    """
    for placeholder_period in placeholder_periods:
        incidents_to_remove = []
        for incident in placeholder_period.incarceration_incidents:
            match = _find_matching_period(
                incident, non_placeholder_periods)
            if match:
                incidents_to_remove.append((match, incident))

        # Remove incidents from placeholder parent after looping through all
        # incidents.
        for match_period, incident in incidents_to_remove:
            add_child_to_entity(
                entity=match_period,
                child_field_name='incarceration_incidents',
                child_to_add=incident)
            remove_child_from_entity(
                entity=placeholder_period,
                child_field_name='incarceration_incidents',
                child_to_remove=incident)


def _find_matching_period(
        incident: schema.StateIncarcerationIncident,
        potential_periods: List[schema.StateIncarcerationPeriod]) -> \
        Optional[schema.StateIncarcerationPeriod]:
    """Given the |incident|, finds a matching StateIncarcerationPeriod from
    the provided |periods|, if one exists.
    """
    incident_date = incident.incident_date
    if not incident_date:
        return None

    for potential_period in potential_periods:
        admission_date = potential_period.admission_date
        release_date = potential_period.release_date

        # Only match to periods with admission_dates
        if not admission_date:
            continue

        # If no release date, we assume the person is still in custody.
        if not release_date:
            release_date = datetime.date.max

        if admission_date <= incident_date <= release_date \
                and incident.facility == potential_period.facility:
            return potential_period
    return None


def update_temporary_holds(ingested_persons: List[schema.StatePerson], region):
    """ND specific logic to handle correct setting of admission and release
    reasons for incarceration periods that are holds and that directly succeed
    holds.
    """
    enum_overrides = region.get_enum_overrides()
    for person in ingested_persons:
        for sentence_group in person.sentence_groups:
            for incarceration_sentence in \
                    sentence_group.incarceration_sentences:
                _update_temporary_holds_helper(
                    incarceration_sentence.incarceration_periods,
                    enum_overrides)


def _update_temporary_holds_helper(
        ips: List[schema.StateIncarcerationPeriod],
        enum_overrides: EnumOverrides) -> None:
    ips_with_admission_dates = [ip for ip in ips if ip.admission_date]
    sorted_ips = sorted(
        ips_with_admission_dates, key=lambda x: x.admission_date)
    _update_ips_to_holds(sorted_ips)
    for idx, ip in enumerate(sorted_ips):
        if not _is_hold(ip):
            _set_preceding_admission_reason(idx, sorted_ips, enum_overrides)


def _update_ips_to_holds(
        sorted_ips: List[schema.StateIncarcerationPeriod]) -> None:
    after_non_hold = False
    previous_ip = None
    for ip in sorted_ips:
        if not previous_ip:
            if _is_hold(ip):
                ip.admission_reason = StateIncarcerationPeriodAdmissionReason. \
                    TEMPORARY_CUSTODY.value
                ip.release_reason = StateIncarcerationPeriodReleaseReason. \
                    RELEASED_FROM_TEMPORARY_CUSTODY.value
            previous_ip = ip
            continue

        if _is_hold(ip):
            # We don't consider holds as actual holds if they follow
            # consecutively after a prison sentence. If a significant period
            # of time has passed after a prison sentence, then it can
            # be considered a hold.
            if not _are_consecutive(previous_ip, ip) or not after_non_hold:
                ip.admission_reason = StateIncarcerationPeriodAdmissionReason. \
                    TEMPORARY_CUSTODY.value
                ip.release_reason = StateIncarcerationPeriodReleaseReason. \
                    RELEASED_FROM_TEMPORARY_CUSTODY.value
                after_non_hold = False
        else:
            after_non_hold = True
        previous_ip = ip


def _set_preceding_admission_reason(
        idx: int, sorted_ips: List[schema.StateIncarcerationPeriod],
        overrides: EnumOverrides) -> None:
    """
    Given a list of |sorted_ips| and an index |idx| which corresponds to a
    DOCR incarceration period, we select the admission reason of the most
    closely preceding period of temporary custody that is consecutive with
    the DOCR incarceration period.
    """

    beginning_ip = sorted_ips[idx]
    if _is_hold(beginning_ip):
        raise EntityMatchingError(
            f'Expected beginning_ip to NOT be a hold, instead '
            f'found {beginning_ip}', 'incarceration_period')

    earliest_hold_admission_raw_text = None
    subsequent_ip = None
    while idx >= 0:
        ip = sorted_ips[idx]
        if not subsequent_ip:
            subsequent_ip = ip
            idx = idx - 1
            continue

        if not _is_hold(ip) or not _are_consecutive(ip, subsequent_ip):
            break

        earliest_hold_admission_raw_text = ip.admission_reason_raw_text
        subsequent_ip = ip
        idx = idx - 1

    # Update the original incarceration period's admission reason if necessary.
    if earliest_hold_admission_raw_text and \
            beginning_ip.admission_reason \
            == StateIncarcerationPeriodAdmissionReason.TRANSFER.value:
        beginning_ip.admission_reason = \
            StateIncarcerationPeriodAdmissionReason.parse(
                earliest_hold_admission_raw_text, overrides).value


def _is_hold(ip: schema.StateIncarcerationPeriod) -> bool:
    """Determines if the provided |ip| represents a temporary hold and not a
    stay in a DOCR overseen facility.
    """

    # Everything before July 1, 2017 was overseen by DOCR.
    if ip.admission_date < datetime.date(year=2017, month=7, day=1):
        return False

    hold_types = [
        StateIncarcerationType.COUNTY_JAIL.value,
        StateIncarcerationType.EXTERNAL_UNKNOWN.value]
    non_hold_types = [StateIncarcerationType.STATE_PRISON.value]
    if ip.incarceration_type in hold_types:
        return True
    if ip.incarceration_type in non_hold_types:
        return False
    raise EntityMatchingError(
        f"Unexpected StateIncarcerationType"
        f"{ip.incarceration_type}.", ip.get_entity_name())


def _are_consecutive(ip1: schema.StateIncarcerationPeriod,
                     ip2: schema.StateIncarcerationPeriod) -> bool:
    """Determines if the provided StateIncarcerationPeriods are consecutive.
    Periods that start/end within 2 days of each other are still considered
    consecutive, as we expect that data to still represent one, same-day
    movement.

    Note this is order sensitive, and assumes that ip1 is the first period.
    """
    return ip1.release_date and ip2.admission_date \
           and (ip2.admission_date - ip1.release_date).days <= 2


def merge_incarceration_periods(ingested_persons: List[schema.StatePerson]):
    """Merges any incomplete StateIncarcerationPeriods in the provided
    |ingested_persons|.
    """
    for person in ingested_persons:
        for sentence_group in person.sentence_groups:
            for incarceration_sentence in \
                    sentence_group.incarceration_sentences:
                incarceration_sentence.incarceration_periods = \
                    _merge_incarceration_periods_helper(
                        incarceration_sentence.incarceration_periods)


def _merge_incarceration_periods_helper(
        incomplete_incarceration_periods: List[schema.StateIncarcerationPeriod]
) -> List[schema.StateIncarcerationPeriod]:
    """Using the provided |incomplete_incarceration_periods|, attempts to merge
    consecutive admission and release periods from the same facility.

    Returns a list containing all merged incarceration periods as well as all
    incarceration periods that could not be merged, all ordered chronologically
    (based on the movement sequence number provided directly from ND).
    """

    placeholder_periods = [
        p for p in incomplete_incarceration_periods if is_placeholder(p)]
    non_placeholder_periods = [
        p for p in incomplete_incarceration_periods if not is_placeholder(p)]

    # Within any IncarcerationSentence, IncarcerationPeriod external_ids are all
    # equivalent, except for their suffixes. Each suffix is based on the
    # ND-provided movement sequence number. We sort directly by that number.
    sorted_periods = sorted(non_placeholder_periods, key=_get_sequence_no)
    merged_periods = []
    last_period = None
    for period in sorted_periods:
        if not last_period:
            last_period = period
            continue
        if is_incomplete_incarceration_period_match(last_period, period):
            merged_periods.append(
                merge_incomplete_periods(period, last_period))
            last_period = None
        else:
            merged_periods.append(last_period)
            last_period = period

    if last_period:
        merged_periods.append(last_period)
    merged_periods.extend(placeholder_periods)
    return merged_periods


_INCARCERATION_PERIOD_ID_DELIMITER = '|'


def merge_incomplete_periods(
        new_entity: schema.StateIncarcerationPeriod,
        old_entity: schema.StateIncarcerationPeriod
) -> schema.StateIncarcerationPeriod:
    """Merges two incarceration periods with information about
    admission and release into one period. Assumes the status of
    the release event is the most relevant, up-to-date status.

    Args:
        new_entity: The out-of-session period (i.e. new to this ingest run).
        old_entity: The in-session period (i.e. pulled out of the DB), if there
                    is one.
    """

    # Complete match, perform normal merge.
    if new_entity.external_id == old_entity.external_id:
        default_merge_flat_fields(new_entity=new_entity, old_entity=old_entity)
        return old_entity

    # Determine updated external_id
    new_complete = is_incarceration_period_complete(new_entity)
    old_complete = is_incarceration_period_complete(old_entity)
    if new_complete != old_complete:
        updated_external_id = new_entity.external_id \
            if new_complete else old_entity.external_id
    else:
        admission_period, release_period = \
            (new_entity, old_entity) if new_entity.admission_date \
            else (old_entity, new_entity)
        updated_external_id = admission_period.external_id \
                              + _INCARCERATION_PERIOD_ID_DELIMITER \
                              + release_period.external_id

    # Keep the new status if the new period is a release period
    updated_status = new_entity.status if new_entity.release_date \
        else old_entity.status
    updated_status_raw_text = new_entity.status_raw_text \
        if new_entity.release_date else old_entity.status_raw_text

    # Copy all fields from new onto old
    new_fields = \
        get_set_entity_field_names(new_entity, EntityFieldType.FLAT_FIELD)
    for child_field_name in new_fields:
        old_entity.set_field(
            child_field_name, new_entity.get_field(child_field_name))

    # Always update the external id and status
    old_entity.external_id = updated_external_id
    old_entity.status = updated_status
    old_entity.status_raw_text = updated_status_raw_text

    return old_entity


def is_incarceration_period_match(
        ingested_entity: Union[EntityTree, StateBase],
        db_entity: Union[EntityTree, StateBase]) -> bool:
    """
    Determines if the provided |ingested_entity| matches the |db_entity| based
    on ND specific StateIncarcerationPeriod matching.
    """
    if isinstance(ingested_entity, EntityTree):
        db_entity = cast(EntityTree, db_entity.entity)
        ingested_entity = ingested_entity.entity

    ingested_entity = cast(schema.StateIncarcerationPeriod, ingested_entity)
    db_entity = cast(schema.StateIncarcerationPeriod, db_entity)

    # Enforce that all objects being compared are for US_ND
    if ingested_entity.state_code != 'US_ND' \
            or db_entity.state_code != 'US_ND':
        return False

    ingested_complete = is_incarceration_period_complete(ingested_entity)
    db_complete = is_incarceration_period_complete(db_entity)
    if not ingested_complete and not db_complete:
        return is_incomplete_incarceration_period_match(
            ingested_entity, db_entity)
    if ingested_complete and db_complete:
        return ingested_entity.external_id == db_entity.external_id

    # Only one of the two is complete
    complete, incomplete = (ingested_entity, db_entity) \
        if ingested_complete else (db_entity, ingested_entity)

    complete_external_ids = complete.external_id.split(
        _INCARCERATION_PERIOD_ID_DELIMITER)
    incomplete_external_id = incomplete.external_id

    if len(complete_external_ids) != 2:
        raise EntityMatchingError(
            f"Could not split external id of complete incarceration period "
            f"{complete.external_id} as expected", "state_incarceration_period")

    return incomplete_external_id in complete_external_ids


def is_incomplete_incarceration_period_match(
        ingested_entity: schema.StateIncarcerationPeriod,
        db_entity: schema.StateIncarcerationPeriod) -> bool:
    """Given two incomplete StateIncarcerationPeriods, determines if they
    should be considered the same StateIncarcerationPeriod.
    """
    # Cannot match with a placeholder StateIncarcerationPeriod
    if is_placeholder(ingested_entity) or is_placeholder(db_entity):
        return False

    ingested_seq_no = _get_sequence_no(ingested_entity)
    db_seq_no = _get_sequence_no(db_entity)

    # Only match incomplete periods if they are adjacent based on seq no.
    if abs(ingested_seq_no - db_seq_no) != 1:
        return False

    # Check that the first period is an admission and second a release
    if ingested_seq_no < db_seq_no:
        first, second = ingested_entity, db_entity
    else:
        first, second = db_entity, ingested_entity
    if not first.admission_date or not second.release_date:
        return False

    # Must have same facility
    if ingested_entity.facility != db_entity.facility:
        return False

    return True


def _get_sequence_no(period: schema.StateIncarcerationPeriod) -> int:
    """Extracts the ND specific Movement Sequence Number from the external id
    of the provided |period|.
    """
    try:
        external_id = cast(str, period.external_id)
        sequence_no = int(external_id.split('-')[-1])
    except Exception:
        raise EntityMatchingError(
            f"Could not parse sequence number from external_id "
            f"{period.external_id}", period.get_entity_name())
    return sequence_no


def is_incarceration_period_complete(
        period: schema.StateIncarcerationPeriod) -> bool:
    """Returns True if the period is considered complete (has both an admission
    and release date).
    """
    return all([period.admission_date, period.release_date])

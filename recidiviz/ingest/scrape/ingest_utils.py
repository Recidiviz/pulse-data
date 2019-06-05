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

"""Utils file for ingest module"""
import logging
from datetime import tzinfo
from typing import Any, Dict, List, Optional, Set, Union

from google.protobuf import json_format
import pytz

from recidiviz.common import common_utils
from recidiviz.ingest.models import ingest_info, ingest_info_pb2
from recidiviz.ingest.scrape import constants
from recidiviz.utils import environment, regions


def lookup_timezone(timezone: Optional[str]) -> Optional[tzinfo]:
    return pytz.timezone(timezone) if timezone else None


def _regions_matching_environment(region_codes: Set[str]) -> Set[str]:
    """Filter to regions with the matching environment.

    If we are running locally, include all supported regions.
    """
    if not environment.in_gae():
        return region_codes
    gae_env = environment.get_gae_environment()
    return {region_code for region_code in region_codes
            if regions.get_region(region_code).environment == gae_env}


def validate_regions(region_codes: List[str],
                     timezone: tzinfo = None) -> Union[Set[str], bool]:
    """Validates the region arguments.

    If any region in |region_codes| is "all", then all supported regions will be
    returned.

    Args:
        region_codes: List of regions from URL parameters
        timezone: If set, returns only regions in the matching timezone

    Returns:
        False if invalid regions
        List of regions to scrape if successful
    """
    region_codes_output = set(region_codes)

    supported_regions = regions.get_supported_region_codes(timezone=timezone)
    for region in region_codes:
        if region == 'all':
            # We only do this if all is passed to still allow people to manually
            # run any scrapers they wish.
            region_codes_output = _regions_matching_environment(
                supported_regions)
        elif region not in supported_regions:
            logging.error("Region '%s' not recognized.", region)
            return False

    return region_codes_output


def validate_scrape_types(
        scrape_type_list: List[str]) -> List[constants.ScrapeType]:
    """Validates the scrape type arguments.

    If any scrape type in |scrape_type_list| is "all", then all supported scrape
    types will be returned.

    Args:
        scrape_type_list: List of scrape types from URL parameters

    Returns:
        False if invalid scrape types
        List of scrape types if successful
    """
    if not scrape_type_list:
        return [constants.ScrapeType.BACKGROUND]

    scrape_types_output = []
    all_types = False
    for scrape_type in scrape_type_list:
        if scrape_type == 'all':
            all_types = True
        else:
            try:
                scrape_types_output.append(constants.ScrapeType(scrape_type))
            except ValueError:
                logging.error("Scrape type '%s' not recognized.", scrape_type)
                return []

    return list(constants.ScrapeType) if all_types else scrape_types_output


def convert_ingest_info_to_proto(ingest_info_py: ingest_info.IngestInfo) \
        -> ingest_info_pb2.IngestInfo:
    """Converts an ingest_info python object to an ingest info proto."""
    proto = ingest_info_pb2.IngestInfo()

    person_map: Dict[str, ingest_info.Person] = {}
    booking_map: Dict[str, ingest_info.Booking] = {}
    charge_map: Dict[str, ingest_info.Charge] = {}
    hold_map: Dict[str, ingest_info.Hold] = {}
    arrest_map: Dict[str, ingest_info.Arrest] = {}
    bond_map: Dict[str, ingest_info.Bond] = {}
    sentence_map: Dict[str, ingest_info.Sentence] = {}

    state_person_map: Dict[str, ingest_info.StatePerson] = {}
    assessment_map: Dict[str, ingest_info.StateAssessment] = {}
    sentence_group_map: Dict[str, ingest_info.StateSentenceGroup] = {}
    supervision_sentence_map: Dict[str, ingest_info.StateSupervisionSentence] \
        = {}
    incarceration_sentence_map: \
        Dict[str, ingest_info.StateIncarcerationSentence] = {}
    fine_map: Dict[str, ingest_info.StateFine] = {}
    state_charge_map: Dict[str, ingest_info.StateCharge] = {}
    court_case_map: Dict[str, ingest_info.StateCourtCase] = {}
    state_bond_map: Dict[str, ingest_info.StateBond] = {}
    incarceration_period_map: Dict[str, ingest_info.StateIncarcerationPeriod] \
        = {}
    supervision_period_map: Dict[str, ingest_info.StateSupervisionPeriod] \
        = {}
    incarceration_incident_map: \
        Dict[str, ingest_info.StateIncarcerationIncident] = {}
    parole_decision_map: Dict[str, ingest_info.StateParoleDecision] = {}
    supervision_violation_map: \
        Dict[str, ingest_info.StateSupervisionViolation] = {}

    id_to_generated: Dict[int, str] = {}

    def _populate_proto(proto_name, ingest_info_source, id_name, proto_map):
        """Populates all of the proto fields from an IngestInfo object.

        Args:
            proto_name: The name of the proto we are populating, like 'people',
                'arrests', etc...
            ingest_info_source: The source object we are copying from
            id_name: The name of the id, is 'person_id', 'arrest_id', etc...
                This is used to decide whether or not to generate a new proto,
                use the given id, or generate a new one
            proto_map: A map of protos we have already created to know whether
                we need to go ahead creating one or if we can just return
                the already created one.
        Returns:
            A populated proto
        """
        obj_id = getattr(ingest_info_source, id_name)
        if not obj_id:
            obj_id = id_to_generated.get(id(ingest_info_source)) or \
                     common_utils.create_generated_id()
            id_to_generated[id(ingest_info_source)] = obj_id

        # If we've already seen this id, we don't need to create it, we can
        # simply return it as is.  Not that we use this local in memory
        # proto map so as to avoid using the map provided in proto to make it
        # simpler for external people to read it.  If we decide that no one
        # but us will ever read the proto then we can remove this logic here
        # and use the built in proto map.
        if obj_id in proto_map:
            return proto_map[obj_id]

        proto_to_populate = getattr(proto, proto_name).add()
        setattr(proto_to_populate, id_name, obj_id)
        _copy_fields(ingest_info_source, proto_to_populate,
                     proto_to_populate.DESCRIPTOR)

        proto_map[obj_id] = proto_to_populate
        return proto_to_populate

    def _populate_state_charge_protos(parent_ingest_object, parent_proto):
        """Populates StateCharge proto fields for some parent object,
        e.g. SupervisionSentence and IncarcerationSentence, which both
        reference StateCharge.
        """
        for state_charge in parent_ingest_object.state_charges:
            proto_state_charge = _populate_proto(
                'state_charges', state_charge,
                'state_charge_id', state_charge_map)
            parent_proto.charge_ids.append(proto_state_charge.state_charge_id)

            if state_charge.court_case:
                proto_court_case = _populate_proto(
                    'state_court_cases', state_charge.state_court_case,
                    'state_court_case_id', court_case_map)
                proto_state_charge.state_court_case_id = \
                    proto_court_case.state_court_case_id

            if state_charge.bond:
                proto_state_bond = _populate_proto(
                    'state_bonds', state_charge.bond,
                    'state_bond_id', state_bond_map)
                proto_charge.state_bond_id = proto_state_bond.state_bond_id

    def _populate_incarceration_period_protos(parent_ingest_object,
                                              parent_proto):
        """Populates IncarcerationPeriod proto fields for some parent object,
        e.g. SupervisionSentence and IncarcerationSentence, which both
        reference IncarcerationPeriod.
        """
        for incarceration_period in \
                parent_ingest_object.state_incarceration_periods:
            proto_period = _populate_proto(
                'state_incarceration_periods', incarceration_period,
                'state_incarceration_period_id', incarceration_period_map)
            parent_proto.state_incarceration_period_ids.append(
                proto_period.state_incarceration_period_id)

            for incident in incarceration_period.state_incarceration_incidents:
                proto_incident = _populate_proto(
                    'state_incarceration_incidents', incident,
                    'state_incarceration_incident_id',
                    incarceration_incident_map)
                proto_period.state_incarceration_incident_ids.append(
                    proto_incident.state_incarceration_incident_id)

            for decision in incarceration_period.state_parole_decisions:
                proto_decision = _populate_proto(
                    'state_parole_decisions', decision,
                    'state_parole_decision_id', parole_decision_map)
                proto_period.state_parole_decision_ids.append(
                    proto_decision.state_fparole_decision_id)

            for period_assessment in incarceration_period.state_assessments:
                proto_period_assessment = _populate_proto(
                    'state_assessments', period_assessment,
                    'state_assessment_id', assessment_map)
                proto_period.state_assessment_ids.append(
                    proto_period_assessment.state_assessment_id)

    def _populate_supervision_period_protos(parent_ingest_object,
                                            parent_proto):
        """Populates SupervisionPeriod proto fields for some parent object,
        e.g. SupervisionSentence and IncarcerationSentence, which both
        reference SupervisionPeriod.
        """
        for supervision_period in \
                parent_ingest_object.state_supervision_periods:
            proto_period = _populate_proto(
                'state_supervision_periods', supervision_period,
                'state_supervision_period_id', supervision_period_map)
            parent_proto.supervision_period_ids.append(
                proto_period.supervision_period_id)

            for violation in supervision_period.state_supervision_violations:
                proto_violation = _populate_proto(
                    'state_supervision_violations', violation,
                    'state_supervision_violation_id', supervision_violation_map)
                proto_period.state_supervision_violation_ids.append(
                    proto_violation.state_supervision_violation_id)

            for period_assessment in supervision_period.state_assessments:
                proto_period_assessment = _populate_proto(
                    'state_assessments', period_assessment,
                    'state_assessment_id', assessment_map)
                proto_period.state_assessment_ids.append(
                    proto_period_assessment.state_assessment_id)

    for person in ingest_info_py.people:
        proto_person = _populate_proto(
            'people', person, 'person_id', person_map)
        for booking in person.bookings:
            proto_booking = _populate_proto(
                'bookings', booking, 'booking_id', booking_map)
            # Can safely append the ids now since they should be unique.
            proto_person.booking_ids.append(proto_booking.booking_id)

            if booking.arrest:
                proto_arrest = _populate_proto(
                    'arrests', booking.arrest, 'arrest_id', arrest_map)
                proto_booking.arrest_id = proto_arrest.arrest_id

            for hold in booking.holds:
                proto_hold = _populate_proto('holds', hold, 'hold_id', hold_map)
                proto_booking.hold_ids.append(proto_hold.hold_id)

            for charge in booking.charges:
                proto_charge = _populate_proto(
                    'charges', charge, 'charge_id', charge_map)
                proto_booking.charge_ids.append(proto_charge.charge_id)

                if charge.bond:
                    proto_bond = _populate_proto(
                        'bonds', charge.bond, 'bond_id', bond_map)
                    proto_charge.bond_id = proto_bond.bond_id

                if charge.sentence:
                    proto_sentence = _populate_proto(
                        'sentences', charge.sentence,
                        'sentence_id', sentence_map)
                    proto_charge.sentence_id = proto_sentence.sentence_id

    for state_person in ingest_info_py.state_people:
        proto_state_person = _populate_proto(
            'state_people', state_person, 'state_person_id', state_person_map)

        for assessment in state_person.state_assessments:
            proto_assessment = _populate_proto(
                'state_assessments', assessment,
                'state_assessment_id', assessment_map)
            proto_state_person.state_assessment_ids.append(
                proto_assessment.state_assessment_id)

        for sentence_group in state_person.state_sentence_groups:
            proto_sentence_group = _populate_proto(
                'state_sentence_groups', sentence_group,
                'state_sentence_group_id', sentence_group_map)
            proto_state_person.state_sentence_group_ids.append(
                proto_sentence_group.state_sentence_group_id)

            for supervision_sentence in \
                    sentence_group.state_supervision_sentences:
                proto_supervision_sentence = _populate_proto(
                    'state_supervision_sentences', supervision_sentence,
                    'state_supervision_sentence_id', supervision_sentence_map)
                proto_sentence_group.state_supervision_sentence_ids.append(
                    proto_supervision_sentence.state_supervision_sentence_id)

                _populate_state_charge_protos(
                    supervision_sentence, proto_supervision_sentence)

                _populate_incarceration_period_protos(
                    supervision_sentence, proto_supervision_sentence)

                _populate_supervision_period_protos(
                    supervision_sentence, proto_supervision_sentence)

            for incarceration_sentence in \
                    sentence_group.state_incarceration_sentences:
                proto_incarceration_sentence = _populate_proto(
                    'state_incarceration_sentences', incarceration_sentence,
                    'state_incarceration_sentence_id',
                    incarceration_sentence_map)
                proto_sentence_group.state_incarceration_sentence_ids\
                    .append(proto_incarceration_sentence
                            .state_incarceration_sentence_id)

                _populate_state_charge_protos(
                    incarceration_sentence, proto_incarceration_sentence)

                _populate_incarceration_period_protos(
                    incarceration_sentence, proto_incarceration_sentence)

                _populate_supervision_period_protos(
                    incarceration_sentence, proto_incarceration_sentence)

            for fine in sentence_group.state_fines:
                proto_fine = _populate_proto('state_fines', fine,
                                             'state_fine_id', fine_map)
                proto_sentence_group.fine_ids.append(proto_fine.state_fine_id)

                _populate_state_charge_protos(fine, proto_fine)

    return proto


def convert_proto_to_ingest_info(
        proto: ingest_info_pb2.IngestInfo) -> ingest_info.IngestInfo:
    """Populates an `IngestInfo` python object from the given proto """

    person_map: Dict[str, ingest_info.Person] = \
        dict(_proto_to_py(person, ingest_info.Person, 'person_id')
             for person in proto.people)
    booking_map: Dict[str, ingest_info.Booking] = \
        dict(_proto_to_py(booking, ingest_info.Booking, 'booking_id')
             for booking in proto.bookings)
    charge_map: Dict[str, ingest_info.Charge] = \
        dict(_proto_to_py(charge, ingest_info.Charge, 'charge_id')
             for charge in proto.charges)
    hold_map: Dict[str, ingest_info.Hold] = \
        dict(_proto_to_py(hold, ingest_info.Hold, 'hold_id')
             for hold in proto.holds)
    arrest_map: Dict[str, ingest_info.Arrest] = \
        dict(_proto_to_py(arrest, ingest_info.Arrest, 'arrest_id')
             for arrest in proto.arrests)
    bond_map: Dict[str, ingest_info.Bond] = \
        dict(_proto_to_py(bond, ingest_info.Bond, 'bond_id')
             for bond in proto.bonds)
    sentence_map: Dict[str, ingest_info.Sentence] = \
        dict(_proto_to_py(sentence, ingest_info.Sentence, 'sentence_id')
             for sentence in proto.sentences)

    state_person_map: Dict[str, ingest_info.StatePerson] = \
        dict(_proto_to_py(state_person, ingest_info.StatePerson,
                          'state_person_id')
             for state_person in proto.state_people)
    assessment_map: Dict[str, ingest_info.StateAssessment] = \
        dict(_proto_to_py(assessment,
                          ingest_info.StateAssessment,
                          'state_assessment_id')
             for assessment in proto.state_assessments)
    sentence_group_map: Dict[str, ingest_info.StateSentenceGroup] = \
        dict(_proto_to_py(sentence_group, ingest_info.StateSentenceGroup,
                          'state_sentence_group_id')
             for sentence_group in proto.state_sentence_groups)
    supervision_sentence_map: Dict[str, ingest_info.StateSupervisionSentence] \
        = dict(_proto_to_py(supervision_sentence,
                            ingest_info.StateSupervisionSentence,
                            'state_supervision_sentence_id')
               for supervision_sentence in proto.state_supervision_sentences)
    incarceration_sentence_map: \
        Dict[str, ingest_info.StateIncarcerationSentence] = \
        dict(_proto_to_py(incarceration_sentence,
                          ingest_info.StateIncarcerationSentence,
                          'state_incarceration_sentence_id')
             for incarceration_sentence in proto.state_incarceration_sentences)
    fine_map: Dict[str, ingest_info.StateFine] = \
        dict(_proto_to_py(fine, ingest_info.StateFine, 'state_fine_id')
             for fine in proto.state_fines)
    state_charge_map: Dict[str, ingest_info.StateCharge] = \
        dict(_proto_to_py(state_charge, ingest_info.StateCharge,
                          'state_charge_id')
             for state_charge in proto.state_charges)
    court_case_map: Dict[str, ingest_info.StateCourtCase] = \
        dict(_proto_to_py(court_case,
                          ingest_info.StateCourtCase,
                          'state_court_case_id')
             for court_case in proto.court_cases)
    state_bond_map: Dict[str, ingest_info.StateBond] = \
        dict(_proto_to_py(state_bond,
                          ingest_info.StateBond,
                          'state_bond_id')
             for state_bond in proto.state_bonds)
    incarceration_period_map: Dict[str, ingest_info.StateIncarcerationPeriod] \
        = dict(_proto_to_py(incarceration_period,
                            ingest_info.StateIncarcerationPeriod,
                            'state_incarceration_period_id')
               for incarceration_period in proto.state_incarceration_periods)
    supervision_period_map: Dict[str, ingest_info.StateSupervisionPeriod] = \
        dict(_proto_to_py(supervision_period,
                          ingest_info.StateSupervisionPeriod,
                          'state_supervision_period_id')
             for supervision_period in proto.state_supervision_periods)
    incarceration_incident_map: \
        Dict[str, ingest_info.StateIncarcerationIncident] = \
        dict(_proto_to_py(incarceration_incident,
                          ingest_info.StateIncarcerationIncident,
                          'state_incarceration_incident_id')
             for incarceration_incident in proto.state_incarceration_incidents)
    parole_decision_map: Dict[str, ingest_info.StateParoleDecision] = \
        dict(_proto_to_py(parole_decision,
                          ingest_info.StateParoleDecision,
                          'state_parole_decision_id')
             for parole_decision in proto.state_parole_decisions)
    supervision_violation_map: \
        Dict[str, ingest_info.StateSupervisionViolation] = \
        dict(_proto_to_py(supervision_violation,
                          ingest_info.StateSupervisionViolation,
                          'state_supervision_violation_id')
             for supervision_violation in proto.state_supervision_violations)

    # Wire bonds and sentences to respective charges
    for proto_charge in proto.charges:
        charge = charge_map[proto_charge.charge_id]
        if proto_charge.bond_id:
            charge.bond = bond_map[proto_charge.bond_id]
        if proto_charge.sentence_id:
            charge.sentence = sentence_map[proto_charge.sentence_id]

    # Wire arrests, charges, and holds to respective bookings
    for proto_booking in proto.bookings:
        booking = booking_map[proto_booking.booking_id]
        if proto_booking.arrest_id:
            booking.arrest = arrest_map[proto_booking.arrest_id]
        booking.charges = [charge_map[proto_id]
                           for proto_id in proto_booking.charge_ids]
        booking.holds = [hold_map[proto_id]
                         for proto_id in proto_booking.hold_ids]

    # Wire bookings to respective people
    for proto_person in proto.people:
        person = person_map[proto_person.person_id]
        person.bookings = [booking_map[proto_id]
                           for proto_id in proto_person.booking_ids]

    def _wire_sentence_proto(proto_sentence, proto_sentence_id,
                             proto_sentence_map):
        """Wires up child entities to their respective sentence types."""
        sentence = proto_sentence_map[proto_sentence_id]

        sentence.state_charges = \
            [state_charge_map[proto_id] for proto_id
             in proto_sentence.state_charge_ids]

        sentence.supervision_periods = \
            [supervision_period_map[proto_id] for proto_id
             in proto_sentence.supervision_period_ids]

        sentence.incarceration_periods = \
            [incarceration_period_map[proto_id] for proto_id
             in proto_sentence.incarceration_period_ids]

    # Wire child entities to respective supervision periods
    for proto_supervision_period in proto.state_supervision_periods:
        supervision_period = supervision_period_map[
            proto_supervision_period.state_supervision_period_id]
        supervision_period.state_supervision_violations = \
            [supervision_violation_map[proto_id] for proto_id
             in proto_supervision_period.state_supervision_violation_ids]
        supervision_period.state_assessments = \
            [assessment_map[proto_id] for proto_id
             in proto_supervision_period.state_assessment_ids]

    # Wire child entities to respective incarceration periods
    for proto_incarceration_period in proto.state_incarceration_periods:
        incarceration_period = incarceration_period_map[
            proto_incarceration_period.state_incarceration_period_id]
        incarceration_period.state_incarceration_incidents = \
            [incarceration_incident_map[proto_id] for proto_id
             in proto_incarceration_period.state_incarceration_incident_ids]
        incarceration_period.state_parole_decisions = \
            [parole_decision_map[proto_id] for proto_id
             in proto_incarceration_period.state_parole_decision_ids]
        incarceration_period.state_assessments = \
            [assessment_map[proto_id] for proto_id
             in proto_incarceration_period.state_assessment_ids]

    # Wire court cases and state bonds to respective state charges
    for proto_state_charge in proto.state_charges:
        state_charge = fine_map[proto_state_charge.state_charge_id]
        if proto_state_charge.state_court_case_id:
            state_charge.court_case = court_case_map[
                proto_state_charge.state_court_case_id]

        if proto_state_charge.state_bond_id:
            state_charge.bond = state_bond_map[
                proto_state_charge.state_bond_id]

    # Wire all state charges and period types to respective sentence types
    for proto_fine in proto.state_fines:
        fine = fine_map[proto_fine.state_fine_id]
        fine.state_charges = [state_charge_map[proto_id] for proto_id
                              in proto_fine.state_charge_ids]

    for proto_incarceration_sentence in proto.state_incarceration_sentences:
        _wire_sentence_proto(
            proto_incarceration_sentence,
            proto_incarceration_sentence.state_incarceration_sentence_id,
            incarceration_sentence_map)

    for proto_supervision_sentence in proto.state_supervision_sentences:
        _wire_sentence_proto(
            proto_supervision_sentence,
            proto_supervision_sentence.state_supervision_sentence_id,
            supervision_sentence_map)

    # Wire all sentence types to respective sentence groups
    for proto_sentence_group in proto.state_sentence_groups:
        sentence_group = sentence_group_map[
            proto_sentence_group.state_sentence_group_id]

        sentence_group.state_fines = [fine_map[proto_id] for proto_id
                                      in proto_sentence_group.state_fine_ids]

        sentence_group.state_incarceration_sentences = \
            [incarceration_sentence_map[proto_id] for proto_id
             in proto_sentence_group.state_incarceration_sentence_ids]

        sentence_group.state_supervision_sentences = \
            [supervision_sentence_map[proto_id] for proto_id
             in proto_sentence_group.state_supervision_sentence_ids]

    # Wire assessments and sentence groups to respective state people
    for proto_state_person in proto.state_people:
        state_person = state_person_map[proto_state_person.state_person_id]

        state_person.state_assessments = \
            [assessment_map[proto_id] for proto_id
             in proto_state_person.state_assessment_ids]

        state_person.state_sentence_groups = \
            [sentence_group_map[proto_id] for proto_id
             in proto_state_person.state_sentence_group_ids]

    # Wire people to ingest info
    ii = ingest_info.IngestInfo()
    ii.people.extend(person_map.values())
    ii.state_people.extend(state_person_map.values())
    return ii


def _proto_to_py(proto, py_type, id_name):
    py_obj = py_type()
    _copy_fields(proto, py_obj, proto.DESCRIPTOR)

    obj_id = getattr(proto, id_name)
    if not common_utils.is_generated_id(obj_id):
        setattr(py_obj, id_name, obj_id)

    return obj_id, py_obj


def _copy_fields(source, dest, descriptor):
    """Copies all fields except ids from the source to the destination

    Since we expect all of the fields to exist in both the destination
    and the source, we can just run through all of the fields in the proto
    and use python magic to retrieve the value from the source and set
    them on the destination.
    """
    field_names = [i.name for i in descriptor.fields]
    for field in field_names:
        if field.endswith('id') or field.endswith('ids'):
            continue
        val = getattr(source, field, None)
        if val is not None:
            setattr(dest, field, val)


def ingest_info_to_serializable(ii: ingest_info.IngestInfo) -> Dict[Any, Any]:
    proto = convert_ingest_info_to_proto(ii)
    return json_format.MessageToDict(proto)


def ingest_info_from_serializable(serializable: Dict[Any, Any]) \
        -> ingest_info.IngestInfo:
    proto = json_format.ParseDict(serializable, ingest_info_pb2.IngestInfo())
    return convert_proto_to_ingest_info(proto)

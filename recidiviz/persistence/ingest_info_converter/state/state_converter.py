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
# ============================================================================

"""Converts ingested IngestInfo data to the persistence layer entities."""

from recidiviz.ingest.models.ingest_info_pb2 import StateSentenceGroup, \
    StatePerson, StateSupervisionSentence, StateIncarcerationSentence, \
    StateCharge, StateIncarcerationPeriod, StateSupervisionPeriod, \
    StateSupervisionViolation, StateFine, StateParoleDecision, \
    StateIncarcerationIncident, StateAssessment, StateCourtCase, \
    StateSupervisionViolationResponse, StateProgramAssignment
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.ingest_info_converter.base_converter import \
    BaseConverter
from recidiviz.persistence.ingest_info_converter.state.entity_helpers import \
    state_person, state_alias, state_person_race, state_person_ethnicity, \
    state_assessment, state_person_external_id, state_sentence_group, \
    state_supervision_sentence, state_incarceration_sentence, state_charge, \
    state_bond, state_court_case, state_incarceration_period, \
    state_supervision_period, state_parole_decision, \
    state_incarceration_incident, state_supervision_violation, \
    state_supervision_violation_response, state_fine, state_agent, \
    state_incarceration_incident_outcome, state_program_assignment, \
    state_supervision_violation_type_entry, \
    state_supervision_violated_condition_entry, \
    state_supervision_violation_response_decision_type_entry
from recidiviz.persistence.ingest_info_converter.utils.converter_utils import fn


class StateConverter(BaseConverter[entities.StatePerson]):
    """Converts between ingest_info objects and persistence layer entities
    for state-level entities."""

    def __init__(self, ingest_info, metadata):
        super().__init__(ingest_info, metadata)

        self.aliases = {a.state_alias_id: a for a in ingest_info.state_aliases}
        self.person_races = {pr.state_person_race_id: pr for pr
                             in ingest_info.state_person_races}
        self.person_ethnicities = {pe.state_person_ethnicity_id: pe for pe
                                   in ingest_info.state_person_ethnicities}
        self.person_external_ids = {pei.state_person_external_id_id: pei for pei
                                    in ingest_info.state_person_external_ids}
        self.assessments = {a.state_assessment_id: a for a
                            in ingest_info.state_assessments}
        self.program_assignments = {pa.state_program_assignment_id: pa for
                                    pa in ingest_info.state_program_assignments}
        self.agents = {a.state_agent_id: a for a in ingest_info.state_agents}
        self.sentence_groups = {sg.state_sentence_group_id: sg for sg
                                in ingest_info.state_sentence_groups}
        self.supervision_sentences = {
            ss.state_supervision_sentence_id: ss for ss
            in ingest_info.state_supervision_sentences}
        self.incarceration_sentences = {
            ins.state_incarceration_sentence_id: ins for ins
            in ingest_info.state_incarceration_sentences}
        self.fines = {f.state_fine_id: f for f in ingest_info.state_fines}
        self.charges = {sc.state_charge_id: sc for sc
                        in ingest_info.state_charges}
        self.bonds = {b.state_bond_id: b for b in ingest_info.state_bonds}
        self.court_cases = {cc.state_court_case_id: cc for cc
                            in ingest_info.state_court_cases}
        self.supervision_periods = {
            sp.state_supervision_period_id: sp for sp
            in ingest_info.state_supervision_periods
        }
        self.incarceration_periods = {
            ip.state_incarceration_period_id: ip for ip
            in ingest_info.state_incarceration_periods
        }
        self.parole_decisions = {
            pd.state_parole_decision_id: pd for pd
            in ingest_info.state_parole_decisions
        }
        self.parole_decisions = {pd.state_parole_decision_id: pd for pd
                                 in ingest_info.state_parole_decisions}
        self.incarceration_incidents = {
            ii.state_incarceration_incident_id: ii
            for ii in ingest_info.state_incarceration_incidents
        }

        self.incarceration_incident_outcomes = {
            iio.state_incarceration_incident_outcome_id: iio
            for iio in ingest_info.state_incarceration_incident_outcomes
        }

        self.supervision_violations = {
            sv.state_supervision_violation_id: sv for sv
            in ingest_info.state_supervision_violations
        }
        self.violated_condition_entries = {
            svce.state_supervision_violated_condition_entry_id: svce for svce
            in ingest_info.state_supervision_violated_condition_entries
        }
        self.violation_type_entries = {
            svte.state_supervision_violation_type_entry_id: svte for svte
            in ingest_info.state_supervision_violation_type_entries
        }

        self.violation_responses = {
            vr.state_supervision_violation_response_id: vr for vr
            in ingest_info.state_supervision_violation_responses
        }

        self.violation_response_decision_type_entries = {
            svrdte.state_supervision_violation_response_decision_type_entry_id:
            svrdte for svrdte
            # pylint: disable=line-too-long
            in ingest_info.state_supervision_violation_response_decision_type_entries
        }

    def _is_complete(self) -> bool:
        if self.ingest_info.state_people:
            return False
        return True

    def _convert_and_pop(self) -> entities.StatePerson:
        return self._convert_state_person(self.ingest_info.state_people.pop())

    def _convert_state_person(self, ingest_state_person: StatePerson) \
            -> entities.StatePerson:
        """Converts an ingest_info proto StatePerson to a persistence entity."""
        state_person_builder = entities.StatePerson.builder()

        state_person.copy_fields_to_builder(
            state_person_builder, ingest_state_person, self.metadata)

        converted_aliases = [state_alias.convert(self.aliases[alias_id],
                                                 self.metadata)
                             for alias_id
                             in ingest_state_person.state_alias_ids]
        state_person_builder.aliases = converted_aliases

        converted_races = [
            state_person_race.convert(self.person_races[race_id],
                                      self.metadata)
            for race_id in ingest_state_person.state_person_race_ids
        ]
        state_person_builder.races = converted_races

        converted_ethnicities = [
            state_person_ethnicity.convert(
                self.person_ethnicities[ethnicity_id], self.metadata)
            for ethnicity_id in ingest_state_person.state_person_ethnicity_ids
        ]
        state_person_builder.ethnicities = converted_ethnicities

        converted_assessments = [
            self._convert_assessment(self.assessments[assessment_id])
            for assessment_id in ingest_state_person.state_assessment_ids
        ]
        state_person_builder.assessments = converted_assessments
        converted_program_assignments = [
            self._convert_program_assignment(
                self.program_assignments[assignment_id])
            for assignment_id in
            ingest_state_person.state_program_assignment_ids
        ]
        state_person_builder.program_assignments = converted_program_assignments

        converted_external_ids = [
            state_person_external_id.convert(
                self.person_external_ids[external_id], self.metadata)
            for external_id in ingest_state_person.state_person_external_ids_ids
        ]
        state_person_builder.external_ids = converted_external_ids

        converted_sentence_groups = [
            self._convert_sentence_group(
                self.sentence_groups[sentence_group_id])
            for sentence_group_id
            in ingest_state_person.state_sentence_group_ids
        ]
        state_person_builder.sentence_groups = converted_sentence_groups

        if ingest_state_person.supervising_officer_id:
            converted_supervising_officer = state_agent.convert(
                self.agents[ingest_state_person.supervising_officer_id],
                self.metadata)
            state_person_builder.supervising_officer = \
                converted_supervising_officer

        return state_person_builder.build()

    def _convert_sentence_group(self,
                                ingest_sentence_group: StateSentenceGroup) \
            -> entities.StateSentenceGroup:
        """Converts an ingest_info proto StateSentenceGroup to a
        persistence entity."""
        sentence_group_builder = entities.StateSentenceGroup.builder()

        state_sentence_group.copy_fields_to_builder(sentence_group_builder,
                                                    ingest_sentence_group,
                                                    self.metadata)

        converted_supervision_sentences = [
            self._convert_supervision_sentence(
                self.supervision_sentences[sentence_id])
            for sentence_id
            in ingest_sentence_group.state_supervision_sentence_ids
        ]
        sentence_group_builder.supervision_sentences = \
            converted_supervision_sentences

        converted_incarceration_sentences = [
            self._convert_incarceration_sentence(
                self.incarceration_sentences[sentence_id])
            for sentence_id
            in ingest_sentence_group.state_incarceration_sentence_ids
        ]
        sentence_group_builder.incarceration_sentences = \
            converted_incarceration_sentences

        converted_fines = [
            self._convert_fine(self.fines[fine_id])
            for fine_id in ingest_sentence_group.state_fine_ids
        ]
        sentence_group_builder.fines = converted_fines

        return sentence_group_builder.build()

    def _convert_supervision_sentence(
            self, ingest_supervision_sentence: StateSupervisionSentence) \
            -> entities.StateSupervisionSentence:
        """Converts an ingest_info proto StateSupervisionSentence to a
        persistence entity."""
        supervision_sentence_builder = \
            entities.StateSupervisionSentence.builder()

        state_supervision_sentence.copy_fields_to_builder(
            supervision_sentence_builder,
            ingest_supervision_sentence,
            self.metadata)

        self._copy_children_to_sentence(supervision_sentence_builder,
                                        ingest_supervision_sentence)

        return supervision_sentence_builder.build()

    def _convert_incarceration_sentence(
            self, ingest_incarceration_sentence: StateIncarcerationSentence) \
            -> entities.StateIncarcerationSentence:
        """Converts an ingest_info proto StateIncarcerationSentence to a
        persistence entity."""
        incarceration_sentence_builder = \
            entities.StateIncarcerationSentence.builder()

        state_incarceration_sentence.copy_fields_to_builder(
            incarceration_sentence_builder,
            ingest_incarceration_sentence,
            self.metadata)

        self._copy_children_to_sentence(incarceration_sentence_builder,
                                        ingest_incarceration_sentence)

        return incarceration_sentence_builder.build()

    def _convert_fine(self, ingest_fine: StateFine) -> entities.StateFine:
        """Converts an ingest_info proto StateFine to a persistence entity."""
        state_fine_builder = entities.StateFine.builder()

        state_fine.copy_fields_to_builder(
            state_fine_builder, ingest_fine, self.metadata)

        self._copy_children_to_sentence(state_fine_builder,
                                        ingest_fine,
                                        copy_periods=False)

        return state_fine_builder.build()

    def _copy_children_to_sentence(self,
                                   sentence_builder,
                                   ingest_sentence,
                                   copy_periods=True):
        converted_charges = [
            self._convert_charge(self.charges[charge_id])
            for charge_id in ingest_sentence.state_charge_ids
        ]
        sentence_builder.charges = converted_charges

        if copy_periods:
            converted_incarceration_periods = [
                self._convert_incarceration_period(
                    self.incarceration_periods[period_id])
                for period_id in ingest_sentence.state_incarceration_period_ids
            ]
            sentence_builder.incarceration_periods = \
                converted_incarceration_periods

            converted_supervision_periods = [
                self._convert_supervision_period(
                    self.supervision_periods[period_id])
                for period_id in ingest_sentence.state_supervision_period_ids
            ]
            sentence_builder.supervision_periods = converted_supervision_periods

    def _convert_charge(self, ingest_charge: StateCharge) \
            -> entities.StateCharge:
        """Converts an ingest_info proto StateCharge to a persistence entity."""
        charge_builder = entities.StateCharge.builder()

        state_charge.copy_fields_to_builder(
            charge_builder, ingest_charge, self.metadata)

        charge_builder.bond = \
            fn(lambda i: state_bond.convert(self.bonds[i], self.metadata),
               'state_bond_id',
               ingest_charge)

        charge_builder.court_case = \
            fn(lambda i: self._convert_court_case(self.court_cases[i]),
               'state_court_case_id',
               ingest_charge)

        return charge_builder.build()

    def _convert_court_case(self, ingest_court_case: StateCourtCase):
        court_case_builder = entities.StateCourtCase.builder()

        state_court_case.copy_fields_to_builder(court_case_builder,
                                                ingest_court_case,
                                                self.metadata)

        court_case_builder.judge = \
            fn(lambda i: state_agent.convert(self.agents[i], self.metadata),
               'judge_id',
               ingest_court_case)

        return court_case_builder.build()

    def _convert_incarceration_period(
            self, ingest_incarceration_period: StateIncarcerationPeriod) \
            -> entities.StateIncarcerationPeriod:
        """Converts an ingest_info proto StateIncarcerationPeriod to a
        persistence entity."""
        incarceration_period_builder = \
            entities.StateIncarcerationPeriod.builder()

        state_incarceration_period.copy_fields_to_builder(
            incarceration_period_builder,
            ingest_incarceration_period,
            self.metadata)

        converted_incidents = [
            self._convert_incarceration_incident(
                self.incarceration_incidents[incident_id])
            for incident_id
            in ingest_incarceration_period.state_incarceration_incident_ids
        ]
        incarceration_period_builder.incarceration_incidents = \
            converted_incidents

        converted_decisions = [
            self._convert_parole_decision(self.parole_decisions[decision_id])
            for decision_id
            in ingest_incarceration_period.state_parole_decision_ids
        ]
        incarceration_period_builder.parole_decisions = converted_decisions

        converted_assessments = [
            self._convert_assessment(self.assessments[assessment_id])
            for assessment_id
            in ingest_incarceration_period.state_assessment_ids
        ]
        incarceration_period_builder.assessments = converted_assessments

        converted_program_assignments = [
            self._convert_program_assignment(
                self.program_assignments[assignment_id])
            for assignment_id in
            ingest_incarceration_period.state_program_assignment_ids
        ]
        incarceration_period_builder.program_assignments = \
            converted_program_assignments

        if ingest_incarceration_period.source_supervision_violation_response_id:
            converted_source_violation_response = \
                self._convert_supervision_violation_response(
                    self.violation_responses[
                        ingest_incarceration_period.
                        source_supervision_violation_response_id])
            incarceration_period_builder.\
                source_supervision_violation_response = \
                converted_source_violation_response

        return incarceration_period_builder.build()

    def _convert_supervision_period(
            self, ingest_supervision_period: StateSupervisionPeriod) \
            -> entities.StateSupervisionPeriod:
        """Converts an ingest_info proto StateSupervisionPeriod to a
        persistence entity."""
        supervision_period_builder = \
            entities.StateSupervisionPeriod.builder()

        state_supervision_period.copy_fields_to_builder(
            supervision_period_builder,
            ingest_supervision_period,
            self.metadata)

        supervision_period_builder.supervising_officer = \
            fn(lambda i: state_agent.convert(self.agents[i], self.metadata),
               'supervising_officer_id',
               ingest_supervision_period)

        converted_violations = [
            self._convert_supervision_violation(
                self.supervision_violations[violation_id])
            for violation_id
            in ingest_supervision_period.state_supervision_violation_ids
        ]
        supervision_period_builder.supervision_violations = converted_violations

        converted_assessments = [
            self._convert_assessment(self.assessments[assessment_id])
            for assessment_id in ingest_supervision_period.state_assessment_ids
        ]
        supervision_period_builder.assessments = converted_assessments

        converted_program_assignments = [
            self._convert_program_assignment(
                self.program_assignments[assignment_id])
            for assignment_id in
            ingest_supervision_period.state_program_assignment_ids
        ]
        supervision_period_builder.program_assignments = \
            converted_program_assignments
        return supervision_period_builder.build()

    def _convert_supervision_violation(
            self, ingest_supervision_violation: StateSupervisionViolation) \
            -> entities.StateSupervisionViolation:
        """Converts an ingest_info proto StateSupervisionViolation to a
        persistence entity."""
        supervision_violation_builder = \
            entities.StateSupervisionViolation.builder()

        state_supervision_violation.copy_fields_to_builder(
            supervision_violation_builder,
            ingest_supervision_violation,
            self.metadata)

        converted_violation_responses = [
            self._convert_supervision_violation_response(
                self.violation_responses[response_id])
            for response_id in
            ingest_supervision_violation.
            state_supervision_violation_response_ids
        ]
        supervision_violation_builder.supervision_violation_responses = \
            converted_violation_responses

        converted_violation_type_entries = [
            state_supervision_violation_type_entry.convert(
                self.violation_type_entries[type_entry_id], self.metadata)
            for type_entry_id in
            # pylint: disable=line-too-long
            ingest_supervision_violation.state_supervision_violation_type_entry_ids
        ]
        supervision_violation_builder.supervision_violation_types \
            = converted_violation_type_entries

        converted_violated_condition_entries = [
            state_supervision_violated_condition_entry.convert(
                self.violated_condition_entries[condition_entry_id],
                self.metadata)
            for condition_entry_id in
            # pylint: disable=line-too-long
            ingest_supervision_violation.state_supervision_violated_condition_entry_ids
        ]
        supervision_violation_builder.supervision_violated_conditions \
            = converted_violated_condition_entries

        return supervision_violation_builder.build()

    def _convert_supervision_violation_response(
            self,
            ingest_supervision_violation_response:
            StateSupervisionViolationResponse
    ) -> entities.StateSupervisionViolationResponse:
        """Converts an ingest_info proto StateSupervisionViolationResponse to a
        persistence entity."""

        supervision_violation_response_builder = \
            entities.StateSupervisionViolationResponse.builder()

        state_supervision_violation_response.copy_fields_to_builder(
            supervision_violation_response_builder,
            ingest_supervision_violation_response,
            self.metadata)

        converted_agents = [
            state_agent.convert(self.agents[agent_id], self.metadata)
            for agent_id
            in ingest_supervision_violation_response.decision_agent_ids
        ]
        supervision_violation_response_builder.decision_agents = \
            converted_agents

        converted_decisions = [
            state_supervision_violation_response_decision_type_entry.convert(
                self.violation_response_decision_type_entries[
                    condition_entry_id],
                self.metadata)
            for condition_entry_id in
            ingest_supervision_violation_response.
            state_supervision_violation_response_decision_type_entry_ids
        ]
        supervision_violation_response_builder.\
            supervision_violation_response_decisions = converted_decisions

        return supervision_violation_response_builder.build()

    def _convert_assessment(self, ingest_assessment: StateAssessment) \
            -> entities.StateAssessment:
        """Converts an ingest_info proto StateAssessment to a
        persistence entity."""
        assessment_builder = entities.StateAssessment.builder()

        state_assessment.copy_fields_to_builder(assessment_builder,
                                                ingest_assessment,
                                                self.metadata)

        assessment_builder.conducting_agent = \
            fn(lambda i: state_agent.convert(self.agents[i], self.metadata),
               'conducting_agent_id',
               ingest_assessment)

        return assessment_builder.build()

    def _convert_program_assignment(
            self, ingest_assignment: StateProgramAssignment):
        """Converts an ingest_info proto StateProgramAssignment to a
        persistence entity"""

        program_assignment_builder = entities.StateProgramAssignment.builder()
        state_program_assignment.copy_fields_to_builder(
            program_assignment_builder, ingest_assignment, self.metadata)

        program_assignment_builder.referring_agent = \
            fn(lambda i: state_agent.convert(self.agents[i], self.metadata),
               'referring_agent_id',
               ingest_assignment)

        return program_assignment_builder.build()

    def _convert_incarceration_incident(
            self, ingest_incident: StateIncarcerationIncident) \
            -> entities.StateIncarcerationIncident:
        """Converts an ingest_info proto StateIncarcerationIncident to a
        persistence entity."""
        incident_builder = entities.StateIncarcerationIncident.builder()

        state_incarceration_incident.copy_fields_to_builder(incident_builder,
                                                            ingest_incident,
                                                            self.metadata)

        incident_builder.responding_officer = \
            fn(lambda i: state_agent.convert(self.agents[i], self.metadata),
               'responding_officer_id',
               ingest_incident)

        converted_outcomes = [
            state_incarceration_incident_outcome.convert(
                self.incarceration_incident_outcomes[outcome_id],
                self.metadata)
            for outcome_id in
            ingest_incident.state_incarceration_incident_outcome_ids
        ]

        incident_builder.incarceration_incident_outcomes = converted_outcomes
        return incident_builder.build()

    def _convert_parole_decision(
            self, ingest_parole_decision: StateParoleDecision) \
            -> entities.StateParoleDecision:
        """Converts an ingest_info proto StateParoleDecision to a
        persistence entity."""
        parole_decision_builder = entities.StateParoleDecision.builder()

        state_parole_decision.copy_fields_to_builder(parole_decision_builder,
                                                     ingest_parole_decision,
                                                     self.metadata)

        converted_agents = [
            state_agent.convert(self.agents[agent_id], self.metadata)
            for agent_id in ingest_parole_decision.decision_agent_ids
        ]
        parole_decision_builder.decision_agents = converted_agents

        return parole_decision_builder.build()

# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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

"""Identifies instances of recidivism and non-recidivism for calculation.

This contains the core logic for identifying recidivism events on a
person-by-person basis, transforming incarceration periods for a given person
into instances of recidivism or non-recidivism as appropriate.


"""
from collections import defaultdict
from datetime import date
from typing import Any, Dict, List, Optional

from recidiviz.calculator.pipeline.metrics.base_identifier import (
    BaseIdentifier,
    IdentifierContextT,
)
from recidiviz.calculator.pipeline.metrics.recidivism.events import (
    NonRecidivismReleaseEvent,
    RecidivismReleaseEvent,
    ReleaseEvent,
)
from recidiviz.calculator.pipeline.utils.entity_normalization.entity_normalization_manager_utils import (
    entity_normalization_managers_for_periods,
)
from recidiviz.calculator.pipeline.utils.entity_normalization.incarceration_period_normalization_manager import (
    StateSpecificIncarcerationNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.entity_normalization.supervision_period_normalization_manager import (
    StateSpecificSupervisionNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.execution_utils import (
    extract_county_of_residence_from_rows,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_incarceration_delegate import (
    StateSpecificIncarcerationDelegate,
)
from recidiviz.calculator.query.state.views.reference.persons_to_recent_county_of_residence import (
    PERSONS_TO_RECENT_COUNTY_OF_RESIDENCE_VIEW_NAME,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason as AdmissionReason,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodReleaseReason as ReleaseReason,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.date import DateRange, DateRangeDiff
from recidiviz.persistence.entity.entity_utils import CoreEntityFieldIndex
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StatePerson,
    StateSupervisionPeriod,
)


class RecidivismIdentifier(BaseIdentifier[Dict[int, List[ReleaseEvent]]]):
    """Identifier class for instances of recidivism and non-recidivism."""

    def __init__(self) -> None:
        self.identifier_event_class = ReleaseEvent
        self.field_index = CoreEntityFieldIndex()

    def find_events(
        self, _person: StatePerson, identifier_context: IdentifierContextT
    ) -> Dict[int, List[ReleaseEvent]]:
        return self._find_release_events(
            ip_normalization_delegate=identifier_context[
                StateSpecificIncarcerationNormalizationDelegate.__name__
            ],
            sp_normalization_delegate=identifier_context[
                StateSpecificSupervisionNormalizationDelegate.__name__
            ],
            incarceration_delegate=identifier_context[
                StateSpecificIncarcerationDelegate.__name__
            ],
            incarceration_periods=identifier_context[StateIncarcerationPeriod.__name__],
            supervision_periods=identifier_context[StateSupervisionPeriod.__name__],
            persons_to_recent_county_of_residence=identifier_context[
                PERSONS_TO_RECENT_COUNTY_OF_RESIDENCE_VIEW_NAME
            ],
        )

    def _find_release_events(
        self,
        ip_normalization_delegate: StateSpecificIncarcerationNormalizationDelegate,
        sp_normalization_delegate: StateSpecificSupervisionNormalizationDelegate,
        incarceration_delegate: StateSpecificIncarcerationDelegate,
        incarceration_periods: List[StateIncarcerationPeriod],
        supervision_periods: List[StateSupervisionPeriod],
        persons_to_recent_county_of_residence: List[Dict[str, Any]],
    ) -> Dict[int, List[ReleaseEvent]]:
        """Finds instances of release and determines if they resulted in recidivism.

        Transforms each StateIncarcerationPeriod from which the person has been released into a mapping from its release
        cohort to the details of the event. The release cohort is an integer for the year, e.g. 2006. The event details
        are a ReleaseEvent object, which can represent events of both recidivism and non-recidivism. That is, each
        StateIncarcerationPeriod is transformed into a recidivism event unless it is the most recent period of
        incarceration and they are still incarcerated, or it is connected to a subsequent StateIncarcerationPeriod by a
        transfer.

        Example output for someone who went to prison in 2006, was released in 2008, went back in 2010, was released in
        2012, and never returned:
        {
        2008: [RecidivismReleaseEvent(original_admission_date="2006-04-05", ...)],
        2012: [NonRecidivismReleaseEvent(original_admission_date="2010-09-17", ...)]
        }

        Args:
            incarceration_periods: list of StateIncarcerationPeriods for a person
            persons_to_recent_county_of_residence: Reference table rows containing the county that the incarcerated person
                lives in (prior to incarceration).

        Returns:
            A dictionary mapping release cohorts to a list of ReleaseEvents for the given person in that cohort.
        """
        release_events: Dict[int, List[ReleaseEvent]] = defaultdict(list)

        if not incarceration_periods:
            return release_events

        county_of_residence = extract_county_of_residence_from_rows(
            persons_to_recent_county_of_residence
        )

        (ip_normalization_manager, _,) = entity_normalization_managers_for_periods(
            ip_normalization_delegate=ip_normalization_delegate,
            sp_normalization_delegate=sp_normalization_delegate,
            incarceration_delegate=incarceration_delegate,
            incarceration_periods=incarceration_periods,
            supervision_periods=supervision_periods,
            # Note: This pipeline cannot be run for any state that relies on
            # StateSupervisionViolationResponse entities in IP normalization
            normalized_violation_responses=None,
            field_index=self.field_index,
            # Note: This pipeline cannot be run for any state that relies on
            # StateIncarcerationSentence and StateSupervisionSentence entities
            # in SP normalization
            incarceration_sentences=None,
            supervision_sentences=None,
        )

        if not ip_normalization_manager:
            raise ValueError("Expected normalized SPs for this pipeline.")

        incarceration_periods = ip_normalization_manager.normalized_incarceration_period_index_for_calculations(
            collapse_transfers=True,
            overwrite_facility_information_in_transfers=True,
        ).incarceration_periods

        for index, incarceration_period in enumerate(incarceration_periods):
            admission_date = incarceration_period.admission_date
            release_date = incarceration_period.release_date
            release_reason = incarceration_period.release_reason
            purpose_for_incarceration = (
                incarceration_period.specialized_purpose_for_incarceration
            )

            event = None
            next_incarceration_period = (
                incarceration_periods[index + 1]
                if index <= len(incarceration_periods) - 2
                else None
            )

            if not self._should_include_in_release_cohort(
                release_date,
                release_reason,
                purpose_for_incarceration,
                next_incarceration_period,
            ):
                # If this release should not be included in a release cohort, then we do not need to produce any events
                # for this period of incarceration
                continue

            if not release_date or not release_reason:
                raise ValueError(
                    "Incarceration_period must have valid release_date and release_reason to be included in"
                    " the release_cohort. Error in should_include_in_release_cohort."
                )

            # Admission data has been validated already
            if admission_date:
                if index == len(incarceration_periods) - 1:
                    event = self._for_incarceration_period_no_return(
                        incarceration_period,
                        admission_date,
                        release_date,
                        county_of_residence,
                    )
                else:
                    reincarceration_period = self._find_valid_reincarceration_period(
                        incarceration_periods, index, release_date
                    )

                    if not reincarceration_period:
                        # We were unable to identify a reincarceration for this period
                        event = self._for_incarceration_period_no_return(
                            incarceration_period,
                            admission_date,
                            release_date,
                            county_of_residence,
                        )
                    else:
                        reincarceration_date = reincarceration_period.admission_date
                        reincarceration_facility = reincarceration_period.facility
                        reincarceration_admission_reason = (
                            reincarceration_period.admission_reason
                        )

                        # These fields have been validated already
                        if (
                            not reincarceration_date
                            or not reincarceration_admission_reason
                        ):
                            raise ValueError(
                                "Incarceration period normalization should have set admission_dates and"
                                "admission_reasons on all periods."
                            )

                        event = self._for_intermediate_incarceration_period(
                            release_ip=incarceration_period,
                            admission_date=admission_date,
                            release_date=release_date,
                            county_of_residence=county_of_residence,
                            reincarceration_date=reincarceration_date,
                            reincarceration_facility=reincarceration_facility,
                        )

            if event:
                if release_date:
                    release_cohort = release_date.year
                    release_events[release_cohort].append(event)

        return release_events

    def _find_valid_reincarceration_period(
        self,
        incarceration_periods: List[StateIncarcerationPeriod],
        index: int,
        release_date: date,
    ) -> Optional[StateIncarcerationPeriod]:
        """Finds a StateIncarcerationPeriod representing an instance of reincarceration
        following a release from prison, where the admission_date on the
        incarceration period occurred on or after the release_date."""

        for i in range(index + 1, len(incarceration_periods)):
            ip = incarceration_periods[i]

            if not ip.admission_date or not ip.admission_reason:
                raise ValueError(
                    "All incarceration periods should have set a admission_date and admission_reason after"
                    "normalization."
                )

            if ip.admission_date < release_date:
                raise ValueError(
                    f"Release from incarceration on date {release_date} overlaps with another period of"
                    " incarceration. Failure in IP normalization or should_include_in_release_cohort"
                    f" function: {incarceration_periods}"
                )

            if ip.specialized_purpose_for_incarceration in (
                StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
                StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
            ):
                # Admissions to parole board holds or other form of temporary custody
                # do not count as reincarceration admissions
                continue

            if ip.admission_reason in (
                AdmissionReason.ADMITTED_IN_ERROR,
                AdmissionReason.RETURN_FROM_ESCAPE,
                AdmissionReason.RETURN_FROM_ERRONEOUS_RELEASE,
                AdmissionReason.RETURN_FROM_TEMPORARY_RELEASE,
                AdmissionReason.TEMPORARY_CUSTODY,
                AdmissionReason.TRANSFER_FROM_OTHER_JURISDICTION,
                AdmissionReason.STATUS_CHANGE,
            ):
                continue
            if ip.admission_reason in (
                AdmissionReason.EXTERNAL_UNKNOWN,
                AdmissionReason.INTERNAL_UNKNOWN,
                AdmissionReason.NEW_ADMISSION,
                AdmissionReason.REVOCATION,
                AdmissionReason.SANCTION_ADMISSION,
                # This should be a rare case, but we are considering this a valid
                # reincarceration admission because this person became reincarcerated
                # at some point after being released.
                AdmissionReason.TRANSFER,
            ):
                return ip

            if ip.admission_reason == AdmissionReason.ADMITTED_FROM_SUPERVISION:
                raise ValueError(
                    "ADMITTED_FROM_SUPERVISION is an ingest-only enum, and we should "
                    "not see this value after IP normalization."
                )

            raise ValueError(
                "Enum case not handled for StateIncarcerationPeriodAdmissionReason of type:"
                f" {ip.admission_reason}."
            )

        return None

    def _for_incarceration_period_no_return(
        self,
        incarceration_period: StateIncarcerationPeriod,
        admission_date: date,
        release_date: date,
        county_of_residence: Optional[str],
    ) -> Optional[ReleaseEvent]:
        """Builds a NonRecidivismReleaseEvent from the attributes of the release from incarceration.

        Returns any non-recidivism event relevant to the person's StateIncarcerationPeriod.

            Args:
                incarceration_period: The StateIncarcerationPeriod
                admission_date: when this StateIncarcerationPeriod started
                release_date: when they were released from this StateIncarcerationPeriod
                county_of_residence: the county that the incarcerated person lives in (prior to incarceration).

            Returns:
                A non-recidivism event.
        """
        return NonRecidivismReleaseEvent(
            state_code=incarceration_period.state_code,
            original_admission_date=admission_date,
            release_date=release_date,
            release_facility=incarceration_period.facility,
            county_of_residence=county_of_residence,
        )

    def _for_intermediate_incarceration_period(
        self,
        release_ip: StateIncarcerationPeriod,
        admission_date: date,
        release_date: date,
        county_of_residence: Optional[str],
        reincarceration_date: date,
        reincarceration_facility: Optional[str],
    ) -> Optional[ReleaseEvent]:
        """Returns the ReleaseEvent relevant to an intermediate
        StateIncarcerationPeriod.

        If this is not the person's last StateIncarcerationPeriod and they have been
        released, there is probably an instance of recidivism to count.

        Args:
            release_ip: The intermediate StateIncarcerationPeriod
            admission_date: when the StateIncarcerationPeriod started
            release_date: when they were released from the StateIncarcerationPeriod
            reincarceration_date: date they were admitted to the subsequent
                StateIncarcerationPeriod
            county_of_residence: the county that the incarcerated person lives in
                (prior to incarceration).
            reincarceration_facility: facility in which the subsequent
                StateIncarcerationPeriod started

        Returns:
            A ReleaseEvent.
        """
        return RecidivismReleaseEvent(
            state_code=release_ip.state_code,
            original_admission_date=admission_date,
            release_date=release_date,
            release_facility=release_ip.facility,
            county_of_residence=county_of_residence,
            reincarceration_date=reincarceration_date,
            reincarceration_facility=reincarceration_facility,
        )

    def _should_include_in_release_cohort(
        self,
        release_date: Optional[date],
        release_reason: Optional[ReleaseReason],
        purpose_for_incarceration: Optional[StateSpecializedPurposeForIncarceration],
        next_incarceration_period: Optional[StateIncarcerationPeriod],
    ) -> bool:
        """Identifies whether a period of incarceration with the given features should
        be included in the release cohort."""
        if not release_date:
            # If the person is still in custody, there is no release to include in a cohort.
            return False

        if not release_reason:
            # If there is no recorded release reason, then we cannot classify this as
            # a valid release for the cohort
            return False

        if purpose_for_incarceration in (
            StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
            StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
        ):
            # If the person was released from a period of temporary custody
            # or from a parole board hold, do not include them in the release cohort.
            return False

        if next_incarceration_period:
            time_range_release = DateRange.for_day(release_date)
            if DateRangeDiff(
                time_range_release, next_incarceration_period.duration
            ).overlapping_range:
                # If the release overlaps with the following incarceration period,
                # this is not an actual release from incarceration
                return False

            if (
                next_incarceration_period.release_date
                and release_date == next_incarceration_period.release_date
            ):
                # This release shares a release_date with the next incarceration period.
                # Do not include this release.
                return False

        if release_reason in [ReleaseReason.DEATH, ReleaseReason.EXECUTION]:
            # If the person was released from this incarceration period because they
            # died or were executed, do not include them in the release cohort.
            return False
        if release_reason == ReleaseReason.ESCAPE:
            # If the person was released from this incarceration period because they
            # escaped, do not include them in the release cohort.
            return False
        if release_reason == ReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY:
            # If the person was released from a period of temporary custody, do not
            # include them in the release_cohort.
            return False
        if release_reason == ReleaseReason.RELEASED_IN_ERROR:
            # If the person was released from this incarceration period due to an error,
            # do not include them in the release cohort.
            return False
        if release_reason == ReleaseReason.TRANSFER:
            # If the person was released from this incarceration period because they
            # were transferred elsewhere, do not include them in the release cohort.
            return False
        if release_reason == ReleaseReason.TRANSFER_TO_OTHER_JURISDICTION:
            # Releases where the person has been transferred out of state don't really
            # count as true releases.
            return False
        if release_reason == ReleaseReason.COURT_ORDER:
            # If the person was released from this incarceration period due to a court
            # order, do not include them in the release cohort.
            return False
        if release_reason == ReleaseReason.STATUS_CHANGE:
            # If the person was released from this incarceration period because their
            # incarceration status changed, do not include them in the release cohort.
            return False
        if release_reason in (
            ReleaseReason.EXTERNAL_UNKNOWN,
            ReleaseReason.INTERNAL_UNKNOWN,
        ):
            # We do not have enough information to determine whether this release
            # qualifies for inclusion in the release cohort.
            return False
        if release_reason in (
            ReleaseReason.COMMUTED,
            ReleaseReason.COMPASSIONATE,
            ReleaseReason.CONDITIONAL_RELEASE,
            ReleaseReason.PARDONED,
            ReleaseReason.RELEASED_FROM_ERRONEOUS_ADMISSION,
            ReleaseReason.RELEASED_TO_SUPERVISION,
            ReleaseReason.SENTENCE_SERVED,
            ReleaseReason.VACATED,
        ):
            return True
        if release_reason == ReleaseReason.TEMPORARY_RELEASE:
            # If the person was released from this incarceration period temporarily, do not include them in the release
            # cohort.
            return False

        raise ValueError(
            "Enum case not handled for "
            "StateIncarcerationPeriodReleaseReason of type:"
            f" {release_reason}."
        )

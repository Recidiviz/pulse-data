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
"""Reference views used by other views."""
from typing import List

from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.calculator.query.state.views.reference.augmented_agent_info import AUGMENTED_AGENT_INFO_VIEW_BUILDER
from recidiviz.calculator.query.state.views.reference.covid_report_weeks import COVID_REPORT_WEEKS_VIEW_BUILDER
from recidiviz.calculator.query.state.views.reference.event_based_revocations_for_matrix import \
    EVENT_BASED_REVOCATIONS_FOR_MATRIX_VIEW_BUILDER
from recidiviz.calculator.query.state.views.reference.incarceration_period_judicial_district_association import \
    INCARCERATION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_BUILDER
from recidiviz.calculator.query.state.views.reference.sentence_group_judicial_district_association import \
    SENTENCE_GROUP_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_BUILDER
from recidiviz.calculator.query.state.views.reference.supervision_period_judicial_district_association import \
    SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_BUILDER
from recidiviz.calculator.query.state.views.reference.supervision_termination_matrix_by_person import \
    SUPERVISION_TERMINATION_MATRIX_BY_PERSON_VIEW_BUILDER
from recidiviz.calculator.query.state.views.reference.us_mo_sentence_statuses import \
    US_MO_SENTENCE_STATUSES_VIEW_BUILDER
from recidiviz.calculator.query.state.views.reference.persons_to_recent_county_of_residence import \
    PERSONS_TO_RECENT_COUNTY_OF_RESIDENCE_VIEW_BUILDER
from recidiviz.calculator.query.state.views.reference.persons_with_last_known_address import \
    PERSONS_WITH_LAST_KNOWN_ADDRESS_VIEW_BUILDER
from recidiviz.calculator.query.state.views.reference.ssvr_to_agent_association import \
    SSVR_TO_AGENT_ASSOCIATION_VIEW_BUILDER
from recidiviz.calculator.query.state.views.reference.supervision_period_to_agent_association import \
    SUPERVISION_PERIOD_TO_AGENT_ASSOCIATION_VIEW_BUILDER
from recidiviz.calculator.query.state.views.reference.event_based_admissions import \
    EVENT_BASED_ADMISSIONS_VIEW_BUILDER
from recidiviz.calculator.query.state.views.reference.event_based_program_referrals import \
    EVENT_BASED_PROGRAM_REFERRALS_VIEW_BUILDER
from recidiviz.calculator.query.state.views.reference.event_based_revocations import \
    EVENT_BASED_REVOCATIONS_VIEW_BUILDER
from recidiviz.calculator.query.state.views.reference.event_based_supervision import \
    EVENT_BASED_SUPERVISION_VIEW_BUILDER
from recidiviz.calculator.query.state.views.reference.revocations_matrix_by_person import \
    REVOCATIONS_MATRIX_BY_PERSON_VIEW_BUILDER
from recidiviz.calculator.query.state.views.reference.supervision_matrix_by_person import \
    SUPERVISION_MATRIX_BY_PERSON_VIEW_BUILDER

# NOTE: These views must be listed in order of dependency. For example, if reference view Y depends on reference view X,
# then view X should appear in the list before view Y.
REFERENCE_VIEW_BUILDERS: List[BigQueryViewBuilder] = [
    EVENT_BASED_REVOCATIONS_FOR_MATRIX_VIEW_BUILDER,
    AUGMENTED_AGENT_INFO_VIEW_BUILDER,
    SSVR_TO_AGENT_ASSOCIATION_VIEW_BUILDER,
    SUPERVISION_PERIOD_TO_AGENT_ASSOCIATION_VIEW_BUILDER,
    PERSONS_WITH_LAST_KNOWN_ADDRESS_VIEW_BUILDER,
    PERSONS_TO_RECENT_COUNTY_OF_RESIDENCE_VIEW_BUILDER,
    EVENT_BASED_ADMISSIONS_VIEW_BUILDER,
    EVENT_BASED_PROGRAM_REFERRALS_VIEW_BUILDER,
    EVENT_BASED_REVOCATIONS_VIEW_BUILDER,
    EVENT_BASED_SUPERVISION_VIEW_BUILDER,
    REVOCATIONS_MATRIX_BY_PERSON_VIEW_BUILDER,
    SUPERVISION_MATRIX_BY_PERSON_VIEW_BUILDER,
    US_MO_SENTENCE_STATUSES_VIEW_BUILDER,
    COVID_REPORT_WEEKS_VIEW_BUILDER,
    SENTENCE_GROUP_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_BUILDER,
    INCARCERATION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_BUILDER,
    SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_BUILDER,
    SUPERVISION_TERMINATION_MATRIX_BY_PERSON_VIEW_BUILDER
]

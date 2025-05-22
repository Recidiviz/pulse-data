# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Configuration lists for state-specific logic within the sentence processing pipeline"""
from recidiviz.common.constants.state.state_incarceration_period import (
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodSupervisionType,
)
from recidiviz.common.constants.states import StateCode

# States that do not infer a sentence completion date when a person transitions to liberty/death
STATES_WITHOUT_INFERRED_SENTENCE_COMPLETION_DATE = [
    # ME completion dates are hydrated once the sentence expiration date has passed and so all sentences without a
    # hydrated completion date are considered "active"
    StateCode.US_ME.name,
]

# States that have separate sentence preprocessed views -- either for all sentences or incarceration sentences only
STATES_WITH_SEPARATE_SENTENCES_PREPROCESSED = [
    StateCode.US_TN.name,
]
STATES_WITH_SEPARATE_INCARCERATION_SENTENCES_PREPROCESSED = [
    StateCode.US_CO.name,
    StateCode.US_ND.name,
]

# Only include supervision sentences & supervision projected completion dates in states that solely use supervision
# sentences for all clients on supervision (including parole)
STATES_WITH_NO_INCARCERATION_SENTENCES_ON_SUPERVISION = [
    StateCode.US_MI.name,
    StateCode.US_ND.name,
]

# Do not infer the latest supervision completion date span as open if the supervision session
# is currently open but the latest sentence span is closed in order to avoid ingest/data quality issues
STATES_WITH_NO_INFERRED_OPEN_SPANS = [
    StateCode.US_ME.name,
    StateCode.US_MI.name,
    StateCode.US_UT.name,
]

# Handle the processing of these states separately until all states have migrated to the sentence v2 schema
STATES_NOT_MIGRATED_TO_SENTENCE_V2_SCHEMA = [
    StateCode.US_AR.name,
    StateCode.US_CO.name,
    StateCode.US_IX.name,
    StateCode.US_ME.name,
    StateCode.US_MI.name,
    StateCode.US_ND.name,
    StateCode.US_OR.name,
    StateCode.US_PA.name,
    StateCode.US_TN.name,
]

# Designate the session compartment level 2 types that are not expected to have corresponding sentences
# throughout the session
COMPARTMENT_LEVEL_2_VALUES_WITHOUT_SENTENCES = [
    StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY.name,
    StateSupervisionPeriodSupervisionType.INFORMAL_PROBATION.name,
    StateSupervisionPeriodSupervisionType.WARRANT_STATUS.name,
    StateSupervisionPeriodSupervisionType.BENCH_WARRANT.name,
    StateSupervisionPeriodSupervisionType.DEPORTED.name,
    StateSupervisionPeriodSupervisionType.ABSCONSION.name,
    StateSupervisionPeriodSupervisionType.INVESTIGATION.name,
    StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN.name,
    StateSupervisionPeriodSupervisionType.EXTERNAL_UNKNOWN.name,
]

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

"""Constants that correspond to the largely opaque column names from the
MODOC data warehouse."""


STATE_ID = 'EK$SID'
FBI_ID = 'EK$FBI'
LICENSE_ID = 'EK$OLN'

CHARGE_COUNTY_CODE = 'BS$CNT'

SENTENCE_COUNTY_CODE = 'BS$CNS'
SENTENCE_OFFENSE_DATE = 'BS$DO'
SENTENCE_COMPLETED_FLAG = 'BS$SCF'
SENTENCE_STATUS_CODE = 'BW$SCD'

INCARCERATION_SENTENCE_LENGTH_YEARS = 'BT$SLY'
INCARCERATION_SENTENCE_LENGTH_MONTHS = 'BT$SLM'
INCARCERATION_SENTENCE_LENGTH_DAYS = 'BT$SLD'
INCARCERATION_SENTENCE_MIN_RELEASE_TYPE = 'BT$CRR'
INCARCERATION_SENTENCE_PAROLE_INELIGIBLE_YEARS = 'BT$PIE'
INCARCERATION_SENTENCE_START_DATE = 'BT$EM'

SUPERVISION_SENTENCE_LENGTH_YEARS = 'BU$SBY'
SUPERVISION_SENTENCE_LENGTH_MONTHS = 'BU$SBM'
SUPERVISION_SENTENCE_LENGTH_DAYS = 'BU$SBD'

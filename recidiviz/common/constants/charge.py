# Recidiviz - a platform for tracking granular recidivism metrics in real time
# Copyright (C) 2018 Recidiviz, Inc.
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

"""Constants related to a charge entity."""

class ChargeDegree(object):
    FIRST = 'FIRST'
    SECOND = 'SECOND'
    THIRD = 'THIRD'
    UNKNOWN = 'UNKNOWN'


class ChargeClass(object):
    FELONY = 'FELONY'
    MISDEMEANOR = 'MISDEMEANOR'
    PAROLE_VIOLATION = 'PAROLE VIOLATION'
    PROBATION_VIOLATION = 'PROBATION_VIOLATION'
    UNKNOWN = 'UNKNOWN'


class ChargeStatus(object):
    # TODO: Fix inconsistency between Python and DB enums
    PENDING = 'Pending'
    PRETRIAL = 'PRETRIAL'
    ACQUITTED = 'ACQUITTED'
    DROPPED = 'DROPPED'
    CONVICTED = 'CONVICTED'
    SENTENCED = 'SENTENCED'
    COMPLETED_SENTENCE = 'COMPLETED SENTENCE'
    UNKNOWN = 'UNKNOWN'


class CourtType(object):
    DISTRICT = 'DISTRICT'
    SUPERIOR = 'SUPERIOR'
    CIRCUIT = 'CIRCUIT'
    OTHER = 'OTHER'
    UNKNOWN = 'UNKNOWN'


CHARGE_DEGREE_MAP = {
    'FIRST': ChargeDegree.FIRST,
    'SECOND': ChargeDegree.SECOND,
    'THIRD': ChargeDegree.THIRD,
    'UNKNOWN': ChargeDegree.UNKNOWN
}
CHARGE_CLASS_MAP = {
    'FELONY': ChargeClass.FELONY,
    'MISDEMEANOR': ChargeClass.MISDEMEANOR,
    'PAROLE VIOLATION': ChargeClass.PAROLE_VIOLATION,
    'PROBATION VIOLATION': ChargeClass.PROBATION_VIOLATION,
    'UNKNOWN': ChargeClass.UNKNOWN
}
CHARGE_STATUS_MAP = {
    'PENDING': ChargeStatus.PENDING,
    'PRETRIAL': ChargeStatus.PRETRIAL,
    'ACQUITTED': ChargeStatus.ACQUITTED,
    'DROPPED': ChargeStatus.DROPPED,
    'CONVICTED': ChargeStatus.CONVICTED,
    'SENTENCED': ChargeStatus.SENTENCED,
    'COMPLETED SENTENCE': ChargeStatus.COMPLETED_SENTENCE,
    'UNKNOWN': ChargeStatus.UNKNOWN
}
COURT_TYPE_MAP = {
    'DISTRICT': CourtType.DISTRICT,
    'SUPERIOR': CourtType.SUPERIOR,
    'CIRCUIT': CourtType.CIRCUIT,
    'OTHER': CourtType.OTHER,
    'UNKNOWN': CourtType.UNKNOWN
}

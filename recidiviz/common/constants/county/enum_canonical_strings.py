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


"""Strings used as underlying representation of enum values for county-specific
application and database code.

NOTE: Changing ANY STRING VALUE in this file will require a database migration.
The Python values pointing to the strings can be renamed without issue.

SQLAlchemy represents SQL enums as strings, and uses the string representation
to pass values to the database. This means any change to the string values
requires a database migration. Therefore in order to keep the code as flexible
as possible, the string values should never be used directly. Storing the
strings in this file and only referring to them by their values here allows
us to structure the application layer code any way we want, while only
requiring the database to be updated when an enum value is created or removed.
"""

# Shared

# This value should be used ONLY in cases where the external data source
# explicitly specifies a value as "unknown". It should NOT be treated as a
# default value for enums that are not provided (which should be represented
# with None/NULL).
external_unknown = "EXTERNAL_UNKNOWN"
# This value should not be used by scrapers directly. It is used by status
# enums to denote that no status for an entity was provided by the source, but
# the entity itself was found in the source.
present_without_info = "PRESENT_WITHOUT_INFO"
# This value should not be used by scrapers directly. It is only used in the
# situation that an entity is removed from the website, and we cannot infer
# anything about what removal means (i.e. 'INFER_DROPPED')
removed_without_info = "REMOVED_WITHOUT_INFO"
# This value is used when the external data source specifies a known value for
# an enum field, but we internally don't have an enum value that it should map
# to. This should NOT be treated as a default value for enums fields that are
# not provided.
internal_unknown = "INTERNAL_UNKNOWN"

# person_characteristics.py

gender_female = "FEMALE"
gender_male = "MALE"
gender_other = "OTHER"
gender_trans = "TRANS"
gender_trans_female = "TRANS_FEMALE"
gender_trans_male = "TRANS_MALE"

race_american_indian = "AMERICAN_INDIAN_ALASKAN_NATIVE"
race_asian = "ASIAN"
race_black = "BLACK"
race_hawaiian = "NATIVE_HAWAIIAN_PACIFIC_ISLANDER"
race_other = "OTHER"
race_white = "WHITE"

ethnicity_hispanic = "HISPANIC"
ethnicity_not_hispanic = "NOT_HISPANIC"

residency_status_homeless = "HOMELESS"
residency_status_permanent = "PERMANENT"
residency_status_transient = "TRANSIENT"

# bond.py

bond_type_cash = "CASH"
bond_type_denied = "DENIED"
bond_type_not_required = "NOT_REQUIRED"
bond_type_partial_cash = "PARTIAL_CASH"
bond_type_secured = "SECURED"
bond_type_unsecured = "UNSECURED"

bond_status_inferred_set = "INFERRED_SET"
bond_status_pending = "PENDING"
bond_status_posted = "POSTED"
bond_status_revoked = "REVOKED"
bond_status_set = "SET"

# booking.py

admission_reason_escape = "ESCAPE"
admission_reason_new_commitment = "NEW_COMMITMENT"
admission_reason_parole_violation = "PAROLE_VIOLATION"
admission_reason_probation_violation = "PROBATION_VIOLATION"
admission_reason_supervision_violation_for_sex_offense = (
    "SUPERVISION_VIOLATION_FOR_SEX_OFFENSE"
)
admission_reason_transfer = "TRANSFER"

release_reason_acquittal = "ACQUITTAL"
release_reason_bond = "BOND"
release_reason_case_dismissed = "CASE_DISMISSED"
release_reason_death = "DEATH"
release_reason_escape = "ESCAPE"
release_reason_expiration = "EXPIRATION_OF_SENTENCE"
release_reason_recognizance = "OWN_RECOGNIZANCE"
release_reason_parole = "PAROLE"
release_reason_probation = "PROBATION"
release_reason_transfer = "TRANSFER"

custody_status_escaped = "ESCAPED"
custody_status_elsewhere = "HELD_ELSEWHERE"
custody_status_in_custody = "IN_CUSTODY"
custody_status_inferred_release = "INFERRED_RELEASE"
custody_status_released = "RELEASED"

classification_high = "HIGH"
classification_low = "LOW"
classification_maximum = "MAXIMUM"
classification_medium = "MEDIUM"
classification_minimum = "MINIMUM"
classification_work_release = "WORK_RELEASE"

# charge.py

charge_class_civil = "CIVIL"
charge_class_felony = "FELONY"
charge_class_infraction = "INFRACTION"
charge_class_misdemeanor = "MISDEMEANOR"
charge_class_other = "OTHER"
charge_class_parole_violation = "PAROLE_VIOLATION"
charge_class_probation_violation = "PROBATION_VIOLATION"
charge_class_supervision_violation_for_sex_offense = (
    "SUPERVISION_VIOLATION_FOR_SEX_OFFENSE"
)

degree_first = "FIRST"
degree_second = "SECOND"
degree_third = "THIRD"
degree_fourth = "FOURTH"

charge_status_acquitted = "ACQUITTED"
charge_status_adjudicated = "ADJUDICATED"
charge_status_completed = "COMPLETED_SENTENCE"
charge_status_convicted = "CONVICTED"
charge_status_dropped = "DROPPED"
charge_status_inferred_dropped = "INFERRED_DROPPED"
charge_status_pending = "PENDING"
charge_status_pretrial = "PRETRIAL"
charge_status_sentenced = "SENTENCED"
charge_status_transferred_away = "TRANSFERRED_AWAY"

# hold.py

hold_status_active = "ACTIVE"
hold_status_inactive = "INACTIVE"
hold_status_inferred_dropped = "INFERRED_DROPPED"

# sentence.py

sentence_status_commuted = "COMMUTED"
sentence_status_completed = "COMPLETED"
sentence_status_serving = "SERVING"

# SentenceRelationship

sentence_relationship_type_concurrent = "CONCURRENT"
sentence_relationship_type_consecutive = "CONSECUTIVE"

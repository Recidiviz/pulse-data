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

"""Strings used as underlying representation of enum values for shared
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

# charge.py

charge_status_acquitted = "ACQUITTED"
charge_status_completed = "COMPLETED_SENTENCE"
charge_status_convicted = "CONVICTED"
charge_status_dropped = "DROPPED"
charge_status_inferred_dropped = "INFERRED_DROPPED"
charge_status_pending = "PENDING"
charge_status_pretrial = "PRETRIAL"
charge_status_sentenced = "SENTENCED"

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
"""SQLAlchemy enums shared between multiple schemas.

The different schemas, which are really just grouped sets of tables and
their corresponding entities, can share enum types. Enums should only be defined
in this file if they are actually used by more than one schema, otherwise, they
should be defined in state/county/aggregate-specific schema.py files, for better
clarity around where they are used.

Regardless of where enums are defined, all SQLAlchemy enums must have globally
unique names (enforced in tests).
"""
from sqlalchemy import Enum

import recidiviz.common.constants.enum_canonical_strings as enum_strings

# Person/StatePerson

gender = Enum(
    enum_strings.external_unknown,
    enum_strings.gender_female,
    enum_strings.gender_male,
    enum_strings.gender_other,
    enum_strings.gender_trans,
    enum_strings.gender_trans_female,
    enum_strings.gender_trans_male,
    name="gender",
)

race = Enum(
    enum_strings.race_american_indian,
    enum_strings.race_asian,
    enum_strings.race_black,
    enum_strings.external_unknown,
    enum_strings.race_hawaiian,
    enum_strings.race_other,
    enum_strings.race_white,
    name="race",
)

ethnicity = Enum(
    enum_strings.external_unknown,
    enum_strings.ethnicity_hispanic,
    enum_strings.ethnicity_not_hispanic,
    name="ethnicity",
)

residency_status = Enum(
    enum_strings.residency_status_homeless,
    enum_strings.residency_status_permanent,
    enum_strings.residency_status_transient,
    name="residency_status",
)

# Charge

charge_status = Enum(
    enum_strings.charge_status_acquitted,
    enum_strings.charge_status_adjudicated,
    enum_strings.charge_status_completed,
    enum_strings.charge_status_convicted,
    enum_strings.charge_status_dropped,
    enum_strings.charge_status_inferred_dropped,
    enum_strings.external_unknown,
    enum_strings.charge_status_pending,
    enum_strings.charge_status_pretrial,
    enum_strings.charge_status_sentenced,
    enum_strings.charge_status_transferred_away,
    enum_strings.present_without_info,
    enum_strings.removed_without_info,
    name="charge_status",
)

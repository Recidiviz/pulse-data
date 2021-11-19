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

"""Strings representing different types of external ids ingested by our system.

NOTE: Changing ANY STRING VALUE in this file will require a database migration.
The Python values pointing to the strings can be renamed without issue.

At present, these are specifically for cataloging the kinds of ids ingested into
the StatePersonExternalId entity. In this context, the id types represent the
source that actually creates the id in the real world.
"""

# StatePersonExternalId.id_type

US_ID_DOC = "US_ID_DOC"

US_MO_DOC = "US_MO_DOC"
US_MO_SID = "US_MO_SID"
US_MO_FBI = "US_MO_FBI"
US_MO_OLN = "US_MO_OLN"

# ND Elite ID - tracks someone across all incarceration stays
US_ND_ELITE = "US_ND_ELITE"
# ND Booking ID - tracks someone across incarceration stays that are related to
# the same sentence. A person may be associated with more than one Booking ID.
US_ND_ELITE_BOOKING = "US_ND_ELITE_BOOKING"
# ND State ID
US_ND_SID = "US_ND_SID"

# PA Control Number - tracks someone across all incarceration stays (theoretically)
US_PA_CONTROL = "US_PA_CONT"
# PA Parole Number - tracks someone across all supervision terms (theoretically)
US_PA_PBPP = "US_PA_PBPP"
# PA Inmate Number - associated with a single contiguous incarceration stay
US_PA_INMATE = "US_PA_INMATE"
# Legacy type for un-hashed state ID values.
US_PA_SID = "US_PA_SID"

US_TN_DOC = "US_TN_DOC"
US_ME_DOC = "US_ME_DOC"

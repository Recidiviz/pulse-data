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
""""Views related to Charges."""
#pylint: disable=line-too-long

from recidiviz.calculator.query.county.views.charges.charge_text_counts import \
    CHARGE_TEXT_COUNTS_VIEW
from recidiviz.calculator.query.county.views.charges.charge_class_severity_ranks import \
    CHARGE_CLASS_SEVERITY_RANKS_VIEW
from recidiviz.calculator.query.county.views.charges.charges_and_severity import \
    CHARGES_AND_SEVERITY_VIEW
from recidiviz.calculator.query.county.views.charges.charge_severity_all_bookings import \
    CHARGE_SEVERITY_ALL_BOOKINGS_VIEW
from recidiviz.calculator.query.county.views.charges.charge_severity_counts_all_bookings import \
    CHARGE_SEVERITY_COUNTS_ALL_BOOKINGS_VIEW

CHARGE_VIEWS = [
    CHARGE_TEXT_COUNTS_VIEW,
    CHARGE_CLASS_SEVERITY_RANKS_VIEW,
    CHARGES_AND_SEVERITY_VIEW,
    CHARGE_SEVERITY_ALL_BOOKINGS_VIEW,
    CHARGE_SEVERITY_COUNTS_ALL_BOOKINGS_VIEW
]

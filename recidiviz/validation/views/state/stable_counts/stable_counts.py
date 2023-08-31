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
"""Dictionary containing entities to check for stable counts for and dates to aggregate by."""
from datetime import date
from typing import Dict, List

import attr

from recidiviz.common.constants.states import StateCode


@attr.define
class DateCol:
    # name of date column to aggregate by
    date_column_name: str
    # dictionary mapping state code to list of dates that should be exempted
    # for by month counts, specify first of the month
    exemptions: Dict[StateCode, List[date]] = {}


@attr.define
class StableCountsTableConfig:
    date_columns_to_check: List[DateCol]
    # TODO(#21848): add functionality for disaggregation
    # disaggregation_columns: List[str]


ENTITIES_WITH_EXPECTED_STABLE_COUNTS_OVER_TIME: Dict[str, StableCountsTableConfig] = {
    # CA: We exclude validations for 2023-01 because this is the month we began receiving
    # data for CA. We have some historical information, but not much, which causes
    # many validation failures on 2023-01.
    "state_supervision_violation": StableCountsTableConfig(
        date_columns_to_check=[
            DateCol(
                date_column_name="violation_date",
                exemptions={
                    # examples:
                    # StateCode.US_PA: [date(2023, 2, 1), date(2023, 3, 1)],
                    # StateCode.US_MI: [date(2023, 4, 1)]
                    StateCode.US_CA: [date(2023, 1, 1)]
                },
            )
        ]
    ),
    "state_incarceration_period": StableCountsTableConfig(
        date_columns_to_check=[
            DateCol(
                date_column_name="admission_date",
                exemptions={StateCode.US_CA: [date(2023, 1, 1)]},
            ),
            DateCol(
                date_column_name="release_date",
                exemptions={StateCode.US_CA: [date(2023, 1, 1)]},
            ),
        ],
        # TODO(#21848): add functionality for disaggregation
        # disaggregation_columns=["specialized_purpose_for_incarceration"]
    ),
    "state_supervision_period": StableCountsTableConfig(
        date_columns_to_check=[
            DateCol(
                date_column_name="start_date",
                exemptions={StateCode.US_CA: [date(2023, 1, 1)]},
            ),
            DateCol(
                date_column_name="termination_date",
                exemptions={StateCode.US_CA: [date(2023, 1, 1)]},
            ),
        ]
        # TODO(#21848): add functionality for disaggregation
        # "disaggregation_columns": ["supervision_type","supervision_level"]
    ),
    "state_supervision_violation_response": StableCountsTableConfig(
        date_columns_to_check=[
            DateCol(
                date_column_name="response_date",
                exemptions={StateCode.US_CA: [date(2023, 1, 1)]},
            )
        ]
    ),
}

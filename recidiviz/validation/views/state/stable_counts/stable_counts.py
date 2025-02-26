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
    "state_supervision_violation": StableCountsTableConfig(
        date_columns_to_check=[
            DateCol(
                date_column_name="violation_date",
                exemptions={
                    # examples:
                    # StateCode.US_PA: [date(2023, 2, 1), date(2023, 3, 1)],
                    # StateCode.US_MI: [date(2023, 4, 1)]
                    # CA: We exclude validations for 2023-01 because this is the month we began receiving
                    # data for CA. We have some historical information, but not much, which causes
                    # many validation failures on 2023-01.
                    StateCode.US_CA: [
                        date(2023, 1, 1),
                        date(2023, 2, 1),
                    ],
                    StateCode.US_IX: [
                        date(2022, 11, 1),
                        date(2022, 12, 1),
                    ],  # Known issue that we don't observe as many violations in Atlas as we did in their old system
                },
            )
        ]
    ),
    "state_incarceration_period": StableCountsTableConfig(
        date_columns_to_check=[
            DateCol(
                date_column_name="admission_date",
                exemptions={
                    # CA: We exclude validations for 2023-01 because this is the month we began receiving
                    # data for CA. We have some historical information, but not much, which causes
                    # many validation failures on 2023-01.
                    StateCode.US_CA: [
                        date(2023, 1, 1),
                        date(2023, 2, 1),
                    ],
                    StateCode.US_MO: [
                        date(2022, 11, 1),
                        date(2022, 12, 1),
                        date(2023, 3, 1),
                    ],  # Unknown fluctuations around the end of the year/beginning of the next seen here. However, the counts have remained stable the past 6 months, so we exclude these dates for now.
                    StateCode.US_OR: [
                        date(2023, 8, 1)
                    ],  # TODO(#23918): Remove once OR sends data
                    StateCode.US_ND: [
                        date(2023, 4, 1)
                    ],  # Not sure why there was these fluctuations for 4/2023, but it's only 29.5% so I think it's part of normal fluctuations
                },
            ),
            DateCol(
                date_column_name="release_date",
                exemptions={
                    # CA: We exclude validations for 2023-01 because this is the month we began receiving
                    # data for CA. We have some historical information, but not much, which causes
                    # many validation failures on 2023-01.
                    StateCode.US_CA: [
                        date(2023, 1, 1),
                        date(2023, 2, 1),
                    ],
                    StateCode.US_MO: [
                        date(2022, 11, 1),
                        date(2022, 12, 1),
                        date(2023, 2, 1),
                    ],  # Unknown fluctuations around the end of the year/beginning of the next seen here. However, the counts have remained stable the past 6 months, so we exclude these dates for now.
                    StateCode.US_OR: [
                        date(2023, 8, 1)
                    ],  # TODO(#23918): Remove once OR sends data
                },
            ),
        ],
        # TODO(#21848): add functionality for disaggregation
        # disaggregation_columns=["specialized_purpose_for_incarceration"]
    ),
    "state_supervision_period": StableCountsTableConfig(
        date_columns_to_check=[
            DateCol(
                date_column_name="start_date",
                exemptions={
                    # CA: We exclude validations for 2023-01 because this is the month we began receiving
                    # data for CA. We have some historical information, but not much, which causes
                    # many validation failures on 2023-01.
                    StateCode.US_CA: [
                        date(2023, 1, 1),
                        date(2023, 2, 1),
                    ],
                    StateCode.US_PA: [
                        date(2023, 1, 1),
                        date(2023, 2, 1),
                        date(2023, 3, 1),
                        date(2022, 12, 1),
                        date(2022, 11, 1),
                    ],  # Unknown fluctuations around the end of the year/beginning of the next seen here. However, the counts have remained stable the past 6 months, so we exclude these dates for now.
                    StateCode.US_ND: [
                        date(2023, 2, 1),
                        date(2023, 3, 1),
                    ],  # Unknown fluctuations in the month of 2/2023, but returned back to the normal rate 3/23 and has been stable since then.
                    StateCode.US_TN: [
                        date(2023, 1, 1),
                        date(2023, 2, 1),
                        date(2022, 12, 1),
                    ],  # Unknown fluctuations around the end of the year/beginning of the next seen here. However, the counts have remained stable the past 6 months, so we exclude these dates for now.
                    StateCode.US_OR: [
                        date(2023, 8, 1)
                    ],  # TODO(#23918): Remove once OR sends data
                    StateCode.US_MI: [
                        date(2023, 8, 1),
                        date(2023, 9, 1),
                    ],  # This was the month of the COMS migration and the refactor started a bunch of periods on 8/14/23
                    StateCode.US_IX: [
                        # The Atlas migration happened in 11/2022 and leading up to it there was a data cleanup effort, and following it there as a data correction effort, so this might be related to that.
                        # There is also a lot of fluctuation in start date in these months, which supports this theory.
                        # In addition, the actual supervision population seems stable across this time, so it seems like it's just due to more status changes during this time.
                        date(2022, 9, 1),
                        date(2022, 10, 1),
                        date(2022, 11, 1),
                        date(2022, 12, 1),
                        date(2023, 1, 1),
                    ],
                },
            ),
            DateCol(
                date_column_name="termination_date",
                exemptions={
                    # CA: We exclude validations for 2023-01 because this is the month we began receiving
                    # data for CA. We have some historical information, but not much, which causes
                    # many validation failures on 2023-01.
                    StateCode.US_CA: [
                        date(2023, 1, 1),
                        date(2023, 2, 1),
                    ],
                    StateCode.US_PA: [
                        date(2023, 1, 1),
                        date(2023, 2, 1),
                        date(2023, 3, 1),
                        date(2022, 12, 1),
                    ],  # Unknown fluctuations around the end of the year/beginning of the next seen here. However, the counts have remained stable the past 6 months, so we exclude these dates for now.
                    StateCode.US_TN: [
                        date(2023, 1, 1),
                        date(2023, 2, 1),
                        date(2022, 12, 1),
                    ],  # Unknown fluctuations around the end of the year/beginning of the next seen here. However, the counts have remained stable the past 6 months, so we exclude these dates for now.
                    StateCode.US_OR: [
                        date(2023, 8, 1)
                    ],  # TODO(#23918): Remove once OR sends data
                    StateCode.US_MI: [
                        date(2023, 8, 1),
                        date(2023, 9, 1),
                    ],  # This was the month of the COMS migration and the refactor ended a bunch of periods on 8/14/23
                    StateCode.US_IX: [
                        # The Atlas migration happened in 11/2022 and leading up to it there was a data cleanup effort, and following it there as a data correction effort, so this might be related to that.
                        # There is also a lot of fluctuation in start date in these months, which supports this theory.
                        # In addition, the actual supervision population seems stable across this time, so it seems like it's just due to more status changes during this time.
                        date(2022, 9, 1),
                        date(2022, 10, 1),
                        date(2022, 11, 1),
                        date(2022, 12, 1),
                        date(2023, 1, 1),
                        # Not sure why there was these fluctuations for 4/2023, but it's only 25.4% (so just barely over the threshold) so I think it's part of normal fluctuations
                        date(2023, 4, 1),
                    ],
                },
            ),
        ]
        # TODO(#21848): add functionality for disaggregation
        # "disaggregation_columns": ["supervision_type","supervision_level"]
    ),
    "state_supervision_violation_response": StableCountsTableConfig(
        date_columns_to_check=[
            DateCol(
                date_column_name="response_date",
                exemptions={
                    # CA: We exclude validations for 2023-01 because this is the month we began receiving
                    # data for CA. We have some historical information, but not much, which causes
                    # many validation failures on 2023-01.
                    StateCode.US_CA: [
                        date(2023, 1, 1),
                        date(2023, 2, 1),
                    ],
                    StateCode.US_MI: [
                        date(2023, 8, 1),
                        date(2023, 9, 1),
                    ],  # This is probably because we haven't ingested decision responses from COMS yet.  Check this again when we have TODO(#23960)
                },
            )
        ]
    ),
}

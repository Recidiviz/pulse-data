# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Produces the INSERT INTO queries that should be used to add the race and ethnicity
population counts from the U.S. Census.

    python -m recidiviz.tools.calculator.add_state_race_ethnicity_population_counts \
     --state_code [STATE_CODE] \
     --total_pop [TOTAL_POPULATION_COUNT] \
     --black_percent [BLACK_ALONE_PERCENT] \
     --american_indian_percent [AMERICAN_INDIAN_ALASKAN_NATIVE_ALONE_PERCENT] \
     --asian_percent [ASIAN_ALONE_PERCENT] \
     --native_hawaiian_percent [NATIVE_HAWAIIAN_PACIFIC_ISLANDER_ALONE_PERCENT] \
     --two_or_more_races_percent [TWO_OR_MORE_RACES_PERCENT] \
     --hispanic_percent [HISPANIC_PERCENT] \
     --white_percent [WHITE_ALONE_NOT_HISPANIC_PERCENT]

For example, you would add US_ND 2020 census numbers with the following args:
    python -m recidiviz.tools.calculator.add_state_race_ethnicity_population_counts \
     --state_code US_ND \
     --total_pop 774,948 \
     --black_percent 3.4 \
     --american_indian_percent 5.6 \
     --asian_percent 1.7 \
     --native_hawaiian_percent 0.1 \
     --two_or_more_races_percent 2.3 \
     --hispanic_percent 4.1 \
     --white_percent 83.7

Note: As of 2000, the U.S. Census allows for individuals to select more than one race
category. The census also (accurately) treats "Hispanic or Latino" as an ethnicity
instead of a race, allowing for an individual to indicate that they are
Hispanic/Latino as well as selecting the other race(s) that apply to them. While this
approach allows for individuals to accurately identify as multiple races/ethnicities,
the way that the Census Bureau reports this information makes it difficult to calculate
racial/ethnic population percentages that sum to 100%. Unfortunately, for our
analytical purposes we require state-wide race/ethnicity percentage breakdowns to sum
to 100%.

This script calculates the rough population estimates using the following methodology:
- Determine which racial/ethnic category has the largest % in the state
- Calculate the population sum of all racial and ethnic categories *besides*
whichever category has the largest %
- Attribute the rest of the population to the category with the largest %
- Validate that the % of individuals in that category is within a 5% difference
of what is reported as the percentage in the census
"""
import argparse
import logging
import sys
from typing import Dict, List, Tuple

from recidiviz.common.constants.state.state_person import StateEthnicity, StateRace
from recidiviz.tools.utils.script_helpers import prompt_for_confirmation
from recidiviz.utils.environment import GCP_PROJECTS
from recidiviz.utils.string import StrictStringFormatter

RACE_ETH_POP_COUNT_SUBQUERY_TEMPLATE = """
    SELECT '{state_code}' as state_code, '{race_eth}' as race_or_ethnicity, {pop_count} as population_count, {rep_priority} as representation_priority
"""


INSERT_QUERY_TEMPLATE = """
INSERT INTO `{project_id}.static_reference_tables.state_race_ethnicity_population_counts`
(
    {pop_count_subquery}
);
"""


DELETE_FROM_QUERY_TEMPLATE = """
DELETE FROM `{project_id}.static_reference_tables.state_race_ethnicity_population_counts`
WHERE state_code = '{state_code}';
"""


def main(
    state_code: str,
    total_pop: float,
    black_percent: float,
    american_indian_percent: float,
    asian_percent: float,
    native_hawaiian_percent: float,
    hispanic_percent: float,
    two_or_more_races_percent: float,
    white_percent: float,
) -> None:
    """Produces two INSERT INTO statements that should be used to add the correct
    race/ethnicity population counts to the state_race_ethnicity_population_counts
    tables."""
    race_eth_counts: Dict[str, float] = {
        StateRace.AMERICAN_INDIAN_ALASKAN_NATIVE.value: round(
            total_pop * american_indian_percent / 100
        ),
        StateRace.ASIAN.value: round(total_pop * asian_percent / 100),
        StateRace.BLACK.value: round(total_pop * black_percent / 100),
        StateEthnicity.HISPANIC.value: round(total_pop * hispanic_percent / 100),
        StateRace.NATIVE_HAWAIIAN_PACIFIC_ISLANDER.value: round(
            total_pop * native_hawaiian_percent / 100
        ),
        StateRace.OTHER.value: round(total_pop * two_or_more_races_percent / 100),
        StateRace.WHITE.value: round(total_pop * white_percent / 100),
    }

    max_category = max(race_eth_counts, key=race_eth_counts.__getitem__)
    max_category_raw_percentage = race_eth_counts[max_category] / total_pop * 100

    non_max_category_sum = sum(
        [
            value
            for category, value in race_eth_counts.items()
            if category != max_category
        ]
    )

    max_category_count = total_pop - non_max_category_sum
    race_eth_counts[max_category] = max_category_count
    race_eth_counts["ALL"] = total_pop

    max_category_percent_calculated = max_category_count / total_pop * 100
    percent_diff = (
        max_category_raw_percentage - max_category_percent_calculated
    ) / max_category_raw_percentage
    if percent_diff > 0.05:
        raise ValueError(
            "Core assumption of script broken. Cannot subtract from largest "
            "racial/ethnic group without significantly impacting that group's "
            "percentage in the state. Must revisit methodology for calculating "
            "racial and ethnic population numbers. "
            f"Max_category: {max_category} \n"
            f"max_category_percent_calculated: {max_category_percent_calculated}\n"
            f"percent_diff: {percent_diff}"
        )

    sub_queries: List[str] = []
    representation_priority = 1

    for race_eth in sorted(race_eth_counts, key=race_eth_counts.__getitem__):
        sub_queries.append(
            StrictStringFormatter().format(
                RACE_ETH_POP_COUNT_SUBQUERY_TEMPLATE,
                state_code=state_code,
                race_eth=race_eth,
                pop_count=int(race_eth_counts[race_eth]),
                rep_priority=representation_priority,
            )
        )

        representation_priority += 1

    for project_id in GCP_PROJECTS:
        print(
            StrictStringFormatter().format(
                INSERT_QUERY_TEMPLATE,
                project_id=project_id,
                pop_count_subquery="UNION ALL\n".join(sub_queries),
            )
        )


def parse_arguments(argv: List[str]) -> Tuple[argparse.Namespace, List[str]]:
    """Parses the arguments needed to run the script."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--state_code",
        dest="state_code",
        type=str,
        required=True,
    )
    parser.add_argument(
        "--total_pop",
        dest="total_pop",
        type=float,
        required=True,
    )
    parser.add_argument(
        "--white_percent",
        dest="white_percent",
        type=float,
        required=True,
    )
    parser.add_argument(
        "--black_percent",
        dest="black_percent",
        type=float,
        required=True,
    )
    parser.add_argument(
        "--american_indian_percent",
        dest="american_indian_percent",
        type=float,
        required=True,
    )
    parser.add_argument(
        "--asian_percent",
        dest="asian_percent",
        type=float,
        required=True,
    )
    parser.add_argument(
        "--native_hawaiian_percent",
        dest="native_hawaiian_percent",
        type=float,
        required=True,
    )
    parser.add_argument(
        "--hispanic_percent",
        dest="hispanic_percent",
        type=float,
        required=True,
    )
    parser.add_argument(
        "--two_or_more_races_percent",
        dest="two_or_more_races_percent",
        type=float,
        required=True,
    )

    return parser.parse_known_args(argv)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    known_args, _ = parse_arguments(sys.argv)

    print(
        "\n\nIf there are existing rows for state_code = "
        f"{known_args.state_code} in the following tables: \n"
        f"`recidiviz-123.static_reference_tables.state_race_ethnicity_population_counts`\n"
        f"`recidiviz-staging.static_reference_tables.state_race_ethnicity_population_counts`\n"
        f"then please delete them with the following statements before continuing: \n"
    )

    for project in GCP_PROJECTS:
        print(
            StrictStringFormatter().format(
                DELETE_FROM_QUERY_TEMPLATE,
                project_id=project,
                state_code=known_args.state_code,
            )
        )

    prompt_for_confirmation(
        f"Have you confirmed that there are no existing rows for state_code = "
        f"{known_args.state_code} in the following tables: \n"
        f"`recidiviz-123.static_reference_tables.state_race_ethnicity_population_counts`\n"
        f"`recidiviz-staging.static_reference_tables.state_race_ethnicity_population_counts`\n"
    )

    main(
        state_code=known_args.state_code,
        total_pop=known_args.total_pop,
        black_percent=known_args.black_percent,
        american_indian_percent=known_args.american_indian_percent,
        asian_percent=known_args.asian_percent,
        native_hawaiian_percent=known_args.native_hawaiian_percent,
        hispanic_percent=known_args.hispanic_percent,
        two_or_more_races_percent=known_args.two_or_more_races_percent,
        white_percent=known_args.white_percent,
    )

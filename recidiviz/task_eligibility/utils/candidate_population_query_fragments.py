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
"""
Helper SQL queries to generate candidate population queries
"""


def supervision_population_eligible_levels_additional_filters(
    start_date: str = "1900-01-01",
    excluded_correctional_levels: str = "('UNASSIGNED','IN_CUSTODY','INTERSTATE_COMPACT','WARRANT','ABSCONDED','ABSCONSION','EXTERNAL_UNKNOWN','INTERNAL_UNKNOWN')",
    excluded_compartment_level_2: str = "('')",
) -> list[str]:
    """Helper method that returns the filters to generate the
    population on active supervision from a specific start date
    Args:
        start_date (str): Default set to "1900-01-01"
        excluded_correctional_levels (str): Defaults set to shared list of levels not eligible for supervision level downgrades
        excluded_compartment_level_2 (str): Defaults to all compartment level 2
    """
    # TODO(#17654) align on ABSCONDED/ABSCONSION terminology
    return [
        'compartment_level_2 IN ("PAROLE", "PROBATION", "DUAL")',
        f"correctional_level NOT IN {excluded_correctional_levels}",
        f"compartment_level_2 NOT IN {excluded_compartment_level_2}",
        f"start_date >= '{start_date}'",
    ]


def active_supervision_population_additional_filters(
    start_date: str = "1900-01-01",
    excluded_correctional_levels: str = "('IN_CUSTODY','WARRANT','ABSCONDED','ABSCONSION','EXTERNAL_UNKNOWN')",
    included_compartment_level_2: str = "('PAROLE', 'DUAL', 'PROBATION', 'INFORMAL_PROBATION', 'COMMUNITY_CONFINEMENT')",
    excluded_compartment_level_2: str = "('INTERNAL_UNKNOWN', 'ABSCONSION', 'BENCH_WARRANT')",
    allow_null_correctional_level: bool = False,
) -> list[str]:
    """Helper method that returns the filters to generate the
    population on active supervision from a specific start date
    Args:
        start_date (str): Default set to "1900-01-01"
        included_compartment_level_2 (str): Defaults to Parole, Dual, and Probation
        excluded_correctional_levels (str): Defaults set to shared list of levels not considered active
        excluded_compartment_level_2 (str): Defaults set to shared list of levels not considered active
    """
    # TODO(#17654) align on ABSCONDED/ABSCONSION terminology
    correctional_level_filter = (
        f"correctional_level NOT IN {excluded_correctional_levels}"
    )
    if allow_null_correctional_level:
        correctional_level_filter = (
            f"({correctional_level_filter} OR correctional_level IS NULL)"
        )
    return [
        f"compartment_level_2 IN {included_compartment_level_2}",
        f"compartment_level_2 NOT IN {excluded_compartment_level_2}",
        # TODO(#19411) align on how to treat null supervision levels
        correctional_level_filter,
        f"start_date >= '{start_date}'",
    ]

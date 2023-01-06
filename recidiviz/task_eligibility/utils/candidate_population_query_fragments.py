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


def supervision_population_active_levels_additional_filters(
    start_date: str = "1900-01-01",
) -> list[str]:
    """Helper method that returns the filters to generate the
    population on active supervision from a specific start date
    Args:
        start_date (str): Default set to "1900-01-01"
    """
    # TODO(#17654) align on ABSCONDED/ABSCONSION terminology
    return [
        'attr.compartment_level_2 IN ("PAROLE", "PROBATION", "DUAL")',
        "attr.correctional_level NOT IN "
        '("UNASSIGNED","IN_CUSTODY","INTERSTATE_COMPACT","WARRANT","ABSCONDED","ABSCONSION","EXTERNAL_UNKNOWN","INTERNAL_UNKNOWN")',
        f"start_date >= '{start_date}'",
    ]
